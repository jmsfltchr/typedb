/*
 * Copyright (C) 2021 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.reasoner.resolution.resolver;

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.iterator.Iterators;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.AnswerCache;
import grakn.core.reasoner.resolution.framework.AnswerCache.PartialAnswerCache;
import grakn.core.reasoner.resolution.framework.AnswerCache.Subsumable.ConceptMapCache;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.RequestState.CachingRequestState;
import grakn.core.reasoner.resolution.framework.RequestState.Exploration;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ConcludableResolver extends Resolver<ConcludableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules;
    private final Map<Driver<ConclusionResolver>, Rule> resolverRules;
    private final grakn.core.logic.resolvable.Concludable concludable;
    private final LogicManager logicMgr;
    private final Map<Request, CachingRequestState<?, ConceptMap>> requestStates;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private boolean isInitialised;
    protected final Map<Driver<? extends Resolver<?>>, Pair<Map<ConceptMap, AnswerCache<?, ConceptMap>>, Integer>> cacheRegistersByRoot;

    public ConcludableResolver(Driver<ConcludableResolver> driver, grakn.core.logic.resolvable.Concludable concludable,
                               ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                               LogicManager logicMgr, boolean resolutionTracing) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.logicMgr = logicMgr;
        this.concludable = concludable;
        this.applicableRules = new LinkedHashMap<>();
        this.resolverRules = new HashMap<>();
        this.requestStates = new HashMap<>();
        this.unboundVars = unboundVars(concludable.pattern());
        this.isInitialised = false;
        this.cacheRegistersByRoot = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        CachingRequestState<?, ConceptMap> requestState = getOrReplaceRequestState(fromUpstream, iteration);
        if (iteration < requestState.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == requestState.iteration();
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);

        // assert cacheRegisters.get(fromUpstream.partialAnswer().root()).isRegistered(fromUpstream.partialAnswer().conceptMap()); // TODO: This throws in "conjunctions of untyped reasoned relations are correctly resolved" but without consequence
        assert requestState.isExploration();
        requestState.asExploration().newAnswer(fromDownstream.answer(), fromDownstream.answer().requiresReiteration());

        if (iteration < requestState.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == requestState.iteration();
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    /*
    When we only require 1 answer (eg. when the conjunction is already fully bound), we can short circuit
    and prevent exploration of further rules.

    One latency optimisation we could do here, is keep track of how many N repeated requests are received,
    forward them downstream (to parallelise searching for the single answer), and when the first one finds an answer,
    we respond for all N ahead of time. Then, when the rules actually return an answer to this concludable, we do nothing.
     */
    private void answerFound(Partial.Compound<?, ?> upstreamAnswer, Request fromUpstream, int iteration) {
        CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);
        if (requestState.isExploration() && requestState.asExploration().singleAnswerRequired()) {
            requestState.asExploration().downstreamManager().clearDownstreams();
            // TODO: Should we set the cache complete here (and is that correct?), or is that already achieved implicitly?
        }
        answerToUpstream(upstreamAnswer, fromUpstream, iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);

        if (iteration < requestState.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == requestState.iteration();
            if (requestState.isExploration()) {
                requestState.asExploration().downstreamManager().removeDownstream(fromDownstream.sourceRequest());
            }
            nextAnswer(fromUpstream, requestState, iteration);
        }
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        concludable.getApplicableRules(conceptMgr, logicMgr).forEachRemaining(rule -> concludable.getUnifiers(rule)
                .forEachRemaining(unifier -> {
                    if (isTerminated()) return;
                    try {
                        Driver<ConclusionResolver> conclusionResolver = registry.registerConclusion(rule.conclusion());
                        applicableRules.putIfAbsent(conclusionResolver, new HashSet<>());
                        applicableRules.get(conclusionResolver).add(unifier);
                        resolverRules.put(conclusionResolver, rule);
                    } catch (GraknException e) {
                        terminate(e);
                    }
                }));
        if (!isTerminated()) isInitialised = true;
    }

    private void nextAnswer(Request fromUpstream, CachingRequestState<?, ConceptMap> requestState, int iteration) {
        Optional<Partial.Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(Partial::asCompound); // TODO: This returns a partial, but it won't contain the correct explanation yet

        if (upstreamAnswer.isPresent()) {
            answerFound(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            Exploration exploration;
            if (requestState.isExploration() && !requestState.answerCache().isComplete()) {
                if ((exploration = requestState.asExploration()).downstreamManager().hasDownstream()) {
                    requestFromDownstream(exploration.downstreamManager().nextDownstream(), fromUpstream, iteration);
                } else {
                    requestState.answerCache().setComplete(); // TODO: The cache should not be set as complete during recursion
                    failToUpstream(fromUpstream, iteration);
                }
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }

    }

    private CachingRequestState<?, ConceptMap> getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            CachingRequestState<?, ConceptMap> requestState = createRequestState(fromUpstream, iteration);
            requestStates.put(fromUpstream, requestState);
        } else {
            CachingRequestState<?, ConceptMap> requestState = this.requestStates.get(fromUpstream);

            if (requestState.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                CachingRequestState<?, ConceptMap> newRequestState = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, newRequestState);
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected CachingRequestState<?, ConceptMap> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        Map<ConceptMap, AnswerCache<?, ConceptMap>> cacheRegister = cacheRegisterForRoot(root, iteration);
        ConceptMap answerFromUpstream = fromUpstream.partialAnswer().conceptMap();
        CachingRequestState<?, ConceptMap> requestState;
        assert fromUpstream.partialAnswer().isConcludable();
        if (cacheRegister.containsKey(answerFromUpstream)) {
            assert !fromUpstream.partialAnswer().asConcludable().isExplain();
            if (cacheRegister.get(answerFromUpstream).isConceptMapCache()) {
                ConceptMapCache answerCache = cacheRegister.get(answerFromUpstream).asConceptMapCache();
                requestState = new FollowingMatchRequestState(fromUpstream, answerCache, iteration, true, true);
            } else if (cacheRegister.get(answerFromUpstream).isPartialAnswerCache()) {
                PartialAnswerCache answerCache = cacheRegister.get(answerFromUpstream).asPartialAnswerCache();
                requestState = new FollowingExplainRequestState(fromUpstream, answerCache, iteration, true, true);
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        } else {
            if (fromUpstream.partialAnswer().asConcludable().isExplain()) {
                PartialAnswerCache answerCache = new PartialAnswerCache(cacheRegister, answerFromUpstream);
                cacheRegister.put(answerFromUpstream, answerCache);
                if (!answerCache.isComplete()) {
                    answerCache.cache(
                            traversalIterator(concludable.pattern(), answerFromUpstream)
                                    .map(ans -> fromUpstream.partialAnswer().asConcludable())
                    );
                }
                requestState = new LeadingExplainRequestState(fromUpstream, answerCache, iteration, false);
            } else if (fromUpstream.partialAnswer().asConcludable().isMatch()) {
                ConceptMapCache answerCache = new ConceptMapCache(cacheRegister, answerFromUpstream);
                cacheRegister.put(answerFromUpstream, answerCache);
                if (!answerCache.isComplete()) {
                    answerCache.cache(traversalIterator(concludable.pattern(), answerFromUpstream));
                }
                boolean singleAnswerRequired = answerFromUpstream.concepts().keySet().containsAll(unboundVars());
                requestState = new LeadingMatchRequestState(fromUpstream, answerCache, iteration, singleAnswerRequired, true);
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
            registerRules(fromUpstream, requestState.asExploration());
        }
        return requestState;
    }

    private Map<ConceptMap, AnswerCache<?, ConceptMap>> cacheRegisterForRoot(Driver<? extends Resolver<?>> root, int iteration) {
        if (cacheRegistersByRoot.containsKey(root) && cacheRegistersByRoot.get(root).second() < iteration) {
            cacheRegistersByRoot.remove(root);
        }
        cacheRegistersByRoot.putIfAbsent(root, new Pair<>(new HashMap<>(), iteration));
        return cacheRegistersByRoot.get(root).first();
    }

    private void registerRules(Request fromUpstream, Exploration exploration) {
        // loop termination: when receiving a new request, we check if we have seen it before from this root query
        // if we have, we do not allow rules to be registered as possible downstreams
        Partial.Concludable<?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry : applicableRules.entrySet()) {
            Driver<ConclusionResolver> conclusionResolver = entry.getKey();
            for (Unifier unifier : entry.getValue()) {
                Optional<? extends Partial.Conclusion<?, ?>> unified = partialAnswer.toDownstream(unifier, resolverRules.get(conclusionResolver));
                if (unified.isPresent()) {
                    Request toDownstream = Request.create(driver(), conclusionResolver, unified.get());
                    exploration.downstreamManager().addDownstream(toDownstream);
                }
            }
        }
    }

    private Set<Identifier.Variable.Retrievable> unboundVars() {
        return unboundVars;
    }

    private Set<Identifier.Variable.Retrievable> unboundVars(Conjunction conjunction) {
        Set<Identifier.Variable.Retrievable> missingBounds = new HashSet<>();
        iterate(conjunction.variables()).filter(var -> var.id().isRetrievable()).forEachRemaining(var -> {
            if (var.isType() && !var.asType().label().isPresent()) missingBounds.add(var.asType().id().asRetrievable());
            else if (var.isThing() && !var.asThing().iid().isPresent())
                missingBounds.add(var.asThing().id().asRetrievable());
        });
        return missingBounds;
    }

    private class FollowingMatchRequestState extends CachingRequestState<ConceptMap, ConceptMap> {

        public FollowingMatchRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache,
                                          int iteration, boolean deduplicate, boolean mayCauseReiteration) {
            super(fromUpstream, answerCache, iteration, mayCauseReiteration, deduplicate);
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(ConceptMap conceptMap) {
            Partial.Concludable<?> partial = fromUpstream.partialAnswer().asConcludable();
            assert !partial.isExplain() && partial.isMatch(); // TODO: Can the typing be tightened up to remove these assertions?
            return Iterators.single(partial.asMatch().toUpstreamLookup(conceptMap,
                                                                       concludable.isInferredAnswer(conceptMap)));
        }

    }

    private class LeadingMatchRequestState extends FollowingMatchRequestState implements Exploration {

        private final DownstreamManager downstreamManager;
        private final boolean singleAnswerRequired;

        public LeadingMatchRequestState(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache,
                                        int iteration, boolean singleAnswerRequired, boolean deduplicate) {
            super(fromUpstream, answerCache, iteration, deduplicate, false);
            this.downstreamManager = new DownstreamManager();
            this.singleAnswerRequired = singleAnswerRequired;
        }

        public boolean isExploration() {
            return true;
        }

        public Exploration asExploration() {
            return this;
        }

        public DownstreamManager downstreamManager() { // TODO: Don't use this, move to use it from the new AnswerStateMachine
            return downstreamManager;
        }

        public void newAnswer(Partial<?> partial, boolean requiresReiteration) {
            answerCache.cache(partial.conceptMap());
            if (requiresReiteration) answerCache.setRequiresReexploration();
        }

        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }
    }

    private static class FollowingExplainRequestState extends CachingRequestState<Partial.Concludable<?>, ConceptMap> {

        public FollowingExplainRequestState(Request fromUpstream,
                                            AnswerCache<Partial.Concludable<?>, ConceptMap> answerCache,
                                            int iteration, boolean mayCauseReiteration, boolean deduplicate) {
            super(fromUpstream, answerCache, iteration, mayCauseReiteration, deduplicate);
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(Partial.Concludable<?> partial) {
            if (partial.asExplain().ofInferred()) return Iterators.single(partial.asExplain().toUpstreamInferred());
            return Iterators.empty();
        }
    }

    private static class LeadingExplainRequestState extends FollowingExplainRequestState implements Exploration {

        private final DownstreamManager downstreamManager;

        public LeadingExplainRequestState(Request fromUpstream, AnswerCache<Partial.Concludable<?>, ConceptMap> answerCache,
                                          int iteration, boolean mayCauseReiteration) {
            super(fromUpstream, answerCache, iteration, mayCauseReiteration, false);
            this.downstreamManager = new DownstreamManager();
        }

        public boolean isExploration() {
            return true;
        }

        public Exploration asExploration() {
            return this;
        }

        public DownstreamManager downstreamManager() { // TODO: Don't use this, move to use it from the new AnswerStateMachine
            return downstreamManager;
        }

        public void newAnswer(Partial<?> partial, boolean requiresReiteration) {
            answerCache.cache(partial.asConcludable());
            if (requiresReiteration) answerCache.setRequiresReexploration(); // TODO: Make it a responsibility of the cache to mark all answers it yields as requiresReiteration if the cache is marked as RequiresReiteration
        }

        @Override
        public boolean singleAnswerRequired() {
            return false;
        }

    }

}
