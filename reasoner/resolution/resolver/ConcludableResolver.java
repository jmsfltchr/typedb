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

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.iterator.Iterators;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.AnswerCache;
import grakn.core.reasoner.resolution.framework.AnswerCache.ConceptMapCache;
import grakn.core.reasoner.resolution.framework.AnswerCache.ConcludableExplanationCache;
import grakn.core.reasoner.resolution.framework.AnswerCache.Register;
import grakn.core.reasoner.resolution.framework.AnswerManager.CachingAnswerManager;
import grakn.core.reasoner.resolution.framework.AnswerManager.Exploration;
import grakn.core.reasoner.resolution.framework.Request;
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

import static grakn.core.common.iterator.Iterators.iterate;

public class ConcludableResolver extends Resolver<ConcludableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules;
    private final Map<Driver<ConclusionResolver>, Rule> resolverRules;
    private final grakn.core.logic.resolvable.Concludable concludable;
    private final LogicManager logicMgr;
    private final Map<Request, CachingAnswerManager<?, ConceptMap>> answerManagers;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private boolean isInitialised;
    protected final Map<Actor.Driver<? extends Resolver<?>>, Register<AnswerCache<?, ConceptMap>, ConceptMap>> cacheRegisters;

    public ConcludableResolver(Driver<ConcludableResolver> driver, grakn.core.logic.resolvable.Concludable concludable,
                               ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                               LogicManager logicMgr, boolean resolutionTracing) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.logicMgr = logicMgr;
        this.concludable = concludable;
        this.applicableRules = new LinkedHashMap<>();
        this.resolverRules = new HashMap<>();
        this.answerManagers = new HashMap<>();
        this.unboundVars = unboundVars(concludable.pattern());
        this.isInitialised = false;
        this.cacheRegisters = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        CachingAnswerManager<?, ConceptMap> requestState = getOrReplaceRequestState(fromUpstream, iteration);
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
        CachingAnswerManager<?, ConceptMap> answerManager = this.answerManagers.get(fromUpstream);

        // assert cacheRegisters.get(fromUpstream.partialAnswer().root()).isRegistered(fromUpstream.partialAnswer().conceptMap()); // TODO: This throws in "conjunctions of untyped reasoned relations are correctly resolved" but without consequence
        assert answerManager.isExploration();
        answerManager.asExploration().newAnswer(fromDownstream.answer(), fromDownstream.answer().requiresReiteration());

        if (iteration < answerManager.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == answerManager.iteration();
            nextAnswer(fromUpstream, answerManager, iteration);
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
        CachingAnswerManager<?, ConceptMap> answerManager = this.answerManagers.get(fromUpstream);
        if (answerManager.isExploration() && answerManager.asExploration().singleAnswerRequired()) {
            answerManager.asExploration().downstreamManager().clearDownstreams();
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
        CachingAnswerManager<?, ConceptMap> answerManager = this.answerManagers.get(fromUpstream);

        if (iteration < answerManager.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == answerManager.iteration();
            if (answerManager.isExploration()) {
                answerManager.asExploration().downstreamManager().removeDownstream(fromDownstream.sourceRequest());
            }
            nextAnswer(fromUpstream, answerManager, iteration);
        }
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        answerManagers.clear();
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

    private void nextAnswer(Request fromUpstream, CachingAnswerManager<?, ConceptMap> answerManager, int iteration) {
        Optional<Partial.Compound<?, ?>> upstreamAnswer = answerManager.nextAnswer().map(Partial::asCompound); // TODO: This returns a partial, but it won't contain the correct explanation yet

        if (upstreamAnswer.isPresent()) {
            answerFound(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            Exploration exploration;
            if (answerManager.isExploration() && !answerManager.answerCache().isComplete()) {
                if ((exploration = answerManager.asExploration()).downstreamManager().hasDownstream()) {
                    requestFromDownstream(exploration.downstreamManager().nextDownstream(), fromUpstream, iteration);
                } else {
                    answerManager.answerCache().setComplete(); // TODO: The cache should not be set as complete during recursion
                    failToUpstream(fromUpstream, iteration);
                }
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }

    }

    private CachingAnswerManager<?, ConceptMap> getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!answerManagers.containsKey(fromUpstream)) {
            CachingAnswerManager<?, ConceptMap> answerManager = createRequestState(fromUpstream, iteration);
            answerManagers.put(fromUpstream, answerManager);
        } else {
            CachingAnswerManager<?, ConceptMap> answerManager = this.answerManagers.get(fromUpstream);

            if (answerManager.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                CachingAnswerManager<?, ConceptMap> newAnswerManager = createRequestState(fromUpstream, iteration);
                this.answerManagers.put(fromUpstream, newAnswerManager);
            }
        }
        return answerManagers.get(fromUpstream);
    }

    protected CachingAnswerManager<?, ConceptMap> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        Register<AnswerCache<?, ConceptMap>, ConceptMap> cacheRegister = getOrCreateCacheRegister(root, iteration);
        ConceptMap answerFromUpstream = fromUpstream.partialAnswer().conceptMap();

        assert fromUpstream.partialAnswer().isConcludable();
        if (fromUpstream.partialAnswer().asConcludable().isExplain()) {
            assert !cacheRegister.isRegistered(answerFromUpstream);
            ConcludableExplanationCache answerCache = new ConcludableExplanationCache(cacheRegister, answerFromUpstream);
            cacheRegister.register(answerFromUpstream, answerCache);
            if (!answerCache.isComplete()) {
                answerCache.cache(
                        traversalIterator(concludable.pattern(), answerFromUpstream)
                                .map(ans -> fromUpstream.partialAnswer().asConcludable())
                );
            }
            ExplainingAnswerManager answerManager = new ExplainingAnswerManager(fromUpstream, answerCache, iteration, false);
            registerRules(fromUpstream, answerManager.asExploration());
            return answerManager;

        } else {
            if (cacheRegister.isRegistered(answerFromUpstream)) {
                AnswerCache<?, ConceptMap> answerCache = cacheRegister.get(answerFromUpstream);
                // TODO: can we always cast to a ConceptMapCache?
                return new ConceptMapAnswerManager(fromUpstream, answerCache.asConceptMapCache(), iteration, true, true);
            } else {
                ConceptMapCache answerCache = new ConceptMapCache(cacheRegister, answerFromUpstream);
                cacheRegister.register(answerFromUpstream, answerCache);
                if (!answerCache.isComplete()) {
                    answerCache.cache(traversalIterator(concludable.pattern(), answerFromUpstream));
                }
                boolean singleAnswerRequired = answerFromUpstream.concepts().keySet().containsAll(unboundVars());
                ConceptMapAnswerManager answerManager = new RuleAnswerManager(fromUpstream, answerCache, iteration, singleAnswerRequired, true);
                registerRules(fromUpstream, answerManager.asExploration());
                return answerManager;
            }
        }
    }

    private Register<AnswerCache<?, ConceptMap>, ConceptMap> getOrCreateCacheRegister(Driver<? extends Resolver<?>> root, int iteration) {
        cacheRegisters.putIfAbsent(root, new Register<>(iteration));
        Register<AnswerCache<?, ConceptMap>, ConceptMap> cacheRegister = cacheRegisters.get(root);
        if (cacheRegister.iteration() < iteration) {
            cacheRegister.nextIteration(iteration);
        }
        return cacheRegister;
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

    private class ConceptMapAnswerManager extends CachingAnswerManager<ConceptMap, ConceptMap> {

        public ConceptMapAnswerManager(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache,
                                       int iteration, boolean deduplicate, boolean mayCauseReiteration) {
            super(fromUpstream, answerCache, iteration, mayCauseReiteration, deduplicate);
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(ConceptMap conceptMap) {
            Partial.Concludable<?> partial = fromUpstream.partialAnswer().asConcludable();
            assert !partial.isExplain() && partial.isMatch(); // TODO: Can the typing be tightened up to remove these assertions?
            Partial.Compound<?, ?> upstreamAnswer = partial.asMatch().toUpstreamLookup(conceptMap, concludable.isInferredAnswer(conceptMap));
            if (answerCache.requiresReiteration()) upstreamAnswer.setRequiresReiteration(); // TODO: Make it a responsibility of the cache to mark all answers it yields as requiresReiteration if the cache is marked as RequiresReiteration
            return Iterators.single(upstreamAnswer);
        }

    }

    private class RuleAnswerManager extends ConceptMapAnswerManager implements Exploration {

        private final DownstreamManager downstreamManager;
        private final boolean singleAnswerRequired;

        public RuleAnswerManager(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache,
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
            if (requiresReiteration) answerCache.setRequiresReiteration();
        }

        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }
    }

    private static class ExplainingAnswerManager extends CachingAnswerManager<Partial.Concludable<?>, ConceptMap> implements Exploration {

        private final DownstreamManager downstreamManager;

        public ExplainingAnswerManager(Request fromUpstream, AnswerCache<Partial.Concludable<?>, ConceptMap> answerCache,
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
            if (requiresReiteration) answerCache.setRequiresReiteration(); // TODO: Make it a responsibility of the cache to mark all answers it yields as requiresReiteration if the cache is marked as RequiresReiteration
        }

        @Override
        public boolean singleAnswerRequired() {
            return false;
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(Partial.Concludable<?> partial) {
            if (partial.asExplain().ofInferred()) {
                Partial<?> upstreamAnswer = partial.asExplain().toUpstreamInferred();
                if (answerCache.requiresReiteration()) upstreamAnswer.setRequiresReiteration(); // TODO: Make it a responsibility of the cache to mark all answers it yields as requiresReiteration if the cache is marked as RequiresReiteration
                return Iterators.single(upstreamAnswer);
            }
            return Iterators.empty();
        }

    }

}
