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
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
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

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ConcludableResolver extends Resolver<ConcludableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcludableResolver.class);

    private final LinkedHashMap<Driver<ConclusionResolver>, Set<Unifier>> applicableRules;
    private final Map<Driver<ConclusionResolver>, Rule> resolverRules;
    private final grakn.core.logic.resolvable.Concludable concludable;
    private final LogicManager logicMgr;
    private final Map<Driver<? extends Resolver<?>>, RecursionState> recursionStates;
    private final Map<Request, RequestState> requestStates;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private boolean isInitialised;
    protected final Map<Actor.Driver<? extends Resolver<?>>, CacheTracker<ConceptMap>> cacheTrackers;

    public ConcludableResolver(Driver<ConcludableResolver> driver, grakn.core.logic.resolvable.Concludable concludable,
                               ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                               LogicManager logicMgr, boolean resolutionTracing) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.logicMgr = logicMgr;
        this.concludable = concludable;
        this.applicableRules = new LinkedHashMap<>();
        this.resolverRules = new HashMap<>();
        this.recursionStates = new HashMap<>();
        this.requestStates = new HashMap<>();
        this.unboundVars = unboundVars(concludable.pattern());
        this.isInitialised = false;
        this.cacheTrackers = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        RequestState requestState = getOrReplaceRequestState(fromUpstream, iteration);
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
        RequestState requestState = this.requestStates.get(fromUpstream);

        assert cacheTrackers.get(fromUpstream.partialAnswer().root()).isTracked(fromUpstream.partialAnswer().conceptMap());
        assert requestState.isExploration();
        requestState.asExploration().newAnswer(fromDownstream.answer().conceptMap(), fromDownstream.answer().requiresReiteration());

        if (fromDownstream.answer().asConcludable().isExplain()) {
            answerFound(fromDownstream.answer().asConcludable().toUpstreamInferred(), fromUpstream, iteration);
        } else if (iteration == requestState.iteration()) {
            nextAnswer(fromUpstream, requestState, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
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
        RequestState requestState = this.requestStates.get(fromUpstream);
        if (requestState.isExploration() && requestState.singleAnswerRequired() && !upstreamAnswer.isExplain()) {
            requestState.asExploration().downstreamManager().clearDownstreams();
        }
        answerToUpstream(upstreamAnswer, fromUpstream, iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);

        if (iteration < requestState.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
            return;
        }
        if (requestState.isExploration()) requestState.asExploration().downstreamManager().removeDownstream(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
        recursionStates.clear();
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

    private void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
        // Go straight downstream or fail if explaining
        if (fromUpstream.partialAnswer().asConcludable().isExplain()) {
            RuleExplorationRequestState exploration;
            if ((exploration = requestState.asExploration()).downstreamManager().hasDownstream()) {
                requestFromDownstream(exploration.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        } else {
            Optional<Partial.Compound<?, ?>> upstreamAnswer = requestState.nextAnswer().map(Partial::asCompound);
            if (upstreamAnswer.isPresent()) {
                answerFound(upstreamAnswer.get(), fromUpstream, iteration);
            } else {
                RuleExplorationRequestState exploration;
                if (requestState.isExploration() && !requestState.cacheComplete()) {
                    if ((exploration = requestState.asExploration()).downstreamManager().hasDownstream()) {
                        requestFromDownstream(exploration.downstreamManager().nextDownstream(), fromUpstream, iteration);
                    } else {
                        requestState.setCacheComplete(); // TODO: The cache should not be set as complete during recursion
                        failToUpstream(fromUpstream, iteration);
                    }
                } else {
                    failToUpstream(fromUpstream, iteration);
                }
            }
        }
    }

    private RequestState getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            RequestState newRequestState = createRequestState(fromUpstream, iteration);
            requestStates.put(fromUpstream, newRequestState);
        } else {
            RequestState requestState = this.requestStates.get(fromUpstream);

            if (requestState.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState newRequestState = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, newRequestState);
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected RequestState createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        recursionStates.putIfAbsent(root, new RecursionState(iteration));
        RecursionState recursionState = recursionStates.get(root);
        if (recursionState.iteration() < iteration) {
            recursionState.nextIteration(iteration);
        }

        cacheTrackers.putIfAbsent(root, new CacheTracker<>(iteration, new ConceptMapSubsumption()));
        CacheTracker<ConceptMap> tracker = cacheTrackers.get(root);
        if (tracker.iteration() < iteration) {
            tracker.nextIteration(iteration);
        }

        ConceptMap answerFromUpstream = fromUpstream.partialAnswer().conceptMap();
        boolean singleAnswerRequired = answerFromUpstream.concepts().keySet().containsAll(unboundVars());
        assert tracker.isTracked(answerFromUpstream) == recursionState.hasReceived(answerFromUpstream); // TODO: Should be the same, and therefore can remove this part of recursionState
        if (tracker.isTracked(answerFromUpstream)) {
            CacheTracker<ConceptMap>.AnswerCache answerCache = tracker.getAnswerCache(answerFromUpstream);
            RetrievalRequestState requestState = new RetrievalRequestState(fromUpstream, answerCache, iteration, singleAnswerRequired);
            if (!recursionState.hasReceivedFrom(answerFromUpstream, fromUpstream)) requestState.asRetrieval().setRequiresReiteration();
            return requestState;
        } else {
            assert fromUpstream.partialAnswer().isConcludable();
            CacheTracker<ConceptMap>.AnswerCache answerCache = tracker.createAnswerCache(answerFromUpstream, true);
            if (!answerCache.exhausted()) {
                FunctionalIterator<ConceptMap> traversal = traversalIterator(concludable.pattern(), answerFromUpstream);
                answerCache.recordNewAnswers(traversal);
            }
            recursionState.recordReceived(fromUpstream);
            RequestState requestState = new RuleExplorationRequestState(fromUpstream, answerCache, iteration, singleAnswerRequired);
            registerRules(fromUpstream, requestState.asExploration());
            return requestState;
        }
    }

    private void registerRules(Request fromUpstream, RuleExplorationRequestState requestState) {
        // loop termination: when receiving a new request, we check if we have seen it before from this root query
        // if we have, we do not allow rules to be registered as possible downstreams
        Partial.Concludable<?> partialAnswer = fromUpstream.partialAnswer().asConcludable();
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry : applicableRules.entrySet()) {
            Driver<ConclusionResolver> conclusionResolver = entry.getKey();
            for (Unifier unifier : entry.getValue()) {
                Optional<? extends Partial.Conclusion<?, ?>> unified = partialAnswer.toDownstream(unifier, resolverRules.get(conclusionResolver));
                if (unified.isPresent()) {
                    Request toDownstream = Request.create(driver(), conclusionResolver, unified.get());
                    requestState.downstreamManager().addDownstream(toDownstream);
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

    private static class ConceptMapSubsumption extends CacheTracker.SubsumptionOperation<ConceptMap> {

        @Override
        protected boolean subsumes(ConceptMap conceptMap, ConceptMap contained) {
            return conceptMap.concepts().entrySet().containsAll(contained.concepts().entrySet());
        }
    }

    protected abstract class RequestState extends Resolver.CachingRequestState<ConceptMap> {

        private final ProducedRecorder producedRecorder;
        private final boolean singleAnswerRequired;

        public RequestState(Request fromUpstream, CacheTracker<ConceptMap>.AnswerCache answerCache, int iteration, boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, iteration);
            this.singleAnswerRequired = singleAnswerRequired;
            this.producedRecorder = new ProducedRecorder();
        }

        @Override
        protected boolean isDuplicate(ConceptMap conceptMap) {
            return producedRecorder.produced(conceptMap);
        }

        @Override
        protected Optional<? extends Partial<?>> toUpstream(ConceptMap conceptMap) {
            Partial.Concludable<?> partial = fromUpstream.partialAnswer().asConcludable();
            if (answerCache.requiresReiteration()) partial.requiresReiteration(true); // TODO: Changing the upstream's reiteration flag seems wrong

            if (partial.isMatch()) {
                return Optional.of(partial.asMatch().toUpstreamLookup(conceptMap, concludable.isInferredAnswer(conceptMap)));
            } else if (partial.isExplain()) {
                assert conceptMap.concepts().equals(partial.conceptMap().concepts());
                return Optional.of(partial.asExplain().toUpstreamInferred());
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        public boolean isExploration() {
            return false;
        }

        public RuleExplorationRequestState asExploration() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(RuleExplorationRequestState.class));
        }

        public boolean isRetrieval() {
            return false;
        }

        public RetrievalRequestState asRetrieval() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(RetrievalRequestState.class));
        }

        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }
    }

    private class RuleExplorationRequestState extends RequestState {

        private final DownstreamManager downstreamManager;

        public RuleExplorationRequestState(Request fromUpstream, CacheTracker<ConceptMap>.AnswerCache answerCache,
                                           int iteration, boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, iteration, singleAnswerRequired);
            this.downstreamManager = new DownstreamManager();
        }

        @Override
        protected Optional<ConceptMap> next() {
            return answerCache.next(pointer, true);
        }

        @Override
        public boolean isExploration() {
            return true;
        }

        @Override
        public RuleExplorationRequestState asExploration() {
            return this;
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        public void newAnswer(ConceptMap conceptMap, boolean requiresReiteration) {
            answerCache.recordNewAnswer(conceptMap);
            if (requiresReiteration) answerCache.setRequiresReiteration();
        }
    }

    private class RetrievalRequestState extends RequestState {

        public RetrievalRequestState(Request fromUpstream, CacheTracker<ConceptMap>.AnswerCache answerCache,
                                     int iteration, boolean singleAnswerRequired) {
            super(fromUpstream, answerCache, iteration, singleAnswerRequired);
        }

        @Override
        protected Optional<ConceptMap> next() {
            return answerCache.next(pointer, false);
        }

        @Override
        public boolean isRetrieval() {
            return true;
        }

        @Override
        public RetrievalRequestState asRetrieval() {
            return this;
        }

        private void setRequiresReiteration() {
            answerCache.setRequiresReiteration();
        }
    }

    /**
     * Maintain iteration state per root query
     * This allows us to share resolvers across different queries
     * while maintaining the ability to do loop termination within a single query
     */
    private static class RecursionState {
        private Set<ConceptMap> receivedMaps;
        private int iteration;
        private final Map<ConceptMap, Request> exploringRequests;

        RecursionState(int iteration) {
            this.iteration = iteration;
            this.receivedMaps = new HashSet<>();
            this.exploringRequests = new HashMap<>();
        }

        public int iteration() {
            return iteration;
        }

        public void nextIteration(int newIteration) {
            assert newIteration > iteration;
            iteration = newIteration;
            receivedMaps = new HashSet<>();
        }

        public void recordReceived(Request fromUpstream) {
            ConceptMap conceptMap = fromUpstream.partialAnswer().conceptMap();
            receivedMaps.add(conceptMap);
            exploringRequests.put(conceptMap, fromUpstream);
        }

        public boolean hasReceived(ConceptMap conceptMap) {
            return receivedMaps.contains(conceptMap);
        }

        public boolean hasReceivedFrom(ConceptMap conceptMap, Request from) {
            return receivedMaps.contains(conceptMap) && exploringRequests.get(conceptMap).equals(from);
        }
    }
}

