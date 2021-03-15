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
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Unified;
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
    private final Concludable concludable;
    private final LogicManager logicMgr;
    private final Map<Driver<? extends Resolver<?>>, RecursionState> recursionStates;
    private final Driver<ResolutionRecorder> resolutionRecorder;
    private final Map<Request, RequestState> requestStates;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private boolean isInitialised;
    protected final Map<Actor.Driver<? extends Resolver<?>>, RequestStatesTracker> requestStatesTrackers;

    public ConcludableResolver(Driver<ConcludableResolver> driver, Concludable concludable,
                               Driver<ResolutionRecorder> resolutionRecorder, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                               boolean resolutionTracing) {
        super(driver, ConcludableResolver.class.getSimpleName() + "(pattern: " + concludable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.logicMgr = logicMgr;
        this.resolutionRecorder = resolutionRecorder;
        this.concludable = concludable;
        this.applicableRules = new LinkedHashMap<>();
        this.recursionStates = new HashMap<>();
        this.requestStates = new HashMap<>();
        this.unboundVars = unboundVars(concludable.pattern());
        this.isInitialised = false;
        this.requestStatesTrackers = new HashMap<>();
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
            if (requestState.isCached()) {
                nextCachedAnswer(fromUpstream, requestState.asCached(), iteration);
            } else if (requestState.isExploration()) {
                nextAnswer(fromUpstream, requestState.asExploration(), iteration);
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);
        requestStatesTrackers.get(fromUpstream.partialAnswer().root()).recordFromDownstream(
                fromUpstream.partialAnswer().conceptMap(), fromDownstream.answer().conceptMap());
        if (requestState.isCached()) {
            if (iteration == requestState.iteration()) {
                nextCachedAnswer(fromUpstream, requestState.asCached(), iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        } else if (requestState.isExploration()) {
            ExplorationRequestState exploration = requestState.asExploration();
            Partial<?> upstreamAnswer = fromDownstream.answer().asMapped().toUpstream();

            if (!exploration.producedRecorder().hasProduced(upstreamAnswer.conceptMap())) {
                exploration.producedRecorder().recordProduced(upstreamAnswer.conceptMap());
                answerFound(upstreamAnswer, fromUpstream, iteration);
            } else {
                if (fromDownstream.answer().recordExplanations()) {
                    LOG.trace("{}: Recording deduplicated answer derivation: {}", name(), upstreamAnswer);
                    resolutionRecorder.execute(actor -> actor.record(upstreamAnswer));
                }
                nextAnswer(fromUpstream, exploration, iteration);
            }
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
    }

    /*
    When we only require 1 answer (eg. when the conjunction is already fully bound), we can short circuit
    and prevent exploration of further rules.

    One latency optimisation we could do here, is keep track of how many N repeated requests are received,
    forward them downstream (to parallelise searching for the single answer), and when the first one finds an answer,
    we respond for all N ahead of time. Then, when the rules actually return an answer to this concludable, we do nothing.
     */
    private void answerFound(Partial<?> upstreamAnswer, Request fromUpstream, int iteration) {
        ExplorationRequestState requestState = this.requestStates.get(fromUpstream).asExploration();
        if (requestState.singleAnswerRequired()) {
            requestState.downstreamManager().clearDownstreams();
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
        if (requestState.isCached()) {
            nextCachedAnswer(fromUpstream, requestState.asCached(), iteration);
        } else if (requestState.isExploration()) {
            requestState.asExploration().downstreamManager().removeDownstream(fromDownstream.sourceRequest());
            nextAnswer(fromUpstream, requestState.asExploration(), iteration);
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
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
                    } catch (GraknException e) {
                        terminate(e);
                    }
                }));
        if (!isTerminated()) isInitialised = true;
    }

    private void nextAnswer(Request fromUpstream, ExplorationRequestState requestState, int iteration) {
        if (requestState.hasUpstreamAnswer()) {
            Partial<?> upstreamAnswer = requestState.upstreamAnswers().next();
            requestState.producedRecorder().recordProduced(upstreamAnswer.conceptMap());
            answerFound(upstreamAnswer, fromUpstream, iteration);
        } else {
            if (requestState.downstreamManager().hasDownstream()) {
                requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                RecursionState recursionState = recursionStates.get(fromUpstream.partialAnswer().root());
                if (!recursionState.hasReceived(fromUpstream.partialAnswer().conceptMap())) {
                    requestStatesTrackers.putIfAbsent(fromUpstream.partialAnswer().root(), new RequestStatesTracker(iteration));
                    assert requestStatesTrackers.get(fromUpstream.partialAnswer().root()).iteration() == iteration;
                    requestStatesTrackers.get(fromUpstream.partialAnswer().root()).setDownstreamExplored(fromUpstream.partialAnswer().conceptMap());
                }
                failToUpstream(fromUpstream, iteration);
            }
        }
    }

    private void nextCachedAnswer(Request fromUpstream, CachedRequestState requestState, int iteration) {
        if (requestState.hasUpstreamAnswer()) {
            Partial<?> upstreamAnswer = requestState.upstreamAnswers().next();
            answerToUpstream(upstreamAnswer, fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    private RequestState getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            RequestState newRequestState = requestStateCreate(fromUpstream, iteration);
            requestStates.put(fromUpstream, newRequestState);
        } else {
            RequestState requestState = this.requestStates.get(fromUpstream);
            assert requestState.iteration() == iteration || requestState.iteration() + 1 == iteration;

            if (requestState.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState newRequestState = requestStateCreate(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, newRequestState);
            } else {
                Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
                RequestState exploration;
                if (!(exploration = requestStates.get(fromUpstream)).isCached()) {
                    if (requestStatesTrackers.get(root).fullyExplored(fromUpstream.partialAnswer().conceptMap())) {
                        assert !requestState.asExploration().downstreamManager().hasDownstream();
                        FunctionalIterator<Partial<?>> completedUpstreamAnswers = requestStatesTrackers.get(root).completeIterator(fromUpstream.partialAnswer().conceptMap())
                                .map(c -> fromUpstream.partialAnswer().asMapped().aggregateToUpstream(c));
                        completedUpstreamAnswers = completedUpstreamAnswers.filter(c -> !exploration.asExploration().producedRecorder().produced().contains(c.conceptMap()));
                        this.requestStates.put(fromUpstream, new CachedRequestState(completedUpstreamAnswers, iteration));
                    }
                }
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected RequestState requestStateCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating new Responses for iteration{}, request: {}", name(), iteration, fromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        recursionStates.putIfAbsent(root, new RecursionState(iteration));
        RecursionState recursionState = recursionStates.get(root);
        if (recursionState.iteration() < iteration) {
            recursionState.nextIteration(iteration);
        }

        requestStatesTrackers.putIfAbsent(root, new RequestStatesTracker(iteration));
        RequestStatesTracker completableStatesTracker = requestStatesTrackers.get(root);
        if (completableStatesTracker.iteration() < iteration) {
            completableStatesTracker.nextIteration(iteration);
        }

        if (completableStatesTracker.fullyExplored(fromUpstream.partialAnswer().conceptMap())) {
            FunctionalIterator<Partial<?>> completedUpstreamAnswers = completableStatesTracker.completeIterator(fromUpstream.partialAnswer().conceptMap())
                    .map(c -> fromUpstream.partialAnswer().asMapped().aggregateToUpstream(c));
            return new CachedRequestState(completedUpstreamAnswers, iteration);
        } else {
            assert fromUpstream.partialAnswer().isMapped();
            FunctionalIterator<Partial<?>> upstreamAnswers =
                    traversalIterator(concludable.pattern(), fromUpstream.partialAnswer().conceptMap())
                            .map(conceptMap -> {
                                // TODO: Record the conceptMaps generated by traversal
                                requestStatesTrackers.get(fromUpstream.partialAnswer().root()).recordFromTraversal(
                                        fromUpstream.partialAnswer().conceptMap(), conceptMap);
                                return conceptMap;
                            })
                            .map(conceptMap -> fromUpstream.partialAnswer().asMapped().aggregateToUpstream(conceptMap));

            boolean singleAnswerRequired = fromUpstream.partialAnswer().conceptMap().concepts().keySet().containsAll(unboundVars());
            ExplorationRequestState requestState = new ExplorationRequestState(upstreamAnswers, iteration, singleAnswerRequired);
            if (!recursionState.hasReceived(fromUpstream.partialAnswer().conceptMap())) {
                registerRules(fromUpstream, requestState);
                recursionState.recordReceived(fromUpstream.partialAnswer().conceptMap());
            }
            return requestState;
        }
    }

    private void registerRules(Request fromUpstream, ExplorationRequestState requestState) {
        // loop termination: when receiving a new request, we check if we have seen it before from this root query
        // if we have, we do not allow rules to be registered as possible downstreams
        for (Map.Entry<Driver<ConclusionResolver>, Set<Unifier>> entry : applicableRules.entrySet()) {
            Driver<ConclusionResolver> conclusionResolver = entry.getKey();
            for (Unifier unifier : entry.getValue()) {
                Optional<Unified> unified = fromUpstream.partialAnswer().unifyToDownstream(unifier, conclusionResolver);
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

    Set<Identifier.Variable.Retrievable> unboundVars(Conjunction conjunction) {
        Set<Identifier.Variable.Retrievable> missingBounds = new HashSet<>();
        iterate(conjunction.variables()).filter(var -> var.id().isRetrievable()).forEachRemaining(var -> {
            if (var.isType() && !var.asType().label().isPresent()) missingBounds.add(var.asType().id().asRetrievable());
            else if (var.isThing() && !var.asThing().iid().isPresent())
                missingBounds.add(var.asThing().id().asRetrievable());
        });
        return missingBounds;
    }

    protected abstract static class RequestState {

        public abstract boolean hasUpstreamAnswer();

        public abstract FunctionalIterator<Partial<?>> upstreamAnswers();

        public abstract int iteration();

        public boolean isExploration() {
            return false;
        }

        public ExplorationRequestState asExploration() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(ExplorationRequestState.class));
        }

        public boolean isCached() {
            return false;
        }

        public CachedRequestState asCached() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(CachedRequestState.class));
        }
    }

    // TODO: Create a hierarchy in this class of 3 RequestStates to fix casting issues and remove generics.
    private static class ExplorationRequestState extends RequestState {
        private final FunctionalIterator<Partial<?>> newUpstreamAnswers;
        private final int iteration;
        private final boolean singleAnswerRequired;
        private final DownstreamManager downstreamManager;
        private final ProducedRecorder producedRecorder;

        public ExplorationRequestState(FunctionalIterator<Partial<?>> upstreamAnswers, int iteration, boolean singleAnswerRequired) {
            this(upstreamAnswers, iteration, new DownstreamManager(), new ProducedRecorder(), singleAnswerRequired);
        }

        public ExplorationRequestState(FunctionalIterator<Partial<?>> upstreamAnswers, int iteration,
                                       DownstreamManager downstreamManager, ProducedRecorder producedRecorder,
                                       boolean singleAnswerRequired) {
            this.newUpstreamAnswers = upstreamAnswers.filter(partial -> !producedRecorder.hasProduced(partial.conceptMap()));
            this.iteration = iteration;
            this.singleAnswerRequired = singleAnswerRequired;
            this.downstreamManager = downstreamManager;
            this.producedRecorder = producedRecorder;
        }

        public ExplorationRequestState update(FunctionalIterator<Partial<?>> upstreamAnswers) {
            return new ExplorationRequestState(upstreamAnswers, iteration, downstreamManager, producedRecorder, singleAnswerRequired);
        }

        @Override
        public boolean isExploration() {
            return true;
        }

        @Override
        public ExplorationRequestState asExploration() {
            return this;
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        public ProducedRecorder producedRecorder() {
            return producedRecorder;
        }

        public boolean hasUpstreamAnswer() {
            return newUpstreamAnswers.hasNext();
        }

        public FunctionalIterator<Partial<?>> upstreamAnswers() {
            return newUpstreamAnswers;
        }

        public boolean singleAnswerRequired() {
            return singleAnswerRequired;
        }

        public int iteration() {
            return iteration;
        }
    }

    private static class CachedRequestState extends RequestState {

        private final FunctionalIterator<Partial<?>> newUpstreamAnswers;
        private final int iteration;

        public CachedRequestState(FunctionalIterator<Partial<?>> answers, int iteration) {
            this.newUpstreamAnswers = answers;
            this.iteration = iteration;
        }

        @Override
        public boolean hasUpstreamAnswer() {
            return newUpstreamAnswers.hasNext();
        }

        @Override
        public FunctionalIterator<Partial<?>> upstreamAnswers() {
            return newUpstreamAnswers;
        }

        @Override
        public int iteration() {
            return iteration;
        }

        @Override
        public boolean isCached() {
            return true;
        }

        @Override
        public CachedRequestState asCached() {
            return this;
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

        RecursionState(int iteration) {
            this.iteration = iteration;
            this.receivedMaps = new HashSet<>();
        }

        public int iteration() {
            return iteration;
        }

        public void nextIteration(int newIteration) {
            assert newIteration > iteration;
            iteration = newIteration;
            receivedMaps = new HashSet<>();
        }

        public void recordReceived(ConceptMap conceptMap) {
            receivedMaps.add(conceptMap);
        }

        public boolean hasReceived(ConceptMap conceptMap) {
            return receivedMaps.contains(conceptMap);
        }
    }
}

