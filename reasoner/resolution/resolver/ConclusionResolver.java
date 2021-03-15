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
 */

package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.Rule;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ConclusionResolver extends Resolver<ConclusionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConclusionResolver.class);

    private final Driver<ResolutionRecorder> resolutionRecorder;
    private final Rule.Conclusion conclusion;
    private final Map<Request, RequestState> requestStates;
    private Driver<ConditionResolver> ruleResolver;
    private boolean isInitialised;
    protected final Map<Actor.Driver<? extends Resolver<?>>, RequestStatesTracker> completableStatesTrackers;

    public ConclusionResolver(Driver<ConclusionResolver> driver, Rule.Conclusion conclusion, ResolverRegistry registry,
                              Driver<ResolutionRecorder> resolutionRecorder, TraversalEngine traversalEngine,
                              ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, ConclusionResolver.class.getSimpleName() + "(" + conclusion.rule().getLabel() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.conclusion = conclusion;
        this.resolutionRecorder = resolutionRecorder;
        this.requestStates = new HashMap<>();
        this.isInitialised = false;
        this.completableStatesTrackers = new HashMap<>();
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
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);

        FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);
        if (!materialisations.hasNext()) throw GraknException.of(ILLEGAL_STATE);

        FunctionalIterator<Partial<?>> materialisedAnswers = materialisations
                .map(concepts -> fromUpstream.partialAnswer().asUnified().aggregateToUpstream(concepts))
                .filter(Optional::isPresent)
                .map(Optional::get);
        requestState.addResponses(materialisedAnswers);
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);

        if (iteration < requestState.iteration()) {
            // short circuit old iteration fail messages to upstream
            failToUpstream(fromUpstream, iteration);
            return;
        }

        requestState.downstreamManager().removeDownstream(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        requestStates.clear();
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        try {
            ruleResolver = registry.registerCondition(conclusion.rule().condition());
            isInitialised = true;
        } catch (GraknException e) {
            terminate(e);
        }
    }

    private void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
        Optional<Partial<?>> answer = requestState.nextResponse();
        if (answer.isPresent()) {
            answerToUpstream(answer.get(), fromUpstream, iteration);
        } else if (requestState.downstreamManager().hasDownstream()) {
            requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    private RequestState getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            RequestState requestState = this.requestStates.get(fromUpstream);
            assert requestState.iteration() == iteration || requestState.iteration() + 1 == iteration;

            if (requestState.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState requestStateNextIter = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, requestStateNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    private RequestState createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);

        RequestState requestState = new RequestState(iteration);

        ConceptMap partialAnswer = fromUpstream.partialAnswer().conceptMap();
        // we do a extra traversal to expand the partial answer if we already have the concept that is meant to be generated
        // and if there's extra variables to be populated
        assert conclusion.retrievableIds().containsAll(partialAnswer.concepts().keySet());
        if (conclusion.generating().isPresent() && conclusion.retrievableIds().size() > partialAnswer.concepts().size() &&
                partialAnswer.concepts().containsKey(conclusion.generating().get().id())) {
            FunctionalIterator<Partial.Filtered> completedAnswers = candidateAnswers(fromUpstream, partialAnswer);
            completedAnswers.forEachRemaining(answer -> requestState.downstreamManager().addDownstream(Request.create(driver(), ruleResolver,
                                                                                                  answer)));
        } else {
            Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
            Partial.Filtered downstreamAnswer = fromUpstream.partialAnswer().filterToDownstream(named, ruleResolver);
            requestState.downstreamManager().addDownstream(Request.create(driver(), ruleResolver, downstreamAnswer));
        }

        return requestState;
    }

    private FunctionalIterator<Partial.Filtered> candidateAnswers(Request fromUpstream, ConceptMap answer) {
        Traversal traversal1 = boundTraversal(conclusion.conjunction().traversal(), answer);
        FunctionalIterator<ConceptMap> traversal = traversalEngine.iterator(traversal1).map(conceptMgr::conceptMap);
        Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
        return traversal.map(ans -> fromUpstream.partialAnswer().asUnified().extend(ans).filterToDownstream(named, ruleResolver));
    }

    @Override
    public String toString() {
        return name() + ": then " + conclusion.rule().then();
    }

    private static class RequestState {

        private final List<FunctionalIterator<Partial<?>>> materialisedAnswers;
        private final int iteration;
        private final Set<Partial<?>> produced;
        private final DownstreamManager downstreamManager;

        public RequestState(int iteration) {
            this.materialisedAnswers = new LinkedList<>();
            this.iteration = iteration;
            this.produced = new HashSet<>();
            this.downstreamManager = new DownstreamManager();
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

//        @Override
//        public boolean hasUpstreamAnswer() {
//            // TODO Collapse with hasResponse()
//            return false;
//        }
//
//        @Override
//        public FunctionalIterator<Partial<?>> upstreamAnswers() {
//            // TODO Collapse with nextResponse()
//            return null;
//        }

        public int iteration() {
            return iteration;
        }

        public void addResponses(FunctionalIterator<Partial<?>> materialisations) {
            materialisedAnswers.add(materialisations);
        }

        public Optional<Partial<?>> nextResponse() {
            if (hasResponse()) {
                Partial<?> nextResponse = materialisedAnswers.get(0).next();
                produced.add(nextResponse);
                return Optional.of(nextResponse);
            }
            else return Optional.empty();
        }

        private boolean hasResponse() {
            while (!materialisedAnswers.isEmpty() && !materialisedAnswers.get(0).hasNext()) {
                materialisedAnswers.remove(0);
            }
            return !materialisedAnswers.isEmpty();
        }
    }
}
