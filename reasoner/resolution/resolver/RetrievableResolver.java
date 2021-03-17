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
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class RetrievableResolver extends Resolver<RetrievableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);

    private final Retrievable retrievable;
    private final Map<Request, RequestState> requestStates;
    protected final Map<Actor.Driver<? extends Resolver<?>>, RequestStatesTracker> requestStatesTrackers;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              registry, traversalEngine, conceptMgr, explanations);
        this.retrievable = retrievable;
        this.requestStates = new HashMap<>();
        this.requestStatesTrackers = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (isTerminated()) return;

        RequestState requestStates = getOrReplaceRequestState(fromUpstream, iteration);
        if (iteration < requestStates.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == requestStates.iteration();
            nextAnswer(fromUpstream, requestStates, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Answer fromDownstream, int iteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw GraknException.of(ILLEGAL_STATE);
    }

    private RequestState getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            RequestState requestStates = this.requestStates.get(fromUpstream);
            assert iteration <= requestStates.iteration() + 1;

            if (requestStates.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState responseProducerNextIter = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, responseProducerNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected RequestState createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for iteration:{}, request: {}", name(), iteration, fromUpstream);
        assert fromUpstream.partialAnswer().isFiltered();
        ConceptMap answerFromUpstream = fromUpstream.partialAnswer().conceptMap();
        FunctionalIterator<ConceptMap> traversal = traversalIterator(retrievable.pattern(), answerFromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        requestStatesTrackers.putIfAbsent(root, new RequestStatesTracker(iteration));
        RequestStatesTracker.ExplorationState exploration = requestStatesTrackers.get(root)
                .newExplorationState(answerFromUpstream, traversal);
        return new RequestState(fromUpstream, exploration, iteration);
    }

    private void nextAnswer(Request fromUpstream, RequestState responseProducer, int iteration) {
        Optional<Partial<?>> upstreamAnswer = responseProducer.nextAnswer();
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    private static class RequestState extends CachingRequestState {

        public RequestState(Request fromUpstream, RequestStatesTracker.ExplorationState explorationState, int iteration) {
            super(fromUpstream, explorationState, iteration);
        }

        @Override
        protected Partial<?> toUpstream(ConceptMap conceptMap) {
            Partial.Filtered filtered = fromUpstream.partialAnswer().asFiltered();
            if (explorationState.requiresReiteration())
                filtered.requiresReiteration(true);
            return filtered.aggregateToUpstream(conceptMap);
        }

        @Override
        protected boolean isDuplicate(ConceptMap conceptMap) {
            return false;
        }

        @Override
        protected Optional<ConceptMap> next() {
            return explorationState.next(pointer, false);
        }
    }
}
