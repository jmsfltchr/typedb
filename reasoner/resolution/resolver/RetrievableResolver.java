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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class RetrievableResolver extends Resolver<RetrievableResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);

    private final Retrievable retrievable;
    private final Map<Request, RequestStates> requestStates;
    protected final Map<Actor.Driver<? extends Resolver<?>>, CompletableStatesTracker> completableStatesTrackers;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              registry, traversalEngine, conceptMgr, explanations);
        this.retrievable = retrievable;
        this.requestStates = new HashMap<>();
        this.completableStatesTrackers = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (isTerminated()) return;

        RequestStates requestStates = getOrReplaceRequestState(fromUpstream, iteration);
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

    private RequestStates getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            RequestStates requestStates = this.requestStates.get(fromUpstream);
            assert iteration <= requestStates.iteration() + 1;

            if (requestStates.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestStates responseProducerNextIter = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, responseProducerNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected RequestStates createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for iteration:{}, request: {}", name(), iteration, fromUpstream);
        assert fromUpstream.partialAnswer().isFiltered();
        FunctionalIterator<Partial<?>> upstreamAnswers =
                traversalIterator(retrievable.pattern(), fromUpstream.partialAnswer().conceptMap())
                        .map(conceptMap -> fromUpstream.partialAnswer().asFiltered().aggregateToUpstream(conceptMap));
        return new RequestStates(upstreamAnswers, iteration);
    }

    private void nextAnswer(Request fromUpstream, RequestStates responseProducer, int iteration) {
        if (responseProducer.hasUpstreamAnswer()) {
            Partial<?> upstreamAnswer = responseProducer.upstreamAnswers().next();
            answerToUpstream(upstreamAnswer, fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    private static class RequestStates extends RequestState<RequestStates> {

        private final FunctionalIterator<Partial<?>> newUpstreamAnswers;
        private final int iteration;
        private final Set<Partial<?>> produced;

        public RequestStates(FunctionalIterator<Partial<?>> upstreamAnswers, int iteration) {
            this.newUpstreamAnswers = upstreamAnswers;
            this.iteration = iteration;
            this.produced = new HashSet<>();
        }

        public boolean hasUpstreamAnswer() {
            return newUpstreamAnswers.hasNext();
        }

        public FunctionalIterator<Partial<?>> upstreamAnswers() {
            return iterate(newUpstreamAnswers.map(i -> {
                produced.add(i);
                return i;
            }));
        }

        public int iteration() {
            return iteration;
        }

        public Set<Partial<?>> produced() {
            return produced;
        }
    }
}
