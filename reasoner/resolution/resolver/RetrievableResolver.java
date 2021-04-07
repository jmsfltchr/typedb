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
    protected final Map<Actor.Driver<? extends Resolver<?>>, CacheRegister<ConceptMap>> cacheRegisters;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.requestStates = new HashMap<>();
        this.cacheRegisters = new HashMap<>();
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

            if (requestStates.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState responseProducerNextIter = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, responseProducerNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    protected RequestState createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for iteration:{}, request: {}", name(), iteration, fromUpstream);
        assert fromUpstream.partialAnswer().isRetrievable();
        ConceptMap answerFromUpstream = fromUpstream.partialAnswer().conceptMap();
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        cacheRegisters.putIfAbsent(root, new CacheRegister<>(iteration));
        CacheRegister<ConceptMap> cacheRegister = cacheRegisters.get(root);
        AnswerCache<ConceptMap> answerCache;
        if (cacheRegister.isRegistered(answerFromUpstream)) {
            answerCache = cacheRegister.get(answerFromUpstream);
        } else {
            answerCache = new ConceptMapCache(cacheRegister, answerFromUpstream, false);
            FunctionalIterator<ConceptMap> traversal = traversalIterator(retrievable.pattern(), answerFromUpstream);
            answerCache.cache(traversal);
        }
        return new RequestState(fromUpstream, answerCache, iteration);
    }

    private void nextAnswer(Request fromUpstream, RequestState responseProducer, int iteration) {
        Optional<Partial.Compound<?, ?>> upstreamAnswer = responseProducer.nextAnswer().map(Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            requestStates.get(fromUpstream).answerCache().setComplete();
            failToUpstream(fromUpstream, iteration);
        }
    }

    protected static class ConceptMapCache extends AnswerCache<ConceptMap> {

        protected ConceptMapCache(CacheRegister<ConceptMap> cacheRegister, ConceptMap state, boolean useSubsumption) {
            super(cacheRegister, state, useSubsumption);
        }

        @Override
        protected boolean subsumes(ConceptMap conceptMap, ConceptMap contained) {
            return conceptMap.concepts().entrySet().containsAll(contained.concepts().entrySet());
        }
    }

    private static class RequestState extends CachingRequestState<ConceptMap> {

        public RequestState(Request fromUpstream, AnswerCache<ConceptMap> answerCache, int iteration) {
            super(fromUpstream, answerCache, iteration);
        }

        @Override
        protected Optional<? extends Partial<?>> toUpstream(ConceptMap conceptMap) {
            Partial.Retrievable<?> retrievable = fromUpstream.partialAnswer().asRetrievable();
            Partial.Compound<?, ?> upstreamAnswer = retrievable.aggregateToUpstream(conceptMap);
            if (answerCache.requiresReiteration()) upstreamAnswer.setRequiresReiteration();
            return Optional.of(upstreamAnswer);
        }

        @Override
        protected boolean optionallyDeduplicate(ConceptMap conceptMap) {
            return false;
        }

        @Override
        protected Optional<ConceptMap> next() {
            return answerCache.next(pointer, true);
        }
    }
}
