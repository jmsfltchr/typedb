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
import grakn.core.common.iterator.Iterators;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.AnswerCache;
import grakn.core.reasoner.resolution.framework.AnswerCache.ConceptMapCache;
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
    private final Map<Request, AnswerManager> answerManagers;
    protected final Map<Actor.Driver<? extends Resolver<?>>, CacheRegister<ConceptMapCache, ConceptMap>> cacheRegisters;

    public RetrievableResolver(Driver<RetrievableResolver> driver, Retrievable retrievable, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.retrievable = retrievable;
        this.answerManagers = new HashMap<>();
        this.cacheRegisters = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (isTerminated()) return;

        AnswerManager answerManager = getOrReplaceRequestState(fromUpstream, iteration);
        if (iteration < answerManager.iteration()) {
            // short circuit old iteration failed messages to upstream
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == answerManager.iteration();
            nextAnswer(fromUpstream, answerManager, iteration);
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

    private AnswerManager getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!answerManagers.containsKey(fromUpstream)) {
            answerManagers.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            AnswerManager answerManager = this.answerManagers.get(fromUpstream);

            if (answerManager.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                AnswerManager responseProducerNextIter = createRequestState(fromUpstream, iteration);
                this.answerManagers.put(fromUpstream, responseProducerNextIter);
            }
        }
        return answerManagers.get(fromUpstream);
    }

    protected AnswerManager createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ResponseProducer for iteration:{}, request: {}", name(), iteration, fromUpstream);
        assert fromUpstream.partialAnswer().isRetrievable();
        ConceptMap answerFromUpstream = fromUpstream.partialAnswer().conceptMap();
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        cacheRegisters.putIfAbsent(root, new CacheRegister<>(iteration));
        CacheRegister<ConceptMapCache, ConceptMap> cacheRegister = cacheRegisters.get(root);
        ConceptMapCache answerCache;
        if (cacheRegister.isRegistered(answerFromUpstream)) {
            answerCache = cacheRegister.get(answerFromUpstream);
        } else {
            answerCache = new ConceptMapCache(cacheRegister, answerFromUpstream);
            cacheRegister.register(answerFromUpstream, answerCache);
            if (!answerCache.isComplete()) answerCache.cache(traversalIterator(retrievable.pattern(), answerFromUpstream));
        }
        return new AnswerManager(fromUpstream, answerCache, iteration);
    }

    private void nextAnswer(Request fromUpstream, AnswerManager responseProducer, int iteration) {
        Optional<Partial.Compound<?, ?>> upstreamAnswer = responseProducer.nextAnswer().map(Partial::asCompound);
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else {
            answerManagers.get(fromUpstream).answerCache().setComplete();
            failToUpstream(fromUpstream, iteration);
        }
    }

    private static class AnswerManager extends CachingAnswerManager<ConceptMap, ConceptMap> {

        public AnswerManager(Request fromUpstream, AnswerCache<ConceptMap, ConceptMap> answerCache, int iteration) {
            super(fromUpstream, answerCache, iteration, true); // TODO do we want this to cause reiteration?
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(ConceptMap answer) {
            Partial.Retrievable<?> retrievable = fromUpstream.partialAnswer().asRetrievable();
            Partial.Compound<?, ?> upstreamAnswer = retrievable.aggregateToUpstream(answer);
            if (answerCache.requiresReiteration()) upstreamAnswer.setRequiresReiteration();
            return Iterators.single(upstreamAnswer);
        }

        @Override
        protected boolean optionallyDeduplicate(ConceptMap conceptMap) {
            return false;
        }

    }
}
