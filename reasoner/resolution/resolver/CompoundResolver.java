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

import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class CompoundResolver<RESOLVER extends CompoundResolver<RESOLVER>> extends Resolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(CompoundResolver.class);

    final Map<Request, AnswerManager> answerManagers;
    boolean isInitialised;

    protected CompoundResolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry,
                               TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, name, registry, traversalEngine, conceptMgr, resolutionTracing);
        this.answerManagers = new HashMap<>();
        this.isInitialised = false;
    }

    protected abstract void nextAnswer(Request fromUpstream, AnswerManager answerManager, int iteration);

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        AnswerManager answerManager = getOrUpdateRequestState(fromUpstream, iteration);
        if (iteration < answerManager.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == answerManager.iteration();
            nextAnswer(fromUpstream, answerManager, iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted, with iter {}: {}", name(), iteration, fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        AnswerManager answerManager = answerManagers.get(fromUpstream);

        if (iteration < answerManager.iteration()) {
            // short circuit old iteration failed messages back out of the actor model
            failToUpstream(fromUpstream, iteration);
            return;
        }
        answerManager.downstreamManager().removeDownstream(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, answerManager, iteration);
    }

    private AnswerManager getOrUpdateRequestState(Request fromUpstream, int iteration) {
        if (!answerManagers.containsKey(fromUpstream)) {
            answerManagers.put(fromUpstream, requestStateCreate(fromUpstream, iteration));
        } else {
            AnswerManager answerManager = answerManagers.get(fromUpstream);

            if (answerManager.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                AnswerManager responseProducerNextIter = requestStateReiterate(fromUpstream, answerManager, iteration);
                this.answerManagers.put(fromUpstream, responseProducerNextIter);
            }
        }
        return answerManagers.get(fromUpstream);
    }

    abstract AnswerManager requestStateCreate(Request fromUpstream, int iteration);

    abstract AnswerManager requestStateReiterate(Request fromUpstream, AnswerManager priorResponses, int iteration);

    static class AnswerManager {

        private final int iteration;
        private final DownstreamManager downstreamManager;
        private final ProducedRecorder producedRecorder;

        public AnswerManager(int iteration) {
            this(iteration, new HashSet<>());
        }

        public AnswerManager(int iteration, Set<ConceptMap> produced) {
            this.iteration = iteration;
            this.downstreamManager = new DownstreamManager();
            this.producedRecorder = new ProducedRecorder(produced);
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        public int iteration() {
            return iteration;
        }

        public ProducedRecorder producedRecorder() {
            return producedRecorder;
        }
    }
}
