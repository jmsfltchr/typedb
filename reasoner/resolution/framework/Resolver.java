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

package grakn.core.reasoner.resolution.framework;

import grakn.common.collection.Either;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.poller.Poller;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.producer.Producer;
import grakn.core.concurrent.producer.Producers;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Response.Answer;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier.Variable.Retrievable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static grakn.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;

public abstract class Resolver<RESOLVER extends Resolver<RESOLVER>> extends Actor<RESOLVER> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    private final Map<Request, Request> requestRouter;
    protected final ResolverRegistry registry;
    protected final TraversalEngine traversalEngine;
    protected final ConceptManager conceptMgr;
    private final boolean resolutionTracing;
    private boolean terminated;
    private static final AtomicInteger messageCount = new AtomicInteger(0);

    protected Resolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry, TraversalEngine traversalEngine,
                       ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, name);
        this.registry = registry;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.resolutionTracing = resolutionTracing;
        this.terminated = false;
        this.requestRouter = new HashMap<>();
        // Note: initialising downstream actors in constructor will create all actors ahead of time, so it is non-lazy
        // additionally, it can cause deadlock within ResolverRegistry as different threads initialise actors
    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof GraknException && ((GraknException) e).code().isPresent()) {
            String code = ((GraknException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Resolver interrupted by resource close: {}", e.getMessage());
                registry.terminateResolvers(e);
                return;
            }
        }
        LOG.error("Actor exception", e);
        registry.terminateResolvers(e);
    }

    public abstract void receiveRequest(Request fromUpstream, int iteration);

    protected abstract void receiveAnswer(Response.Answer fromDownstream, int iteration);

    protected abstract void receiveFail(Response.Fail fromDownstream, int iteration);

    public void terminate(Throwable cause) {
        LOG.debug("Resolver terminated. ", cause);
        this.terminated = true;
    }

    public boolean isTerminated() { return terminated; }

    protected abstract void initialiseDownstreamResolvers();

    protected Request fromUpstream(Request toDownstream) {
        assert requestRouter.containsKey(toDownstream);
        return requestRouter.get(toDownstream);
    }

    private void logMessage(int iteration) {
        int i = messageCount.incrementAndGet();
        if (i % 100 == 0) {
            LOG.info("Message count: {} (iteration {})", i, iteration);
        }
    }

    protected void requestFromDownstream(Request request, Request fromUpstream, int iteration) {
        LOG.trace("{} : Sending a new answer Request to downstream: {}", name(), request);
        if (resolutionTracing) ResolutionTracer.get().request(this.name(), request.receiver().name(), iteration,
                                                              request.partialAnswer().conceptMap().concepts().keySet().toString());
        // TODO: we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Driver<? extends Resolver<?>> receiver = request.receiver();
        logMessage(iteration);
        receiver.execute(actor -> actor.receiveRequest(request, iteration));
    }

    protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
        assert answer.isPartial();
        Answer response = Answer.create(fromUpstream, answer.asPartial());
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (resolutionTracing) ResolutionTracer.get().responseAnswer(
                this.name(), fromUpstream.sender().name(), iteration,
                response.asAnswer().answer().conceptMap().concepts().keySet().toString()
        );
        logMessage(iteration);
        fromUpstream.sender().execute(actor -> actor.receiveAnswer(response, iteration));
    }

    protected void failToUpstream(Request fromUpstream, int iteration) {
        Response.Fail response = new Response.Fail(fromUpstream);
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (resolutionTracing) ResolutionTracer.get().responseExhausted(
                this.name(), fromUpstream.sender().name(), iteration
        );
        logMessage(iteration);
        fromUpstream.sender().execute(actor -> actor.receiveFail(response, iteration));
    }

    protected FunctionalIterator<ConceptMap> traversalIterator(Conjunction conjunction, ConceptMap bounds) {
        return compatibleBounds(conjunction, bounds).map(c -> {
            Traversal traversal = boundTraversal(conjunction.traversal(), c);
            return traversalEngine.iterator(traversal).map(conceptMgr::conceptMap);
        }).orElse(Iterators.empty());
    }

    protected Producer<ConceptMap> traversalProducer(Conjunction conjunction, ConceptMap bounds, int parallelisation) {
        return compatibleBounds(conjunction, bounds).map(b -> {
            Traversal traversal = boundTraversal(conjunction.traversal(), b);
            return traversalEngine.producer(traversal, Either.first(INCREMENTAL), parallelisation).map(conceptMgr::conceptMap);
        }).orElse(Producers.empty());
    }

    private Optional<ConceptMap> compatibleBounds(Conjunction conjunction, ConceptMap bounds) {
        Map<Retrievable, Concept> newBounds = new HashMap<>();
        for (Map.Entry<Retrievable, ? extends Concept> entry : bounds.concepts().entrySet()) {
            Retrievable id = entry.getKey();
            Concept bound = entry.getValue();
            Variable conjVariable = conjunction.variable(id);
            assert conjVariable != null;
            if (conjVariable.isThing()) {
                if (!conjVariable.asThing().iid().isPresent()) newBounds.put(id, bound);
                else if (!Arrays.equals(conjVariable.asThing().iid().get().iid(), bound.asThing().getIID())) {
                    return Optional.empty();
                }
            } else if (conjVariable.isType()) {
                if (!conjVariable.asType().label().isPresent()) newBounds.put(id, bound);
                else if (!conjVariable.asType().label().get().properLabel().equals(bound.asType().getLabel())) {
                    return Optional.empty();
                }
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }
        return Optional.of(new ConceptMap(newBounds));
    }

    protected Traversal boundTraversal(Traversal traversal, ConceptMap bounds) {
        bounds.concepts().forEach((id, concept) -> {
            if (concept.isThing()) traversal.iid(id.asVariable(), concept.asThing().getIID());
            else {
                traversal.clearLabels(id.asVariable());
                traversal.labels(id.asVariable(), concept.asType().getLabel());
            }
        });
        return traversal;
    }

    // TODO: Continue trying to remove the AnswerCacheRegister to reduce it to just a Map
    // TODO: The larger objective is to create an interface that does no caching that the ConclusionResolver can use while we add proper recursion detection

    public static class CacheRegister<ANSWER_CACHE extends AnswerCache<?, SUBSUMES>, SUBSUMES> {
        Map<ConceptMap, ANSWER_CACHE> answerCaches;
        private int iteration;

        public CacheRegister(int iteration) {
            this.iteration = iteration;
            this.answerCaches = new HashMap<>();
        }

        public void register(ConceptMap fromUpstream, ANSWER_CACHE answerCache) {
            assert !answerCaches.containsKey(fromUpstream);
            answerCaches.put(fromUpstream, answerCache);
        }

        public boolean isRegistered(ConceptMap conceptMap) {
            return answerCaches.containsKey(conceptMap);
        }

        public int iteration() {
            return iteration;
        }

        public void nextIteration(int newIteration) {
            assert newIteration > iteration;
            iteration = newIteration;
            answerCaches = new HashMap<>();
        }

        public ANSWER_CACHE get(ConceptMap fromUpstream) {
            return answerCaches.get(fromUpstream);
        }
    }

    public static class DownstreamManager {
        private final LinkedHashSet<Request> downstreams;
        private Iterator<Request> downstreamSelector;

        public DownstreamManager() {
            this.downstreams = new LinkedHashSet<>();
            this.downstreamSelector = downstreams.iterator();
        }

        public boolean hasDownstream() {
            return !downstreams.isEmpty();
        }

        public Request nextDownstream() {
            if (!downstreamSelector.hasNext()) downstreamSelector = downstreams.iterator();
            return downstreamSelector.next();
        }

        public void addDownstream(Request request) {
            assert !(downstreams.contains(request)) : "downstream answer producer already contains this request";

            downstreams.add(request);
            downstreamSelector = downstreams.iterator();
        }

        public void removeDownstream(Request request) {
            boolean removed = downstreams.remove(request);
            // only update the iterator when removing an element, to avoid resetting and reusing first request too often
            // note: this is a large performance win when processing large batches of requests
            if (removed) downstreamSelector = downstreams.iterator();
        }

        public void clearDownstreams() {
            downstreams.clear();
            downstreamSelector = Iterators.empty();
        }

        public boolean contains(Request downstreamRequest) {
            return downstreams.contains(downstreamRequest);
        }
    }
}
