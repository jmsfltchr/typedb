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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static grakn.core.common.iterator.Iterators.iterate;
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
        LOG.error("Actor exception: {}", e.getMessage());
        registry.terminateResolvers(e);
    }

    public abstract void receiveRequest(Request fromUpstream, int iteration);

    protected abstract void receiveAnswer(Response.Answer fromDownstream, int iteration);

    protected abstract void receiveFail(Response.Fail fromDownstream, int iteration);

    public void terminate(Throwable cause) {
        LOG.error("error", cause);
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

    protected static abstract class RequestState {

        private final int iteration;

        protected RequestState(int iteration) {this.iteration = iteration;}

        public abstract Optional<? extends Partial<?>> nextAnswer();

        public int iteration() {
            return iteration;
        }
    }

    protected abstract static class CachingRequestState<ANSWER> extends RequestState {

        protected final Request fromUpstream;
        protected final AnswerCache<ANSWER> answerCache;
        protected int pointer;

        public CachingRequestState(Request fromUpstream, AnswerCache<ANSWER> answerCache, int iteration) {
            super(iteration);
            this.fromUpstream = fromUpstream;
            this.answerCache = answerCache;
            this.pointer = 0;
        }

        public Optional<? extends Partial<?>> nextAnswer() {
            Optional<? extends Partial<?>> upstreamAnswer = Optional.empty();
            while (true) {
                Optional<ANSWER> answer = next();
                if (answer.isPresent()) {
                    pointer++;
                    upstreamAnswer = toUpstream(answer.get()).filter(partial -> !optionallyDeduplicate(partial.conceptMap()));
                    if (upstreamAnswer.isPresent()) break;
                } else {
                    break;
                }
            }
            return upstreamAnswer;
        }

        protected abstract Optional<? extends Partial<?>> toUpstream(ANSWER conceptMap);

        protected abstract boolean optionallyDeduplicate(ConceptMap conceptMap);

        protected abstract Optional<ANSWER> next();

        public AnswerCache<ANSWER> answerCache() {
            return answerCache;
        }
    }

    public static class CacheRegister<ANSWER> {
        Map<ConceptMap, AnswerCache<ANSWER>> answerCaches;
        private int iteration;

        public CacheRegister(int iteration) {
            this.iteration = iteration;
            this.answerCaches = new HashMap<>();
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

        public AnswerCache<ANSWER> get(ConceptMap fromUpstream) {
            return answerCaches.get(fromUpstream);
        }

        private void register(ConceptMap fromUpstream, AnswerCache<ANSWER> answerCache) {
            assert !answerCaches.containsKey(fromUpstream);
            answerCaches.put(fromUpstream, answerCache);
        }
    }

    // TODO: Continue trying to remove the AnswerCacheRegister to reduce it to just a Map
    // TODO: The larger objective is to create an interface that does no caching that the ConclusionResolver can use while we add proper recursion detection
    protected static abstract class AnswerCache<ANSWER> {

        private final List<ANSWER> answers;
        private final Set<ANSWER> answersSet;
        private final Set<ConceptMap> subsumingCacheKeys;
        private boolean retrievalBlockedByUnexplored;
        private boolean requiresReiteration;
        private FunctionalIterator<ANSWER> unexploredAnswers;
        private boolean complete;
        private final CacheRegister<ANSWER> cacheRegister;
        private final ConceptMap state;

        protected AnswerCache(CacheRegister<ANSWER> cacheRegister, ConceptMap state, boolean useSubsumption) {
            this.cacheRegister = cacheRegister;
            this.state = state;
            this.subsumingCacheKeys = useSubsumption ? getSubsumingCacheKeys(state) : set();
            this.unexploredAnswers = Iterators.empty();
            this.answers = new ArrayList<>(); // TODO: Replace answer list and deduplication set with a bloom filter
            this.answersSet = new HashSet<>();
            this.retrievalBlockedByUnexplored = false;
            this.requiresReiteration = false;
            this.complete = false;
            this.cacheRegister.register(state, this);
        }

        // TODO: cacheIfAbsent?
        // TODO: Align with the behaviour for adding an iterator to the cache
        public void cache(ANSWER newAnswer) {
            if (!isComplete()) addIfAbsent(newAnswer);
        }

        public void cache(Iterator<ANSWER> newAnswers) {
            assert !isComplete();
            unexploredAnswers = unexploredAnswers.link(newAnswers);
        }

        // TODO: A method called next shouldn't take an index
        // TODO: canRecordNewAnswers only makes sense in the caller
        public Optional<ANSWER> next(int index, boolean flagRetrievalBlocking) {
            assert index >= 0;
            if (index < answers.size()) {
                return Optional.of(answers.get(index));
            } else if (index == answers.size()) {
                while (unexploredAnswers.hasNext()) {
                    Optional<ANSWER> nextAnswer = addIfAbsent(unexploredAnswers.next());
                    if (nextAnswer.isPresent()) return nextAnswer;
                }
                if (flagRetrievalBlocking) retrievalBlockedByUnexplored = true;
                return Optional.empty();
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        private Optional<ANSWER> addIfAbsent(ANSWER answer) {
            if (answersSet.contains(answer)) return Optional.empty();
            answers.add(answer);
            answersSet.add(answer);
            if (retrievalBlockedByUnexplored) this.requiresReiteration = true;
            return Optional.of(answer);
        }

        public void setRequiresReiteration() {
            this.requiresReiteration = true;
        }

        public void setComplete() {
            assert !unexploredAnswers.hasNext();
            complete = true;
        }

        public boolean isComplete() {
            if (complete) return true;
            Optional<AnswerCache<ANSWER>> subsumingCache;
            if ((subsumingCache = completeSubsumingCache()).isPresent()) {
                completeFromSubsumer(subsumingCache.get());
                return true;
            } else {
                return false;
            }
        }

        private Optional<AnswerCache<ANSWER>> completeSubsumingCache() {
            for (ConceptMap subsumingCacheKey : subsumingCacheKeys) {
                if (cacheRegister.isRegistered(subsumingCacheKey)) {
                    AnswerCache<ANSWER> subsumingCache;
                    if ((subsumingCache = cacheRegister.get(subsumingCacheKey)).isComplete()) {
                        // TODO: Gets the first complete cache we find. Getting the smallest could be more efficient.
                        return Optional.of(subsumingCache);
                    }
                }
            }
            return Optional.empty();
        }

        private void completeFromSubsumer(AnswerCache<ANSWER> subsumingCache) {
            setCompletedAnswers(subsumingCache.answers);
            complete = true;
            unexploredAnswers = Iterators.empty();
            if (subsumingCache.requiresReiteration()) setRequiresReiteration();
        }

        private void setCompletedAnswers(List<ANSWER> completeAnswers) {
            List<ANSWER> subsumingAnswers = iterate(completeAnswers).filter(e -> subsumes(e, state)).toList();
            subsumingAnswers.forEach(this::addIfAbsent);
        }

        protected abstract boolean subsumes(ANSWER answer, ConceptMap contained);

        public boolean requiresReiteration() {
            return requiresReiteration;
        }

        private static Set<ConceptMap> getSubsumingCacheKeys(ConceptMap fromUpstream) {
            Set<ConceptMap> subsumingCacheKeys = new HashSet<>();
            Map<Retrievable, Concept> concepts = new HashMap<>(fromUpstream.concepts()); // TODO: Copying HashMap just to satisfy generics
            powerSet(concepts.entrySet()).forEach(powerSet -> subsumingCacheKeys.add(toConceptMap(powerSet)));
            subsumingCacheKeys.remove(fromUpstream);
            return subsumingCacheKeys;
        }

        private static <T> Set<Set<T>> powerSet(Set<T> set) {
            Set<Set<T>> powerSet = new HashSet<>();
            powerSet.add(set);
            set.forEach(el -> {
                Set<T> s = new HashSet<>(set);
                s.remove(el);
                powerSet.addAll(powerSet(s));
            });
            return powerSet;
        }

        private static ConceptMap toConceptMap(Set<Map.Entry<Retrievable, Concept>> conceptsEntrySet) {
            HashMap<Retrievable, Concept> map = new HashMap<>();
            conceptsEntrySet.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
            return new ConceptMap(map);
        }
    }

    public static class ProducedRecorder {
        private final Set<ConceptMap> produced;

        public ProducedRecorder() {
            this(new HashSet<>());
        }

        public ProducedRecorder(Set<ConceptMap> produced) {
            this.produced = produced;
        }

        public boolean record(ConceptMap conceptMap) {
            if (produced.contains(conceptMap)) return true;
            produced.add(conceptMap);
            return false;
        }

        public boolean hasRecorded(ConceptMap conceptMap) { // TODO method shouldn't be needed
            return produced.contains(conceptMap);
        }

        public Set<ConceptMap> recorded() {
            return produced;
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
