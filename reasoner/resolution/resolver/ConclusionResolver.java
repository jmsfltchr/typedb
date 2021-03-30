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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ConclusionResolver extends Resolver<ConclusionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConclusionResolver.class);

    private final Rule.Conclusion conclusion;
    private final Map<Request, RequestState> requestStates;
    private Driver<ConditionResolver> ruleResolver;
    private boolean isInitialised;
    protected final Map<Actor.Driver<? extends Resolver<?>>, CacheTracker<Map<Identifier.Variable, Concept>>> cacheTrackers;

    public ConclusionResolver(Driver<ConclusionResolver> driver, Rule.Conclusion conclusion, ResolverRegistry registry,
                              TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, ConclusionResolver.class.getSimpleName() + "(" + conclusion + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.conclusion = conclusion;
        this.requestStates = new HashMap<>();
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
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        RequestState requestState = this.requestStates.get(fromUpstream);

        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {  //TODO: Should we use the upstream or downstream to determine whether to explain?
            FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                    .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);
            FunctionalIterator<Partial.Concludable<?>> materialisedAnswers = materialisations
                    .map(concepts -> fromDownstream.answer().asConclusion().aggregateToUpstream(concepts))
                    .filter(Optional::isPresent)
                    .map(Optional::get);
            requestState.addExplainAnswers(materialisedAnswers);

            Optional<Partial.Concludable<?>> nextAnswer;
            if ((nextAnswer = requestState.nextExplainAnswer()).isPresent()) {
                answerToUpstream(nextAnswer.get(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        } else {
            assert cacheTrackers.get(fromUpstream.partialAnswer().root()).isTracked(fromUpstream.partialAnswer().conceptMap());
            if (!requestState.cacheComplete()) {
                FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                        .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);
                if (!materialisations.hasNext()) throw GraknException.of(ILLEGAL_STATE);
                requestState.newMaterialisedAnswers(materialisations, fromDownstream.answer().requiresReiteration());
            }
            nextAnswer(fromUpstream, requestState, iteration);
        }
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
        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            if (requestState.downstreamManager().hasDownstream()) {
                requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        } else {
            Optional<Partial.Concludable<?>> upstreamAnswer = requestState.nextAnswer().map(Partial::asConcludable);
            if (upstreamAnswer.isPresent()) {
                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
            } else if (!requestState.cacheComplete() && requestState.downstreamManager().hasDownstream()) {
                requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                // requestState.setCacheComplete(); // TODO: Reinstate once Conclusion caching works in recursive settings
                failToUpstream(fromUpstream, iteration);
            }
        }
    }

    private RequestState getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            RequestState requestState = this.requestStates.get(fromUpstream);

            if (requestState.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                RequestState requestStateNextIter = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, requestStateNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    private RequestState createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        cacheTrackers.putIfAbsent(root, new CacheTracker<>(iteration, new FullMapSubsumption()));
        CacheTracker<Map<Identifier.Variable, Concept>> tracker = cacheTrackers.get(root);

        ConceptMap answerFromUpstream = fromUpstream.partialAnswer().conceptMap();
        boolean deduplicate;
        boolean useSubsumption;
        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            deduplicate = false;
            useSubsumption = false;
        } else {
            deduplicate = true;
            useSubsumption = true;
        }

        CacheTracker<Map<Identifier.Variable, Concept>>.AnswerCache answerCache;
        boolean isTracked = tracker.isTracked(answerFromUpstream);
        if (isTracked) {
            answerCache = tracker.getAnswerCache(answerFromUpstream);
        } else {
            answerCache = tracker.createAnswerCache(answerFromUpstream, useSubsumption);
        }
        RequestState requestState = new RequestState(fromUpstream, answerCache, iteration, deduplicate);
        assert fromUpstream.partialAnswer().isConclusion();
        Partial.Conclusion<?, ?> partialAnswer = fromUpstream.partialAnswer().asConclusion();
        // we do a extra traversal to expand the partial answer if we already have the concept that is meant to be generated
        // and if there's extra variables to be populated
        if (!requestState.cacheComplete()) {
            assert conclusion.retrievableIds().containsAll(partialAnswer.conceptMap().concepts().keySet());
            if (conclusion.generating().isPresent() && conclusion.retrievableIds().size() > partialAnswer.conceptMap().concepts().size() &&
                    partialAnswer.conceptMap().concepts().containsKey(conclusion.generating().get().id())) {
                FunctionalIterator<Partial.Compound<?, ?>> completedDownstreamAnswers = candidateAnswers( partialAnswer);
                completedDownstreamAnswers.forEachRemaining(answer -> requestState.downstreamManager()
                        .addDownstream(Request.create(driver(), ruleResolver, answer)));
            } else {
                Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
                Partial.Compound<?, ?> downstreamAnswer = partialAnswer.toDownstream(named);
                requestState.downstreamManager().addDownstream(Request.create(driver(), ruleResolver, downstreamAnswer));
            }
        }
        return requestState;
    }

    private FunctionalIterator<Partial.Compound<?, ?>> candidateAnswers(Partial.Conclusion<?, ?> partialAnswer) {
        Traversal traversal = boundTraversal(conclusion.conjunction().traversal(), partialAnswer.conceptMap());
        FunctionalIterator<ConceptMap> answers = traversalEngine.iterator(traversal).map(conceptMgr::conceptMap);
        Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
        return answers.map(ans -> partialAnswer.extend(ans).toDownstream(named));
    }

    @Override
    public String toString() {
        return name();
    }

    private static class FullMapSubsumption extends CacheTracker.SubsumptionOperation<Map<Identifier.Variable, Concept>> {

        @Override
        protected boolean subsumes(Map<Identifier.Variable, Concept> map, ConceptMap contained) {
            return map.entrySet().containsAll(contained.concepts().entrySet());
        }
    }

    private static class RequestState extends CachingRequestState<Map<Identifier.Variable, Concept>> {

        private final DownstreamManager downstreamManager;
        private final ProducedRecorder producedRecorder;
        private final List<FunctionalIterator<Partial.Concludable<?>>> materialisedAnswers;
        private final boolean deduplicate;


        public RequestState(Request fromUpstream, CacheTracker<Map<Identifier.Variable, Concept>>.AnswerCache answerCache,
                            int iteration, boolean deduplicate) {
            super(fromUpstream, answerCache, iteration);
            this.deduplicate = deduplicate;
            this.materialisedAnswers = new LinkedList<>();
            this.downstreamManager = new DownstreamManager();
            this.producedRecorder = new ProducedRecorder();
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        @Override
        protected Optional<? extends Partial<?>> toUpstream(Map<Identifier.Variable, Concept> answer) {
            Partial.Conclusion<?, ?> conclusion = fromUpstream.partialAnswer().asConclusion();
            return  conclusion.aggregateToUpstream(answer).map(p -> {
                if (answerCache.requiresReiteration()) p.setRequiresReiteration();
                return p;
            });
        }

        @Override
        protected boolean optionallyDeduplicate(ConceptMap conceptMap) {
            if (deduplicate) return producedRecorder.produced(conceptMap);
            return false;
        }

        public void newMaterialisedAnswers(FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations, boolean requiresReiteration) {
            answerCache.recordNewAnswers(materialisations);
            if (requiresReiteration) answerCache.setRequiresReiteration();
        }

        @Override
        protected Optional<Map<Identifier.Variable, Concept>> next() {
            return answerCache.next(pointer, true);
        }

        @Override
        public boolean cacheComplete() {
            // TODO: Remove this method in favour of the parent's once Conclusion caching works in recursive settings
            return false;
        }

        public void addExplainAnswers(FunctionalIterator<Partial.Concludable<?>> materialisations) {
            materialisedAnswers.add(materialisations);
        }

        public Optional<Partial.Concludable<?>> nextExplainAnswer() {
            if (hasExplainAnswer()) return Optional.of(materialisedAnswers.get(0).next());
            else return Optional.empty();
        }

        private boolean hasExplainAnswer() {
            while (!materialisedAnswers.isEmpty() && !materialisedAnswers.get(0).hasNext()) {
                materialisedAnswers.remove(0);
            }
            return !materialisedAnswers.isEmpty();
        }
    }
}
