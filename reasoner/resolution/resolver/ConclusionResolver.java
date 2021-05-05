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
import grakn.core.reasoner.resolution.framework.AnswerCache;
import grakn.core.reasoner.resolution.framework.AnswerCache.Register;
import grakn.core.reasoner.resolution.framework.AnswerCache.Subsumable;
import grakn.core.reasoner.resolution.framework.AnswerManager;
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
    private final Map<Request, ConclusionAnswerManager> answerManagers;
    private Driver<ConditionResolver> ruleResolver;
    private boolean isInitialised;
    protected final Map<Actor.Driver<? extends Resolver<?>>, Register<IdentifiedConceptsCache, Map<Identifier.Variable, Concept>>> cacheRegisters;

    public ConclusionResolver(Driver<ConclusionResolver> driver, Rule.Conclusion conclusion, ResolverRegistry registry,
                              TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, ConclusionResolver.class.getSimpleName() + "(" + conclusion + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.conclusion = conclusion;
        this.answerManagers = new HashMap<>();
        this.isInitialised = false;
        this.cacheRegisters = new HashMap<>();
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        ConclusionAnswerManager answerManager = getOrReplaceRequestState(fromUpstream, iteration);

        if (iteration < answerManager.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == answerManager.iteration();
            nextAnswer(fromUpstream, answerManager, iteration);
        }
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received Answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ConclusionAnswerManager answerManager = this.answerManagers.get(fromUpstream);

        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {  //TODO: Should we use the upstream or downstream to determine whether to explain?
            FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                    .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);
            FunctionalIterator<Partial.Concludable<?>> materialisedAnswers = materialisations
                    .flatMap(concepts -> fromDownstream.answer().asConclusion().aggregateToUpstream(concepts))
                    .map(Partial.Concludable::asConcludable); // TODO can we get rid of the cast
            answerManager.addExplainAnswers(materialisedAnswers);

            Optional<Partial.Concludable<?>> nextAnswer;
            if ((nextAnswer = answerManager.nextExplainAnswer()).isPresent()) {
                answerToUpstream(nextAnswer.get(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        } else {
            assert cacheRegisters.get(fromUpstream.partialAnswer().root()).isRegistered(fromUpstream.partialAnswer().conceptMap());
            if (!answerManager.answerCache().isComplete()) {
                FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                        .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);
                if (!materialisations.hasNext()) throw GraknException.of(ILLEGAL_STATE);
                answerManager.newMaterialisedAnswers(materialisations, fromDownstream.answer().requiresReiteration());
            }
            nextAnswer(fromUpstream, answerManager, iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ConclusionAnswerManager answerManager = this.answerManagers.get(fromUpstream);

        if (iteration < answerManager.iteration()) {
            // short circuit old iteration fail messages to upstream
            failToUpstream(fromUpstream, iteration);
            return;
        }

        answerManager.downstreamManager().removeDownstream(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, answerManager, iteration);
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        answerManagers.clear();
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

    private void nextAnswer(Request fromUpstream, ConclusionAnswerManager answerManager, int iteration) {
        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            if (answerManager.downstreamManager().hasDownstream()) {
                requestFromDownstream(answerManager.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        } else {
            Optional<Partial.Concludable<?>> upstreamAnswer = answerManager.nextAnswer().map(Partial::asConcludable);
            if (upstreamAnswer.isPresent()) {
                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
            } else if (!answerManager.answerCache().isComplete() && answerManager.downstreamManager().hasDownstream()) {
                requestFromDownstream(answerManager.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                // answerManager.answerCache().setComplete(); // TODO: Reinstate once Conclusion caching works in recursive settings
                failToUpstream(fromUpstream, iteration);
            }
        }
    }

    private ConclusionAnswerManager getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!answerManagers.containsKey(fromUpstream)) {
            answerManagers.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            ConclusionAnswerManager answerManager = this.answerManagers.get(fromUpstream);

            if (answerManager.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                ConclusionAnswerManager answerManagerNextIter = createRequestState(fromUpstream, iteration);
                this.answerManagers.put(fromUpstream, answerManagerNextIter);
            }
        }
        return answerManagers.get(fromUpstream);
    }

    private ConclusionAnswerManager createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        cacheRegisters.putIfAbsent(root, new Register<>(iteration));
        Register<IdentifiedConceptsCache, Map<Identifier.Variable, Concept>> cacheRegister = cacheRegisters.get(root);

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

        IdentifiedConceptsCache answerCache;
        if (cacheRegister.isRegistered(answerFromUpstream)) {
            answerCache = cacheRegister.get(answerFromUpstream);
        } else {
            answerCache = new IdentifiedConceptsCache(cacheRegister, answerFromUpstream);
            cacheRegister.register(answerFromUpstream, answerCache);
        }
        ConclusionAnswerManager answerManager = new ConclusionAnswerManager(fromUpstream, answerCache, iteration, deduplicate);
        assert fromUpstream.partialAnswer().isConclusion();
        Partial.Conclusion<?, ?> partialAnswer = fromUpstream.partialAnswer().asConclusion();
        // we do a extra traversal to expand the partial answer if we already have the concept that is meant to be generated
        // and if there's extra variables to be populated
        if (!answerManager.answerCache().isComplete()) {
            assert conclusion.retrievableIds().containsAll(partialAnswer.conceptMap().concepts().keySet());
            if (conclusion.generating().isPresent() && conclusion.retrievableIds().size() > partialAnswer.conceptMap().concepts().size() &&
                    partialAnswer.conceptMap().concepts().containsKey(conclusion.generating().get().id())) {
                FunctionalIterator<Partial.Compound<?, ?>> completedDownstreamAnswers = candidateAnswers(partialAnswer);
                completedDownstreamAnswers.forEachRemaining(answer -> answerManager.downstreamManager()
                        .addDownstream(Request.create(driver(), ruleResolver, answer)));
            } else {
                Set<Identifier.Variable.Retrievable> named = iterate(conclusion.retrievableIds()).filter(Identifier::isName).toSet();
                Partial.Compound<?, ?> downstreamAnswer = partialAnswer.toDownstream(named);
                answerManager.downstreamManager().addDownstream(Request.create(driver(), ruleResolver, downstreamAnswer));
            }
        }
        return answerManager;
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


    // TODO Needs a better name
    private static class IdentifiedConceptsCache extends Subsumable<Map<Identifier.Variable, Concept>, Map<Identifier.Variable, Concept>> {

        protected IdentifiedConceptsCache(Register<IdentifiedConceptsCache, Map<Identifier.Variable, Concept>> cacheRegister,
                                          ConceptMap state) {
            super(cacheRegister, state);
        }

        @Override
        protected Optional<AnswerCache<?, Map<Identifier.Variable, Concept>>> getCompletedSubsumingCache() {
            for (ConceptMap subsumingCacheKey : subsumingCacheKeys) {
                if (cacheRegister.isRegistered(subsumingCacheKey)) {
                    AnswerCache<?, Map<Identifier.Variable, Concept>> subsumingCache;
                    if ((subsumingCache = cacheRegister.get(subsumingCacheKey)).isComplete()) {
                        // TODO: Gets the first complete cache we find. Getting the smallest could be more efficient.
                        return Optional.of(subsumingCache);
                    }
                }
            }
            return Optional.empty();
        }

        @Override
        protected List<Map<Identifier.Variable, Concept>> answers() {
            return answers;
        }

        @Override
        protected boolean subsumes(Map<Identifier.Variable, Concept> identifiedConcepts, ConceptMap contained) {
            return identifiedConcepts.entrySet().containsAll(contained.concepts().entrySet());
        }
    }

    private static class ConclusionAnswerManager extends AnswerManager.CachingAnswerManager<Map<Identifier.Variable, Concept>, Map<Identifier.Variable, Concept>> {

        private final DownstreamManager downstreamManager;
        private final ProducedRecorder producedRecorder;
        private final List<FunctionalIterator<Partial.Concludable<?>>> materialisedAnswers;
        private final boolean deduplicate;


        public ConclusionAnswerManager(Request fromUpstream, AnswerCache<Map<Identifier.Variable, Concept>, Map<Identifier.Variable, Concept>> answerCache,
                                       int iteration, boolean deduplicate) {
            super(fromUpstream, answerCache, iteration, false);
            this.deduplicate = deduplicate;
            this.materialisedAnswers = new LinkedList<>();
            this.downstreamManager = new DownstreamManager();
            this.producedRecorder = new ProducedRecorder();
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        @Override
        protected FunctionalIterator<? extends Partial<?>> toUpstream(Map<Identifier.Variable, Concept> answer) {
            Partial.Conclusion<?, ?> conclusion = fromUpstream.partialAnswer().asConclusion();
            return  conclusion.aggregateToUpstream(answer).map(p -> {
                if (answerCache.requiresReiteration()) p.setRequiresReiteration();
                return p;
            });
        }

        @Override
        protected boolean optionallyDeduplicate(ConceptMap conceptMap) {
            if (deduplicate) return producedRecorder.record(conceptMap);
            return false;
        }

        public void newMaterialisedAnswers(FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations, boolean requiresReiteration) {
            answerCache.cache(materialisations);
            if (requiresReiteration) answerCache.setRequiresReiteration();
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
