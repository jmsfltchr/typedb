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
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.Rule;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ConclusionResolver extends Resolver<ConclusionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConclusionResolver.class);

    private final Rule.Conclusion conclusion;
    private final Map<Request, ConclusionAnswerManager<?>> answerManagers;
    private Driver<ConditionResolver> ruleResolver;
    private boolean isInitialised;

    public ConclusionResolver(Driver<ConclusionResolver> driver, Rule.Conclusion conclusion, ResolverRegistry registry,
                              TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, ConclusionResolver.class.getSimpleName() + "(" + conclusion + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.conclusion = conclusion;
        this.answerManagers = new HashMap<>();
        this.isInitialised = false;
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        ConclusionAnswerManager<?> answerManager = getOrReplaceRequestState(fromUpstream, iteration);

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
        ConclusionAnswerManager<? extends Partial.Concludable<?>> answerManager = this.answerManagers.get(fromUpstream);
        FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);

        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            answerManager.newMaterialisedAnswers(materialisations, fromDownstream.answer().requiresReiteration());
            Optional<? extends Partial.Concludable<?>> nextAnswer;
            if ((nextAnswer = answerManager.nextAnswer()).isPresent()) {
                answerToUpstream(nextAnswer.get(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        } else {
            if (!answerManager.isComplete()) {
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
        ConclusionAnswerManager<?> answerManager = this.answerManagers.get(fromUpstream);

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

    private void nextAnswer(Request fromUpstream, ConclusionAnswerManager<?> answerManager, int iteration) {
        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            if (answerManager.downstreamManager().hasDownstream()) {
                requestFromDownstream(answerManager.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        } else {
            Optional<? extends Partial.Concludable<?>> upstreamAnswer = answerManager.nextAnswer();
            if (upstreamAnswer.isPresent()) {
                answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
            } else if (!answerManager.isComplete() && answerManager.downstreamManager().hasDownstream()) {
                requestFromDownstream(answerManager.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                answerManager.setComplete();
                failToUpstream(fromUpstream, iteration);
            }
        }
    }

    private ConclusionAnswerManager<?> getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!answerManagers.containsKey(fromUpstream)) {
            answerManagers.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            ConclusionAnswerManager<?> answerManager = this.answerManagers.get(fromUpstream);

            if (answerManager.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                ConclusionAnswerManager<?> answerManagerNextIter = createRequestState(fromUpstream, iteration);
                this.answerManagers.put(fromUpstream, answerManagerNextIter);
            }
        }
        return answerManagers.get(fromUpstream);
    }

    private ConclusionAnswerManager<?> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);

        ConclusionAnswerManager<?> answerManager;
        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            answerManager = new ConclusionAnswerManager.Explaining(fromUpstream, iteration);
        } else {
            answerManager = new ConclusionAnswerManager.Rule(fromUpstream, iteration);
        }

        assert fromUpstream.partialAnswer().isConclusion();
        Partial.Conclusion<?, ?> partialAnswer = fromUpstream.partialAnswer().asConclusion();
        // we do a extra traversal to expand the partial answer if we already have the concept that is meant to be generated
        // and if there's extra variables to be populated
        if (!answerManager.isComplete()) {
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

    private static abstract class ConclusionAnswerManager<CONCLUDABLE extends Partial.Concludable<?>> extends AnswerManager {

        private final DownstreamManager downstreamManager;
        protected FunctionalIterator<CONCLUDABLE> answerIterator;
        private boolean complete;

        public ConclusionAnswerManager(Request fromUpstream, int iteration) {
            super(fromUpstream, iteration);
            this.downstreamManager = new DownstreamManager();
            this.answerIterator = Iterators.empty();
            this.complete = false;
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
        }

        @Override
        public Optional<CONCLUDABLE> nextAnswer() {
            if (!answerIterator.hasNext()) return Optional.empty();
            return Optional.of(answerIterator.next());
        }

        public boolean isComplete() {
            // TODO:  Placeholder for re-introducing caching
            return complete;
        }

        public void setComplete() {
            // TODO: Placeholder for re-introducing caching
            //  Should only be used once Conclusion caching works in recursive settings
            complete = true;
        }

        protected abstract FunctionalIterator<CONCLUDABLE> toUpstream(Map<Identifier.Variable, Concept> answer);

        public abstract void newMaterialisedAnswers(FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations, boolean requiresReiteration);

        private static class Rule extends ConclusionAnswerManager<Partial.Concludable.Match<?>> {

            private final ProducedRecorder producedRecorder;

            public Rule(Request fromUpstream, int iteration) {
                super(fromUpstream, iteration);
                this.producedRecorder = new ProducedRecorder();
            }

            @Override
            protected FunctionalIterator<Partial.Concludable.Match<?>> toUpstream(Map<Identifier.Variable, Concept> answer) {
                return fromUpstream.partialAnswer().asConclusion().asMatch().aggregateToUpstream(answer);
            }

            @Override
            public void newMaterialisedAnswers(FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations, boolean requiresReiteration) {
                this.answerIterator = this.answerIterator
                        .link(materialisations.flatMap(this::toUpstream))
                        .filter(ans -> !producedRecorder.hasRecorded(ans.conceptMap()))
                        .map(ans -> {
                            if (requiresReiteration) ans.setRequiresReiteration();
                            producedRecorder.record(ans.conceptMap());
                            return ans;
                        });
            }
        }

        private static class Explaining extends ConclusionAnswerManager<Partial.Concludable.Explain> {

            public Explaining(Request fromUpstream, int iteration) {
                super(fromUpstream, iteration);
            }

            @Override
            protected FunctionalIterator<Partial.Concludable.Explain> toUpstream(Map<Identifier.Variable, Concept> answer) {
                return fromUpstream.partialAnswer().asConclusion().asExplain().aggregateToUpstream(answer);
            }

            @Override
            public void newMaterialisedAnswers(FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations, boolean requiresReiteration) {
                this.answerIterator = this.answerIterator
                        .link(materialisations.flatMap(this::toUpstream))
                        .map(ans -> {
                            if (requiresReiteration) ans.setRequiresReiteration();
                            return ans;
                        });
            }
        }
    }

}
