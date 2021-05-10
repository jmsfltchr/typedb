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
import grakn.core.reasoner.resolution.framework.RequestState;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ConclusionResolver extends Resolver<ConclusionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConclusionResolver.class);

    private final Rule.Conclusion conclusion;
    private final Map<Request, ConclusionRequestState<?>> requestStates;
    private Driver<ConditionResolver> ruleResolver;
    private boolean isInitialised;

    public ConclusionResolver(Driver<ConclusionResolver> driver, Rule.Conclusion conclusion, ResolverRegistry registry,
                              TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, ConclusionResolver.class.getSimpleName() + "(" + conclusion + ")",
              registry, traversalEngine, conceptMgr, resolutionTracing);
        this.conclusion = conclusion;
        this.requestStates = new HashMap<>();
        this.isInitialised = false;
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        ConclusionRequestState<?> requestState = getOrReplaceRequestState(fromUpstream, iteration);

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
        ConclusionRequestState<? extends Partial.Concludable<?>> requestState = this.requestStates.get(fromUpstream);
        FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations = conclusion
                .materialise(fromDownstream.answer().conceptMap(), traversalEngine, conceptMgr);

        if (!requestState.isComplete()) {
            if (!materialisations.hasNext()) throw GraknException.of(ILLEGAL_STATE);
            requestState.newMaterialisedAnswers(fromDownstream, materialisations,
                                                 fromDownstream.answer().requiresReiteration());
        }
        nextAnswer(fromUpstream, requestState, iteration);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Fail: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ConclusionRequestState<?> requestState = this.requestStates.get(fromUpstream);

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

    private void nextAnswer(Request fromUpstream, ConclusionRequestState<?> requestState, int iteration) {
        Optional<? extends Partial<?>> upstreamAnswer = requestState.nextAnswer();
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream, iteration);
        } else if (!requestState.isComplete() && requestState.downstreamManager().hasDownstream()) {
            requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
        } else {
            requestState.setComplete();
            failToUpstream(fromUpstream, iteration);
        }
    }

    private ConclusionRequestState<?> getOrReplaceRequestState(Request fromUpstream, int iteration) {
        if (!requestStates.containsKey(fromUpstream)) {
            requestStates.put(fromUpstream, createRequestState(fromUpstream, iteration));
        } else {
            ConclusionRequestState<?> requestState = this.requestStates.get(fromUpstream);

            if (requestState.iteration() < iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                ConclusionRequestState<?> requestStateNextIter = createRequestState(fromUpstream, iteration);
                this.requestStates.put(fromUpstream, requestStateNextIter);
            }
        }
        return requestStates.get(fromUpstream);
    }

    private ConclusionRequestState<?> createRequestState(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new ConclusionResponse for request: {}", name(), fromUpstream);

        ConclusionRequestState<?> requestState;
        if (fromUpstream.partialAnswer().asConclusion().isExplain()) {
            requestState = new ConclusionRequestState.Explaining(fromUpstream, iteration);
        } else {
            requestState = new ConclusionRequestState.Rule(fromUpstream, iteration);
        }

        assert fromUpstream.partialAnswer().isConclusion();
        Partial.Conclusion<?, ?> partialAnswer = fromUpstream.partialAnswer().asConclusion();
        // we do a extra traversal to expand the partial answer if we already have the concept that is meant to be generated
        // and if there's extra variables to be populated
        if (!requestState.isComplete()) {
            assert conclusion.retrievableIds().containsAll(partialAnswer.conceptMap().concepts().keySet());
            if (conclusion.generating().isPresent() && conclusion.retrievableIds().size() > partialAnswer.conceptMap().concepts().size() &&
                    partialAnswer.conceptMap().concepts().containsKey(conclusion.generating().get().id())) {
                FunctionalIterator<Partial.Compound<?, ?>> completedDownstreamAnswers = candidateAnswers(partialAnswer);
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

    private static abstract class ConclusionRequestState<CONCLUDABLE extends Partial.Concludable<?>> extends RequestState {

        private final DownstreamManager downstreamManager;
        protected FunctionalIterator<CONCLUDABLE> answerIterator;
        private boolean complete;

        public ConclusionRequestState(Request fromUpstream, int iteration) {
            super(fromUpstream, iteration);
            this.downstreamManager = new DownstreamManager();
            this.answerIterator = Iterators.empty();
            this.complete = false;
        }

        public DownstreamManager downstreamManager() {
            return downstreamManager;
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

        protected abstract FunctionalIterator<CONCLUDABLE> toUpstream(Response.Answer fromDownstream,
                                                                      Map<Identifier.Variable, Concept> answer);

        public abstract void newMaterialisedAnswers(Response.Answer fromDownstream,
                                                    FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations,
                                                    boolean requiresReiteration);

        private static class Rule extends ConclusionRequestState<Partial.Concludable.Match<?>> {

            private final Set<ConceptMap> deduplicationSet;

            public Rule(Request fromUpstream, int iteration) {
                super(fromUpstream, iteration);
                this.deduplicationSet = new HashSet<>();
            }

            @Override
            protected FunctionalIterator<Partial.Concludable.Match<?>> toUpstream(Response.Answer fromDownstream,
                                                                                  Map<Identifier.Variable, Concept> answer) {
                return fromDownstream.answer().asConclusion().asMatch().aggregateToUpstream(answer);
            }

            @Override
            public void newMaterialisedAnswers(Response.Answer fromDownstream,
                                               FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations,
                                               boolean requiresReiteration) {
                this.answerIterator = this.answerIterator
                        .link(materialisations.flatMap(m -> toUpstream(fromDownstream, m)))
                        .filter(ans -> !deduplicationSet.contains(ans.conceptMap()))
                        .map(ans -> {
                            if (requiresReiteration) ans.setRequiresReiteration();
                            return ans;
                        });
            }

            @Override
            public Optional<Partial.Concludable.Match<?>> nextAnswer() {
                if (!answerIterator.hasNext()) return Optional.empty();
                Partial.Concludable.Match<?> ans = answerIterator.next();
                deduplicationSet.add(ans.conceptMap());
                return Optional.of(ans);
            }

        }

        private static class Explaining extends ConclusionRequestState<Partial.Concludable.Explain> {

            public Explaining(Request fromUpstream, int iteration) {
                super(fromUpstream, iteration);
            }

            @Override
            protected FunctionalIterator<Partial.Concludable.Explain> toUpstream(Response.Answer fromDownstream,
                                                                                 Map<Identifier.Variable, Concept> answer) {
                return fromDownstream.answer().asConclusion().asExplain().aggregateToUpstream(answer);
            }

            public void newMaterialisedAnswers(Response.Answer fromDownstream,
                                               FunctionalIterator<Map<Identifier.Variable, Concept>> materialisations,
                                               boolean requiresReiteration) {
                this.answerIterator = this.answerIterator
                        .link(materialisations.flatMap(m -> toUpstream(fromDownstream, m)))
                        .map(ans -> {
                            if (requiresReiteration) ans.setRequiresReiteration();
                            return ans;
                        });
            }

            @Override
            public Optional<Partial.Concludable.Explain> nextAnswer() {
                if (!answerIterator.hasNext()) return Optional.empty();
                return Optional.of(answerIterator.next());
            }
        }
    }

}
