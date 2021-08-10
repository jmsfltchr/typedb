/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Match.Finished;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public interface RootResolver<TOP extends Top> {

    void submitAnswer(TOP answer);

    void submitFail(int iteration);

    class Conjunction extends ConjunctionResolver<Conjunction> implements RootResolver<Top.Match.Finished> {

        private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

        private final com.vaticle.typedb.core.pattern.Conjunction conjunction;
        private final Consumer<Finished> onAnswer;
        private final Consumer<Integer> onFail;
        private final Consumer<Throwable> onException;

        public Conjunction(Driver<Conjunction> driver, com.vaticle.typedb.core.pattern.Conjunction conjunction,
                           Consumer<Finished> onAnswer, Consumer<Integer> onFail, Consumer<Throwable> onException,
                           ResolverRegistry registry) {
            super(driver, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")", registry);
            this.conjunction = conjunction;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
        }

        @Override
        public com.vaticle.typedb.core.pattern.Conjunction conjunction() {
            return conjunction;
        }

        @Override
        Set<Concludable> concludablesTriggeringRules() {
            return Iterators.iterate(Concludable.create(conjunction))
                    .filter(c -> c.getApplicableRules(registry.conceptManager(), registry.logicManager()).hasNext())
                    .toSet();
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            onException.accept(cause);
        }

        @Override
        public void submitAnswer(Finished answer) {
            LOG.debug("Submitting answer: {}", answer);
            onAnswer.accept(answer);
        }

        @Override
        public void submitFail(int iteration) {
            LOG.debug("Submitting fail in iteration: {}", iteration);
            onFail.accept(iteration);
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
            assert answer.isTop() && answer.asTop().isMatch() && answer.asTop().asMatch().isFinished();
            submitAnswer(answer.asTop().asMatch().asFinished());
        }

        @Override
        protected void failToUpstream(Request fromUpstream, int iteration) {
            submitFail(iteration);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
            if (requestState.downstreamManager().hasDownstream()) {
                requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                submitFail(iteration);
            }
        }

        @Override
        protected Optional<AnswerState> toUpstreamAnswer(Partial.Compound<?, ?> partialAnswer) {
            assert partialAnswer.isRoot() && partialAnswer.isMatch();
            return Optional.of(partialAnswer.asRoot().asMatch().toFinishedTop(conjunction));
        }

        @Override
        boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            RequestState requestState = requestStates.get(fromUpstream);
            if (!requestState.deduplicationSet().contains(upstreamAnswer.conceptMap())) {
                requestState.deduplicationSet().add(upstreamAnswer.conceptMap());
                answerToUpstream(upstreamAnswer, fromUpstream, iteration);
                return true;
            } else {
                return false;
            }
        }

        @Override
        RequestState requestStateNew(int iteration) {
            return new RequestState(iteration);
        }

        @Override
        RequestState requestStateForIteration(RequestState requestStatePrior, int iteration) {
            return new RequestState(iteration, requestStatePrior.deduplicationSet());
        }
    }

    class Disjunction extends DisjunctionResolver<Disjunction> implements RootResolver<Finished> {

        private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);
        private final Consumer<Finished> onAnswer;
        private final Consumer<Integer> onFail;
        private final Consumer<Throwable> onException;

        public Disjunction(Driver<Disjunction> driver, com.vaticle.typedb.core.pattern.Disjunction disjunction,
                           Consumer<Finished> onAnswer, Consumer<Integer> onFail, Consumer<Throwable> onException,
                           ResolverRegistry registry) {
            super(driver, Disjunction.class.getSimpleName() + "(pattern:" + disjunction + ")", disjunction,
                  registry);
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
            this.isInitialised = false;
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            onException.accept(cause);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
            if (requestState.downstreamManager().hasDownstream()) {
                requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                submitFail(iteration);
            }
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
            assert answer.isTop() && answer.asTop().isMatch() && answer.asTop().asMatch().isFinished();
            submitAnswer(answer.asTop().asMatch().asFinished());
        }

        @Override
        protected void failToUpstream(Request fromUpstream, int iteration) {
            submitFail(iteration);
        }

        @Override
        public void submitAnswer(Finished answer) {
            LOG.debug("Submitting answer: {}", answer);
            onAnswer.accept(answer);
        }

        @Override
        public void submitFail(int iteration) {
            onFail.accept(iteration);
        }

        @Override
        protected boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            RequestState requestState = requestStates.get(fromUpstream);
            if (!requestState.deduplicationSet().contains(upstreamAnswer.conceptMap())) {
                requestState.deduplicationSet().add(upstreamAnswer.conceptMap());
                answerToUpstream(upstreamAnswer, fromUpstream, iteration);
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected AnswerState toUpstreamAnswer(Partial.Compound<?, ?> partialAnswer, Response.Answer fromDownstream) {
            assert partialAnswer.isRoot() && partialAnswer.isMatch();
            Driver<? extends Resolver<?>> sender = fromDownstream.sourceRequest().receiver();
            com.vaticle.typedb.core.pattern.Conjunction patternAnswered = downstreamResolvers.get(sender);
            return partialAnswer.asRoot().asMatch().toFinishedTop(patternAnswered);
        }

        @Override
        protected RequestState requestStateForIteration(RequestState requestStatePrior, int newIteration) {
            return new RequestState(newIteration, requestStatePrior.deduplicationSet());
        }
    }

    class Explain extends ConjunctionResolver<Explain> implements RootResolver<Top.Explain.Finished> {

        private static final Logger LOG = LoggerFactory.getLogger(Explain.class);

        private final com.vaticle.typedb.core.pattern.Conjunction conjunction;
        private final Consumer<Top.Explain.Finished> onAnswer;
        private final Consumer<Integer> onFail;
        private final Consumer<Throwable> onException;

        private final Set<Explanation> submittedExplanations;

        public Explain(Driver<Explain> driver, com.vaticle.typedb.core.pattern.Conjunction conjunction,
                       Consumer<Top.Explain.Finished> onAnswer, Consumer<Integer> onFail,
                       Consumer<Throwable> onException, ResolverRegistry registry) {
            super(driver, "Explain(" + conjunction + ")", registry);
            this.conjunction = conjunction;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
            this.submittedExplanations = new HashSet<>();
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            onException.accept(cause);
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
            assert answer.isTop() && answer.asTop().isExplain() && answer.asTop().asExplain().isFinished();
            submitAnswer(answer.asTop().asExplain().asFinished());
        }

        @Override
        protected void failToUpstream(Request fromUpstream, int iteration) {
            submitFail(iteration);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
            if (requestState.downstreamManager().hasDownstream()) {
                requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                submitFail(iteration);
            }
        }

        @Override
        public void submitAnswer(Top.Explain.Finished answer) {
            LOG.debug("Submitting answer: {}", answer);
            onAnswer.accept(answer);
        }

        @Override
        public void submitFail(int iteration) {
            onFail.accept(iteration);
        }

        @Override
        Optional<AnswerState> toUpstreamAnswer(Partial.Compound<?, ?> partialAnswer) {
            assert partialAnswer.isRoot() && partialAnswer.isExplain();
            return Optional.of(partialAnswer.asRoot().asExplain().toFinishedTop());
        }

        @Override
        boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            assert upstreamAnswer.isTop() && upstreamAnswer.asTop().isExplain() && upstreamAnswer.asTop().asExplain().isFinished();
            Top.Explain.Finished finished = upstreamAnswer.asTop().asExplain().asFinished();
            if (!submittedExplanations.contains(finished.explanation())) {
                submittedExplanations.add(finished.explanation());
                answerToUpstream(upstreamAnswer, fromUpstream, iteration);
                return true;
            } else {
                return false;
            }
        }

        @Override
        RequestState requestStateNew(int iteration) {
            return new RequestState(iteration);
        }

        @Override
        RequestState requestStateForIteration(RequestState requestStatePrior, int iteration) {
            return new RequestState(iteration, requestStatePrior.deduplicationSet());
        }

        @Override
        Set<Concludable> concludablesTriggeringRules() {
            Set<Concludable> concludables = Iterators.iterate(Concludable.create(conjunction))
                    .filter(c -> c.getApplicableRules(registry.conceptManager(), registry.logicManager()).hasNext())
                    .toSet();
            assert concludables.size() == 1;
            return concludables;
        }

        @Override
        com.vaticle.typedb.core.pattern.Conjunction conjunction() {
            return conjunction;
        }
    }

}
