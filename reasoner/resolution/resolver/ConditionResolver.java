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

import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

// note: in the future, we may introduce query rewriting here
public class ConditionResolver extends ConjunctionResolver<ConditionResolver> {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionResolver.class);

    private final Rule.Condition condition;

    public ConditionResolver(Driver<ConditionResolver> driver, Rule.Condition condition, ResolverRegistry registry) {
        super(driver, ConditionResolver.class.getSimpleName() + "(" + condition + ")", registry);
        this.condition = condition;
    }

    @Override
    public com.vaticle.typedb.core.pattern.Conjunction conjunction() {
        return condition.rule().when();
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return condition.concludablesTriggeringRules(registry.conceptManager(), registry.logicManager());
    }

    @Override
    protected void nextAnswer(Request fromUpstream, RequestState requestState, int iteration) {
        if (requestState.downstreamManager().hasDownstream()) {
            requestFromDownstream(requestState.downstreamManager().nextDownstream(), fromUpstream, iteration);
        } else {
            failToUpstream(fromUpstream, iteration);
        }
    }

    @Override
    protected Optional<AnswerState> toUpstreamAnswer(Partial.Compound<?, ?> partialAnswer) {
        assert partialAnswer.isCondition();
        return Optional.of(partialAnswer.asCondition().toUpstream());
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
        return new RequestState(iteration);
    }

    @Override
    public String toString() {
        return name();
    }

}
