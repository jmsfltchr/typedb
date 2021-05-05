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

import grakn.core.common.exception.GraknException;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Resolver.DownstreamManager;

import java.util.Optional;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class AnswerManager {

    private final int iteration;

    protected AnswerManager(int iteration) {this.iteration = iteration;}

    public abstract Optional<? extends AnswerState.Partial<?>> nextAnswer();

    public int iteration() { // TODO: Don't use this, move to use it from the new AnswerStateMachine
        return iteration;
    }

    public boolean isExploration() {
        return false;
    }

    public Exploration asExploration() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Exploration.class));
    }

    public interface Exploration {
        void newAnswer(AnswerState.Partial<?> partial, boolean requiresReiteration);

        DownstreamManager downstreamManager();

        boolean singleAnswerRequired();
    }
}