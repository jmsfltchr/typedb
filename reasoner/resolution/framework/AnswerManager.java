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

import grakn.core.reasoner.resolution.answer.AnswerState;

import java.util.Optional;

public abstract class AnswerManager {

    private final int iteration;

    protected AnswerManager(int iteration) {this.iteration = iteration;}

    public abstract Optional<? extends AnswerState.Partial<?>> nextAnswer();

    public int iteration() { // TODO: Don't use this, move to use it from the new AnswerStateMachine
        return iteration;
    }
}