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
 *
 */

package com.vaticle.typedb.core.reasoner.resolution.framework;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.poller.AbstractPoller;
import com.vaticle.typedb.core.common.poller.Poller;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.empty;

public class AnswerCache<ANSWER> {

    private final List<ANSWER> answers;
    private final Set<ANSWER> answersSet;
    private final Supplier<FunctionalIterator<ANSWER>> answerSourceSupplier;
    private boolean reiterateOnAnswerAdded;
    private boolean requiresReiteration;
    private FunctionalIterator<ANSWER> answerSource;
    private boolean complete;
    private boolean sourceCleared;
    private boolean sourceExhausted;

    public AnswerCache(Supplier<FunctionalIterator<ANSWER>> answerSourceSupplier) {
        this.answerSourceSupplier = answerSourceSupplier;
        this.answerSource = null;
        this.answers = new ArrayList<>(); // TODO: Replace answer list and deduplication set with a bloom filter
        this.answersSet = new HashSet<>();
        this.reiterateOnAnswerAdded = false;
        this.requiresReiteration = false;
        this.complete = false;
        this.sourceCleared = false;
        this.sourceExhausted = false;
    }

    public void add(ANSWER answer) {
        assert !isComplete();
        if (addIfAbsent(answer) && reiterateOnAnswerAdded) requiresReiteration = true;
    }

    public void clearSource() {
        if (answerSource != null) answerSource.recycle();
        answerSource = empty();
        sourceCleared = true;
    }

    public Poller<ANSWER> reader(boolean isSubscriber) {
        return new Reader(isSubscriber);
    }

    public void setComplete() {
        complete = true;
        setSourceExhausted();
    }

    public boolean isComplete() {
        return complete;
    }

    public void setSourceExhausted() {
        sourceExhausted = true;
        if (answerSource != null) answerSource.recycle();
    }

    public boolean sourceExhausted() {
        return sourceExhausted;
    }

    public boolean requiresReiteration() {
        return requiresReiteration;
    }

    private boolean addIfAbsent(ANSWER answer) {
        if (answersSet.contains(answer)) return false;
        answers.add(answer);
        answersSet.add(answer);
        return true;
    }

    private class Reader extends AbstractPoller<ANSWER> {

        private final boolean mayReadOverEagerly;
        private int index;

        private Reader(boolean mayReadOverEagerly) {
            this.mayReadOverEagerly = mayReadOverEagerly;
            index = 0;
        }

        @Override
        public Optional<ANSWER> poll() {
            Optional<ANSWER> nextAnswer = get(index, mayReadOverEagerly);
            if (nextAnswer.isPresent()) index++;
            return nextAnswer;
        }

        private Optional<ANSWER> get(int index, boolean isSubscriber) {
            assert index >= 0;
            if (index < answers.size()) {
                return Optional.of(answers.get(index));
            } else if (index == answers.size()) {
                if (isComplete()) return Optional.empty();
                Optional<ANSWER> nextAnswer = searchSourceForAnswer();
                if (nextAnswer.isEmpty() && isSubscriber) reiterateOnAnswerAdded = true;
                return nextAnswer;
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        private Optional<ANSWER> searchSourceForAnswer() {
            if (answerSource == null) answerSource = answerSourceSupplier.get();
            while (answerSource.hasNext()) {
                ANSWER answer = answerSource.next();
                if (addIfAbsent(answer)) {
                    if (reiterateOnAnswerAdded) requiresReiteration = true;
                    return Optional.of(answer);
                }
            }
            if (!sourceCleared) setSourceExhausted();
            return Optional.empty();
        }

        @Override
        public void recycle() {}

    }

}
