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
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.poller.Poller;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.framework.Resolver.DownstreamManager;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

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

    public abstract static class CachingAnswerManager<ANSWER, SUBSUMES> extends AnswerManager {

        protected final Request fromUpstream;
        protected final AnswerCache<ANSWER, SUBSUMES> answerCache;
        protected final boolean mayCauseReiteration;
        protected Poller<? extends AnswerState.Partial<?>> cacheReader;

        public CachingAnswerManager(Request fromUpstream, AnswerCache<ANSWER, SUBSUMES> answerCache, int iteration, boolean mayCauseReiteration) {
            super(iteration);
            this.fromUpstream = fromUpstream;
            this.answerCache = answerCache;
            this.mayCauseReiteration = mayCauseReiteration;
            this.cacheReader = answerCache.reader(mayCauseReiteration)
                    .flatMap(answer -> toUpstream(answer).filter(partial -> !optionallyDeduplicate(partial.conceptMap())));
        }

        public Optional<? extends AnswerState.Partial<?>> nextAnswer() {
            return cacheReader.poll();
        }

        protected abstract FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ANSWER answer);

        protected abstract boolean optionallyDeduplicate(ConceptMap conceptMap);

        public AnswerCache<ANSWER, SUBSUMES> answerCache() {
            return answerCache;
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
}