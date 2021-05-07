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

// TODO: Rename to AnswerProvider
public abstract class AnswerManager {

    protected final Request fromUpstream;
    private final int iteration;

    protected AnswerManager(Request fromUpstream, int iteration) {
        this.fromUpstream = fromUpstream;
        this.iteration = iteration;
    }

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

        protected final AnswerCache<ANSWER, SUBSUMES> answerCache;
        protected final boolean mayCauseReiteration;
        protected Poller<? extends AnswerState.Partial<?>> cacheReader;
        protected final ProducedRecorder producedRecorder;

        public CachingAnswerManager(Request fromUpstream, AnswerCache<ANSWER, SUBSUMES> answerCache, int iteration,
                                    boolean mayCauseReiteration, boolean deduplicate) {
            super(fromUpstream, iteration);
            this.answerCache = answerCache;
            this.mayCauseReiteration = mayCauseReiteration;
            // TODO: Needs to account for the user supplying a produced set to initialise
            this.producedRecorder = deduplicate ? new ProducedRecorder() : null;

            if (deduplicate) {
                this.cacheReader = answerCache.reader(mayCauseReiteration).flatMap(
                        answer -> toUpstream(answer).filter(partial ->!producedRecorder.hasRecorded(partial.conceptMap())));
            } else {
                this.cacheReader = answerCache.reader(mayCauseReiteration).flatMap(this::toUpstream);
            }
        }

        public Optional<? extends AnswerState.Partial<?>> nextAnswer() {
            Optional<? extends AnswerState.Partial<?>> ans = cacheReader.poll();
            if (ans.isPresent() && producedRecorder != null) producedRecorder.record(ans.get().conceptMap());
            return ans;
        }

        protected abstract FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ANSWER answer);

        public AnswerCache<ANSWER, SUBSUMES> answerCache() {
            return answerCache;
        }

    }

    // TODO: Rename to "Deduplicator"
    public static class ProducedRecorder {
        private final Set<ConceptMap> produced;

        public ProducedRecorder() {
            this(new HashSet<>());
        }

        public ProducedRecorder(Set<ConceptMap> produced) {
            this.produced = produced;
        }

        public void record(ConceptMap conceptMap) {
            produced.add(conceptMap);
        }

        public boolean hasRecorded(ConceptMap conceptMap) { // TODO method shouldn't be needed
            return produced.contains(conceptMap);
        }

        public Set<ConceptMap> recorded() {
            return produced;
        }
    }
}