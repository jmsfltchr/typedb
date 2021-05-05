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
import grakn.core.common.iterator.Iterators;
import grakn.core.common.poller.AbstractPoller;
import grakn.core.common.poller.Poller;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Resolver.CacheRegister;
import grakn.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class AnswerCache<ANSWER, SUBSUMES> {

    protected final List<ANSWER> answers;
    private final Set<ANSWER> answersSet;
    private boolean reiterateOnNewAnswers;
    private boolean requiresReiteration;
    protected FunctionalIterator<ANSWER> unexploredAnswers;
    protected boolean complete;
    protected final CacheRegister<? extends AnswerCache<?, SUBSUMES>, SUBSUMES> cacheRegister;
    protected final ConceptMap state;

    protected AnswerCache(CacheRegister<? extends AnswerCache<?, SUBSUMES>, SUBSUMES> cacheRegister, ConceptMap state) {
        this.cacheRegister = cacheRegister;
        this.state = state;
        this.unexploredAnswers = Iterators.empty();
        this.answers = new ArrayList<>(); // TODO: Replace answer list and deduplication set with a bloom filter
        this.answersSet = new HashSet<>();
        this.reiterateOnNewAnswers = false;
        this.requiresReiteration = false;
        this.complete = false;
    }

    public ConceptMapCache asConceptMapCache() {
        throw GraknException.of(ILLEGAL_CAST);
    }

    // TODO: cacheIfAbsent?
    // TODO: Align with the behaviour for adding an iterator to the cache
    public void cache(ANSWER newAnswer) {
        addIfAbsent(newAnswer);
    }

    public void cache(FunctionalIterator<ANSWER> newAnswers) {
        // assert !isComplete(); // TODO: Removed to allow additional answers to propagate upstream, which crucially may be carrying requiresReiteration flags
        unexploredAnswers = unexploredAnswers.link(newAnswers);
    }

    public Poller<ANSWER> reader(boolean mayCauseReiteration) {
        return new Reader(mayCauseReiteration);
    }

    public class Reader extends AbstractPoller<ANSWER> {

        private final boolean mayCauseReiteration;
        private int index;

        public Reader(boolean mayCauseReiteration) {
            this.mayCauseReiteration = mayCauseReiteration;
            index = 0;
        }

        @Override
        public Optional<ANSWER> poll() {
            Optional<ANSWER> nextAnswer = get(index, mayCauseReiteration);
            if (nextAnswer.isPresent()) index++;
            return nextAnswer;
        }

    }

    // TODO: A method called next shouldn't take an index
    // TODO: mayCauseReiteration only makes sense in the caller
    private Optional<ANSWER> get(int index, boolean mayCauseReiteration) {
        assert index >= 0;
        if (index < answers.size()) {
            return Optional.of(answers.get(index));
        } else if (index == answers.size()) {
            while (unexploredAnswers.hasNext()) {
                Optional<ANSWER> nextAnswer = addIfAbsent(unexploredAnswers.next());
                if (nextAnswer.isPresent()) return nextAnswer;
            }
            if (mayCauseReiteration) reiterateOnNewAnswers = true;
            return Optional.empty();
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
    }


    protected Optional<ANSWER> addIfAbsent(ANSWER answer) {
        if (answersSet.contains(answer)) return Optional.empty();
        answers.add(answer);
        answersSet.add(answer);
        if (reiterateOnNewAnswers) this.requiresReiteration = true;
        return Optional.of(answer);
    }

    public void setRequiresReiteration() {
        this.requiresReiteration = true;
    }

    public void setComplete() {
        assert !unexploredAnswers.hasNext();
        complete = true;
    }

    public boolean isComplete() {
        return complete;
    }

    protected abstract List<SUBSUMES> answers();

    public boolean requiresReiteration() {
        return requiresReiteration;
    }

    private static Set<ConceptMap> getSubsumingCacheKeys(ConceptMap fromUpstream) {
        Set<ConceptMap> subsumingCacheKeys = new HashSet<>();
        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>(fromUpstream.concepts()); // TODO: Copying HashMap just to satisfy generics
        powerSet(concepts.entrySet()).forEach(powerSet -> subsumingCacheKeys.add(toConceptMap(powerSet)));
        subsumingCacheKeys.remove(fromUpstream);
        return subsumingCacheKeys;
    }

    private static <T> Set<Set<T>> powerSet(Set<T> set) {
        Set<Set<T>> powerSet = new HashSet<>();
        powerSet.add(set);
        set.forEach(el -> {
            Set<T> s = new HashSet<>(set);
            s.remove(el);
            powerSet.addAll(powerSet(s));
        });
        return powerSet;
    }

    private static ConceptMap toConceptMap(Set<Map.Entry<Identifier.Variable.Retrievable, Concept>> conceptsEntrySet) {
        HashMap<Identifier.Variable.Retrievable, Concept> map = new HashMap<>();
        conceptsEntrySet.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        return new ConceptMap(map);
    }

    public static class ConcludableExplanationCache extends AnswerCache<Partial.Concludable<?>, ConceptMap> {

        public ConcludableExplanationCache(CacheRegister<? extends AnswerCache<?, ConceptMap>, ConceptMap> cacheRegister, ConceptMap state) {
            super(cacheRegister, state);
        }

        @Override
        protected List<ConceptMap> answers() {
            return iterate(answers).map(AnswerState::conceptMap).distinct().toList();
        }

    }

    public static abstract class Subsumable<ANSWER, SUBSUMES> extends AnswerCache<ANSWER, SUBSUMES> {

        protected final Set<ConceptMap> subsumingCacheKeys;

        protected Subsumable(CacheRegister<? extends AnswerCache<?, SUBSUMES>, SUBSUMES> cacheRegister, ConceptMap state) {
            super(cacheRegister, state);
            this.subsumingCacheKeys = getSubsumingCacheKeys(state);
        }

        public boolean isComplete() {
            if (super.isComplete()) return true;
            Optional<AnswerCache<?, ANSWER>> subsumingCache;
            if ((subsumingCache = getCompletedSubsumingCache()).isPresent()) {
                completeFromSubsumer(subsumingCache.get());
                return true;
            } else {
                return false;
            }
        }

        protected abstract Optional<AnswerCache<?, ANSWER>> getCompletedSubsumingCache();

        private void completeFromSubsumer(AnswerCache<?, ANSWER> subsumingCache) {
            setCompletedAnswers(subsumingCache.answers());
            complete = true;
            unexploredAnswers = Iterators.empty();
            if (subsumingCache.requiresReiteration()) setRequiresReiteration();
        }

        private void setCompletedAnswers(List<ANSWER> completeAnswers) {
            List<ANSWER> subsumingAnswers = iterate(completeAnswers).filter(e -> subsumes(e, state)).toList();
            subsumingAnswers.forEach(this::addIfAbsent);
        }

        protected abstract boolean subsumes(ANSWER answer, ConceptMap contained);
    }

    public static class ConceptMapCache extends Subsumable<ConceptMap, ConceptMap> {

        public ConceptMapCache(CacheRegister<? extends AnswerCache<?, ConceptMap>, ConceptMap> cacheRegister, ConceptMap state) {
            super(cacheRegister, state);
        }

        @Override
        protected Optional<AnswerCache<?, ConceptMap>> getCompletedSubsumingCache() {
            for (ConceptMap subsumingCacheKey : subsumingCacheKeys) {
                if (cacheRegister.isRegistered(subsumingCacheKey)) {
                    AnswerCache<?, ConceptMap> subsumingCache;
                    if ((subsumingCache = cacheRegister.get(subsumingCacheKey)).isComplete()) {
                        // TODO: Gets the first complete cache we find. Getting the smallest could be more efficient.
                        return Optional.of(subsumingCache);
                    }
                }
            }
            return Optional.empty();
        }

        @Override
        public ConceptMapCache asConceptMapCache() {
            return this;
        }

        @Override
        protected List<ConceptMap> answers() {
            return answers;
        }

        @Override
        protected boolean subsumes(ConceptMap conceptMap, ConceptMap contained) {
            return conceptMap.concepts().entrySet().containsAll(contained.concepts().entrySet());
        }
    }
}