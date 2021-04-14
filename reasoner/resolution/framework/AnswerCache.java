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
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class AnswerCache<ANSWER> {

    private final List<ANSWER> answers;
    private final Set<ANSWER> answersSet;
    private final Set<ConceptMap> subsumingCacheKeys;
    private boolean reiterateOnNewAnswers;
    private boolean requiresReiteration;
    private FunctionalIterator<ANSWER> unexploredAnswers;
    private boolean complete;
    private final Resolver.CacheRegister<ANSWER> cacheRegister;
    private final ConceptMap state;

    protected AnswerCache(Resolver.CacheRegister<ANSWER> cacheRegister, ConceptMap state, boolean useSubsumption) {
        this.cacheRegister = cacheRegister;
        this.state = state;
        this.subsumingCacheKeys = useSubsumption ? getSubsumingCacheKeys(state) : set();
        this.unexploredAnswers = Iterators.empty();
        this.answers = new ArrayList<>(); // TODO: Replace answer list and deduplication set with a bloom filter
        this.answersSet = new HashSet<>();
        this.reiterateOnNewAnswers = false;
        this.requiresReiteration = false;
        this.complete = false;
        this.cacheRegister.register(state, this);
    }

    // TODO: cacheIfAbsent?
    // TODO: Align with the behaviour for adding an iterator to the cache
    public void cache(ANSWER newAnswer) {
        if (!isComplete()) addIfAbsent(newAnswer);
    }

    public void cache(FunctionalIterator<ANSWER> newAnswers) {
        assert !isComplete();
        unexploredAnswers = unexploredAnswers.link(newAnswers);
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

    public FunctionalIterator<ANSWER> iterator(boolean mayCauseReiteration) {
        return iterate(new Iterator(mayCauseReiteration));
    }

    public class Iterator implements java.util.Iterator<ANSWER> {

        private final boolean mayCauseReiteration;
        private ANSWER next;
        private int index;

        public Iterator(boolean mayCauseReiteration) {
            this.mayCauseReiteration = mayCauseReiteration;
            index = 0;
        }

        @Override
        public boolean hasNext() {
            if (next == null) tryFetch();
            return next != null;
        }

        private void tryFetch() {
            Optional<ANSWER> nextAnswer = get(index, mayCauseReiteration);
            nextAnswer.ifPresent(ans -> next = ans);
        }

        @Override
        public ANSWER next() {
            if (!hasNext()) throw new NoSuchElementException();
            index++;
            ANSWER ans = next;
            next = null;
            return ans;
        }

    }

    private Optional<ANSWER> addIfAbsent(ANSWER answer) {
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
        if (complete) return true;
        Optional<AnswerCache<ANSWER>> subsumingCache;
        if ((subsumingCache = getCompletedSubsumingCache()).isPresent()) {
            completeFromSubsumer(subsumingCache.get());
            return true;
        } else {
            return false;
        }
    }

    private Optional<AnswerCache<ANSWER>> getCompletedSubsumingCache() {
        for (ConceptMap subsumingCacheKey : subsumingCacheKeys) {
            if (cacheRegister.isRegistered(subsumingCacheKey)) {
                AnswerCache<ANSWER> subsumingCache;
                if ((subsumingCache = cacheRegister.get(subsumingCacheKey)).isComplete()) {
                    // TODO: Gets the first complete cache we find. Getting the smallest could be more efficient.
                    return Optional.of(subsumingCache);
                }
            }
        }
        return Optional.empty();
    }

    private void completeFromSubsumer(AnswerCache<ANSWER> subsumingCache) {
        setCompletedAnswers(subsumingCache.answers);
        complete = true;
        unexploredAnswers = Iterators.empty();
        if (subsumingCache.requiresReiteration()) setRequiresReiteration();
    }

    private void setCompletedAnswers(List<ANSWER> completeAnswers) {
        List<ANSWER> subsumingAnswers = iterate(completeAnswers).filter(e -> subsumes(e, state)).toList();
        subsumingAnswers.forEach(this::addIfAbsent);
    }

    protected abstract boolean subsumes(ANSWER answer, ConceptMap contained);

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
}