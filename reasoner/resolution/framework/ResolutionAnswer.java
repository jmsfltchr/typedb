/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.common.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.answer.Aggregator;

import java.util.HashMap;
import java.util.Map;

import static grakn.common.collection.Collections.map;

public class ResolutionAnswer {
    private final Aggregator.Aggregated aggregatedAnswer;
    private final Derivation derivation;
    private final Actor<? extends Resolver<?>> producer;
    private final String patternAnswered;

    public ResolutionAnswer(Aggregator.Aggregated aggregatedAnswer,
                            String patternAnswered,
                            Derivation derivation,
                            Actor<? extends Resolver<?>> producer) {
        this.aggregatedAnswer = aggregatedAnswer;
        this.patternAnswered = patternAnswered;
        this.derivation = derivation;
        this.producer = producer;
    }

    public Aggregator.Aggregated aggregated() {
        return aggregatedAnswer;
    }

    public Derivation derivation() {
        return derivation;
    }

    public boolean isInferred() {
        return !derivation.equals(Derivation.EMPTY);
    }

    public Actor<? extends Resolver<?>> producer() {
        return producer;
    }

    @Override
    public String toString() {
        return "Answer{" +
                "conceptMap=" + aggregatedAnswer +
                ", executionRecord=" + derivation +
                ", patternAnswered='" + patternAnswered + '\'' +
                ", producer=" + producer +
                '}';
    }

    public static class Derivation {
        public static final Derivation EMPTY = new Derivation(map());

        private Map<Actor<? extends Resolver<?>>, ResolutionAnswer> answers;

        public Derivation(Map<Actor<? extends Resolver<?>>, ResolutionAnswer> answers) {
            this.answers = map(answers);
        }

        public Derivation withAnswer(Actor<? extends Resolver<?>> producer, ResolutionAnswer answer) {
            Map<Actor<? extends Resolver<?>>, ResolutionAnswer> copiedResolution = new HashMap<>(answers);
            copiedResolution.put(producer, answer);
            return new Derivation(copiedResolution);
        }

        public void update(Map<Actor<? extends Resolver<?>>, ResolutionAnswer> newResolutions) {
            assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any derivations during an update";
            Map<Actor<? extends Resolver<?>>, ResolutionAnswer> copiedResolutinos = new HashMap<>(answers);
            copiedResolutinos.putAll(newResolutions);
            this.answers = copiedResolutinos;
        }

        public void replace(Map<Actor<? extends Resolver<?>>, ResolutionAnswer> newResolutions) {
            this.answers = map(newResolutions);
        }

        public Map<Actor<? extends Resolver<?>>, ResolutionAnswer> answers() {
            return this.answers;
        }

        @Override
        public String toString() {
            return "Derivation{" + "answers=" + answers + '}';
        }
    }
}
