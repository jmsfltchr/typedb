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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public abstract class Response {

    private final Request.Visit sourceRequest;
    private final Trace trace;

    private Response(Request.Visit sourceRequest, @Nullable Trace trace) {
        this.sourceRequest = sourceRequest;
        this.trace = trace;
    }

    public Request sourceRequest() {
        return sourceRequest;
    }

    public Trace trace() {
        return trace;
    }

    Actor.Driver<? extends Resolver<?>> receiver() {
        return sourceRequest.sender();
    }

    Actor.Driver<? extends Resolver<?>> sender() {
        return sourceRequest.receiver();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Response response = (Response) o;
        return sourceRequest.equals(response.sourceRequest) &&
                Objects.equals(trace, response.trace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceRequest, trace);
    }

    @Override
    public String toString() {
        return "Response{" +
                "sourceRequest=" + sourceRequest +
                ", trace=" + trace +
                '}';
    }

    public static class Answer extends Response {
        private final Partial<?> answer;
        private final int hash;

        private Answer(Request.Visit sourceRequest, Partial<?> answer, @Nullable Trace trace) {
            super(sourceRequest, trace);
            this.answer = answer;
            this.hash = Objects.hash(super.hashCode(), answer);
        }

        public static Answer create(Request.Visit sourceRequest, Partial<?> answer, @Nullable Trace trace) {
            return new Answer(sourceRequest, answer, trace);
        }

        public Partial<?> answer() {
            return answer;
        }

        public int planIndex() {
            return sourceRequest().visit().planIndex();
        }

        @Override
        public String toString() {
            return "Answer{" +
                    "sourceRequest=" + sourceRequest() +
                    ", answer=" + answer +
                    ", trace=" + trace() +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Answer that = (Answer) o;
            return answer.equals(that.answer);
        }

        @Override
        public int hashCode() {
            return hash;
        }

    }

    public static class Fail extends Response {

        public Fail(Request.Visit sourceRequest, @Nullable Trace trace) {
            super(sourceRequest, trace);
        }

        @Override
        public String toString() {
            return "Fail{" +
                    "sourceRequest=" + sourceRequest() +
                    ", trace=" + trace() +
                    '}';
        }
    }

    public static class Blocked extends Response {

        protected Set<Cycle> cycles;
        private final int hash;

        public Blocked(Request.Visit sourceRequest, Set<Cycle> cycles, @Nullable Trace trace) {
            super(sourceRequest, trace);
            this.cycles = cycles;
            this.hash = Objects.hash(super.hashCode(), cycles);
        }

        public Set<Cycle> cycles() {
            return cycles;
        }

        @Override
        public String toString() {
            return "Blocked{" +
                    "sourceRequest=" + sourceRequest() +
                    ", cycles=" + cycles +
                    ", trace=" + trace() +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Blocked blocked = (Blocked) o;
            return cycles.equals(blocked.cycles);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        public static class Cycle {

            private final Pair<Concludable, ConceptMap> origin;
            private final int answersSeen;

            private Cycle(Concludable concludable, ConceptMap conceptMap, int answersSeen) {
                this.origin = new Pair<>(concludable, conceptMap);
                this.answersSeen = answersSeen;
            }

            public static Cycle create(Concludable concludable, ConceptMap conceptMap, int answersSeen) {
                return new Cycle(concludable, conceptMap, answersSeen);
            }

            public Pair<Concludable, ConceptMap> origin() {
                return origin;
            }

            public int answersSeen() {
                return answersSeen;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Cycle cycle = (Cycle) o;
                return answersSeen == cycle.answersSeen &&
                        origin.equals(cycle.origin);
            }

            @Override
            public int hashCode() {
                return Objects.hash(origin, answersSeen);
            }

            @Override
            public String toString() {
                return "Cycle{" +
                        "origin=" + origin +
                        ", numAnswersSeen=" + answersSeen +
                        '}';
            }
        }
    }
}
