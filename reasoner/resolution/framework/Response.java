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
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;

import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public interface Response {
    Request sourceRequest();

    boolean isAnswer();

    boolean isFail();

    default Answer asAnswer() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Answer.class));
    }

    default Fail asFail() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Fail.class));
    }

    default Actor.Driver<? extends Resolver<?>> receiver() {
        return sourceRequest().sender();
    }

    default Actor.Driver<? extends Resolver<?>> sender() {
        return sourceRequest().receiver();
    }

    default ResolutionTracer.TraceId traceId() {
        return sourceRequest().traceId();
    }

    class Answer implements Response {
        private final Request sourceRequest;
        private final Partial<?> answer;

        private Answer(Request sourceRequest, Partial<?> answer) {
            this.sourceRequest = sourceRequest;
            this.answer = answer;
        }

        public static Answer create(Request sourceRequest, Partial<?> answer) {
            return new Answer(sourceRequest, answer);
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        public Partial<?> answer() {
            return answer;
        }

        public int planIndex() {
            return sourceRequest.planIndex();
        }

        @Override
        public boolean isAnswer() {
            return true;
        }

        @Override
        public boolean isFail() {
            return false;
        }

        @Override
        public Answer asAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return "\nAnswer{" +
                    "\nsourceRequest=" + sourceRequest +
                    ",\nanswer=" + answer +
                    "\n}\n";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Answer that = (Answer) o;
            return Objects.equals(sourceRequest, that.sourceRequest) &&
                    Objects.equals(answer, that.answer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceRequest, answer);
        }
    }

    class Fail implements Response {
        private final Request sourceRequest;

        public Fail(Request sourceRequest) {
            this.sourceRequest = sourceRequest;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        @Override
        public boolean isAnswer() {
            return false;
        }

        @Override
        public boolean isFail() {
            return true;
        }

        @Override
        public Fail asFail() {
            return this;
        }


        @Override
        public String toString() {
            return "Exhausted{" +
                    "sourceRequest=" + sourceRequest +
                    '}';
        }
    }

    class Blocked implements Response {

        private final Request sourceRequest;
        protected Set<Origin> blockers;

        public Blocked(Request sourceRequest, Set<Origin> blockers) {
            this.sourceRequest = sourceRequest;
            this.blockers = blockers;
        }

        private Blocked(Request sourceRequest) {
            this.sourceRequest = sourceRequest;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        @Override
        public boolean isAnswer() {
            return false;
        }

        @Override
        public boolean isFail() {
            return false;
        }

        public Set<Origin> blockers() {
            return blockers;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Blocked blocked = (Blocked) o;
            return sourceRequest.equals(blocked.sourceRequest) &&
                    blockers.equals(blocked.blockers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceRequest, blockers);
        }

        public static class Origin extends Blocked {

            private final int numAnswersSeen;

            public Origin(Request sourceRequest, int numAnswersSeen) {
                super(sourceRequest);
                this.blockers = set();
                this.numAnswersSeen = numAnswersSeen;
            }

            public int numAnswersSeen() {
                return numAnswersSeen;
            }

            @Override
            public Set<Origin> blockers() {
                return set(this);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                if (!super.equals(o)) return false;
                Origin origin = (Origin) o;
                return numAnswersSeen == origin.numAnswersSeen;
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), numAnswersSeen);
            }

            @Override
            public String toString() {
                return "Origin{" +
                        "sourceRequest=" + sourceRequest().toString() +
                        ", numAnswersSeen=" + numAnswersSeen +
                        '}';
            }
        }
    }
}
