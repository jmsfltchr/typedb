package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.poller.Poller;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.framework.AnswerCache;
import grakn.core.reasoner.resolution.framework.Request;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public abstract class RequestStateMachineImpl implements RequestStateMachine {

    private final Request fromUpstream;
    private final int iteration;
    private final State.Waiting waiting;
    private final State.Failed failed;
//    protected final Consumer<ConceptMap> onSendUpstream; // TODO:
    private State state;

    RequestStateMachineImpl(Request fromUpstream, int iteration) {
        this.fromUpstream = fromUpstream;
        this.iteration = iteration;
        this.waiting = new WaitingImpl();
        this.failed = new FailedImpl();
        this.state = this.waiting; // Initial state is Waiting
    }

    public Request fromUpstream() { return fromUpstream; }

    public int iteration() { return iteration; }

    public State state() { return state; }

    public State receiveRequest(int requestIteration) {
        assert state == waiting;
        progressStates(waiting.receiveRequest(requestIteration));
        return state;
    }

    private void progressStates(State s) {
        do {
            state = ((State.AutoState) s).nextState(); // TODO: Remove cast
        } while (state instanceof State.AutoState);
    }

    // State getters
    private State.Waiting waiting() { return waiting; }

    protected abstract State.AutoState.FindAnswer findAnswer();

    private State.Failed failed() { return failed; }

    // States
    public class FailedImpl implements State.Failed {}

    public class WaitingImpl implements State.Waiting {
        public NextStateFor.Waiting.Request receiveRequest(int requestIter) {
            if (iteration < requestIter) return failed();
            return findAnswer();
        }

        @Override
        public NextStateFor.Waiting.Answer receiveAnswer(int requestIter) {
            throw new NotImplementedException(); // TODO
        }

        @Override
        public NextStateFor.Waiting.Fail receiveFail(int requestIter) {
            throw new NotImplementedException(); // TODO
        }
    }

    public class RetrievalRequestStateMachineImpl extends RequestStateMachineImpl implements RequestStateMachine.Retrieval {

        private final FindAnswer findAnswer;
        private final AnswerCache<ConceptMap> answerCache;
        private final Poller<ConceptMap> answerPoller;

        RetrievalRequestStateMachineImpl(Request fromUpstream, int iteration, AnswerCache<ConceptMap> answerCache) {
            super(fromUpstream, iteration);
            this.answerCache = answerCache;
            this.answerPoller = this.answerCache.reader(true); // TODO: Check the usage of this flag
            this.findAnswer = new RetrievalFindAnswerImpl();
        }

        @Override
        public State.AutoState.FindAnswer findAnswer() { return findAnswer; }

        public class RetrievalFindAnswerImpl implements FindAnswer {

            @Override
            public NextStateFor.Retrieval.FindAnswer nextState() { // TODO Add generic answer type
                Optional<ConceptMap> answer = answerPoller.poll();
                if (answer.isPresent()) {
                    // sendRequestUpstream() // TODO make callback to Resolver to send answer upstream
                    return waiting();
                }
                return failed();
            }
        }
    }

    public class ExplorationRequestStateMachineImpl extends RequestStateMachineImpl implements RequestStateMachine.Exploration {

        private final Exploration.FindAnswer findAnswer;
        private final SearchDownstreamImpl searchDownstream;
        private final AnswerCache<ConceptMap> answerCache;
        private final Poller<ConceptMap> answerPoller;
        private final Set<Request> downstreams;

        ExplorationRequestStateMachineImpl(Request fromUpstream, int iteration, AnswerCache<ConceptMap> answerCache, Set<Request> downstreams) {
            super(fromUpstream, iteration);
            this.answerCache = answerCache;
            this.answerPoller = this.answerCache.reader(false); // TODO: Check the usage of this flag
            this.downstreams = downstreams;
            this.findAnswer = new ExplorationFindAnswerImpl();
            this.searchDownstream = new SearchDownstreamImpl();
        }

        @Override
        protected Exploration.FindAnswer findAnswer() { return findAnswer; }

        private SearchDownstream toSearchDownstreamState() { return searchDownstream; }

        private class ExplorationFindAnswerImpl implements FindAnswer {

            @Override
            public NextStateFor.Exploration.FindAnswer nextState() { // TODO Add generic answer type
                Optional<ConceptMap> answer = answerPoller.poll();
                if (answer.isPresent()) {
                    // callbackSendRequestUpstream() // TODO make callback to Resolver to send answer upstream
                    return waiting();
                }
                return toSearchDownstreamState();
            }
        }

        private class SearchDownstreamImpl implements RequestStateMachine.Exploration.SearchDownstream {

            public Exploration.SearchDownstream nextState() {
                if (downstreams.isEmpty() || answerCache.isComplete())
                    return failed();
                // callbackToDownstream(nextDownstream) // TODO
                return waiting();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        throw new NotImplementedException(); // TODO
    }

    @Override
    public int hashCode() {
        throw new NotImplementedException(); // TODO
    }
}
