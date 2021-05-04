package grakn.core.reasoner.resolution.resolver;

import grakn.core.common.exception.GraknException;
import grakn.core.common.poller.Poller;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.framework.AnswerCache;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public abstract class RequestStateMachineImpl implements RequestStateMachine {

    private final Request fromUpstream;
    private final int iteration;
    protected int receivedIteration;
    private final State.Waiting waiting;
    private final State.Failed failed;
    private final Set<State> terminationStates;
    protected final Consumer<ConceptMap> onSendUpstream; // TODO
    protected final Supplier<Void> onFail; // TODO
    private State state;

    RequestStateMachineImpl(Request fromUpstream, int iteration, Consumer<ConceptMap> onSendUpstream, Supplier<Void> onFail) {
        this.fromUpstream = fromUpstream;
        this.iteration = iteration;
        this.onSendUpstream = onSendUpstream;
        this.onFail = onFail;
        this.waiting = new WaitingImpl();
        this.failed = new FailedImpl();
        this.state = this.waiting; // Initial state is Waiting
        this.terminationStates = set(this.waiting, this.failed);
    }

    public Request fromUpstream() { return fromUpstream; }

    public int iteration() { return iteration; }

    public State state() { return state; }

    public void receivedIteration(int receivedIteration) {
        this.receivedIteration = receivedIteration;
    }

    public void proceed() {
        State s = state;
        do {
            state = s.nextState();
        } while (!terminationStates.contains(state));
    }

    // State getters
    protected State.Waiting waiting() { return waiting; }

    protected abstract State.FindAnswer findAnswer();

    protected State.Failed failed() { return failed; }

    // States
    public class FailedImpl implements State.Failed {

        @Override
        public NextStateFor nextState() {
            // TODO: How to automatically fail to upstream if in the failed state?
            throw GraknException.of(ILLEGAL_STATE);
        }

    }

    public class WaitingImpl implements State.Waiting {

        @Override
        public NextStateFor nextState() {
            if (iteration < receivedIteration) return failed();
            return findAnswer();
        }

    }

    public static class RetrievalRequestStateMachineImpl extends RequestStateMachineImpl implements RequestStateMachine.Retrieval {

        private final FindAnswer findAnswer;
        private final AnswerCache<ConceptMap, ConceptMap> answerCache;
        private final Poller<ConceptMap> answerPoller;

        RetrievalRequestStateMachineImpl(Request fromUpstream, int iteration, AnswerCache<ConceptMap, ConceptMap> answerCache,
                                         Consumer<ConceptMap> onSendUpstream, Supplier<Void> onFail) {
            super(fromUpstream, iteration, onSendUpstream, onFail);
            this.answerCache = answerCache;
            this.answerPoller = this.answerCache.reader(true); // TODO: Check the usage of this flag
            this.findAnswer = new RetrievalFindAnswerImpl();
        }

        @Override
        public State.FindAnswer findAnswer() { return findAnswer; }

        public class RetrievalFindAnswerImpl implements FindAnswer {

            @Override
            public NextStateFor.Retrieval.FindAnswer nextState() { // TODO Add generic answer type
                Optional<ConceptMap> answer = answerPoller.poll();
                if (answer.isPresent()) {
                    onSendUpstream.accept(answer.get());
                    return waiting();
                }
                return failed();
            }

        }
    }

    public static class ExplorationRequestStateMachineImpl extends RequestStateMachineImpl implements RequestStateMachine.Exploration {

        private final Exploration.FindAnswer findAnswer;
        private final RequestStateMachine.Exploration.SearchDownstream searchDownstream;
        private final AnswerCache<ConceptMap, ConceptMap> answerCache;
        private final Consumer<Request> onSearchDownstream;
        private final Resolver.DownstreamManager downstreamManager;
        private final Poller<ConceptMap> answerPoller;

        ExplorationRequestStateMachineImpl(Request fromUpstream, int iteration, AnswerCache<ConceptMap, ConceptMap> answerCache,
                                           Consumer<ConceptMap> onSendUpstream, Supplier<Void> onFail,
                                           Consumer<Request> onSearchDownstream, Resolver.DownstreamManager downstreamManager) {
            super(fromUpstream, iteration, onSendUpstream, onFail);
            this.answerCache = answerCache;
            this.onSearchDownstream = onSearchDownstream;
            this.downstreamManager = downstreamManager;
            this.answerPoller = this.answerCache.reader(false); // TODO: Check the usage of this flag
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
                    onSendUpstream.accept(answer.get());
                    return waiting();
                }
                return toSearchDownstreamState();
            }

        }

        private class SearchDownstreamImpl implements RequestStateMachine.Exploration.SearchDownstream {

            public Exploration.SearchDownstream nextState() {
                if (!downstreamManager.hasDownstream() || answerCache.isComplete())
                    return failed();
                onSearchDownstream.accept(downstreamManager.nextDownstream());
                return waiting();
            }

        }
    }

    @Override
    public boolean equals(Object o) {
        throw GraknException.of(UNIMPLEMENTED); // TODO
    }

    @Override
    public int hashCode() {
        throw GraknException.of(UNIMPLEMENTED); // TODO
    }
}
