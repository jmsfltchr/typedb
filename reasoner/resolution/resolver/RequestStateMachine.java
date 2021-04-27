package grakn.core.reasoner.resolution.resolver;

public interface RequestStateMachine {

    State state();

    interface Retrieval extends RequestStateMachine {
        interface FindAnswer extends State.AutoState.FindAnswer {
            NextStateFor.FindAnswer nextState();
        }
    }

    interface Exploration extends RequestStateMachine{
        interface FindAnswer extends State.AutoState.FindAnswer {
            NextStateFor.Exploration.FindAnswer nextState();
        }

        interface SearchDownstream extends State.AutoState,
                                           NextStateFor.Exploration.FindAnswer {
            Exploration.SearchDownstream nextState();
        }
    }

    interface State {

        interface Waiting extends State,
                                  NextStateFor.Retrieval.FindAnswer,
                                  NextStateFor.Exploration.FindAnswer,
                                  NextStateFor.Exploration.SearchDownstream {
            NextStateFor.Waiting.Request receiveRequest(int requestIter);
            NextStateFor.Waiting.Answer receiveAnswer(int requestIter);
            NextStateFor.Waiting.Fail receiveFail(int requestIter);
        }

        interface Failed extends State,
                                 NextStateFor.Waiting,
                                 NextStateFor.Retrieval.FindAnswer,
                                 NextStateFor.Waiting.Request,
                                 NextStateFor.Exploration.SearchDownstream {}

        interface AutoState extends State {
            NextStateFor nextState();

            interface FindAnswer extends AutoState,
                                         NextStateFor.Waiting.Request,
                                         NextStateFor.Waiting.Failed {
                NextStateFor.FindAnswer nextState();
            }
        }
    }

    interface NextStateFor extends State {
        interface Waiting extends NextStateFor {
            interface Request extends NextStateFor {}
            interface Answer extends NextStateFor {}
            interface Fail extends NextStateFor {}
        }

        interface FindAnswer extends NextStateFor {}

        interface Retrieval {
            interface FindAnswer extends NextStateFor.FindAnswer {}
        }

        interface Exploration {
            interface FindAnswer extends NextStateFor.FindAnswer {}
            interface SearchDownstream extends NextStateFor {}
        }
    }
}
