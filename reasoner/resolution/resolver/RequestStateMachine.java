package grakn.core.reasoner.resolution.resolver;

public interface RequestStateMachine {

    State state();
    void receivedIteration(int receivedIteration);
    void proceed();

    interface Retrieval extends RequestStateMachine {
        interface FindAnswer extends State.FindAnswer {
            NextStateFor.FindAnswer nextState();
        }
    }

    interface Exploration extends RequestStateMachine{
        interface FindAnswer extends State.FindAnswer {
            NextStateFor.Exploration.FindAnswer nextState();
        }

        interface SearchDownstream extends State,
                                           NextStateFor.Exploration.FindAnswer {
            Exploration.SearchDownstream nextState();
        }
    }

    interface State {

        NextStateFor nextState();

        interface Waiting extends State,
                                  NextStateFor.Retrieval.FindAnswer,
                                  NextStateFor.Exploration.FindAnswer,
                                  NextStateFor.Exploration.SearchDownstream {
        }

        interface Failed extends State,
                                 NextStateFor.Waiting,
                                 NextStateFor.Retrieval.FindAnswer,
                                 NextStateFor.Exploration.SearchDownstream {}

        interface FindAnswer extends State,
                                     NextStateFor.Waiting,
                                     Failed {
            NextStateFor.FindAnswer nextState();
        }
    }

    interface NextStateFor extends State {
        interface Waiting extends NextStateFor {}

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
