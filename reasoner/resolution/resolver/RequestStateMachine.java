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
 */

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
