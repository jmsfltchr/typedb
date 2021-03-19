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

package grakn.core.reasoner.resolution;

import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class GrablTest {

    private static final Path dataDir = Paths.get("/Users/jamesfletcher/programming/grakn/test/integration/reasoner/resolution/data");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().graknDir(Paths.get(System.getProperty("user.dir"))).dataDir(dataDir).logsDir(logDir);
    private static final String database = "grabl";
    private static RocksGrakn grakn;
    private static RocksSession session;
    private static RocksTransaction rocksTransaction;

    @Before
    public void setUp() {
        grakn = RocksGrakn.open(options);
    }

    @After
    public void tearDown() {
        rocksTransaction.close();
        session.close();
        grakn.close();
    }

    @Test
    public void grabl_reasoning_query() {
        session = grakn.session(database, Arguments.Session.Type.DATA);
        rocksTransaction = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().parallel(false).traceInference(true));
        long startTime = System.currentTimeMillis();
        List<ConceptMap> answers = rocksTransaction.query()
                .match(Graql.parseQuery("match\n" +
                                                "$owner isa organisation, has name \"graknlabs\" ;\n" +
                                                "(owner: $owner, repo: $repo) isa repo-owner ;\n" +
                                                "$repo isa repository, has name $name ;\n" +
                                                "$user isa user, has name \"lriuui0x0\" ;\n" +
                                                "(collaborator: $user, repo: $repo) isa repo-collaborator, has permission $permission ;\n" +
                                                "get $name, $permission;").asMatch()).toList();
        long endTime = System.currentTimeMillis();
        System.out.println("Test took " + (endTime - startTime) + " milliseconds");
        assertEquals(162, answers.size());
    }

    @Test
    public void grabl_reasoning_query_simplified() {
        session = grakn.session(database, Arguments.Session.Type.DATA);
        rocksTransaction = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().parallel(false).traceInference(true));
        long startTime = System.currentTimeMillis();
        List<ConceptMap> answers = rocksTransaction.query()
                .match(Graql.parseQuery("match\n" +
                                                "$owner isa organisation, has name \"graknlabs\" ;\n" +
                                                "(owner: $owner, repo: $repo) isa repo-owner ;\n" +
                                                "$repo isa repository, has name $name ;\n" +
                                                "$user isa user, has name \"lriuui0x0\" ;\n" +
                                                "(collaborator: $user, repo: $repo) isa repo-collaborator;\n" +
                                                "get $name, $repo;").asMatch(),
                       new Context.Query(rocksTransaction.context(), new Options.Query().parallel(false))).toList();
        long endTime = System.currentTimeMillis();
        System.out.println("Test took " + (endTime - startTime) + " milliseconds");
        assertEquals(81, answers.size());
        // Message count: 24800 Test took 5523 milliseconds
        // Message count: 3800 Test took 3226 milliseconds
    }
}
