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

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Compound;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static grakn.core.common.iterator.Iterators.iterate;

public abstract class DisjunctionResolver<RESOLVER extends DisjunctionResolver<RESOLVER>> extends CompoundResolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);

    final Map<Driver<ConjunctionResolver.Nested>, grakn.core.pattern.Conjunction> downstreamResolvers;
    final grakn.core.pattern.Disjunction disjunction;

    public DisjunctionResolver(Driver<RESOLVER> driver, String name, grakn.core.pattern.Disjunction disjunction,
                               ResolverRegistry registry, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                               boolean resolutionTracing) {
        super(driver, name, registry, traversalEngine, conceptMgr, resolutionTracing);
        this.disjunction = disjunction;
        this.downstreamResolvers = new HashMap<>();
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        LOG.trace("{}: received answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        AnswerManager answerManager = answerManagers.get(fromUpstream);

        assert fromDownstream.answer().isCompound();
        AnswerState answer = toUpstreamAnswer(fromDownstream.answer().asCompound(), fromDownstream);
        boolean acceptedAnswer = tryAcceptUpstreamAnswer(answer, fromUpstream, iteration);
        if (!acceptedAnswer) nextAnswer(fromUpstream, answerManager, iteration);
    }

    protected abstract boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration);

    protected abstract AnswerState toUpstreamAnswer(Compound<?, ?> answer, Response.Answer fromDownstream);

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        for (grakn.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
            try {
                downstreamResolvers.put(registry.nested(conjunction), conjunction);
            } catch (GraknException e) {
                terminate(e);
                return;
            }
        }
        if (!isTerminated()) isInitialised = true;
    }

    @Override
    protected AnswerManager requestStateCreate(Request fromUpstream, int iteration) {
        LOG.debug("{}: Creating a new AnswerManager for request: {}", name(), fromUpstream);
        assert fromUpstream.partialAnswer().isCompound();
        AnswerManager answerManager = new AnswerManager(iteration);
        for (Driver<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers.keySet()) {
            Compound.Nestable downstream = fromUpstream.partialAnswer().asCompound()
                    .filterToNestable(conjunctionRetrievedIds(conjunctionResolver));
            Request request = Request.create(driver(), conjunctionResolver, downstream);
            answerManager.downstreamManager().addDownstream(request);
        }
        return answerManager;
    }

    @Override
    protected AnswerManager requestStateReiterate(Request fromUpstream, AnswerManager answerManagerPrior,
                                                  int newIteration) {
        LOG.debug("{}: Updating AnswerManager for iteration '{}'", name(), newIteration);

        assert newIteration > answerManagerPrior.iteration() && fromUpstream.partialAnswer().isCompound();

        AnswerManager answerManagerNextIteration = requestStateForIteration(answerManagerPrior, newIteration);
        for (Driver<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers.keySet()) {
            Compound.Nestable downstream = fromUpstream.partialAnswer().asCompound()
                    .filterToNestable(conjunctionRetrievedIds(conjunctionResolver));
            Request request = Request.create(driver(), conjunctionResolver, downstream);
            answerManagerNextIteration.downstreamManager().addDownstream(request);
        }
        return answerManagerNextIteration;
    }

    abstract AnswerManager requestStateForIteration(AnswerManager answerManagerPrior, int newIteration);

    protected Set<Identifier.Variable.Retrievable> conjunctionRetrievedIds(Driver<ConjunctionResolver.Nested> conjunctionResolver) {
        // TODO use a map from resolvable to resolvers, then we don't have to reach into the state and use the conjunction
        return iterate(conjunctionResolver.actor().conjunction().variables()).filter(v -> v.id().isRetrievable())
                .map(v -> v.id().asRetrievable()).toSet();
    }

    public static class Nested extends DisjunctionResolver<Nested> {

        public Nested(Driver<Nested> driver, Disjunction disjunction, ResolverRegistry registry,
                      TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean explanations) {
            super(driver, Nested.class.getSimpleName() + "(pattern: " + disjunction + ")", disjunction,
                  registry, traversalEngine, conceptMgr, explanations);
        }

        @Override
        protected void nextAnswer(Request fromUpstream, AnswerManager answerManager, int iteration) {
            if (answerManager.downstreamManager().hasDownstream()) {
                requestFromDownstream(answerManager.downstreamManager().nextDownstream(), fromUpstream, iteration);
            } else {
                failToUpstream(fromUpstream, iteration);
            }
        }

        @Override
        protected boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream, int iteration) {
            answerToUpstream(upstreamAnswer, fromUpstream, iteration);
            return true;
        }

        @Override
        protected AnswerState toUpstreamAnswer(Compound<?, ?> answer, Response.Answer fromDownstream) {
            assert answer.isNestable();
            return answer.asNestable().toUpstream();
        }

        @Override
        protected AnswerManager requestStateForIteration(AnswerManager answerManagerPrior, int newIteration) {
            return new AnswerManager(newIteration);
        }

    }
}
