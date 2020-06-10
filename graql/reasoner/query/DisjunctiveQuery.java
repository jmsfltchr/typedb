package grakn.core.graql.reasoner.query;

import com.google.common.collect.Iterators;
import com.google.common.collect.SetMultimap;
import grakn.core.graql.reasoner.explanation.DisjunctiveExplanation;
import grakn.core.graql.reasoner.explanation.LookupExplanation;
import grakn.core.kb.concept.api.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.common.util.LazyMergingStream;
import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.ResolutionIterator;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.DisjunctiveState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Disjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DisjunctiveQuery extends ResolvableQuery {
    private final Set<CompositeQuery> clauses;
    private final Set<Variable> bindingVars;

    public DisjunctiveQuery(Disjunction<Conjunction<Pattern>> pattern, Set<Variable> bindingVars, TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        super(traversalExecutor, ctx);
        ReasonerQueryFactory queryFactory = context().queryFactory();
        clauses = pattern.getPatterns().stream().map(queryFactory::composite).collect(Collectors.toSet());
        this.bindingVars = bindingVars;
    }

    public DisjunctiveQuery(Set<CompositeQuery> clauses, Set<Variable> bindingVars, TraversalExecutor traversalExecutor, ReasoningContext ctx) {
        super(traversalExecutor, ctx);
        this.clauses = clauses;
        this.bindingVars = bindingVars;
    }

    @Override
    public CompositeQuery asComposite() {
        throw new RuntimeException("Not composite"); //TODO What exception should this throw?
    }

    @Override
    public DisjunctiveQuery asDisjunctive() {
        return this;
    }

    /**
     * @return conjunction if this disjunction has only one clause, null otherwise
     */
    public CompositeQuery singleClause() {
        if (getClauses().size() == 1) {
            return Iterators.getOnlyElement(getClauses().iterator());
        }
        return null;
    }

    public Set<CompositeQuery> getClauses() {
        return new HashSet<>(clauses);
    }

    public Set<Variable> getBindingVars() {
        return new HashSet<>(bindingVars);
    }

    // Only needed for a disjunction to be the body of a rule
    @Override
    public Set<String> validateOntologically(Label ruleLabel) {
        return getClauses().stream().flatMap(q -> q.validateOntologically(ruleLabel).stream()).collect(Collectors.toSet());
    }

    public HashMap<Variable, Concept> filterBindingVars(Map<Variable, Concept> map) {

        HashMap<Variable, Concept> bindingVarsMap = new HashMap<>();

        bindingVars.forEach(b -> {
            if (map.get(b) != null) {
                bindingVarsMap.put(b, map.get(b));
            }
        });

        return bindingVarsMap;
    }

    @Override
    public ResolvableQuery withSubstitution(ConceptMap sub) {
        return new DisjunctiveQuery(
                getClauses().stream().map(q -> q.withSubstitution(sub)).collect(Collectors.toSet()),
                getBindingVars(),
                traversalExecutor,
                context()
        );
    }

    @Override
    public Stream<ConceptMap> resolve(Set<ReasonerAtomicQuery> subGoals, boolean infer) {

        boolean doNotResolve = !infer || getAtoms().isEmpty() || (isPositive() && !isRuleResolvable());
        if (doNotResolve) {
            Stream<Stream<ConceptMap>> answerStreams = clauses.stream().map(clause ->
                    traversalExecutor.traverse(clause.getPattern()).map(ans -> {
                        ConceptMap clauseAns = new ConceptMap(ans.map(), new LookupExplanation(), clause.getPattern(ans.map()));
                        HashMap<Variable, Concept> bindingVarsSub = filterBindingVars(ans.map());
                        return new ConceptMap(bindingVarsSub, new DisjunctiveExplanation(clauseAns), getPattern(bindingVarsSub));
                    }));
            LazyMergingStream<ConceptMap> mergedStreams = new LazyMergingStream<>(answerStreams);
            return mergedStreams.flatStream();
        } else {
            return new ResolutionIterator(this, subGoals, context().queryCache()).hasStream();
        }
    }

    @Override
    ResolvableQuery inferTypes() {
        return new DisjunctiveQuery(
                getClauses().stream().map(CompositeQuery::inferTypes).collect(Collectors.toSet()),
                getBindingVars(),
                traversalExecutor,
                context()
        );
    }

    @Override
    ResolvableQuery constantValuePredicateQuery() {
        return new DisjunctiveQuery(
                getClauses().stream().map(CompositeQuery::constantValuePredicateQuery).collect(Collectors.toSet()),
                getBindingVars(),
                traversalExecutor,
                context()
        );
    }

    @Override
    ResolvableQuery copy() {
        return new DisjunctiveQuery(
                new HashSet<>(getClauses()),
                getBindingVars(),
                traversalExecutor,
                context());
    }

    @Override
    public boolean isAtomic() {
        // TODO unclear of the meaning of atomicity in this case
        return getClauses().stream().allMatch(CompositeQuery::isAtomic);
    }

    @Override
    boolean isEquivalent(ResolvableQuery q) {
        DisjunctiveQuery that = q.asDisjunctive();
        return that.getClauses().size() == getClauses().size()
                && getClauses().stream().allMatch(clause -> that.getClauses().stream().anyMatch(clause::isEquivalent));
    }

    @Override
    public String toString(){
        return getPattern().toString();
    }

    @Override
    public ReasonerQuery conjunction(ReasonerQuery q) {
        throw new UnsupportedOperationException();
//        return new DisjunctiveQuery(
//                getClauses().stream()
//                        .map(CompositeQuery::getPattern)
//                        .map(p -> Graql.and(
//                                Sets.union(
//                                        p.getPatterns(), getPattern().getPatterns()
//                                )
//                        ))
//                        .map(p -> new CompositeQuery(
//                                p, traversalExecutor, context()
//                        ))
//                        .collect(Collectors.toSet()),
//                traversalExecutor,
//                context()
//        );
    }

    @Override
    public void checkValid() {
        clauses.forEach(CompositeQuery::checkValid);
    }

    @Override
    public Set<Variable> getVarNames() {
        return getClauses().stream().flatMap(c -> c.getVarNames().stream()).collect(Collectors.toSet());
    }

    @Override
    public Set<Atomic> getAtoms() {
        return getClauses().stream().flatMap(c -> c.getAtoms().stream()).collect(Collectors.toSet());
    }

    @Override
    public Disjunction<Pattern> getPattern() {
        // TODO How can we rebuild the disjunction in proper Graql form (with unbound variables outside the disjunction) once it's already in disjunctive normal form?!
        return Graql.or(clauses.stream().map(CompositeQuery::getPattern).collect(Collectors.toSet()));
    }

    @Override
    public Conjunction<Pattern> getPattern(Map<Variable, Concept> map) {
        HashSet<Pattern> patterns = getIdPredicatePatterns(map);
        patterns.add(getPattern());

        return Graql.and(patterns);
    }

    @Override
    public ConceptMap getSubstitution() {
        // TODO What should this do, given that there could be a substitution for any of its clauses
        return null;
    }

    @Override
    public boolean isRuleResolvable() {
        return clauses.stream().anyMatch(ResolvableQuery::isRuleResolvable);
    }

    @Override
    public MultiUnifier getMultiUnifier(ReasonerQuery parent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getUnambiguousType(Variable var, boolean inferTypes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SetMultimap<Variable, Type> getVarTypeMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SetMultimap<Variable, Type> getVarTypeMap(boolean inferTypes) {
        return getVarTypeMap(); // TODO Seems a weird thing to do since that throws an exception
    }

    @Override
    public SetMultimap<Variable, Type> getVarTypeMap(ConceptMap sub) {
        return getVarTypeMap(); // TODO Seems a weird thing to do since that throws an exception
    }

    @Override
    public boolean requiresReiteration() {
        return clauses.stream().anyMatch(ResolvableQuery::requiresReiteration);
    }

    @Override
    public Stream<Atom> selectAtoms() {
        return getAtoms(Atom.class).filter(Atomic::isSelectable);
    }

    @Override
    public boolean requiresDecomposition() {
        return clauses.stream().anyMatch(ResolvableQuery::requiresDecomposition);
    }

    @Override
    public DisjunctiveQuery rewrite() {
        return new DisjunctiveQuery(
                clauses.stream().map(CompositeQuery::rewrite).collect(Collectors.toSet()),
                getBindingVars(),
                traversalExecutor,
                context());
    }

    @Override
    public ResolutionState resolutionState(ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        return new DisjunctiveState(this, sub, u, parent, subGoals);
    }

    @Override
    public Iterator<ResolutionState> innerStateIterator(AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
//        TODO Does the substitution make sense here?
//        TODO Is the unifier correct?
        return clauses.stream().map(c -> c.resolutionState(getSubstitution(), parent.getUnifier(), parent, subGoals)).iterator();
    }

}
