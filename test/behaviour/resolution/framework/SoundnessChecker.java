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

package com.vaticle.typedb.core.test.behaviour.resolution.framework;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.CorrectnessChecker.SoundnessException;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.Materialiser.Materialisation;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class SoundnessChecker {

    private final Materialiser referenceReasoner;
    private final TypeDB.Session session;
    private final Map<Concept, Concept> inferredConceptMapping;

    private SoundnessChecker(Materialiser referenceReasoner, TypeDB.Session session) {
        this.referenceReasoner = referenceReasoner;
        this.session = session;
        this.inferredConceptMapping = new HashMap<>();
    }

    static SoundnessChecker create(Materialiser referenceReasoner, TypeDB.Session session) {
        return new SoundnessChecker(referenceReasoner, session);
    }

    void checkQuery(TypeQLMatch inferenceQuery) {
        try (Transaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                  new Options.Transaction().infer(true).explain(true))) {
            tx.query().match(inferenceQuery).forEachRemaining(ans -> checkAnswer(ans, tx));
        }
    }

    private void checkAnswer(ConceptMap answer, Transaction tx) {
        answer.explainables().iterator().forEachRemaining(explainable -> {
            tx.query().explain(explainable.id()).forEachRemaining(explanation -> {
                checkAnswer(explanation.conditionAnswer(), tx);
                checkExplanationAgainstReference(explanation);
            });
        });
    }

    private void checkExplanationAgainstReference(Explanation explanation) {
        ConceptMap recordedWhen = substituteInferredVarsForReferenceVars(explanation.conditionAnswer());
        Optional<ConceptMap> recordedThen = referenceReasoner
                .materialisationForCondition(explanation.rule(), recordedWhen)
                .map(Materialisation::conclusionAnswer);
        if (recordedThen.isPresent()) {
            // Update the inferred variables mapping between the two reasoners
            assert recordedThen.get().concepts().keySet().equals(explanation.conclusionAnswer().concepts().keySet());
            recordedThen.get().concepts().forEach((var, recordedConcept) -> {
                Concept inferredConcept = explanation.conclusionAnswer().concepts().get(var);
                if (inferredConceptMapping.containsKey(inferredConcept)) {
                    // Check that the mapping stored is one-to-one
                    assert inferredConceptMapping.get(inferredConcept).equals(recordedConcept);
                } else {
                    inferredConceptMapping.put(inferredConcept, recordedConcept);
                }
            });
        } else {
            throw new SoundnessException(String.format("Soundness testing found an answer within an explanation that " +
                                                               "should not be present for rule \"%s\"" +
                                                               ".\nAnswer:\n%s\nIncorrectly derived from " +
                                                               "condition:\n%s",
                                                       explanation.rule().getLabel(), explanation.conclusionAnswer(),
                                                       explanation.conditionAnswer()));
        }
    }

    private ConceptMap substituteInferredVarsForReferenceVars(ConceptMap conditionAnswer) {
        Map<Retrievable, Concept> substituted = new HashMap<>();
        conditionAnswer.concepts().forEach((var, concept) -> {
            substituted.put(var, inferredConceptMapping.getOrDefault(concept, concept));
        });
        return new ConceptMap(substituted);
    }

}
