#
# Copyright (C) 2021 Grakn Labs
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

Feature: Debugging Space

  # ##########################################################
  # test-negation
  # ##########################################################

#  Background: Set up databases for resolution testing
#    Given connection has been opened
#    Given connection does not have any database
#    Given connection create database: reasoned
#    Given connection create database: materialised
#    Given connection open schema sessions for databases:
#      | reasoned     |
#      | materialised |
#    Given for each session, open transactions of type: write
#    Given for each session, graql define
#      """
#      define
#      person sub entity,
#        owns name,
#        owns age,
#        plays friendship:friend,
#        plays employment:employee;
#      company sub entity,
#        owns name,
#        plays employment:employer;
#      place sub entity,
#        owns name,
#        plays location-hierarchy:subordinate,
#        plays location-hierarchy:superior;
#      friendship sub relation,
#        relates friend;
#      employment sub relation,
#        relates employee,
#        relates employer;
#      location-hierarchy sub relation,
#        relates subordinate,
#        relates superior;
#      name sub attribute, value string;
#      age sub attribute, value long;
#      """
#    Given for each session, transaction commits
#    # each scenario specialises the schema further
#    Given for each session, open transactions of type: write
#
#  # Paste any scenarios below for debugging.
#  # Do not commit any changes to this file.
#
#  Scenario: when evaluating negation blocks, completion of incomplete queries is not acknowledged
#    Given for each session, graql define
#      """
#      define
#      resource sub attribute, value string;
#      entity-1 sub entity, owns resource, plays relation-2:role-2, plays relation-3:role-4;
#      entity-2 sub entity, owns resource, plays relation-2:role-1, plays relation-3:role-3, plays relation-3:role-4;
#      entity-3 sub entity, owns resource, plays relation-2:role-1, plays relation-3:role-3, plays relation-3:role-4, plays symmetric-relation:symmetric-role;
#      relation-2 sub relation, relates role-1, relates role-2;
#      relation-3 sub relation, relates role-3, relates role-4;
#      relation-4 sub relation-3;
#      relation-5 sub relation-3;
#      symmetric-relation sub relation, relates symmetric-role;
#      rule rule-1: when {
#          (role-3: $x, role-4: $y) isa relation-5;
#      } then {
#          (role-3: $x, role-4: $y) isa relation-4;
#      };
#      rule rule-2: when {
#          (role-1: $x, role-2: $y) isa relation-2;
#          not { (role-3: $x, role-4: $z) isa relation-5;};
#      } then {
#          (role-3: $x, role-4: $y) isa relation-4;
#      };
#      rule trans-rule: when {
#          (role-3: $y, role-4: $z) isa relation-4;
#          (role-3: $x, role-4: $y) isa relation-4;
#      } then {
#          (role-3: $x, role-4: $z) isa relation-4;
#      };
#      rule rule-3: when {
#          (symmetric-role: $x, symmetric-role: $y) isa symmetric-relation;
#      } then {
#          (role-3: $y, role-4: $x) isa relation-5;
#      };
#      """
#    Given for each session, transaction commits
#    Given connection close all sessions
#    Given connection open data sessions for databases:
#      | reasoned     |
#      | materialised |
#    Given for each session, open transactions of type: write
#    Given for each session, graql insert
#    """
#      insert
#      $d isa entity-1, has resource "d";
#      $e isa entity-2, has resource "e";
#      $a isa entity-3, has resource "a";
#      $b isa entity-3, has resource "b";
#      $c isa entity-3, has resource "c";
#      (role-1: $e, role-2: $d)  isa relation-2;
#      (role-1: $a, role-2: $d) isa relation-2;
#      (role-1: $b, role-2: $d)  isa relation-2;
#      (role-1: $c, role-2: $d) isa relation-2;
#      (role-3: $a, role-4: $e)  isa relation-5;
#      (role-3: $b, role-4: $e)  isa relation-5;
#      (role-3: $c, role-4: $e) isa relation-5;
#      (symmetric-role: $c, symmetric-role: $b ) isa symmetric-relation;
#      """
#    Then materialised database is completed
#    Given for each session, transaction commits
#    Given for each session, open transactions of type: read
#    Then for graql query
#      """
#      match (role-3: $x, role-4: $y) isa relation-4;
#      """
#    Then all answers are correct in reasoned database
#    Then answer size in reasoned database is: 11
#    Then answers are consistent across 5 executions in reasoned database
#    Then materialised and reasoned databases are the same size

  # ##########################################################
  # test-recursion
  # ##########################################################


  Background: Set up databases for resolution testing
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: reasoned
    Given connection create database: materialised
    Given connection open schema sessions for databases:
      | reasoned     |
      | materialised |
    Given for each session, open transactions of type: write
    Given for each session, graql define
      """
      define
      person sub entity,
        owns name,
        plays friendship:friend,
        plays employment:employee;
      company sub entity,
        owns name,
        plays employment:employer;
      place sub entity,
        owns name,
        plays location-hierarchy:subordinate,
        plays location-hierarchy:superior;
      friendship sub relation,
        relates friend;
      employment sub relation,
        relates employee,
        relates employer;
      location-hierarchy sub relation,
        relates subordinate,
        relates superior;
      name sub attribute, value string;
      """
    Given for each session, transaction commits
    Given for each session, open transactions of type: write


  Scenario: ancestor-friend test

  from Vieille - Recursive Axioms in Deductive Databases (QSQ approach) p. 186

    Given for each session, graql define
      """
      define
      person sub entity,
          owns name,
          plays parentship:parent,
          plays parentship:child,
          plays friendship:friend,
          plays friendship:friend,
          plays ancestor-friendship:ancestor,
          plays ancestor-friendship:friend;
      friendship sub relation, relates friend;
      parentship sub relation, relates parent, relates child;
      ancestor-friendship sub relation, relates ancestor, relates friend;
      name sub attribute, value string;
      rule rule-1: when {
        (friend: $x, friend: $y) isa friendship;
      } then {
        (ancestor: $x, friend: $y) isa ancestor-friendship;
      };
      rule rule-2: when {
        (parent: $x, child: $z) isa parentship;
        (ancestor: $z, friend: $y) isa ancestor-friendship;
      } then {
        (ancestor: $x, friend: $y) isa ancestor-friendship;
      };
      """
    Given for each session, transaction commits
    Given connection close all sessions
    Given connection open data sessions for databases:
      | reasoned     |
      | materialised |
    Given for each session, open transactions of type: write
    Given for each session, graql insert
    """
      insert
      $a isa person, has name "a";
      $b isa person, has name "b";
      $c isa person, has name "c";
      $d isa person, has name "d";
      $g isa person, has name "g";
      (parent: $a, child: $b) isa parentship;
      (parent: $b, child: $c) isa parentship;
      (friend: $a, friend: $g) isa friendship;
      (friend: $c, friend: $d) isa friendship;
      """
    Then materialised database is completed
    Given for each session, transaction commits
    Given for each session, open transactions of type: read
    Then for graql query
      """
      match
        (ancestor: $X, friend: $Y) isa ancestor-friendship;
        $X has name 'a';
        $Y has name $name;
      get $Y;
      """
    Then answer size in reasoned database is: 2
    Given for each session, transaction closes
#    Given for each session, open transactions of type: read
#    Then all answers are correct in reasoned database
#    Given for each session, transaction closes
#    Given for each session, open transactions of type: read
#    Then answer set is equivalent for graql query
#      """
#      match
#        $Y has name $name;
#        {$name = 'd';} or {$name = 'g';};
#      get $Y;
#      """
#    Then for each session, transaction closes
#    Given for each session, open transactions of type: read
#    And answer set is equivalent for graql query
#      """
#      match
#        ($X, $Y) isa ancestor-friendship;
#        $X has name 'a';
#      get $Y;
#      """
#    Then for each session, transaction closes
#    Given for each session, open transactions of type: read
#    Then for graql query
#      """
#      match
#        (ancestor: $X, friend: $Y) isa ancestor-friendship;
#        $Y has name 'd';
#      get $X;
#      """
#    Then answer size in reasoned database is: 3
#    Then for each session, transaction closes
#    Given for each session, open transactions of type: read
#    Then all answers are correct in reasoned database
#    Then for each session, transaction closes
#    Given for each session, open transactions of type: read
#    Then answer set is equivalent for graql query
#      """
#      match
#        $X has name $name;
#        {$name = 'a';} or {$name = 'b';} or {$name = 'c';};
#      get $X;
#      """
#    Then for each session, transaction closes
#    Given for each session, open transactions of type: read
#    And answer set is equivalent for graql query
#      """
#      match
#        ($X, $Y) isa ancestor-friendship;
#        $Y has name 'd';
#      get $X;
#      """
#    Then materialised and reasoned databases are the same size