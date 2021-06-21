Feature: Entity - Data Quality Checks - Validate false data does not reach EPH child entities

  @WIP
  Scenario: Validate false WORK data does not reach EPH for all child entities
    Given We have failed data in Work DQ table
    When We get the data from all child entities and dq tables by work id
    Then The data has not reached EPH
    And There is no EPH id created

  @WIP
  Scenario: Validate false MANIFESTATION data does not reach EPH for all child entities
    Given We have failed data in Manifestation DQ table
    When We get the data from product dq table
    Then The product data has failed
    And There is no EPH id created

    @Ready
  Scenario: DQ Tier 1 - Validate that when a person's last name is missing there is a dq error
    Given We have a person with missing last name
    When We get the data from the person dq table
    Then There is a dq error for this person
    And There is a record in dq error table for this person
    And There is no EPH data for the person

#  Scenario: DQ Tier 1 - Validate a dq error is logged when work's type is missing
#    Given We have a work with missing type
#    When We get the data from the work dq table
#    Then There is a dq error for this work
#    And There is a record in dq error table for this work
#    And There is no EPH data for this work

  Scenario Outline: DQ Tier 1 - Validate there is a dq error when the f_type/f_status is missing
    Given We have an <entity> which <column> is unknown
    When We get the data from the <entity> dq table
    Then There is a dq error for this <entity>
    And There is a record in dq error table for this <entity>
    And There is no EPH data for this <ref_type>
    Examples:
    | entity        | column    | ref_type      |
    | wwork         | f_type     | WORK          |
    | wwork         | f_status   | WORK          |
    | wwork         | work_title | WORK         |
    | manifestation | f_type     | MANIFESTATION |
    | manifestation | f_status   | MANIFESTATION |


  Scenario Outline: DQ Tier 1 - Validate a dq error is logged if the record end date is after July 8th 2019
    Given We have an <entity> which record end date is after July 8th 2019
    When We get the data from the <entity> dq table
    Then There is a dq error for this <entity>
    And There is a record in dq error table for this <entity>
    And There is no EPH data for this <ref_type>
    Examples:
      | entity         | ref_type      |
      | wwork          | WORK          |
      | manifestation  | MANIFESTATION |
      | product        | PRODUCT       |








