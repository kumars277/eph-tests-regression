Feature: Entity - Data Quality Checks - Validate false data does not reach EPH child entities

  Scenario: Validate false WORK data does not reach EPH for all child entities
    Given We have failed data in Work DQ table
    When We get the data from all child entities and dq tables by work id
    Then The data has not reached EPH
    And There is no EPH id created

  Scenario: Validate false MANIFESTATION data does not reach EPH for all child entities
    Given We have failed data in Manifestation DQ table
    When We get the data from product dq table
    Then The product data has failed
    And There is no EPH id created

  Scenario: Validate that when a person's last name is missing there is a dq error
    Given We have a person with missing last name
    When We get the data from the dq table
    Then There is a dq error for this person
    And There is no EPH data for this person