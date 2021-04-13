Feature: Entity - Work Relationship Mirrors - Data Mapping Check - Validate data between PMX and EPH - Talend Load

  @Regression
  Scenario: Checking the mirror data between PMX and EPH
    Given We have a number of mirrors to check
    When We get the data for mirrors
    Then The mirror data between PMX and STG is identical
    And The mirror data between STG and SA is identical
    And The mirror data between SA and GD is identical