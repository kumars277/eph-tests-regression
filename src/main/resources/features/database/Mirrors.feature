Feature: Entity - Work Relationship Mirrors - Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario: Checking the count of mirrors between STG and EPH SA
    Given We know the mirrors from STG
    When We get the mirrors from SA
    Then The mirrors between STG and SA are equal

  @Regression
  Scenario: Checking the count of mirrors between EPH SA and EPH GD
    Given We get the mirrors from SA
    When We get the mirrors from GD
    Then The mirrors between SA and GD are equal

  @Regression
  Scenario: Checking the count of mirrors between EPH SA and EPH GD + AE
    Given We get the mirrors from SA
    When We get the mirrors from AE
    Then The mirrors between SA and GD with AE are equal

  @Regression
  Scenario: Checking the mirror data between PMX and EPH
    Given We have a number of mirrors to check
    When We get the data for mirrors
    Then The mirror data between PMX and STG is identical
    And The mirror data between STG and SA is identical
    And The mirror data between SA and GD is identical