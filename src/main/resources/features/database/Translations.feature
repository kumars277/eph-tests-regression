Feature: Entity - Work Relationship Translation - Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario: Checking the count of translations between PMX and STG
    Given We know the number of translations in PMX
    When We know the translations from STG
    Then The translations between PMX and STG are equal

  @Regression
  Scenario: Checking the count of translations between STG and EPH SA
    Given We know the translations from STG
    When We get the translations from SA
    Then The translations between STG and SA are equal

  @Regression
  Scenario: Checking the count of translations between EPH SA and EPH GD
    Given We get the translations from SA
    When We get the translations from GD
    Then The translations between SA and GD are equal

  @Regression
  Scenario: Checking the count of translations between EPH SA and EPH GD + AE
    Given We get the translations from SA
    When We get the translations from AE
    Then The translations between SA and GD with AE are equal

  @Regression
  Scenario: Checking the translations data between PMX and EPH
    Given We have a number of translations to check
    When We get the data for translations
    Then The translations data between PMX and STG is identical
    And The translations data between STG and SA is identical
    And The translations data between SA and GD is identical