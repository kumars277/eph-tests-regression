Feature: Entity - Work Relationship Translation - Data Mapping Check - Validate data between PMX and EPH - Talend Load

  @Regression
  Scenario: Checking the count of work relationship records between PMX and STG
    Given We know the number of work relationship records in PMX
    When We know the work relationship records from STG
    Then The work relationship records between PMX and STG are equal

  @Regression
  Scenario: Checking the count of work relationship records between STG and EPH SA
    Given We know the work relationship records from STG
    When We get the work relationship records from SA
    Then The work relationship records between STG and SA are equal

  @Regression
  Scenario: Checking the count of work relationship records between EPH SA and EPH GD
    Given We get the work relationship records from SA
    When We get the work relationship records from GD
    Then The work relationship records between SA and GD are equal

  @Regression
  Scenario: Checking the count of work relationship records between EPH SA and EPH GD + AE
    Given We get the work relationship records from SA
    When We get the work relationship records from AE
    Then The work relationship records between SA and GD with AE are equal

  @Regression
  Scenario: Checking the translations data between PMX and EPH
    Given We have a number of translations to check
    When We get the data for translations
    Then The translations data between PMX and STG is identical
    And The translations data between STG and SA is identical
    And The translations data between SA and GD is identical