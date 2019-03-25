Feature: Entity - WORK - Count Check - Validate data count between PMX and EPH - Talend Full Load


  @Regression
  Scenario: Verify that all the work data is transferred to PMX Staging
    Given We know the number of Works in PMX
    When The works are in PMX Staging
    Then The work number between PMX and PMX STG is identical

  @Regression
  Scenario: Verify that all the work data is transferred to DQ STG
    Given The works are in PMX Staging
    When The works are transferred to EPH
    Then The work number between PMX STG and DQ STG is identical

  @Regression
  Scenario: Verify that all the work data is transferred to EPH
    Given The works are in PMX Staging
    When The works are transferred to EPH
    Then The work number between DQ STG and SA is identical

  @Regression
  Scenario: Verify that all the work data is transferred to GD
    Given The works are in PMX Staging
    When The works are transferred to EPH
    Then The work number between SA and GD is identical