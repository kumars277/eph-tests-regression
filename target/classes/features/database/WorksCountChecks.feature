Feature: Entity - WORK - Count Check - Validate data count between PMX and EPH - Talend Load


  @Regression
  Scenario: Verify that all the work data is transferred to PMX Staging
    Given We know the number of Works in PMX
    When The works are in PMX Staging
    Then The work number between PMX and EPH STG is identical

  @Regression
  Scenario: Verify that all the work data is transferred to DQ STG
    Given The works are in PMX Staging
    When The works are transferred to EPH
    Then The work number between EPH STG and EPH DQ is identical

  @Regression
  Scenario: Verify that all the work data is transferred to EPH
    Given The works are in PMX Staging
    When The works are transferred to EPH
    Then The work number between EPH DQ and EPH SA is identical

  @Regression
  Scenario: Verify that all the work data is transferred to GD
    Given The works are in PMX Staging
    When The works are transferred to EPH
    Then The work number between EPH SA and EPH GD is identical

  @Regression
  Scenario: Verify that all failed data is in AE
    Given The works are in PMX Staging
    When The works are transferred to EPH
    Then The work number between EPH SA and EPH GD with AE is identical