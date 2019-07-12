Feature: Entity - WORK - Data Mapping Check - Validate data between PMX and EPH - Talend Load

  @Regression @W
  Scenario: Checking the Works data between PMX and EPH
    Given We have a number of works to check
    When We get the product data from PMX, EPH Staging and EPH
    Then The work data between PMX and EPH STG is identical
    And The work data between EPH STG and EPH DQ is identical
    And The work data between EPH DQ and EPH SA is identical
    And The work data between EPH SA and EPH GD is identical