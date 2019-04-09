Feature: Entity - Financial Attributes - Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @WIP
  Scenario: Checking the financial attributes between PMX and EPH
    Given We have a number of financial attributes to check
    When We get the data for financial attributes
    Then The data between DQ and SA is identical
    And The data between SA and GD is identical
