Feature: Entity - Financial Attributes - Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  Scenario: Checking the financial attributes between PMX and EPH
    Given We have to check a number of financial attributes
    When We get the data for financial attributes
    Then The data between DQ and SA is identical
    And The data between SA and GD is identical
