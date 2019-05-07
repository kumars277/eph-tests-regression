Feature: Entity - Financial Attributes - Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario: Checking the count of financial attributes between STG and EPH SA
    Given We know the number of financial attributes in DQ
    When We get the financial attributes from SA
    Then The financial attributes between DQ and SA are equal

  @Regression
  Scenario: Checking the count of financial attributes between EPH SA and EPH GD
    Given We get the financial attributes from SA
    When We get the financial attributes from GD
    Then The financial attributes between SA and GD are equal

  @Regression
  Scenario: Checking the count of financial attributes between EPH SA and EPH GD + AE
    Given We get the financial attributes from SA
    When We get the financial attributes from AE
    Then The financial attributes between SA and GD with AE are equal

  @Regression
  Scenario: Checking the financial attributes between PMX and EPH
    Given We have a number of financial attributes to check
    When We get the data for financial attributes
    Then The data between DQ and SA is identical
    And The data between SA and GD is identical
