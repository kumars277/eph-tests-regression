Feature: Entity - ACCOUNTABLE PRODUCT - Count And Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario: Compare count of accountable product data between PMX and EPH STG
    Given We get the count of accountable product data from PMX
    When We get the count of accountable product data from EPH STG
    Then Compare the count of accountable product data in PMX and EPH STG

  @Regression
  Scenario: Compare count of accountable product data between EPH STG and EPH SA
    Given We get the count of accountable product data from EPH STG processed to SA
    When We get the count of accountable product data from EPH SA
    Then Compare the count of accountable product data in EPH STG and EPH SA

  @Regression
  Scenario: Compare count of accountable product data between EPH SA and EPH GD
    Given We get the count of accountable product data from EPH SA
    When We get the count of accountable product data from EPH GD
    Then Compare the count of accountable product data in EPH SA and EPH GD

  @Regression
  Scenario Outline: Validate accountable product data is transferred from PMX to EPH STG
    Given We get <countOfRandomIds> random ids of accountable product
    When We get the accountable product data from PMX
    Then We get the accountable product data from EPH STG
    And Compare the accountable product data in PMX and EPH STG
    Examples:
      | countOfRandomIds |
      | 10               |

  @Regression
  Scenario Outline: Validate accountable product data is transferred from EPH STG to EPH DQ
    Given We get <countOfRandomIds> random ids of accountable product
    When We get the accountable product data from EPH STG
    Then Compare the accountable product data in EPH STG and EPH DQ
    Examples:
      | countOfRandomIds |
      | 10               |



  @Regression
  Scenario Outline: Check mandatory columns are populated in SA
    Given We get <countOfRandomIds> random ids of accountable product
    When We get the accountable product data from EPH DQ
    Then We get the accountable product data from EPH SA
    And Check the mandatory columns are populated for accountable products role
    Examples:
      | countOfRandomIds |
      | 10               |

  @Regression
  Scenario Outline: Validate accountable product data is transferred from EPH STG to EPH SA
    Given We get <countOfRandomIds> random ids of accountable product
    When We get the accountable product data from EPH DQ
    Then We get the accountable product data from EPH SA
    And Compare the accountable product data in EPH STG and EPH SA
    Examples:
      | countOfRandomIds |
      | 10               |


  @Regression
  Scenario Outline: Validate accountable product data is transferred from EPH SA to EPH GD
    Given We get <countOfRandomIds> random ids of accountable product
    When We get the accountable product data from EPH STG
    When We get the accountable product data from EPH SA
    Then We get the accountable product data from EPH GD
    And Compare the accountable product data in EPH SA and EPH GD
    Examples:
      | countOfRandomIds |
      | 10               |
