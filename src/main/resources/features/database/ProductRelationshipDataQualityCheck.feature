Feature: Entity - PRODUCT RELATIONSHIP - Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario: Verify count of product relationship records in PMX and EPH staging is equal
    Given We get the count of the product relationship records in PMX
    When We get the count of the product relationship records in EPH STG
    Then The count of the product relationship records in PMX and EPH staging table is equal

  @wip
  Scenario: Verify count of product relationship records in EPH staging and EPH SA is equal
    Given We get the count of the product relationship records in EPH STG
    When The product relationship records are transferred to EPH SA
    Then The count of the product relationship records in EPH staging table and EPH SA is equal

  @wip
  Scenario: Verify count of product relationship records in EPH SA and EPH golden data is equal
    Given The product relationship records are transferred to EPH SA
    When The product relationship are transferred to the golden data table
    Then The count of the product relationship records in in EPH SA and EPH GD is equal


  @Regression
  Scenario Outline: Validate product relationship data is transferred from PMX to EPH STG
    Given We get <countOfRandomIds> random ids of product relationship records
    When We get the product relationship records from PMX
    Then We get the product relationship records from EPH STG
    And Compare the product relationship records in PMX and EPH STG
    Examples:
      | countOfRandomIds |
     | 10                |

  @wip
  Scenario Outline: Validate product relationship data is transferred from EPH STG and EPH SA for books
    Given We get <countOfRandomIds> random ids of product relationship records
    When We get the product relationship records from EPH STG
    Then We get the product relationship records from EPH SA
    And We check that mandatory columns for product relationship SA
    And Compare the product relationship data between EPH STG and EPH SA
    And We get the product relationship records from EPH GD
    And Compare the product relationship data between EPH SA and EPH GD
    Examples:
      | countOfRandomIds |
      | 1                |
