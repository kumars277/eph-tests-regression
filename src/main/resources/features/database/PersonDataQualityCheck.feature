Feature: Entity - PERSON  - Validate data between PMX and EPH - Talend Load

  @Regression
  Scenario: Count check between PMX and EPH Staging
    Given Get the count of records for persons in PMX
    When Get the count of records for persons in EPH Staging
    Then Compare the count on records for persons in PMX and EPH Staging

  @Regression
  Scenario: Count check between EPH Staging and EPH DQ
    Given Get the count of records for persons in EPH Staging going to DQ
    When Get the count of records for persons in EPH DQ
    Then Compare the count on records for persons in EPH Staging and EPH DQ

  @Regression
  Scenario: Count check between EPH DQ and EPH SA
    Given Get the count of records for persons in EPH DQ going to SA
    When Get the count of records for persons in EPH SA
    Then Compare the count on records for persons in EPH DQ and EPH SA


  @Regression
  Scenario: Count check between EPH SA and EPH GD
    Given Get the count of records for persons in EPH SA
    When Get the count of records for persons in EPH GD
    Then Compare the count on records for persons in EPH SA and EPH GD


  @Regression
  Scenario Outline: Validate data is transferred from PMX to EPH STG
    Given We get <countOfRandomIds> random ids of persons
    When We get the person records from PMX
    Then We get the person records from EPH STG
    And Compare person records in PMX and EPH STG
    Examples:
      | countOfRandomIds |
      | 10               |


  @Regression
  Scenario Outline: Validate data is transferred from EPH STG to EPH DQ and EPH SA
    Given We get <countOfRandomIds> random ids of persons
    When We get the person records from EPH STG
    Then We get the person records from EPH DQ
    Then Compare person records in EPH STG and EPH DQ
    And Compare person records in EPH DQ and EPH SA
    Examples:
      | countOfRandomIds |
      | 10               |


  @Regression
  Scenario Outline: Validate data is transferred from EPH SA to EPH GD
    Given We get <countOfRandomIds> random ids of persons
    Then We get the person records from EPH SA
    Then We get the person records from EPH GD
    And Compare person records in EPH SA and EPH GD
    Examples:
      | countOfRandomIds |
      | 10               |

