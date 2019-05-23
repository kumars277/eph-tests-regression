Feature: Entity - PERSON PRODUCT ROLE - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario: Count check of persons product role between EPH STG DQ and EPH Stg
    Given Get the count of records for persons product role in EPH STG Product DQ
    When Get the count of records for persons product role in EPH Staging
    Then Compare the count on records for persons product role in PMX and EPH Staging

  @Regression
  Scenario: Count check of persons product role between EPH Staging with DQ and EPH SA
    Given Get the count of records for persons product role in EPH Staging with DQ
    When Get the count of records for persons product role in EPH SA
    Then Compare the count on records for persons product role in EPH Staging with DQ and EPH SA


  @Regression
  Scenario: Count check of persons product role between EPH SA and EPH GD
    Given Get the count of records for persons product role in EPH SA
    When Get the count of records for persons product role in EPH GD
    Then Compare the count on records for persons product role in EPH SA and EPH GD

  @Regression
  Scenario Outline: Validate data is transferred from EPH STG DQ to EPH STG
    Given We get <countOfRandomIds> random ids of persons product role
    When We get the person product role records from EPH STG Can
    Then We get the person product role records from EPH STG
    And Compare person product role records in EPH STG Can and EPH STG
    Examples:
      | countOfRandomIds |
      | 10               |


  @Regression
  Scenario Outline: Validate mandatory columns are populated in EPH SA
    Given We get <countOfRandomIds> random ids of persons product role
    Given We get the person product role records from EPH STG
    When We get the ids of the person product role records in EPH SA from the lookup table
    Then We get the person product role records from EPH SA
    And Check the mandatory columns are populated
    Examples:
      | countOfRandomIds |
      | 10               |


  @Regression
  Scenario Outline: Validate data is transferred from EPH STG to EPH SA
    Given We get <countOfRandomIds> random ids of persons product role
    When We get the person product role records from EPH STG
    Then We get the ids of the person product role records in EPH SA from the lookup table
    Then We get the person product role records from EPH SA
    And Compare person product role records in EPH STG and EPH SA
    Examples:
      | countOfRandomIds |
      | 10               |


  @Regression
  Scenario Outline: Validate data is transferred from EPH SA to EPH GD
    Given We get <countOfRandomIds> random ids of persons product role from SA
    Then We get the random person product role records from EPH SA
    Then We get the person product role records from EPH GD
    And Compare person product role records in EPH SA and EPH GD
    Examples:
      | countOfRandomIds |
      | 10               |