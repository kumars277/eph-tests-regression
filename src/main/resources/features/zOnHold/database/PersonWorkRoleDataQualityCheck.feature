Feature: Entity - PERSON WORK ROLE - Validate data between PMX and EPH - Talend  Load


  @Regression @WP
  Scenario: Count check of persons work role  between PMX and EPH Staging
    Given Get the count of records for persons work role in PMX
    When Get the count of records for persons work role in EPH Staging
    Then Compare the count on records for persons work role in PMX and EPH Staging

  @Regression @WP
  Scenario: Count check of persons work role between EPH Staging and EPH SA
    Given Get the count of records for persons work role in EPH Staging going to SA
    When Get the count of records for persons work role in EPH SA
    Then Compare the count on records for persons work role in EPH Staging and EPH SA

  @Regression @WP
  Scenario: Count check of persons work role between EPH SA and EPH GD
    Given Get the count of records for persons work role in EPH SA going to GD
    When Get the count of records for persons work role in EPH GD
    Then Compare the count on records for persons work role in EPH SA and EPH GD

  @Regression @WP
  Scenario: Verify sum of counts of persons work role in EPH GD and EPH AE is equal to count of persons work role in EPH SA
    Given Get the count of records for persons work role in EPH SA going to GD
    Given Get the count of records for persons work role in EPH AE
    When Get the count of records for persons work role in EPH GD
    Then Verify sum of records for persons work role in EPH GD and EPH AE is equal to number of records in EPH SA

  @Regression @WP
  Scenario Outline: Validate data is transferred from PMX to EPH STG
    Given We get <countOfRandomIds> random ids of persons work role with <type>
    When We get the person work role records with <type> from PMX
    Then We get the person work role records from EPH STG
    And Compare person work role records in PMX and EPH STG for <type>
    Examples:
      | countOfRandomIds | type |
      | 10               | PD   |
      | 10               | AU   |
      | 10               | PU   |
      | 10               | AE   |
      | 10               | BC   |


  @Regression @WP
  Scenario Outline: Validate mandatory columns are populated in EPH SA for persons work role
    Given We get <countOfRandomIds> random ids of persons work role with <type>
    Then We get the person work role records from EPH SA
    And Check the mandatory columns are populated for persons work role
    Examples:
      | countOfRandomIds | type |
      | 5               | PD   |
      | 5               | AU   |
      | 5               | PU   |
      | 5               | AE   |
      | 5               | BC   |



  @Regression @WP
  Scenario Outline: Validate data is transferred from EPH STG to EPH SA
    Given We get <countOfRandomIds> random ids of persons work role with <type>
    When We get the person work role records from EPH STG
    Then We get the person work role records from EPH SA
    And Compare person work role records in EPH STG and EPH SA
    Examples:
      | countOfRandomIds | type |
      | 5               | PD   |
      | 5               | AU   |
      | 5               | PU   |
      | 5               | AE   |
      | 5               | BC   |




  @Regression @WP
  Scenario Outline: Validate data is transferred from EPH SA to EPH GD
    Given We get <countOfRandomIds> random ids of persons work role with <type>
    Then We get the person work role records from EPH SA
    Then We get the person work role records from EPH GD with <type>
    And Compare person work role records in EPH SA and EPH GD
    Examples:
      | countOfRandomIds | type |
      | 3              | PD   |
      | 3               | AU   |
      | 3               | PU   |
      | 3               | AE   |
      | 3              | BC   |


