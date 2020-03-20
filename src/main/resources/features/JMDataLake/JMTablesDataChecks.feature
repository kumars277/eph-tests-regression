Feature:Validate data for JM between MYsql and Data Lake - Inbound

  @JMDL
  Scenario Outline: Validate Allocation Changes data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Allocation records from JMF MySQL of <table>
    Then We get the JMF Allocation records from DL of <table>
    And Compare JMF Allocation records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10              | JMF_ALLOCATION_CHANGE|

  Scenario Outline: Validate Application properties data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random application key of <table>
    When We get the JMF Application properties records from JMF MySQL of <table>
    Then We get the JMF Application properties records from DL of <table>
    And Compare JMF Application properties records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10              | JMF_APPLICATION_PROPERTIES|

  Scenario Outline: Validate Approval Request data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Approval Request records from JMF MySQL of <table>
    Then We get the JMF Approval Request records from DL of <table>
    And Compare JMF Approval Request records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10             | JMF_APPROVAL_REQUEST|

  Scenario Outline: Validate Chronicle Scenario data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Chronicle Scenario records from JMF MySQL of <table>
    Then We get the JMF Chronicle Scenario records from DL of <table>
    And Compare JMF Chronicle Scenario records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10             | JMF_CHRONICLE_SCENARIO|

  Scenario Outline: Validate Chronicle Status data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Chronicle Status records from JMF MySQL of <table>
    Then We get the JMF Chronicle Status records from DL of <table>
    And Compare JMF Chronicle Status records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10             | JMF_CHRONICLE_STATUS|


  Scenario Outline: Validate Family Resource data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random ids of <table>
    When We get the JMF Family Resource records from JMF MySQL of <table>
    Then We get the JMF Family Resource records from DL of <table>
    And Compare JMF Family Resource records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10             | JMF_FAMILY_RESOURCE_DETAILS|
