Feature:Validate data for JM between MYsql and Data Lake - Inbound

  @DLD
  Scenario Outline: Validate Allocation Changes data is transferred from JM MYSQL to DL Inbound
    Given We get <countOfRandomIds> random allocation ids of <table>
    When We get the JMF Allocation records from JMF MySQL of <table>
    Then We get the JMF Allocation records from DL of <table>
    And Compare JMF Allocation records in JMF MySQL and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1              | JMF_ALLOCATION_CHANGE|

