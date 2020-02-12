Feature:Validate data for Work tables between EPH and Data Lake - Outbound


  @DL
  Scenario Outline: Validate data is transferred from EPH to DL Outbound
    Given We get <countOfRandomIds> random ids of work
    When We get the work records from EPH
    Then We get the work records from DL
    And Compare work records in EPH and DL
    Examples:
      | countOfRandomIds |
      | 1              |

