Feature:Validate data for Event tables between EPH and Data Lake - Outbound


  @DL
  Scenario Outline: Validate data is transferred from gd_event EPH to DL Outbound
    Given We get <countOfRandomIds> random event ids of <table>
    When We get the gd event records from EPH
    Then We get the gd event records from DL
    And Compare gd event records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_event|

