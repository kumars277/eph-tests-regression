Feature:Validate data for person tables between EPH and Data Lake - Outbound


  @DL
  Scenario Outline: Validate data is transferred from gd_person EPH to DL Outbound
    Given We get <countOfRandomIds> random person ids of <table>
    When We get the gd person records from EPH
    Then We get the gd person records from DL
    And Compare gd person records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_person|

  @DL
  Scenario Outline: Validate data is transferred from gh_person EPH to DL Outbound
    Given We get <countOfRandomIds> random person ids of <table>
    When We get the gh person records from EPH
    Then We get the gh person records from DL
    And Compare gh person records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gh_person|





