Feature:Validate data for Work tables between EPH and Data Lake - Outbound


  @DL
  Scenario Outline: Validate data is transferred from gd_wwork EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the gd work records from EPH
    Then We get the gd work records from DL
    And Compare gd work records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_wwork|

   @DL
   Scenario Outline: Validate data is transferred from gh_wwork EPH to DL Outbound
     Given We get <countOfRandomIds> random work ids of <table>
     When We get the gh work records from EPH
     Then We get the gh work records from DL
     And Compare gh work records in EPH and DL
     Examples:
       | countOfRandomIds | table  |
       | 10              | gh_wwork|



