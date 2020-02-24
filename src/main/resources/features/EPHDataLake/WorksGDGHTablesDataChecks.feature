Feature:Validate data for Work tables between EPH and Data Lake - Outbound


  @DLW
  Scenario Outline: Validate data is transferred from gd_wwork EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the gd work records from EPH
    Then We get the gd work records from DL
    And Compare gd work records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10            | gd_wwork|

   @DLW
   Scenario Outline: Validate data is transferred from gh_wwork EPH to DL Outbound
     Given We get <countOfRandomIds> random work ids of <table>
     When We get the gh work records from EPH
     Then We get the gh work records from DL
     And Compare gh work records in EPH and DL
     Examples:
       | countOfRandomIds | table  |
       | 10              | gh_wwork|

  @DLW
  Scenario Outline: Validate data is transferred from gd_work_financial_attribs EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the gd work financial attribs from EPH
    Then We get the gd work financial attribs from DL
    And Compare gd work financial attribs in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_work_financial_attribs|

  @DLW
  Scenario Outline: Validate data is transferred from gd_work_identifier EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the gd work identifier from EPH
    Then We get the gd work identifier from DL
    And Compare gd work identifier in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_work_identifier|



