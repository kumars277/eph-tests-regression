Feature:Validate data for Work tables between EPH and Data Lake - Outbound


  @DLw
  Scenario Outline: Validate data is transferred from gd_wwork EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work records from EPH of <table>
    Then We get the work records from DL of <table>
    And Compare work records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 20           | gd_wwork|
      | 20              | gh_wwork|



  @DLw
  Scenario Outline: Validate data is transferred from work_financial_attribs EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work financial attribs from EPH of <table>
    Then We get the work financial attribs from DL of <table>
    And Compare work financial attribs in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1              | gd_work_financial_attribs|
      | 1              | gh_work_financial_attribs|



  @DLw
  Scenario Outline: Validate data is transferred from work_identifier EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work identifier from EPH of <table>
    Then We get the work identifier from DL of <table>
    And Compare work identifier in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 2              | gd_work_identifier|
      | 2              | gh_work_identifier|


  @DLw
  Scenario Outline: Validate data is transferred from work_relationship EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work relationship from EPH of <table>
    Then We get the work relationship from DL of <table>
    And Compare work relationship in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10               | gd_work_relationship|
      | 2                | gh_work_relationship|

  @DLw
  Scenario Outline: Validate data is transferred from work_subject_area_link EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work subject from EPH of <table>
    Then We get the work subject from DL of <table>
    And Compare work subject in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1                | gd_work_subject_area_link|
      | 1                | gh_work_subject_area_link|


  @DLw
  Scenario Outline: Validate data is transferred from work_copyright_owner_link EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work copyright from EPH of <table>
    Then We get the work copyright from DL of <table>
    And Compare work copyright in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 5                 | gd_work_copyright_owner_link|
      | 5                | gh_work_copyright_owner_link|


  @DLw
  Scenario Outline: Validate data is transferred from work_relationship_edition EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work relationship_edition from EPH of <table>
    Then We get the work relationship_edition from DL of <table>
    And Compare work relationship_edition in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1                 | gd_work_relationship_edition|
      | 1                | gh_work_relationship_edition|









