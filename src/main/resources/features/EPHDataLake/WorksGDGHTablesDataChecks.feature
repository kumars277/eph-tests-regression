Feature:Validate data for Work tables between EPH and Data Lake - Outbound


  @DLD
  Scenario Outline: Validate data is transferred from gd_wwork EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work records from EPH of <table>
    Then We get the work records from DL of <table>
    And Compare work records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100           | gd_wwork|
      | 100              | gh_wwork|



  @DLD
  Scenario Outline: Validate data is transferred from work_financial_attribs EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work financial attribs from EPH of <table>
    Then We get the work financial attribs from DL of <table>
    And Compare work financial attribs in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100              | gd_work_financial_attribs|
      | 100              | gh_work_financial_attribs|



  @DLD
  Scenario Outline: Validate data is transferred from work_identifier EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work identifier from EPH of <table>
    Then We get the work identifier from DL of <table>
    And Compare work identifier in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100              | gd_work_identifier|
      | 100              | gh_work_identifier|


  @DLD
  Scenario Outline: Validate data is transferred from work_relationship EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work relationship from EPH of <table>
    Then We get the work relationship from DL of <table>
    And Compare work relationship in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100               | gd_work_relationship|
      | 100                | gh_work_relationship|

  @DLD
  Scenario Outline: Validate data is transferred from work_subject_area_link EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work subject from EPH of <table>
    Then We get the work subject from DL of <table>
    And Compare work subject in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100                | gd_work_subject_area_link|
      | 100                | gh_work_subject_area_link|


  @DLD
  Scenario Outline: Validate data is transferred from work_copyright_owner_link EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work copyright from EPH of <table>
    Then We get the work copyright from DL of <table>
    And Compare work copyright in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100                 | gd_work_copyright_owner_link|
      | 100                | gh_work_copyright_owner_link|


  @DLD
  Scenario Outline: Validate data is transferred from work_relationship_edition EPH to DL Outbound
    Given We get <countOfRandomIds> random work ids of <table>
    When We get the work relationship_edition from EPH of <table>
    Then We get the work relationship_edition from DL of <table>
    And Compare work relationship_edition in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100                 | gd_work_relationship_edition|
      | 100                | gh_work_relationship_edition|









