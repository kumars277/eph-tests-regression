#confluence v11
#https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45487296633/BCS+Inbound

Feature:Validate data count for workday inbound table

  @workday
  Scenario Outline: Verify Data and Count for workday inbound tables
    Given Get the total count of workday_data_orgdata
    Then  Get total count of workday_reference_v
    And Compare count of workday_data_orgdata and workday_reference_v
    Given Get the <countOfRandomIds> random ids from workday_data_orgdata
    When Records from inbound table workday_data_orgdata
    Then Records from workday_reference_v
    And Compare the records for workday_data_orgdata and workday_reference_v
    Examples:
      | countOfRandomIds|
      |       100         |

