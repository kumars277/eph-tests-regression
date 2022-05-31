Feature:Validate data for workday ETL Core in Data Lake Access Layer

  @workdayCore
  Scenario Outline: Verify Data for workday ETL Core tables is transferred from workday_reference_v
    Given Get the total count of workday_reference_v which is inbound
    Then  Get the total count of eph_ingestion_person_delta_v
    And   Compare count of workday_reference_v and eph_ingestion_person_delta_v
   # Given Get the <countOfRandomIds> from workday_reference_v
   # Then  Get the Data from the workday_reference_v
   # And   Get the Data from the eph_ingestion_person_delta_v
   # Then  Compare data of workday_reference_v and eph_ingestion_person_delta_v
    Examples:
      |countOfRandomIds|
      |1              |
