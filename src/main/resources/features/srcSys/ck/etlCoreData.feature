Feature:Validate data for ck ETL tables

#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/90784826471/CK+Inbound

  @CKETLCORE
  Scenario Outline: Verify that all CK data is transferred between Delta History and Delta Current tables
    Given We know the number of CK <DeltaHistorytablename> data in Delta History
    Then Get the count for CK <DeltaCurrenttablename> Delta Current
    And Compare the CK count for <DeltaHistorytablename> table between History and Delta Current
    Given We get the <numberOfRecords> random CK Delta ids of <DeltaCurrenttablename>
    When We get the CK Delta Current Records from <DeltaCurrenttablename>
    Then We get the CK Delta History records from <DeltaHistorytablename>
    And Compare CK records in Delta Current and Delta History of <DeltaCurrenttablename>

    Examples:
      |numberOfRecords |DeltaHistorytablename                    |DeltaCurrenttablename               |
      | 5              |ck_delta_history_package_work_part       |ck_delta_current_package_work       |
      | 5              |ck_delta_history_work_subject_area_part  |ck_delta_current_work_subject_area  |














