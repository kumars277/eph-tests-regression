Feature:Validate data for ck ETL tables

#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/90784826471/CK+Inbound

  @CKETLCORE
  Scenario Outline: Verify that all CK data is transferred between Delta History and Delta Current tables
    Given We know the number of CK <DeltaHistorytablename> data in Delta History
    Then Get the count for CK <DeltaCurrenttablename> Delta Current
    And Compare the CK count for <DeltaHistorytablename> table between History and Delta Current

#    Given We get the <numberOfReck_delta_history_package_partcords> random CK ids of <Currenttablename>
#    When We get the CK Current records from <Currenttablename>
#    Then We get the CK Inbound Source records from <InboundSourcetablename>
#    And Compare CK records in Inbound Source and Current of <InboundSourcetablename>

    Examples:
      |numberOfRecords |DeltaHistorytablename                    |DeltaCurrenttablename                    |
      | 5              |ck_delta_history_package_part            |ck_delta_current_package                 |
      | 5              |ck_delta_history_subject_area_part       |ck_delta_current_package_work            |
      | 5              |ck_delta_history_work_part               |ck_delta_current_package_work_url        |
      | 5              |ck_delta_history_package_work_part       |ck_delta_current_subject_area            |
      | 5              |ck_delta_history_work_subject_area_part  |ck_delta_current_work                    |
      | 5              |ck_delta_history_package_work_url_part   |ck_delta_current_work_subject_area       |













