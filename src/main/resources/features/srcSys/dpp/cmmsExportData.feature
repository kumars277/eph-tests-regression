Feature:Validate data for DPP_EPH_CMMS_Export_dag tables

#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/119483627562926/EPH-CMMS+Integration

  @DPPCMMSEXPORT
  Scenario Outline: Verify that all CK data is transferred into DPP CMMS VIEW
    Given Get the count from the inbound for <DPPCMMSView>
    Then Get the count for <DPPCMMSView> views
    And Compare the count between the inbounds and <DPPCMMSView> views
    Given We get the <numberOfRecords> random CK CMMS View ids of <DPPCMMSView>
    When Get the Inbound Records for <DPPCMMSView>
    Then We get the records from the views of <DPPCMMSView>
    And Compare records between Inbound and the view of <DPPCMMSView>

    Examples:
      |numberOfRecords |DPPCMMSView                  |
      | 5              |cmms_workflow_v              |
      | 5              |cmms_work1_v                 |
      | 5              |cmms_work2_identifiers_v     |
      | 5              |cmms_work3_subject_areas_v   |
      | 5              |cmms_package1_v              |
      | 5              |cmms_package2_works_v        |
