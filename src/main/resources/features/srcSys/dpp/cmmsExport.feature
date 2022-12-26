Feature:Validate data for DPP_EPH_CMMS_Export_dag tables

#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/119483627562926/EPH-CMMS+Integration

  @DPPCMMSEXPORT
  Scenario Outline: Verify that all CK data is transferred between DPP CMMS VIEW and DPP CMMS tables
    Given We know the number of CK <DPPCMMSView> data in CMMS Outbound View
    Then Get the count for CK <DPPCMMSTable> CMMS Table
    And Compare the CK count for <DPPCMMSTable> table between CMMS View and CMMS Table
    Given We get the <numberOfRecords> random CK CMMS View ids of <DPPCMMSView>
    When We get the CK CMMS View Records from <DPPCMMSView>
    Then We get the CK CMMS Table records from <DPPCMMSTable>
    And Compare CK records in CMMS View and Table of <DPPCMMSTable>

    Examples:
      |numberOfRecords |DPPCMMSView                  |DPPCMMSTable             |
      | 5              |cmms_workflow_v              |cmms_workflow            |
      | 5              |cmms_work1_v                 |cmms_work1               |
      | 5              |cmms_work2_identifiers_v     |cmms_work2_identifiers   |
      | 5              |cmms_work3_subject_areas_v   |cmms_work3_subject_areas |
      | 5              |cmms_package1_v              |cmms_package1            |
      | 5              |cmms_package2_works_v        |cmms_package2_works      |


#scenario should be DDL Query (i.e view's show/edit query) Vs select * from cmms_workflow_v