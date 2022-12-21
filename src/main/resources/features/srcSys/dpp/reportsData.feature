Feature:Validate data for DPP_Reports_dag tables

  @DPPReports
  Scenario Outline: Verify that all data is transferred between DPP Reports VIEW and DPP Reports tables
    Given We know the number of <DPPReportsView> data in Reports View
    Then Get the count for <DPPReportsTable> Reports Table
    And Compare the count for <DPPReportsTable> table between Reports View and Reports Table
    Given We get the <numberOfRecords> random CK Reports View ids of <DPPReportsView>
    When We get the Reports View Records from <DPPReportsView>
    Then We get the Reports Table records from <DPPReportsTable>
    And Compare records in Reports View and Table of <DPPReportsTable>

    Examples:
      |numberOfRecords |DPPReportsView                       |DPPReportsTable                   |
      | 5              |ck_workflow_tableau_v                |ck_workflow_tableau               |
      | 5              |ck_workflow_control_p1_v             |ck_workflow_control_p1            |
      | 5              |ck_workflow_control_p2_v             |ck_workflow_control_p2            |
      | 5              |ck_workflow_tableau_package_works_v  |ck_workflow_tableau_package_works |
      | 5              |ck_transaction_workflow_v            |ck_transaction_workflow           |

