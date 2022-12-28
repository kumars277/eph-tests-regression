Feature:Validate data for DPP_Reports_dag tables

  @DPPReports
  Scenario Outline: Verify that all data is transferred to DPP Reports tables
    Given We know the count from the inbound tables <DPPReportsTable>
    Then Get the count for <DPPReportsTable> Reports Table
    And Compare the count between Inbound and Reports Table <DPPReportsTable>
    Given We get the <numberOfRecords> randomIds for <DPPReportsTable>
    When We get the Records from Inbound <DPPReportsTable>
    Then We get the Reports Table records from <DPPReportsTable>
    And Compare records in Reports View and Table of <DPPReportsTable>

    Examples:
      |numberOfRecords |DPPReportsTable                   |
      | 5              |ck_workflow_tableau               |
      | 5              |ck_workflow_control_p1            |
      | 5              |ck_workflow_control_p2            |
      | 5              |ck_workflow_tableau_package_works |
      | 5              |ck_transaction_workflow           |
