Feature:Validate data for DPP_EPH_CMMS_INBOUND_dag tables

  @DPPCMMSINBOUND
  Scenario Outline: Verify that all data is transferred between CMMS Inbound View and Tables
    Given We know the number of <CMMSInboundView> data in CMMS View
#    Then Get the count for <DPPReportsTable> Reports Table
#    And Compare the count for <DPPReportsTable> table between Reports View and Reports Table
#    Given We get the <numberOfRecords> random CK Reports View ids of <DPPReportsView>
#    When We get the Reports View Records from <DPPReportsView>
#    Then We get the Reports Table records from <DPPReportsTable>
#    And Compare records in Reports View and Table of <DPPReportsTable>

    Examples:
      |numberOfRecords   |CMMSInboundView                 |CMMSInboundTable                   |
      | 5                |ck_workflow_tableau_v           |cmms_durable_url_inbound_part                |

