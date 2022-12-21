Feature:Validate data for DPP_EPH_CMMS_INBOUND_dag tables

  @DPPCMMSINBOUND
  Scenario Outline: Verify that all data is transferred between CMMS Inbound View and creation queries
    Given We know the number of <CMMSInboundView> data in CMMS View
    Then Get the Query count for <CMMSInboundView> CMMSInbound
    And Compare the count for <CMMSInboundView> table between CMMS Inbound View and CMMSInbound query
    Given We get the <numberOfRecords> random CMMS Inbound View ids of <CMMSInboundView>
    When We get the CKCMMSInbound View Records from <CMMSInboundView>
    Then We get the CKCMMSInbound Query records from <CMMSInboundView>
    And Compare records in CKCMMSInbound View and CMMSInbound query of <CMMSInboundView>
#
    Examples:
      |numberOfRecords   |CMMSInboundView                 |
      | 5                |cmms_durable_url1_form_v           |
      |5                 |cmms_durable_url2_form_v           |
      |5                 |cmms_durable_url3_form_v           |
      |5                 |cmms_durable_url_transform_v       |


    #'cmms_durable_url_inbound_part','cmms_transform_current_durable_url','cmms_transform_file_history_durable_url_part',