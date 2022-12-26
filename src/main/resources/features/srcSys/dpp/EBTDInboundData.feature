Feature:Validate data for DPP_EPH_EBTD_Inbound_dag tables

#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/119483632526251/DPP-INT-12+Availability+Flag+Import

  @DPPEBTDINBOUND
  Scenario Outline: Verify that all CK data is transferred to DPP EBTD VIEW
    Given We know the number of CK Inbound data in EBTD View
    Then Get the count for CK <DPPEBTDView> EBTD Table
    And Compare the CK count for <DPPEBTDView> and Inbound
    Given We get the <numberOfRecords> random Ids from Inbound
    When We get the Records from Inbound DDL
    Then We get the CK EBTD View records
    And Compare CK records in EBTD View and Inbound

    Examples:
      |numberOfRecords |DPPEBTDView               |
      | 5              |ck_h300_availability_v    |



