Feature:Validate data for DPP_EPH_EBTD_Inbound_dag tables

#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/119483632526251/DPP-INT-12+Availability+Flag+Import

  @DPPEBTDINBOUND
  Scenario Outline: Verify that all CK data is transferred between DPP EBTD VIEW and DPP EBTD tables
    Given We know the number of CK <DPPEBTDView> data in EBTD View
    Then Get the count for CK <DPPEBTDTable> EBTD Table
    And Compare the CK count for <DPPEBTDTable> table between EBTD View and EBTD Table
    Given We get the <numberOfRecords> random CK EBTD View ids of <DPPEBTDView>
    When We get the CK EBTD View Records from <DPPEBTDView>
    Then We get the CK EBTD Table records from <DPPEBTDTable>
    And Compare CK records in EBTD View and Table of <DPPEBTDTable>

    Examples:
      |numberOfRecords |DPPEBTDView               |DPPEBTDTable              |
      | 5              |ck_h300_availability_v    |ck_h300_availability      |


