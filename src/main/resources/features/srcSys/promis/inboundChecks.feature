#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45487295035/PROMIS+Inbound

Feature:Validate data for Promis Inbound tables

  @PROMISINBOUND
  Scenario Outline: Verify that all Promis data is transferred between inbound and current tables
    Given We know the number of Promis <Inboundtablename> data in inbound
    Then Get the count for Promis <Currenttablename> current
    And Compare the Promis count for <Inboundtablename> table between inbound and current
    Given We get the <numberOfRecords> random Promis ids of <Currenttablename>
    When We get the Promis Inbound records from <Inboundtablename>
    Then We get the Promis Current records from <Currenttablename>
    And Compare Promis records in Inbound and Current of <Inboundtablename>

    Examples:
      |numberOfRecords |Inboundtablename       |Currenttablename             |
      | 5               |promis_prmautpubt_part  |promis_prmautpubt_current  |
      | 5               |promis_prmclscodt_part  |promis_prmclscodt_current  |
      | 5               |promis_prmclst_part     |promis_prmclst_current     |
      | 5               |promis_prm_londes_2_html_part  |promis_prm_londes_2_html_current  |
      | 5               |promis_prmpricest_part  |promis_prmpricest_current  |
      | 5               |promis_prmpubinft_part  |promis_prmpubinft_current  |
      | 5               |promis_prmpubrelt_part  |promis_prmpubrelt_current  |
      | 5               |promis_prmincpmct_part  |promis_prmincpmct_current  |
      | 5               |promis_prmpmccodt_part  |promis_prmpmccodt_current  |














