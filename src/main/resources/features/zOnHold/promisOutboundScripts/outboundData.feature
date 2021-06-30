Feature:Validate data for PRM between Oracle and Data Lake - Inbound

Feature:Validate data count for PRM between Oracle and Data Lake - Inbound

  @PRMDL
  Scenario Outline: Verify that all PRM data is transferred from PRM Oracle to DL Inbound
    Given We know the number of PRM <tableName> data in Oracle
    Then Get the PRM <tableName> data is in the DL
    And Compare the PRM count for <tableName> table between Oracle and DL are equal
    Examples:
      | tableName                |
      |PRMCLSCODT                |
      |PRMCLST                   |
      |PRMLONDEST                |
      |PRMPRICEST                |
      |PRMPUBINFT                |
      |PRMPUBRELT                |

  @PRMDL
  Scenario Outline: Verify that all PRM PRMAUTPUBT is transferred from PRM Oracle to DL Inbound
    Given We get the <numberOfRecords> random PRM ids of <table>
    When We get the PRM PRMAUTPUBT records from Oracle of <table>
    Then We get the PRM PRMAUTPUBT records from DL of <table>
   And Compare PRM PRMAUTPUBT records in PRM Oracle and DL of <table>
    Examples:
      | numberOfRecords | table  |
      | 15              | PRMAUTPUBT|
  @PRMDL
  Scenario Outline: Verify that all PRM PRMCLSCODT is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMCLSCODT records from Oracle of <table>
    Then We get the PRM PRMCLSCODT records from DL of <table>
    And Compare PRM PRMCLSCODT records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 15               | PRMCLSCODT|
  @PRMDL
  Scenario Outline: Verify that all PRM PRMCLST is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMCLST records from Oracle of <table>
    Then We get the PRM PRMCLST records from DL of <table>
    And Compare PRM PRMCLST records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 15               | PRMCLST|
  @PRMDL
  Scenario Outline: Verify that all PRM PRMLONDEST is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMLONDEST records from Oracle of <table>
    Then We get the PRM PRMLONDEST records from DL of <table>
    And Compare PRM PRMLONDEST records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 15                 | PRMLONDEST|
  @PRMDL
  Scenario Outline: Verify that all PRM PRMPRICEST is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMPRICEST records from Oracle of <table>
    Then We get the PRM PRMPRICEST records from DL of <table>
    And Compare PRM PRMPRICEST records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 15                 | PRMPRICEST|
  @PRMDL
  Scenario Outline: Verify that all PRM PRMPUBINFT is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMPUBINFT records from Oracle of <table>
    Then We get the PRM PRMPUBINFT records from DL of <table>
    And Compare PRM PRMPUBINFT records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 15                 | PRMPUBINFT|
@PRMDL
  Scenario Outline: Verify that all PRM PRMPUBRELT is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMPUBRELT records from Oracle of <table>
    Then We get the PRM PRMPUBRELT records from DL of <table>
    And Compare PRM PRMPUBRELT records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 15                 | PRMPUBRELT|
