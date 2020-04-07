Feature:Validate data for PRM between Oracle and Data Lake - Inbound

  Scenario Outline: Verify that all PRM PRMAUTPUBT is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMAUTPUBT records from Oracle of <table>
    Then We get the PRM PRMAUTPUBT records from DL of <table>
   And Compare PRM PRMAUTPUBT records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10               | PRMAUTPUBT|

  Scenario Outline: Verify that all PRM PRMCLSCODT is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMCLSCODT records from Oracle of <table>
    Then We get the PRM PRMCLSCODT records from DL of <table>
    And Compare PRM PRMCLSCODT records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10                 | PRMCLSCODT|

  Scenario Outline: Verify that all PRM PRMCLST is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMCLST records from Oracle of <table>
    Then We get the PRM PRMCLST records from DL of <table>
    And Compare PRM PRMCLST records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10                 | PRMCLST|

  Scenario Outline: Verify that all PRM PRMLONDEST is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMLONDEST records from Oracle of <table>
    Then We get the PRM PRMLONDEST records from DL of <table>
    And Compare PRM PRMLONDEST records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10                 | PRMLONDEST|

  Scenario Outline: Verify that all PRM PRMPRICEST is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMPRICEST records from Oracle of <table>
    Then We get the PRM PRMPRICEST records from DL of <table>
    And Compare PRM PRMPRICEST records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10                 | PRMPRICEST|

  Scenario Outline: Verify that all PRM PRMPUBINFT is transferred from PRM Oracle to DL Inbound
    Given We get the <countOfRandomIds> random PRM ids of <table>
    When We get the PRM PRMPUBINFT records from Oracle of <table>
    Then We get the PRM PRMPUBINFT records from DL of <table>
    And Compare PRM PRMPUBINFT records in PRM Oracle and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10                 | PRMPUBINFT|
#
#  Scenario Outline: Verify that all PRM PRMPUBRELT is transferred from PRM Oracle to DL Inbound
#    Given We get the <countOfRandomIds> random PRM ids of <table>
#    When We get the PRM PRMPUBRELT records from Oracle of <table>
#    Then We get the PRM PRMPUBRELT records from DL of <table>
#    And Compare PRM PRMPUBRELT records in PRM Oracle and DL of <table>
#    Examples:
#      | countOfRandomIds | table  |
#      | 2000                 | PRMPUBRELT|
