Feature:Validate data count for PRM between Oracle and Data Lake - Inbound

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




