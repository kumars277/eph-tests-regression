Feature:SDRM ETL DAG - Count Check - Validate data count between ETL Tables

#Created by Ranjithkumar S on 23/10/2020
  #Confluence:https://confluence.cbsels.com/display/EPH/SDRM
  #Git for Query: https://github.com/elsevier-bts/eph-datalabs-dag/blob/master_v1/src/dag/resources/property_substituted/sdrm_inbound/eph_sdrm_transform_product_availability_v.txt


  @SDRM
  Scenario Outline: Count check between SDRM Inbound and SDRM Current Product Availability
    Given Get the total count of SDRM Data from Inbound Load <tableName>
    Then  We know the total count of Current SDRM data from <tableName>
    And Compare count of SDRM Inbound load with current <tableName> table are identical
    Examples:
      | tableName                                |
      |sdrm_transform_current_product_availability              |


  @SDRM
  Scenario Outline: Verify Data count for SDRM transform product history tables are transferred from transform current product table
    Given We know the total count of SDRM Current product data from <SourceTableName>
    Then Get the count of SDRM transform product history <TargettableName>
    And Check count of SDRM current product table <SourceTableName> and SDRM product history <TargettableName> are identical

    Examples:
      |SourceTableName                      |TargettableName                                 |
      |sdrm_transform_current_product_availability         |sdrm_transform_history_product_availability_part   |


  @SDRM
  Scenario Outline: Verify Data count for SDRM transform product file history tables are transferred from transform current product table
    Given We know the total count of SDRM Current product data from <SourceTableName>
    Then Get the count of SDRM transform product file history <TargettableName>
    And Check count of SDRM current product table <SourceTableName> and SDRM product file history <TargettableName> are identical

    Examples:
      |SourceTableName                      |TargettableName                                 |
      |sdrm_transform_current_product_availability         |sdrm_transform_file_history_product_availability_part   |

  @SDRM
  Scenario Outline: Verify Data count for SDRM delta product history tables are transferred from SDRM delta current product table
    Given We know the total count of SDRM Delta Current product data from <SourceTableName>
    Then Get the count of SDRM delta product history <TargettableName>
    And Check count of SDRM Delta current product table <SourceTableName> and SDRM delta product history <TargettableName> are identical

    Examples:
      |SourceTableName                      |TargettableName                                 |
      |sdrm_delta_current_product_availability         |sdrm_delta_history_product_availability_part   |


  @SDRM
  Scenario Outline: Verify Data count for SDRM history excl delta tables are transferred from SDRM delta current product table and SDRM Product history table
    Given We know the total count of difference between SDRM Delta Current product data and SDRM Product History <SourceTableName>
    Then Get the count of SDRM transform history excl delta <TargettableName>
    And Check count of between SDRM Delta Current product data and SDRM Product History <SourceTableName> and SDRM transform history excl delta <TargettableName> are identical

    Examples:
      |SourceTableName                      |TargettableName                                 |
      |sdrm_delta_current_product_availability         |sdrm_transform_history_excl_delta   |

  @SDRM
  Scenario Outline: Verify Data count for SDRM transform latest product availability tables are transferred from SDRM delta current product table and SDRM history excl delta table
    Given We know the total count of SDRM Delta Current product data and SDRM History Excl Delta <SourceTableName>
    Then Get the count of SDRM transform latest product <TargettableName>
    And Check count of between SDRM Delta Current product data and SDRM Product History <SourceTableName> and SDRM transform latest product <TargettableName> are identical

    Examples:
      |SourceTableName                      |TargettableName                                 |
      |sdrm_delta_current_product_availability         |sdrm_transform_latest_product_availability   |

  @SDRM
  Scenario Outline: Verify Duplicate Entry for SDRM transform latest tables
    Given Get the SDRM Duplicate count in <SourceTableName> table
    Then Check the SDRM count should be equal to Zero <SourceTableName>
    Examples:
      |SourceTableName                      |
      |sdrm_transform_latest_product_availability           |

  @SDRM
  Scenario Outline: Verify Data count for SDRM delta current product tables are transferred from difference between current and previous timestamps of the SDRM transform file history product table
    Given We know the total count of difference between Current and Previous timestamps of the SDRM transform file history <SourceTableName>
    Then Get the count of SDRM delta current product <TargettableName>
    And Check count of difference between current and previous timestamps of the SDRM transform file history product <SourceTableName> and SDRM delta current product <TargettableName> are identical

    Examples:
      |SourceTableName                      |TargettableName                                 |
      |sdrm_transform_file_history_product_availability_part         |sdrm_delta_current_product_availability   |

