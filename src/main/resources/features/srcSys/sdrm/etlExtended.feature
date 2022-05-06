Feature:Validate data between SDRM ETL Tables

  #Confluence:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45487293809/SDRM+Inbound
  #Git for Query: https://github.com/elsevier-bts/eph-datalabs-dag/blob/master_v1/src/dag/resources/property_substituted/sdrm_inbound/eph_sdrm_transform_product_availability_v.txt

  @SDRM
  Scenario Outline: Check between SDRM Inbound and SDRM Current Product Availability
    Given Get the total count of SDRM Data from Inbound Load
    Then  We know the total count Current SDRM data from Current Product Availability
    And Compare count of SDRM Inbound load with current Product Availability table are identical
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from sdrm inbound table
    Then  Get the data from sdrm transform current product
    And we compare the records of SDRM Inbound and SDRM current product
    Examples:
      | tableName                                               |countOfRandomIds |
      |sdrm_inbound_part                                        |50               |

  @SDRM
  Scenario Outline: Verify Data for SDRM transform product file history tables are transferred from transform current product table
   Given We know the total SDRM Current product availability data
    Then Get the count of SDRM transform product file history
    And Check count of SDRM current product availability table and SDRM product availability file history are identical
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from sdrm transform current product
    Then Get the data from sdrm transform product file history
    And we compare the records of SDRM current product and SDRM transform product file history
    Examples:
      | tableName                                               |    countOfRandomIds|
        |sdrm_transform_current_product_availability              |50               |

  @SDRM
  Scenario Outline: Verify Data for SDRM transform latest product availability tables are transferred from SDRM delta current product table and SDRM history excl delta table
    Given We know the total SDRM Delta Current product availability data and SDRM prod availability History Excl Delta
    Then Get the count of SDRM transform prod availability latest table
    And Check count of between SDRM Delta Current product availability data and SDRM Product availability History and SDRM transform latest product availability are identical
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When Get the data from sdrm delta current product and History Excl Delta table
    Then Get the data from sdrm transform latest product table
    And we compare the records of SDRM Delta current and History Excl delta table with SDRM transform latest product table
    Examples:
      | tableName                                              |    countOfRandomIds|
      |sdrm_transform_latest_product_availability              |50               |

  @SDRM
  Scenario Outline: Verify Duplicate Entry for SDRM transform latest tables
    Given Get the SDRM Duplicate count in <SourceTableName> table
    Then Check the SDRM count should be equal to Zero <SourceTableName>
    Examples:
      |SourceTableName                                      |
      |sdrm_transform_latest_product_availability           |
############################3
  @notUsed
  Scenario Outline: Verify Data for SDRM delta current product tables are transferred from difference between current and previous timestamps of the SDRM transform file history product table
    Given We know the total count of difference between Current and Previous timestamps of the SDRM transform product availability file history
    Then Get the count of SDRM delta current product availability
    And Check count of difference between current and previous timestamps of the SDRM transform file history product availability and SDRM delta current product availability are identical
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When Get the data from difference between sdrm current and prev file history table
    Then Get the data from sdrm delta current product table
    And we compare the records of difference between SDRM Current and Prev file history with SDRM Delta current Product
    Examples:
      | tableName                                                         |    countOfRandomIds|
      |sdrm_transform_file_history_product_availability_part              |50               |

  @notUsed
  Scenario Outline: Verify Data for SDRM history excl delta tables are transferred from SDRM delta current product table and SDRM Product history table
    Given We know the total count of difference between SDRM Delta Current product availability data and SDRM Product availability History
    Then Get the count of SDRM transform product availability history excl delta
    And Check count of between SDRM Delta Current product availability data and SDRM Product availability History and SDRM transform availability history excl delta are identical
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When Get the data from sdrm delta current product and Delta Product History table
    Then Get the data from sdrm history excl delta table
    And we compare the records of SDRM Delta current and Delta History table with SDRM History Excl Delta table
    Examples:
      | tableName                                                     |    countOfRandomIds|
      |sdrm_transform_history_product_availability_part               |50                  |

  @notUsed
  Scenario Outline: Verify Data count for SDRM delta product history tables are transferred from SDRM delta current product table
    Given We know the total count of SDRM Delta Current product availability data
    Then Get the count of SDRM delta product availability history
    And Check count of SDRM Delta current product availability table and SDRM delta product availability history are identical
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When Get the data from sdrm delta current product table
    Then Get the data from sdrm delta product history product
    And compare the records of SDRM Delta current product and SDRM Delta product history
    Examples:
      | tableName                                           |    countOfRandomIds|
      |sdrm_delta_current_product_availability              |50               |


  @notUsed
  Scenario Outline: Verify Data for SDRM transform product history tables are transferred from transform current product table
    Given We know the total count of SDRM Current product availability data
    Then Get the count of SDRM transform product availability history
    And Check count of current product availability and product history availability are identical
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When Get the data from sdrm transform current product
    Then Get the data from sdrm transform product history
    And we compare the records of SDRM current product and SDRM transform product history
    Examples:
      | tableName                                               |    countOfRandomIds|
      |sdrm_transform_current_product_availability              |50               |
