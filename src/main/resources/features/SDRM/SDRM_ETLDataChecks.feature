Feature:Validate data count for SDRM in Data Lake Access Layer

#  Created by Ranjithkumar S on 02/11/2020

  @SDRM
  Scenario Outline: Verify Data for SDRM Current product is transferred from sdrm source inbound
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from sdrm inbound table
    Then  Get the data from sdrm transform current product
    And   Compare the records of SDRM Inbound and SDRM current product


    Examples:
      | tableName                        |    countOfRandomIds|
      |sdrm_inbound_part                |50               |

  @SDRM
  Scenario Outline: Verify Data for SDRM product history is transferred from sdrm current product
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from sdrm transform current product
    Then  Get the data from sdrm transform product history
    And   Compare the records of SDRM current product and SDRM transform product history
    Examples:
      | tableName                                               |    countOfRandomIds|
      |sdrm_transform_current_product_availability              |50               |


  @SDRM
  Scenario Outline: Verify Data for SDRM product file history is transferred from sdrm current product
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from sdrm transform current product
    Then  Get the data from sdrm transform product file history
    And   Compare the records of SDRM current product and SDRM transform product file history
    Examples:
      | tableName                                               |    countOfRandomIds|
      |sdrm_transform_current_product_availability              |50               |

  @SDRM
  Scenario Outline: Verify Data for SDRM Delta Product History is transferred from SDRM Delta Current Product
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from sdrm delta current product table
    Then  Get the data from sdrm delta product history product
    And   Compare the records of SDRM Delta current product and SDRM Delta product history


    Examples:
      | tableName                        |    countOfRandomIds|
      |sdrm_delta_current_product_availability              |50               |

  @SDRM
  Scenario Outline: Verify Data for SDRM History Excl delta is transferred from SDRM Delta Current Product and delta history table
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from sdrm delta current product and Delta Product History table
    Then  Get the data from sdrm history excl delta table
    And   Compare the records of SDRM Delta current and Delta History table with SDRM History Excl Delta table


    Examples:
      | tableName                        |    countOfRandomIds|
      |sdrm_transform_history_product_availability_part              |50               |

  @SDRM
  Scenario Outline: Verify Data for SDRM Transform latest product is transferred from SDRM Delta Current Product and History Excl Delta
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from sdrm delta current product and History Excl Delta table
    Then  Get the data from sdrm transform latest product table
    And   Compare the records of SDRM Delta current and History Excl delta table with SDRM transform latest product table


    Examples:
      | tableName                        |    countOfRandomIds|
      |sdrm_transform_latest_product_availability              |50               |

  @SDRM
  Scenario Outline: Verify Data for SDRM Delta Current is transferred from difference between SDRM Current and Prev File History
    Given We get the <countOfRandomIds> random SDRM ISBN ids <tableName>
    When  Get the data from difference between sdrm current and prev file history table
    Then  Get the data from sdrm delta current product table
    And   Compare the records of difference between SDRM Current and Prev file history with SDRM Delta current Product


    Examples:
      | tableName                        |    countOfRandomIds|
      |sdrm_transform_file_history_product_availability_part              |50               |
