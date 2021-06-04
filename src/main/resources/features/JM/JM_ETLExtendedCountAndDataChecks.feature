Feature:Validate data count for JM Extended tables

#  Created by Dinesh S on 20/04/2020
  #confluence link: https://confluence.cbsels.com/display/EPH/Product+Extended+Availability+Topic+Process.
  #confluence version: v37



  @JMETLExtended
  Scenario Outline: Verify Data Count for JM ETL Extended tables is transferred from Inbound Tables
    Given Get the total count of JM ETL Extended Tables <tableName>
    When We know the total count of JM source tables <tableName>
    Then Compare count of JM source Inbound and JM ETL Extended <tableName> tables are identical
    Examples:
      | tableName                                |
      |jnl_fulfilment_system                 |
      |jnl_new_fulfilment_system              |


   @JMETLExtended
  Scenario Outline: Verify Data for JM_ETL Extended tables is transferred from Inbound Tables
    Given Get the <countOfRandomIds> of JM ETL Extended data from Inbound Tables <tableName>
    Then  Get the Data from the Source Tables <tableName>
    And   Data from the JM ETL Extended Tables to compare source Check <tableName>
    Then  Compare data of JM ETL Extended Tables and source <tableName> tables are identical
    Examples:
      | tableName                               |countOfRandomIds|
      |jnl_new_fulfilment_system                |2              |
      |jnl_fulfilment_system                     |2               |


  Scenario: Verify Data driven
   # Given We have the expected data
  # Then Verify data with file upoad
   And Check the Count from Excel

