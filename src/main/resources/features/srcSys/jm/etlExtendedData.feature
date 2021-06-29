Feature:Validate data count for JM Extended tables

  #confluence link: https://confluence.cbsels.com/display/EPH/Product+Extended+Availability+Topic+Process.
  #confluence version: v37

   @JMETLExtended
  Scenario Outline: Verify Data for JM_ETL Extended tables is transferred from Inbound Tables
     Given Get the total count of JM ETL Extended Tables <tableName>
     When We know the total count of JM source tables <tableName>
     Then Compare count of JM source Inbound and JM ETL Extended <tableName> tables are identical
     Given Get the <countOfRandomIds> of JM ETL Extended data from Inbound Tables <tableName>
     Then  Get the Data from the Source Tables <tableName>
     And   Data from the JM ETL Extended Tables to compare source Check <tableName>
     Then  Compare data of JM ETL Extended Tables and source <tableName> tables are identical
    Examples:
      | tableName                               |countOfRandomIds|
      |jnl_new_fulfilment_system                |2              |
      |jnl_fulfilment_system                     |2               |


 #Feature: Validating JM_Extended data (Count and Data Checks) between JM and product Extended DB

  @JMETLExtended
  Scenario Outline: Validate Count Check between JM and Product Extended DB
    Given We know the count of JM Extended data in JM
    Then Get the count for Product Extended DB
    And Compare the count for JM Extended Data between JM and Product Extended
    Given We get the <numberOfRecords> random JM Extended ids
    When We get the JM Extended DB records
    Then We get the ProductExtended records
    And Compare JM records in between JM Extended DB and Product Extended DB
    Examples:
      | numberOfRecords |
      | 5               |


