Feature:Validate data checks for BCS ETL Core in Data Lake Access Layer

#  Created by Dinesh on 06/10/2020

  @BCSCore
  Scenario Outline: Verify Data for BCS_ETL Core tables is transferred from Inbound Tables
    Given Get the <countOfRandomIds> of BCS Core data from Current Tables <tableName>
    Then  Get the Data from the Inbound Tables <tableName>
    And   Get the Data from the BCS Core Current Tables <tableName>
    Then  Compare data of BCS Inbound and BCS Core <tableName> tables are identical
    Examples:
      | tableName                                |countOfRandomIds|
    # |etl_accountable_product_current_v         |10              |
    #  |etl_manifestation_current_v               |10              |
     # |etl_person_current_v                      |10              | since sourceref integer after running the query the result not stored in the list.
     # |etl_product_current_v                     |10              |
     # |etl_work_person_role_current_v            |10              |
      |etl_work_relationship_current_v           |10              |
       # when ever in source query Date appears script failed bcoz of invalid format
      #|etl_work_current_v                        |10              |
      #|etl_work_identifier_current_v             |10              |
      #|etl_manifestation_identifier_current_v    |10              |


