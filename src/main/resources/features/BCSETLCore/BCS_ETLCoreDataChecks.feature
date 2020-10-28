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


  @BCSCore
  Scenario Outline: Verify Data for BCS Core Current_History tables are transferred from Current Tables
    Given Get the <countOfRandomIds> of BCS Core data from Current Tables <sourceTable>
    When Get the Data from the BCS Core Current Tables <sourceTable>
    Then We Get the records from transform BCS Current History <targetTable>
    And Compare the records of BCS Core current and BCS Current_History <targetTable>
    Examples:
      | sourceTable                           |  targetTable                                        |countOfRandomIds     |
      | etl_accountable_product_current_v     |  etl_transform_history_accountable_product_part     |   10                |
      | etl_manifestation_current_v           |  etl_transform_history_manifestation_part           |   10                |
      |etl_manifestation_identifier_current_v |  etl_transform_history_manifestation_identifier_part|   10                |
   #  |   etl_person_current_v                | etl_transform_history_person_part                   |   10                |
      |   etl_product_current_v                | etl_transform_history_product_part                 |   10                |
      | etl_work_person_role_current_v         | etl_transform_history_work_person_role_part        |   10                |
      | etl_work_relationship_current_v        | etl_transform_history_work_relationship_part       |   10                |
      |etl_work_current_v                       |etl_transform_history_work_part                    |   10                |
      | etl_work_identifier_current_v           | etl_transform_history_work_identifier_part        |   10                |

  @BCSCore
  Scenario Outline: Verify Data for BCS Core Person_Current_History tables are transferred from Person Current Tables
    Given Get the <countOfRandomIds> of BCS Core data from Person Current Tables
    When Get the Data from the BCS Core Person Current Tables
    Then We Get the records from transform BCS Person Current History
    And Compare the records of BCS Core Person current and BCS Person Current_History
    Examples:
         |countOfRandomIds     |
         |   10                |