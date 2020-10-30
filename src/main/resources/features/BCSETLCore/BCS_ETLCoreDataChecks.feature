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

  @BCSCore
  Scenario Outline: Verify Data for BCS Core Transform_file tables are transferred from Current Tables
    Given Get the <countOfRandomIds> of BCS Core data from Currhent Tables <sourceTable>
    When Get the Data from the BCS Core Current Tables <sourceTable>
    Then We Get the records from transform BCS Transform_File <targetTable>
    And Compare the records of BCS Core current and BCS Transform_File <targetTable>
    Examples:
      | sourceTable                           |  targetTable                                              |countOfRandomIds     |
      | etl_accountable_product_current_v     |  etl_accountable_product_transform_file_history_part     |   10                |
      | etl_manifestation_current_v           |  etl_manifestation_transform_file_history_part                 |   10                |
      |etl_manifestation_identifier_current_v |  etl_manifestation_identifier_transform_file_history_part|   10                |
   #  |   etl_person_current_v                | etl_person_transform_file_history_part                         |   10                |
      |   etl_product_current_v                | etl_product_transform_file_history_part                       |   10                |
      | etl_work_person_role_current_v         | etl_work_person_role_transform_file_history_part              |   10                |
      | etl_work_relationship_current_v        | etl_work_relationship_transform_file_history_part             |   10                |
      |etl_work_current_v                       |etl_work_transform_file_history_part                          |   10                |
      | etl_work_identifier_current_v           | etl_work_identifier_transform_file_history_part              |   10                |


  @BCSCore
  Scenario Outline: Verify Data for BCS Core Person_Transform_File tables are transferred from Person Current Tables
    Given Get the <countOfRandomIds> of BCS Core data from Person Current Tables
    When Get the Data from the BCS Core Person Current Tables
    Then We Get the records from Transform file Person of BCS Core
    And Compare the records of BCS Core Person current and BCS Person Transform_File
    Examples:
      |countOfRandomIds     |
      |   10                |


  @BCSCore
  Scenario Outline: Verify Data for BCS Core Delta_Current tables are transferred from Transform_file Tables
    Given Get the <countOfRandomIds> of BCS Core data from transform_file Tables <sourceTable>
    When Get the Data from the Difference of Current and Previous transform_file Tables <sourceTable>
    Then We Get the records from delta current table BCS core <targetTable>
    And Compare the records of BCS Core delta current and BCS diff of Transform_File <targetTable>
    Examples:
      | targetTable                               |  sourceTable                                                 |countOfRandomIds     |
      | etl_delta_current_accountable_product     |  etl_accountable_product_transform_file_history_part         |   10                |
      | etl_delta_current_manifestation           |  etl_manifestation_transform_file_history_part               |   10                |
      |etl_delta_current_manifestation_identifier |  etl_manifestation_identifier_transform_file_history_part    |   10                |
   #  |   etl_delta_current_person                | etl_person_transform_file_history_part                       |   10                |
      |   etl_delta_current_product               | etl_product_transform_file_history_part                      |   10                |
      | etl_delta_current_work_person_role        | etl_work_person_role_transform_file_history_part             |   10                |
      | etl_delta_current_work_relationship       | etl_work_relationship_transform_file_history_part            |   10                |
      |etl_delta_current_work                     |etl_work_transform_file_history_part                          |   10                |
      | etl_delta_current_work_identifier         | etl_work_identifier_transform_file_history_part              |   10                |

  @BCSCore
  Scenario Outline: Verify Data for BCS Core Person Delta_Current tables are transferred from Person Transform_file Tables
    Given Get the <countOfRandomIds> of BCS Core data from person transform_file Tables
    When Get the Data from the Difference of Current and Previous person transform_file Tables
    Then We Get the records from person delta current table BCS core
    And Compare the records of BCS Core person delta current and BCS person diff of Transform_File
    Examples:
      |countOfRandomIds     |
      |   10                |
