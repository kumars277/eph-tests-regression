Feature:Validate data checks for BCS ETL Extended in Data Lake Access Layer

#  Created by Dinesh on 14/01/2021

  @BCSExtended
  Scenario Outline: Verify Data for BCS_ETL Extended tables is transferred from Inbound Tables
    Given Get the <countOfRandomIds> of BCS Extende data from Inbound Tables <tableName>
    Then  Get the Data from the BCS Extended Inbound Tables <tableName>
    And   Data from the BCS Extended Current Tables <tableName>
   Then  Compare data of BCS Inbound and BCS Extended <tableName> tables are identical
    Examples:
      | tableName                                               |countOfRandomIds |
      | etl_availability_extended_current_v                     |10             |
      | etl_manifestation_extended_current_v                    |10             |
      | etl_page_count_extended_current_v                       |10             |
      | etl_url_extended_current_v                              |10             |
      | etl_work_extended_current_v                             |10             |
      | etl_work_subject_area_extended_current_v                |10             |
      | etl_manifestation_restrictions_extended_current_v       |10             |
      | etl_product_prices_extended_current_v                   |10             |
      | etl_work_person_role_extended_current_v                 |10             |

  @BCSExtended
  Scenario Outline: Verify Data for BCS Extended Current_History tables are transferred from Current Tables
    Given Get the <countOfRandomIds> of BCS Extended data from Current Tables <sourceTable>
    When Data from the BCS Extended Current Tables <sourceTable>
    Then We Get the records from transform BCS Ext Current History <targetTable>
    And Compare the records of BCS Extended current and BCS Current_History <targetTable>
    Examples:
      | sourceTable                                           |  targetTable                                            |countOfRandomIds     |
      #|etl_availability_extended_current_v               |etl_transform_history_extended_availability_part              | 10                  |
      #|etl_manifestation_extended_current_v              |etl_transform_history_extended_manifestation_part             |10                   |
      #|etl_page_count_extended_current_v                 |etl_transform_history_extended_page_count_part                |10                   |
      #|etl_url_extended_current_v                        |etl_transform_history_extended_url_part                       |10                   |
      #|etl_work_extended_current_v                       |etl_transform_history_extended_work_part                      |10                   |
      #|etl_work_subject_area_extended_current_v          |etl_transform_history_extended_work_subject_area_part         |10                   |
      #|etl_manifestation_restrictions_extended_current_v |etl_transform_history_extended_manifestation_restrictions_part|10                   |
      #|etl_product_prices_extended_current_v             |etl_transform_history_extended_product_prices_part            |10                   |
      |etl_work_person_role_extended_current_v           |etl_transform_history_extended_work_person_role_part          |10                   |