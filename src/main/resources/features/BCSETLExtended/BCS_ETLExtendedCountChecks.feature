Feature:Validate data count for BCS Extended tables

#  Created by Dinesh S on 05/01/2020



  @BCSExtended
  Scenario Outline: Verify Data Count for BCS Extended tables is transferred from Inbound Tables
    Given Get the total count of BCS Extended from Current Tables <tableName>
    When We know the total count of BCS Extended Inbound tables <tableName>
    Then Compare count of BCS Inbound and BCS Extended <tableName> tables are identical
    Examples:
      | tableName                                |
      | etl_availability_extended_current_v      |
      | etl_manifestation_extended_current_v      |
      | etl_page_count_extended_current_v      |
      | etl_url_extended_current_v      |
      | etl_work_extended_current_v      |
     | etl_work_subject_area_extended_current_v      |
      | etl_manifestation_restrictions_extended_current_v      |
      | etl_product_prices_extended_current_v      |
      #| etl_work_person_role_extended_current_v      |


  @BCSExtended
  Scenario Outline: Verify Data count for BCS Extended delta history tables are transferred from delta_current tables
    Given We know the total count of BCS Extended delta current <SourceTableName>
    Then Get the count of BCS Extended delta history of current timestamp from <TargettableName>
    And Check count of BCS Extended delta current table <SourceTableName> and delta history <TargettableName> are identical
    Examples:
      |SourceTableName                                            |TargettableName                                  |
      |etl_delta_current_extended_availability                    |etl_delta_history_extended_availability_part       |
      |etl_delta_current_extended_manifestation                   |etl_delta_history_extended_manifestation_part             |
      |etl_delta_current_extended_page_count                      |etl_delta_history_extended_page_count_part                    |
      |etl_delta_current_extended_url                             |etl_delta_history_extended_url_part                   |
      |etl_delta_current_extended_work                            |etl_delta_history_extended_work_part          |
      |etl_delta_current_extended_work_subject_area               |etl_delta_history_extended_work_subject_area_part         |
      |etl_delta_current_extended_manifestation_restrictions      |etl_delta_history_extended_manifestation_restrictions_part                      |
      |etl_delta_current_extended_product_prices                  |etl_delta_history_extended_product_prices_part           |
      |etl_delta_current_extended_work_person_role                |etl_delta_history_extended_work_person_role_part  |

  @BCSExtended
  Scenario Outline: Verify Data count for BCS core history tables are transferred from current tables
    Given Get the total count of BCS Extended from Current Tables <SourceTableName>
    Then Get the count of BCS Extended history <TargettableName>
    And Compare count of BCS Extended current <SourceTableName> and history <TargettableName> are identical

    Examples:
      |SourceTableName                                   |TargettableName                                    |
      |etl_availability_extended_current_v               |etl_transform_history_extended_availability_part     |
      |etl_manifestation_extended_current_v              |etl_transform_history_extended_manifestation_part           |
      |etl_page_count_extended_current_v                 |etl_transform_history_extended_page_count_part                  |
      |etl_url_extended_current_v                        |etl_transform_history_extended_url_part                 |
      |etl_work_extended_current_v                       |etl_transform_history_extended_work_part        |
      |etl_work_subject_area_extended_current_v          |etl_transform_history_extended_work_subject_area_part       |
      |etl_manifestation_restrictions_extended_current_v |etl_transform_history_extended_manifestation_restrictions_part                    |
      |etl_product_prices_extended_current_v             |etl_transform_history_extended_product_prices_part         |
      |etl_work_person_role_extended_current_v           |etl_transform_history_extended_work_person_role_part|