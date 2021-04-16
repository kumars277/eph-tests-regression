Feature:Validate data count for BCS Extended tables

#  Created by Dinesh S on 05/01/2020
  #confluence link: https://confluence.cbsels.com/display/EPH/Extended+Transformed+View+Mappings
  #confluence version: v68



  @BCSExtendedCount
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
| etl_work_person_role_extended_current_v      |

  @BCSExtendedCount
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

  @BCSExtendedCount
  Scenario Outline: Verify Data count for BCS Extended history tables are transferred from current tables
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



  @BCSExtendedCount
  Scenario Outline: Verify Data count for BCS Extended transform_file tables are transferred from current tables
    Given Get the total count of BCS Extended from Current Tables <SourceTableName>
    Then Get the count of BCS Extended transform_file <TargettableName>
    And Compare BCS Extended count of current <SourceTableName> and tranform_file <TargettableName> are identical

    Examples:
      |SourceTableName                                      |TargettableName                                         |
      |etl_availability_extended_current_v                  |etl_availability_extended_transform_file_history_part     |
      |etl_manifestation_extended_current_v                 |etl_manifestation_extended_transform_file_history_part           |
      |etl_page_count_extended_current_v                    |etl_page_count_extended_transform_file_history_part                  |
      |etl_url_extended_current_v                           |etl_url_extended_transform_file_history_part                 |
      |etl_work_extended_current_v                          |etl_work_extended_transform_file_history_part        |
      |etl_work_subject_area_extended_current_v             |etl_work_subject_area_extended_transform_file_history_part       |
      |etl_manifestation_restrictions_extended_current_v    |etl_manifestation_restrictions_extended_transform_file_history_part                    |
      |etl_product_prices_extended_current_v                |etl_product_prices_extended_transform_file_history_part         |
      |etl_work_person_role_extended_current_v              |etl_work_person_role_extended_transform_file_history_part|

  @BCSExtendedCount
  Scenario Outline: Verify Data count for BCS Extended delta_current tables are transferred from transform_file tables
    Given Get the total count of BCS Extended transform_file by diff of current and previous timestamp <SourceTableName>
    Then We know the total count of BCS Extended delta current <TargetTableName>
    And Compare count of BCS Ext tranform_file <SourceTableName> and delta current <TargetTableName> are identical

    Examples:
      |TargetTableName                                        |SourceTableName                                         |
     |etl_delta_current_extended_availability                |etl_availability_extended_transform_file_history_part     |
     |etl_delta_current_extended_manifestation               |etl_manifestation_extended_transform_file_history_part           |
     |etl_delta_current_extended_page_count                  |etl_page_count_extended_transform_file_history_part                  |
    |etl_delta_current_extended_url                         |etl_url_extended_transform_file_history_part                 |
    |etl_delta_current_extended_work                        |etl_work_extended_transform_file_history_part        |
    |etl_delta_current_extended_work_subject_area           |etl_work_subject_area_extended_transform_file_history_part       |
     |etl_delta_current_extended_manifestation_restrictions  |etl_manifestation_restrictions_extended_transform_file_history_part                    |
     |etl_delta_current_extended_product_prices              |etl_product_prices_extended_transform_file_history_part         |
     |etl_delta_current_extended_work_person_role            |etl_work_person_role_extended_transform_file_history_part|

  @BCSExtendedCount
  Scenario Outline: Verify Data count for BCS Extended delta_current_exclude are transferred from delta_current and current_history tables
    Given Get the BCS Extended Ext total count difference between delta current and transform current history Table <TargetTable>
    Then Get the BCS Extended <TargetTable> exclude data count
    And Compare BCS Extended exclude count of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Examples:
      |FirstSourceTable                               |SecondSourceTable                                            |TargetTable                                          |
      |etl_delta_current_extended_availability        |etl_transform_history_extended_availability_part               |etl_transform_history_extended_availability_excl_delta |
      |etl_delta_current_extended_manifestation       |etl_transform_history_extended_manifestation_part              |etl_transform_history_extended_manifestation_excl_delta       |
      |etl_delta_current_extended_page_count          |etl_transform_history_extended_page_count_part                  |etl_transform_history_extended_page_count_excl_delta              |
      |etl_delta_current_extended_url                 |etl_transform_history_extended_url_part                        |etl_transform_history_extended_url_excl_delta             |
      |etl_delta_current_extended_work                | etl_transform_history_extended_work_part                      |etl_transform_history_extended_work_excl_delta    |
      |etl_delta_current_extended_work_subject_area   |etl_transform_history_extended_work_subject_area_part         |etl_transform_history_extended_work_subject_area_excl_delta   |
      |etl_delta_current_extended_manifestation_restrictions|etl_transform_history_extended_manifestation_restrictions_part|etl_transform_history_extended_manifestation_restrictions_excl_delta                |
      |etl_delta_current_extended_product_prices      |etl_transform_history_extended_product_prices_part            |etl_transform_history_extended_product_prices_excl_delta     |
      |etl_delta_current_extended_work_person_role    |etl_transform_history_extended_work_person_role_part          |etl_transform_history_extended_work_person_role_excl_delta |


  @BCSExtendedCount
  Scenario Outline: Verify Data count for BCS Extended delta_latest tables are transferred from delta_current and Current_Exclude tables
    Given Get the sum of total count between BCS Extended delta current and Current_Exclude Table <TargetTable>
    Then Get the BCS Extended <TargetTable> latest data count
    And Compare BCS Extended latest counts of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Examples:
      |FirstSourceTable                                       |SecondSourceTable                                                          |TargetTable                                      |
      |etl_delta_current_extended_availability                |etl_transform_history_extended_availability_excl_delta                     |etl_transform_history_extended_availability_latest |
      |etl_delta_current_extended_manifestation               |etl_transform_history_extended_manifestation_excl_delta                     |etl_transform_history_extended_manifestation_latest       |
      |etl_delta_current_extended_page_count                   |etl_transform_history_extended_page_count_excl_delta                            |etl_transform_history_extended_page_count_latest              |
     |etl_delta_current_extended_url                        |etl_transform_history_extended_url_excl_delta                                  |etl_transform_history_extended_url_latest             |
    |etl_delta_current_extended_work                      | etl_transform_history_extended_work_excl_delta                              |etl_transform_history_extended_work_latest    |
      |etl_delta_current_extended_work_subject_area         |etl_transform_history_extended_work_subject_area_excl_delta                 |etl_transform_history_extended_work_subject_area_latest   |
      |etl_delta_current_extended_manifestation_restrictions |etl_transform_history_extended_manifestation_restrictions_excl_delta        |etl_transform_history_extended_manifestation_restrictions_latest                |
      |etl_delta_current_extended_product_prices             |etl_transform_history_extended_product_prices_excl_delta                   |etl_transform_history_extended_product_prices_latest     |
      |etl_delta_current_extended_work_person_role            |etl_transform_history_extended_work_person_role_excl_delta             |etl_transform_history_extended_work_person_role_latest |


  @BCSExtendedCount
  Scenario Outline: Verify Duplicate Entry for BCS Extended in transform latest tables
    Given Get the BCS Extended duplicate count in <SourceTableName> table
    Then Check the BCS Extended count should be equal to Zero <SourceTableName>
    Examples:
      |SourceTableName                      |
      |etl_transform_history_extended_availability_latest           |
      |etl_transform_history_extended_manifestation_latest                 |
      |etl_transform_history_extended_page_count_latest                        |
      |etl_transform_history_extended_url_latest                       |
      |etl_transform_history_extended_work_latest              |
      |etl_transform_history_extended_work_subject_area_latest             |
      |etl_transform_history_extended_manifestation_restrictions_latest                          |
      |etl_transform_history_extended_product_prices_latest               |
      |etl_transform_history_extended_work_person_role_latest      |


