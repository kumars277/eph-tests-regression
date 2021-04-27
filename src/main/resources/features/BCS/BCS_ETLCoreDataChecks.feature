Feature:Validate data checks for BCS ETL Core in Data Lake Access Layer

#  Created by Dinesh on 06/50/2020
  #confluence latest vesrion used:v.70 updated the script on 17/03/2020
   #confluence Link: https://confluence.cbsels.com/display/EPH/Core+Transformed+View+Mappings

  @BCSCore
  Scenario Outline: Verify Data for BCS_ETL Core tables is transferred from Inbound Tables
    Given Get the <countOfRandomIds> of BCS Core data from Inbound Tables <tableName>
    Then  Get the Data from the Inbound Tables <tableName>
    And   Data from the BCS Core Current Tables to compare Inbound Check <tableName>
    Then  Compare data of BCS Inbound and BCS Core <tableName> tables are identical
    Examples:
      | tableName                               |countOfRandomIds|
      |etl_accountable_product_current_v           |50              |
      |etl_manifestation_current_v                 |50              |
      |etl_person_current_v                        |50              |
      |etl_product_current_v                    |50              |
      |etl_work_person_role_current_v             |50              |
      |etl_work_relationship_current_v            |50              |
     |etl_work_current_v                            |50              |
      |etl_work_identifier_current_v             |50              |
      |etl_manifestation_identifier_current_v     |50              |
      |all_manifestation_statuses_v               |50              |
      |all_manifestation_pubdates_v               |50              |


  @BCSCore
  Scenario Outline: Verify Data for BCS Core Current_History tables are transferred from Current Tables
    Given Get the <countOfRandomIds> of BCS Core data from Current Tables <sourceTable>
    When Get the Data from the BCS Core Current Tables <sourceTable>
    Then We Get the records from transform BCS Current History <targetTable>
    And Compare the records of BCS Core current and BCS Current_History <targetTable>
    Examples:
      | sourceTable                           |  targetTable                                        |countOfRandomIds     |
      | etl_accountable_product_current_v     |  etl_transform_history_accountable_product_part     |   50                |
      | etl_manifestation_current_v           |  etl_transform_history_manifestation_part           |   50                |
      |etl_manifestation_identifier_current_v |  etl_transform_history_manifestation_identifier_part|   50                |
      |   etl_person_current_v                   | etl_transform_history_person_part                   |   50                |
      |   etl_product_current_v                | etl_transform_history_product_part                 |   50                |
      | etl_work_person_role_current_v         | etl_transform_history_work_person_role_part        |   50                |
      | etl_work_relationship_current_v        | etl_transform_history_work_relationship_part       |   50                |
      |etl_work_current_v                       |etl_transform_history_work_part                    |   50                |
      | etl_work_identifier_current_v           | etl_transform_history_work_identifier_part        |   50                |

  @BCSCore
  Scenario Outline: Verify Data for BCS Core Transform_file tables are transferred from Current Tables
    Given Get the <countOfRandomIds> of BCS Core data from Current Tables <sourceTable>
    When Get the Data from the BCS Core Current Tables <sourceTable>
    Then We Get the records from transform BCS Transform_File <targetTable>
    And Compare the records of BCS Core current and BCS Transform_File <targetTable>
    Examples:
      | sourceTable                           |  targetTable                                              |countOfRandomIds     |
      | etl_accountable_product_current_v     |  etl_accountable_product_transform_file_history_part     |   50                |
      | etl_manifestation_current_v           |  etl_manifestation_transform_file_history_part                 |   50                |
      |etl_manifestation_identifier_current_v |  etl_manifestation_identifier_transform_file_history_part|   50                |
      |   etl_person_current_v                | etl_person_transform_file_history_part                         |   50                |
      |   etl_product_current_v                | etl_product_transform_file_history_part                       |   50                |
      | etl_work_person_role_current_v         | etl_work_person_role_transform_file_history_part              |   50                |
      | etl_work_relationship_current_v        | etl_work_relationship_transform_file_history_part             |   50                |
      |etl_work_current_v                       |etl_work_transform_file_history_part                          |   50                |
      | etl_work_identifier_current_v           | etl_work_identifier_transform_file_history_part              |   50                |


  @BCSCore
  Scenario Outline: Verify Data for BCS Core Delta_Current tables are transferred from Transform_file Tables
    Given Get the <countOfRandomIds> of BCS Core data from transform_file Tables <sourceTable>
    When Get the Data from the Difference of Current and Previous transform_file Tables <sourceTable>
    Then We Get the records from delta current table BCS core <targetTable>
    And Compare the records of BCS Core delta current and BCS diff of Transform_File <targetTable>
    Examples:
      | targetTable                               |  sourceTable                                                 |countOfRandomIds     |
      | etl_delta_current_accountable_product     |  etl_accountable_product_transform_file_history_part         |   50                |
      | etl_delta_current_manifestation           |  etl_manifestation_transform_file_history_part               |   50                |
      |etl_delta_current_manifestation_identifier |  etl_manifestation_identifier_transform_file_history_part    |   50                |
      |   etl_delta_current_person                | etl_person_transform_file_history_part                       |   50                |
      |   etl_delta_current_product               | etl_product_transform_file_history_part                      |   50                |
      | etl_delta_current_work_person_role        | etl_work_person_role_transform_file_history_part             |   50                |
      | etl_delta_current_work_relationship       | etl_work_relationship_transform_file_history_part            |   50                |
      |etl_delta_current_work                     |etl_work_transform_file_history_part                          |   50                |
      | etl_delta_current_work_identifier         | etl_work_identifier_transform_file_history_part              |   50                |


  @BCSCore
  Scenario Outline: Verify Data for BCS Core Delta_Current tables are transferred from Delta_Current Hist Tables
    Given Get the <countOfRandomIds> of BCS Core data from delta_Current_hist Tables <sourceTable>
    When Get the Data from the Delta_Current_History Tables <sourceTable>
    Then We Get the records from delta current table BCS core <targetTable>
    And Compare the records of BCS Core delta current and delta_Current_history <targetTable>
    Examples:
      | targetTable                               |  sourceTable                                        |countOfRandomIds     |
      | etl_delta_current_accountable_product     |  etl_delta_history_accountable_product_part         |   50                |
      | etl_delta_current_manifestation           |  etl_delta_history_manifestation_part               |   50                |
      |etl_delta_current_manifestation_identifier |  etl_delta_history_manifestation_identifier_part    |   50                |
      |   etl_delta_current_person                | etl_delta_history_person_part                       |   50                |
      |   etl_delta_current_product               | etl_delta_history_product_part                      |   50                |
      | etl_delta_current_work_person_role        | etl_delta_history_work_person_role_part             |   50                |
      | etl_delta_current_work_relationship       | etl_delta_history_work_relationship_part            |   50                |
      |etl_delta_current_work                     |etl_delta_history_work_part                          |   50                |
      | etl_delta_current_work_identifier         | etl_delta_history_work_identifier_part              |   50                |


  @BCSCore
  Scenario Outline: Verify Data from the difference of BCS Core Delta_Current and Current_history is transferred to BCS core exclude table
    Given Get the <countOfRandomIds> from diff of delta_current and current_hist tables <targetTable>
    When Get the records from the diff of delta_current and current_hist tables <targetTable>
    Then Get the records from <targetTable> exclude table
    And  Compare the records of Exclude with diff of delta_current and current_hist tables <targetTable>
    Examples:
      | targetTable                                              |  countOfRandomIds     |
      | etl_transform_history_accountable_product_excl_delta     |  50                |
      | etl_transform_history_manifestation_excl_delta           |  50                |
      |etl_transform_history_manifestation_identifier_excl_delta |  50                |
      |   etl_transform_history_person_excl_delta                | 50                |
      |   etl_transform_history_product_excl_delta               | 50                |
      | etl_transform_history_work_person_role_excl_delta        | 50                |
      | etl_transform_history_work_relationship_excl_delta       | 50                |
      |etl_transform_history_work_excl_delta                     |50                |
      | etl_transform_history_work_identifier_excl_delta         |50                |


  @BCSCore
  Scenario Outline: Verify Data from the difference of BCS Core Delta_Current and Exclude is transferred to BCS core Latest table
    Given Get the <countOfRandomIds> from sum of delta_current and exclude_delta tables <targetTable>
    When Get the records from the sum of delta_current and exclude_delta tables <targetTable>
    Then Get the records from <targetTable> BCS core latest table
    And  Compare the records of Latest with sum of delta_current and Exclude_Delta tables <targetTable>
    Examples:
      | targetTable                                               |  countOfRandomIds     |
      | etl_transform_history_accountable_product_latest          |  50                |
      | etl_transform_history_manifestation_latest                |  50               |
      |etl_transform_history_manifestation_identifier_latest      |  50                |
      |   etl_transform_history_person_latest                     | 50                |
      |   etl_transform_history_product_latest                    | 50               |
      | etl_transform_history_work_person_role_latest             | 50                |
      | etl_transform_history_work_relationship_latest            | 50                |
      |etl_transform_history_work_latest                          |50              |
      | etl_transform_history_work_identifier_latest              |50                |

