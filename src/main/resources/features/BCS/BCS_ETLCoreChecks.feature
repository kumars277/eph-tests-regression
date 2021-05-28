Feature:Validate data count for BCS ETL Core in Data Lake Access Layer

#  Created by Dinesh on 30/09/2020
  #confluence latest vesrion used:v.70 updated the script on 17/03/2020
  #confluence Link: https://confluence.cbsels.com/display/EPH/Core+Transformed+View+Mappings



  @BCSCore
  Scenario Outline: Verify Data Count for BCS_ETL Core tables is transferred from Inbound Tables
    Given Get the total count of BCS Core from Current Tables <tableName>
    Then  We know the total count of Inbound tables <tableName>
    And Compare count of BCS Inbound and BCS Core <tableName> tables are identical
    Given Get the <countOfRandomIds> of BCS Core data from Inbound Tables <tableName>
    Then  Get the Data from the Inbound Tables <tableName>
    And   Data from the BCS Core Current Tables to compare Inbound Check <tableName>
    Then  Compare data of BCS Inbound and BCS Core <tableName> tables are identical
    Examples:
      | tableName                                   |countOfRandomIds|
      |etl_accountable_product_current_v            |50              |
      |etl_manifestation_current_v                  |50              |
      |etl_person_current_v                         |50              |
      |etl_product_current_v                        |50              |
      |etl_work_person_role_current_v               |50              |
      |etl_work_relationship_current_v              |50              |
      |etl_work_current_v                           |50              |
      |etl_work_identifier_current_v                |50              |
      |etl_manifestation_identifier_current_v       |50              |
      |all_manifestation_statuses_v                 |50              |
      |all_manifestation_pubdates_v                 |50              |

  @BCSCore
  Scenario Outline: Verify Data count for BCS core delta history tables are transferred from delta_current tables
    Given We know the total count of delta current <SourceTableName>
    Then Get the count of delta history of current timestamp from <TargettableName>
    And Check count of delta current table <SourceTableName> and delta history <TargettableName> are identical

    Examples:
      |SourceTableName                                |TargettableName                                  |
      |etl_delta_current_accountable_product          |etl_delta_history_accountable_product_part       |
      |etl_delta_current_manifestation                |etl_delta_history_manifestation_part             |
      |etl_delta_current_person                       |etl_delta_history_person_part                    |
      |etl_delta_current_product                      |etl_delta_history_product_part                   |
      |etl_delta_current_work_person_role             |etl_delta_history_work_person_role_part          |
      |etl_delta_current_work_relationship            |etl_delta_history_work_relationship_part         |
      |etl_delta_current_work                         |etl_delta_history_work_part                      |
      |etl_delta_current_work_identifier              |etl_delta_history_work_identifier_part           |
      |etl_delta_current_manifestation_identifier     |etl_delta_history_manifestation_identifier_part  |

  @BCSCore
  Scenario Outline: Verify Data count for BCS core history tables are transferred from current tables
    Given Get the total count of BCS Core from Current Tables <sourceTable>
    Then Get the count of BCS core history <targetTable>
    And Compare count of current <sourceTable> and history <targetTable> are identical
    Given Get the <countOfRandomIds> of BCS Core data from Current Tables <sourceTable>
    When Get the Data from the BCS Core Current Tables <sourceTable>
    Then We Get the records from transform BCS Current History <targetTable>
    And Compare the records of BCS Core current and BCS Current_History <targetTable>

    Examples:
      | sourceTable                           |  targetTable                                        |countOfRandomIds     |
      | etl_accountable_product_current_v     |  etl_transform_history_accountable_product_part     |   50                |
      | etl_manifestation_current_v           |  etl_transform_history_manifestation_part           |   50                |
      |etl_manifestation_identifier_current_v |  etl_transform_history_manifestation_identifier_part|   50                |
      |   etl_person_current_v                | etl_transform_history_person_part                   |   50                |
      |   etl_product_current_v               | etl_transform_history_product_part                  |   50                |
      | etl_work_person_role_current_v        | etl_transform_history_work_person_role_part         |   50                |
      | etl_work_relationship_current_v       | etl_transform_history_work_relationship_part        |   50                |
      |etl_work_current_v                     |etl_transform_history_work_part                      |   50                |
      | etl_work_identifier_current_v         | etl_transform_history_work_identifier_part          |   50                |

  @BCSCore
  Scenario Outline: Verify Data count for BCS core transform_file tables are transferred from current tables
    Given Get the total count of BCS Core from Current Tables <sourceTable>
    Then Get the count of BCS core transform_file <targetTable>
    And Compare count of current <sourceTable> and tranform_file <targetTable> are identical
    Given Get the <countOfRandomIds> of BCS Core data from Current Tables <sourceTable>
    When Get the Data from the BCS Core Current Tables <sourceTable>
    Then We Get the records from transform BCS Transform_File <targetTable>
    And Compare the records of BCS Core current and BCS Transform_File <targetTable>
    Examples:
      | sourceTable                            |  targetTable                                                   |countOfRandomIds     |
      | etl_accountable_product_current_v      |  etl_accountable_product_transform_file_history_part           |   50                |
      | etl_manifestation_current_v            |  etl_manifestation_transform_file_history_part                 |   50                |
      |etl_manifestation_identifier_current_v  |  etl_manifestation_identifier_transform_file_history_part      |   50                |
      |   etl_person_current_v                 | etl_person_transform_file_history_part                         |   50                |
      |   etl_product_current_v                | etl_product_transform_file_history_part                       |   50                |
      | etl_work_person_role_current_v         | etl_work_person_role_transform_file_history_part              |   50                |
      | etl_work_relationship_current_v        | etl_work_relationship_transform_file_history_part             |   50                |
      |etl_work_current_v                      |etl_work_transform_file_history_part                          |   50                |
      | etl_work_identifier_current_v          | etl_work_identifier_transform_file_history_part              |   50                |



  Scenario Outline: Verify Data count for BCS core delta_current tables are transferred from transform_file tables
    Given Get the total count of BCS Core transform_file by diff of current and previous timestamp <sourceTable>
    Then We know the total count of delta current <targetTable>
    And Compare count of tranform_file <sourceTable> and delta current <targetTable> are identical
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

  Scenario Outline: Verify Data count for BCS Core delta_current_exclude are transferred from delta_current and current_history tables
    Given Get the BCSCore total count difference between delta current and transform current history Table <TargetTable>
    Then Get the BCSCore <TargetTable> exclude data count
    And Compare BCScore exclude count of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Given Get the <countOfRandomIds> from diff of delta_current and current_hist tables <TargetTable>
    When Get the records from the diff of delta_current and current_hist tables <TargetTable>
    Then Get the records from <TargetTable> exclude table
    And  Compare the records of Exclude with diff of delta_current and current_hist tables <TargetTable>
    Examples:
      |FirstSourceTable                           |SecondSourceTable                                            |TargetTable                                          |countOfRandomIds |
     |etl_delta_current_accountable_product       |etl_transform_history_accountable_product_part               |etl_transform_history_accountable_product_excl_delta |50              |
      |etl_delta_current_manifestation            |etl_transform_history_manifestation_part                     |etl_transform_history_manifestation_excl_delta       |50              |
      |etl_delta_current_person                   |etl_transform_history_person_part                            |etl_transform_history_person_excl_delta              |50              |
      |etl_delta_current_product                  |etl_transform_history_product_part                           |etl_transform_history_product_excl_delta             |50              |
      |etl_delta_current_work_person_role         | etl_transform_history_work_person_role_part                 |etl_transform_history_work_person_role_excl_delta    |50              |
      |etl_delta_current_work_relationship        |etl_transform_history_work_relationship_part                 |etl_transform_history_work_relationship_excl_delta   |50              |
      |etl_delta_current_work                     |etl_transform_history_work_part                              |etl_transform_history_work_excl_delta                |50              |
      |etl_delta_current_work_identifier          |etl_transform_history_work_identifier_part                   |etl_transform_history_work_identifier_excl_delta     |50              |
      |etl_delta_current_manifestation_identifier |etl_transform_history_manifestation_identifier_part          |etl_transform_history_manifestation_identifier_excl_delta |50              |



    @BCSCore
    Scenario Outline: Verify Data count for BCSCore delta_latest tables are transferred from delta_current and Current_Exclude tables
      Given Get the sum of total count between BCSCore delta current and and Current_Exclude Table <TargetTable>
      Then Get the BCSCore <TargetTable> latest data count
      And Compare BCSCore latest counts of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
      Given Get the <countOfRandomIds> from sum of delta_current and exclude_delta tables <TargetTable>
      When Get the records from the sum of delta_current and exclude_delta tables <TargetTable>
      Then Get the records from <TargetTable> BCS core latest table
      And  Compare the records of Latest with sum of delta_current and Exclude_Delta tables <TargetTable>
    Examples:
      |FirstSourceTable                           |SecondSourceTable                                                  |TargetTable                                           |countOfRandomIds |
      |etl_delta_current_accountable_product      |etl_transform_history_accountable_product_excl_delta               |etl_transform_history_accountable_product_latest      |50               |
      |etl_delta_current_manifestation            |etl_transform_history_manifestation_excl_delta                     |etl_transform_history_manifestation_latest            |50               |
      |etl_delta_current_person                   |etl_transform_history_person_excl_delta                            |etl_transform_history_person_latest                   |50               |
      |etl_delta_current_product                  |etl_transform_history_product_excl_delta                           |etl_transform_history_product_latest                  |50               |
      |etl_delta_current_work_person_role         | etl_transform_history_work_person_role_excl_delta                 |etl_transform_history_work_person_role_latest         |50               |
      |etl_delta_current_work_relationship        |etl_transform_history_work_relationship_excl_delta                 |etl_transform_history_work_relationship_latest        |50               |
      |etl_delta_current_work                     |etl_transform_history_work_excl_delta                              |etl_transform_history_work_latest                     |50               |
      |etl_delta_current_work_identifier          |etl_transform_history_work_identifier_excl_delta                   |etl_transform_history_work_identifier_latest          |50               |
      |etl_delta_current_manifestation_identifier |etl_transform_history_manifestation_identifier_excl_delta          |etl_transform_history_manifestation_identifier_latest |50               |

  @BCSCore
  Scenario Outline: Verify Duplicate Entry for BCS COre in transform latest tables
    Given Get the BCCore Duplicate count in <SourceTableName> table
    Then Check the BCSCore count should be equal to Zero <SourceTableName>
    Examples:
    |SourceTableName                                            |
    |etl_transform_history_accountable_product_latest           |
    |etl_transform_history_manifestation_latest                 |
    |etl_transform_history_person_latest                        |
    |etl_transform_history_product_latest                       |
    |etl_transform_history_work_person_role_latest              |
    |etl_transform_history_work_relationship_latest             |
    |etl_transform_history_work_latest                          |
    |etl_transform_history_work_identifier_latest               |
    |etl_transform_history_manifestation_identifier_latest      |

