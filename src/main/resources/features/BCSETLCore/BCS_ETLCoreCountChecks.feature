Feature:Validate data count for BCS ETL Core in Data Lake Access Layer

#  Created by Dinesh on 30/09/2020



  @SD
  Scenario Outline: Verify Data Count for BCS_ETL Core tables is transferred from Inbound Tables
    Given Get the total count of BCS Core from Inbound Tables <tableName>
    Then  We know the total count of Inbound tables <tableName>
    And Compare count of BCS Inbound and BCS Core <tableName> tables are identical
    Examples:
      | tableName                                |
      |etl_accountable_product_current_v         |
      |etl_manifestation_current_v               |
      |etl_person_current_v                      |
      |etl_product_current_v                     |
      |etl_work_person_role_current_v            |
      |etl_work_relationship_current_v           |
      |etl_work_current_v                        |
      |etl_work_identifier_current_v             |
      |etl_manifestation_identifier_current_v    |
      |etl_accountable_product_previous_v        |
      |etl_manifestation_previous_v              |
      |etl_person_previous_v                     |
      |etl_product_previous_v                    |
      |etl_work_person_role_previous_v           |
      |etl_work_relationship_previous_v          |
      |etl_work_previous_v                       |
      |etl_work_identifier_previous_v            |
      |etl_manifestation_identifier_previous_v   |



  @SD
  Scenario Outline: Verify Data count for BCS tables are transferred from transformed_current tables
    Given We know the total count of Current SD data from <SourceTableName>
    Then Get the count of SD transform_current_history <TargettableName>
    And Check count of SD current table <SourceTableName> and SD current history <TargettableName> are identical
    Examples:
      |SourceTableName                      |TargettableName                                 |
      |sdbooks_transform_current_urls       |sdbooks_transform_history_urls_part   |

  @SD
  Scenario Outline: Verify Data count for SD transform_previous_history tables are transferred from transformed_previous tables
    Given We know the total count of Previous SD data from <SourceTableName>
    Then Get the count of SD transform_previous_history <TargettableName>
    And Check count of SD previous table <SourceTableName> and SD previous history <TargettableName> are identical

    Examples:
      |SourceTableName                      |TargettableName                                 |
      |sdbooks_transform_previous_urls         |sdbooks_transform_history_urls_part_previous   |

    @SD
     Scenario Outline: Verify Data count for SDBooks delta_current tables are transferred from Current and Previous tables
      Given Get the difference of total count between current and previous of SDbooks data <TargetTable>
      Then We know the delta current count for Sdbooks tables <TargetTable>
      And Compare SDbooks delta count of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
      Examples:
        |FirstSourceTable                 |SecondSourceTable                        |TargetTable                            |
        |sdbooks_transform_current_urls   |sdbooks_transform_previous_urls          |sdbooks_delta_current_urls |

      @SD
      Scenario Outline: Verify Data count for SDBooks delta_current_history tables are transferred from delta_current_work tables
        Given We know the delta current count for Sdbooks tables <SourceTableName>
        Then Get the count of SDBook delta current history <TargettableName> table
        And Compare SD delta current <SourceTableName> table and delta history <TargettableName> are identical
        Examples:
          |SourceTableName                   |TargettableName                                 |
          |sdbooks_delta_current_urls           |sdbooks_delta_history_urls_part   |

    @SD
     Scenario Outline: Verify Data count for SDBooks delta_latest tables are transferred from delta_current and Current_Exclude tables
    Given Get the sum of total count between SDBooks delta current and and Current_Exclude Table <TargetTable>
    Then Get the SDbooks <TargetTable> latest data count
    And Compare SDBooks latest counts of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Examples:
      |FirstSourceTable                 |SecondSourceTable                               |TargetTable                            |
        |sdbooks_delta_current_urls          |sdbooks_transform_history_excl_delta          |sdbooks_transform_latest_urls |

  @SD
  Scenario Outline: Verify Data count for SDBooks delta_current_exclude are transferred from delta_current and current_history tables
    Given Get the SDBooks total count difference between delta current and transform current history Table <TargetTable>
    Then Get the SDBooks <TargetTable> exclude data count
    And Compare SDBooks exclude count of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Examples:
      |FirstSourceTable                 |SecondSourceTable                               |TargetTable                            |
      |sdbooks_delta_current_urls          |sdbooks_transform_history_urls_part          |sdbooks_transform_history_excl_delta |

  @SD
  Scenario Outline: Verify Duplicate Entry for SDBoks in transform latest tables
    Given Get the SDBooks Duplicate count in <SourceTableName> table
    Then Check the SDBooks count should be equal to Zero <SourceTableName>
    Examples:
      |SourceTableName                      |
      |sdbooks_transform_latest_urls           |