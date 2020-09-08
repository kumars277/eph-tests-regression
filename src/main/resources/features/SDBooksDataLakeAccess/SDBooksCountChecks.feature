Feature:Validate data count for SDBooks in Data Lake Access Layer

#  Created by Dinesh on 08/09/2020



  @SD
  Scenario Outline: Verify Data Count for SDBooks transform_Current_tables is transferred from Source Table
    Given Get the total count of SD Data from Full Load <tableName>
    Then  We know the total count of Current SD data from <tableName>
    And Compare count of SD Full load with current <tableName> table are identical
    Examples:
      | tableName                                |
      |sdbooks_transform_current_urls              |



  @SD
  Scenario Outline: Verify Data count for SD transform_current_history tables are transferred from transformed_current tables
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
