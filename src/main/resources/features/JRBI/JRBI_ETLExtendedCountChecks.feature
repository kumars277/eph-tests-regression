Feature:Validate data count for JRBI Work,Manifestation and Person tables in Data Lake

#  Created by Dinesh on 26/05/2020
  #updated Dinesh on 07/04/2021
  #confluence Version: v.3
  #Confluence LinK: https://confluence.cbsels.com/pages/viewpage.action?pageId=168466078


  @JRBIETLExtended
  Scenario Outline: Verify Data Count for JRBI transform_Current_tables is transferred from Source Table
    Given Get the total count of JRBI Data from Full Load <tableName>
    Then  We know the total count of Current JRBI data from <tableName>
    And Compare count of Full load with current <tableName> table are identical
    Examples:
      | tableName                                |
      |jrbi_transform_current_work                 |
      |jrbi_transform_current_manifestation       |
      |jrbi_transform_current_person              |


  @JRBIETLExtended
  Scenario Outline: Verify Data count for JRBI transform_current_history tables are transferred from transformed_current tables
    Given We know the total count of Current JRBI data from <SourceTableName>
    Then Get the count of transform_current_history <TargettableName>
    And Check count of current table <SourceTableName> and current history <TargettableName> are identical
    Examples:
      |SourceTableName                      |TargettableName                                 |
      |jrbi_transform_current_work          |jrbi_transform_current_work_history_part   |
      |jrbi_transform_current_manifestation |jrbi_transform_current_manifestation_history_part   |
      |jrbi_transform_current_person        |jrbi_transform_current_person_history_part   |

  @JRBIETLExtended
  Scenario Outline: Verify Data count for JRBI transform_previous_history tables are transferred from transformed_previous tables
    Given We know the total count of Previous tables from <SourceTableName>
    Then We Get the JRBI <TargettableName> previous history data count
    And Check count of previous table <SourceTableName> and previous history <TargettableName> are identical

    Examples:
      |SourceTableName                      |TargettableName                                 |
      |jrbi_transform_previous_work         |jrbi_transform_previous_work_history_part   |
      |jrbi_transform_previous_manifestation|jrbi_transform_previous_manifestation_history_part   |
      |jrbi_transform_previous_person       |jrbi_transform_previous_person_history_part   |


  @JRBIETLExtended
  Scenario Outline: Verify Data count for JRBI delta_current tables are transferred from Current and Previous tables
    Given Get the difference of total count between current and previous Table <TargetTable>
    Then We know the delta current count for tables <TargetTable>
    And Compare delta count of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Examples:
      |FirstSourceTable                     |SecondSourceTable                     |TargetTable                            |
      |jrbi_transform_current_work          |jrbi_transform_previous_work          |jrbi_delta_current_work |
      | jrbi_transform_current_manifestation|jrbi_transform_previous_manifestation |jrbi_delta_current_manifestation|
      |jrbi_transform_current_person        |jrbi_transform_previous_person        |jrbi_delta_current_person|

  @JRBIETLExtended
  Scenario Outline: Verify Data count for JRBI delta_current_exclude are transferred from delta_current and current_history tables
    Given Get the total count difference between delta current and transform current history Table <TargetTable>
    Then Get the JRBI <TargetTable> exclude data count
    And Compare exclude count of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Examples:
      |FirstSourceTable                 |SecondSourceTable                               |TargetTable                            |
      |jrbi_delta_current_work          |jrbi_transform_current_work_history_part          |jrbi_transform_history_work_excl_delta |
      |jrbi_delta_current_manifestation |jrbi_transform_current_manifestation_history_part |jrbi_transform_history_manifestation_excl_delta|
      |jrbi_delta_current_person        |jrbi_transform_current_person_history_part        |jrbi_transform_history_person_excl_delta|

  @JRBIETLExtended
  Scenario Outline: Verify Data count for JRBI delta_latest tables are transferred from delta_current and Current_Exclude tables
    Given Get the sum of total count between delta current and and Current_Exclude Table <TargetTable>
    Then Get the JRBI <TargetTable> latest data count
    And Compare latest counts of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Examples:
      |FirstSourceTable                 |SecondSourceTable                               |TargetTable                            |
      |jrbi_delta_current_work          |jrbi_transform_history_work_excl_delta          |jrbi_transform_latest_work |
      |jrbi_delta_current_manifestation |jrbi_transform_history_manifestation_excl_delta |jrbi_transform_latest_manifestation|
      |jrbi_delta_current_person        |jrbi_transform_history_person_excl_delta        |jrbi_transform_latest_person|

  @JRBIETLExtended
  Scenario Outline: Verify Data count for JRBI delta_current_history tables are transferred from delta_current_work tables
    Given We know the delta current count for tables <SourceTableName>
    Then Get the count of delta current history <TargettableName> table
    And Compare delta current <SourceTableName> table and delta history <TargettableName> are identical
    Examples:
      |SourceTableName                   |TargettableName                                 |
      |jrbi_delta_current_work           |jrbi_transform_delta_work_history_part   |
      |jrbi_delta_current_manifestation  |jrbi_transform_delta_manifestation_history_part   |
      |jrbi_delta_current_person         |jrbi_transform_delta_person_history_part   |

    @JRBIETLExtended
    Scenario Outline: Verify Duplicate Entry for JRBI in transform latest tables
      Given Get the Duplicate count in <SourceTableName> table
      Then Check the count should be equal to Zero <SourceTableName>
      Examples:
        |SourceTableName                      |
        |jrbi_transform_latest_work           |
        |jrbi_transform_latest_manifestation  |
        |jrbi_transform_latest_person         |


