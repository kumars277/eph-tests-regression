#Created by Nishant @ 04 Aug 2020
Feature:Validate data count for BCS tables in Data Lake

  @BCS
  Scenario Outline: Verify Data Count for BCS stg_Current_tables transferred from Source Table initial_ingest
    Given Get the total count of BCS Data from initial_ingest <tableName>
    Then  Get total count of BCS Current table <tableName>
    And Compare count of initial ingest with current table <tableName>
    Examples:
      | tableName                    |
      | stg_current_classification   |
      | stg_current_content          |
      | stg_current_extobject        |
      | stg_current_fullversionfamily|
      | stg_current_originatoraddress|
      | stg_current_originators      |
      | stg_current_pricing          |
      | stg_current_product          |
      | stg_current_production       |
      | stg_current_relations        |
      | stg_current_responsibilities |
      | stg_current_sublocation      |
      | stg_current_text             |
      | stg_current_versionfamily    |


  @BCSDebug
  Scenario Outline: Verify Data count for BCS stg_history tables are transferred from stg_previous tables
    Given We know the total count of stg_previous BCS data from <SourceTableName>
    Then Get the count of BCS stg_history <TargettableName>
    And Check count of previous table <SourceTableName> and history <TargettableName> are identical
    Examples:
      |SourceTableName                     | TargettableName                   |
      |stg_previous_classification         | stg_history_classification_part   |