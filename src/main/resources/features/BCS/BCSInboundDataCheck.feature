#created by Nishant @ 29 Sep 2020
Feature:Validate data check for BCS tables in Data Lake

  @BCSInbound
Scenario Outline: Verify Data for BCS is transferred from initial ingest
Given We get the <countOfRandomIds> random ids from initial ingest <targetTable>
When Get the data records from initial ingest for <targetTable>
Then Get the records from current tables <targetTable>
And Compare the records of initial ingest and current table <targetTable>
Examples:
| targetTable                  | countOfRandomIds|
|stg_current_classification   |       1         |
|stg_current_content          |       2         |
|stg_current_extobject        |       1         |
|stg_current_fullversionfamily|       1         |
|stg_current_originatoraddress|       1         |
|stg_current_originators      |       1         |
|stg_current_pricing          |       1         |
|stg_current_product          |       1         |
|stg_current_production       |       1         |
|stg_current_relations        |       1         |
|stg_current_responsibilities |       1         |
|stg_current_sublocation      |       1         |
|stg_current_text             |       1         |
|stg_current_versionfamily    |       1         |


  @BCSInbound
  Scenario Outline: Data check for BCS stg_previous tables are transferred to stg_Current_history
    Given We get <countOfRandomIds> randomIds for BCS Current table <SourceTableName>
    When Get data from BCS stg_current <SourceTableName>
    Then Get data from BCS stg_history <TargetTableName>
    And Data check of current table <SourceTableName> and history <TargetTableName> are identical
    Examples:
      | SourceTableName                       | TargetTableName                   |countOfRandomIds |
  #    |stg_current_classification             |stg_history_classification_part    |     1           |
      |stg_current_content                    |stg_history_content_part           |     1           |
      |stg_current_extobject                  |stg_history_extobject_part         |     1           |
      |stg_current_fullversionfamily          |stg_history_fullversionfamily_part |     1           |
      |stg_current_originators                |stg_history_originators_part       |     1           |
      |stg_current_pricing                    |stg_history_pricing_part           |     1           |
      |stg_current_product                    |stg_history_product_part           |     1           |
      |stg_current_production                 |stg_history_production_part        |     1           |
      |stg_current_relations                  |stg_history_relations_part         |     1           |
      |stg_current_responsibilities           |stg_history_responsibilities_part  |     1           |
      |stg_current_sublocation                |stg_history_sublocation_part       |1                |
      |stg_current_text                       |stg_history_text_part              |1                |
      |stg_current_versionfamily              |stg_history_versionfamily_part     |1                |
