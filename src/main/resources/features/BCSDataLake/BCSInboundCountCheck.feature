#Created by Nishant @ 04 Aug 2020
#confluence page link
#https://confluence.cbsels.com/pages/viewpage.action?spaceKey=EPH&title=BCS+Inbound

Feature:Validate data count for BCS tables in Data Lake

  @BCSInbound
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


  @BCSInbound
  Scenario Outline: Verify Data Count for BCS stg_history tables are transferred from stg_Current_tables
    Given Get total count of BCS Current table <SourceTableName>
    Then Get the count of BCS stg_history <TargettableName> for current comparision
    And Check count of current table <SourceTableName> and history <TargettableName> are identical
    Examples:
      | SourceTableName                      | TargettableName                   |
      |stg_current_classification	         |stg_history_classification_part    |
      |stg_current_content                   |stg_history_content_part           |
      |stg_current_extobject	             |stg_history_extobject_part         |
      |stg_current_fullversionfamily	     |stg_history_fullversionfamily_part |
      |stg_current_originators      	     |stg_history_originators_part       |
      |stg_current_pricing          	     |stg_history_pricing_part           |
      |stg_current_product	                 |stg_history_product_part           |
      |stg_current_production	             |stg_history_production_part        |
      |stg_current_relations	             |stg_history_relations_part         |
      |stg_current_responsibilities	         |stg_history_responsibilities_part  |
      |stg_current_sublocation	             |stg_history_sublocation_part       |
      |stg_current_text                      |stg_history_text_part              |
      |stg_current_versionfamily	         |stg_history_versionfamily_part     |


      #below scenario not applicable any more as previous tables are no longer valid
  @BCS_NA
  Scenario Outline: Verify Data count for BCS stg_history tables are transferred from stg_previous tables
    Given We know the total count of stg_previous BCS data from <SourceTableName>
    Then Get the count of BCS stg_history <TargettableName> for previous comparision
    And Check count of previous table <SourceTableName> and history <TargettableName> are identical
    Examples:
      | SourceTableName                      | TargettableName                    |
      | stg_previous_classification          | stg_history_classification_part    |
      | stg_previous_content                 | stg_history_content_part           |
      | stg_previous_extobject               | stg_history_extobject_part         |
      | stg_previous_fullversionfamily       | stg_history_fullversionfamily_part |
      | stg_previous_originators             | stg_history_originators_part       |
      | stg_previous_pricing                 | stg_history_pricing_part           |
      | stg_previous_product                 | stg_history_product_part           |
      | stg_previous_production              | stg_history_production_part        |
      | stg_previous_relations               | stg_history_relations_part         |
      | stg_previous_responsibilities        | stg_history_responsibilities_part  |
      | stg_previous_sublocation             | stg_history_sublocation_part       |
      | stg_previous_text                    | stg_history_text_part              |
      | stg_previous_versionfamily           | stg_history_versionfamily_part     |
