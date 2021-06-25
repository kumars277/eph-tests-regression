#Created by Nishant @ 04 Aug 2020, updated by Nishant @ 02 Jun 2021
#confluence page link
#https://confluence.cbsels.com/pages/viewpage.action?spaceKey=EPH&title=BCS+Inbound

Feature:Validate data count for BCS tables in Data Lake

  @BCSInbound
  Scenario Outline: Verify Data Count for BCS stg_Current_tables transferred from Source Table initial_ingest
    Given Get the total count of BCS Data from initial_ingest <targetTable>
    Then  Get total count of BCS Current table <targetTable>
    And Compare count of initial ingest with current table <targetTable>
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
 Scenario Outline: Verify Count & Data for BCS stg_history tables are transferred from stg_Current_tables
   Given Get total count of BCS Current table <SourceTableName>
   Then Get the count of BCS stg_history <TargetTableName> for current comparision
   And Check count of current table <SourceTableName> and history <TargetTableName> are identical
    Given We get <countOfRandomIds> randomIds for BCS Current table <SourceTableName>
    When Get data from BCS stg_current <SourceTableName>
    Then Get data from BCS stg_history <TargetTableName>
    And Data check of current table <SourceTableName> and history <TargetTableName> are identical
    Examples:
      | SourceTableName                       | TargetTableName                   |countOfRandomIds |
      |stg_current_classification             |stg_history_classification_part    |     1           |
      |stg_current_content                    |stg_history_content_part           |     1           |
      |stg_current_extobject                  |stg_history_extobject_part         |     1           |
      |stg_current_fullversionfamily          |stg_history_fullversionfamily_part |     1           |
      |stg_current_originators                |stg_history_originators_part       |     1           |
      |stg_current_pricing                    |stg_history_pricing_part           |     1           |
      |stg_current_product                    |stg_history_product_part           |     1           |
      |stg_current_production                 |stg_history_production_part        |     1           |
      |stg_current_relations                  |stg_history_relations_part         |     1           |
      |stg_current_responsibilities           |stg_history_responsibilities_part  |     1           |
      |stg_current_sublocation                |stg_history_sublocation_part       |     1           |
      |stg_current_text                       |stg_history_text_part              |     1           |
      |stg_current_versionfamily              |stg_history_versionfamily_part     |     1           |


# merged in above scenario
  #@BCSInbound
 # Scenario Outline: Verify Data Count for BCS stg_history tables are transferred from stg_Current_tables
 #   Given Get total count of BCS Current table <SourceTableName>
 #   Then Get the count of BCS stg_history <TargettableName> for current comparision
 #   And Check count of current table <SourceTableName> and history <TargettableName> are identical
 #   Examples:
 #     | SourceTableName                      | TargettableName                   |
 #     |stg_current_classification	         |stg_history_classification_part    |
 #     |stg_current_content                   |stg_history_content_part           |
 #     |stg_current_extobject	             |stg_history_extobject_part         |
 #     |stg_current_fullversionfamily	     |stg_history_fullversionfamily_part |
 #     |stg_current_originators      	     |stg_history_originators_part       |
 #     |stg_current_pricing          	     |stg_history_pricing_part           |
 #     |stg_current_product	                 |stg_history_product_part           |
 #     |stg_current_production	             |stg_history_production_part        |
 #     |stg_current_relations	             |stg_history_relations_part         |
 #     |stg_current_responsibilities	         |stg_history_responsibilities_part  |
 #     |stg_current_sublocation	             |stg_history_sublocation_part       |
 #     |stg_current_text                      |stg_history_text_part              |
 #     |stg_current_versionfamily	         |stg_history_versionfamily_part     |

# not applicable anymore as previouse table no longer applicable
 # Scenario Outline: Verify Data count for BCS stg_history tables are transferred from stg_previous tables
 #   Given We know the total count of stg_previous BCS data from <SourceTableName>
 #   Then Get the count of BCS stg_history <TargetTableName> for previous comparision
 #   And Check count of previous table <SourceTableName> and history <TargetTableName> are identical
 #   Examples:
 #     | SourceTableName                      | TargettableName                   |