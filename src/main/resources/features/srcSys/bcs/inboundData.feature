#confluence v11
#https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45487296633/BCS+Inbound

Feature:Validate data count for BCS tables in Data Lake

  @BCSInbound
  Scenario Outline: Verify Data and Count for BCS stg_Current_tables for product series transferred from Source Table initial_ingest
    Given Get the total count of BCS Data from initial_ingest <targetTable>
    Then  Get total count of BCS Current table <targetTable>
    And Compare count of initial ingest with current table <targetTable>
    Given Get the <countOfRandomIds> random ids from initial ingest <targetTable>
    When Get the data records from initial ingest for <targetTable>
    Then we Get the records from current tables <targetTable>
    And Compare the records of initial ingest and current table <targetTable>
    Examples:
      | targetTable                 | countOfRandomIds|
      |stg_current_classification   |       1         |
      |stg_current_content          |       2         |
      |stg_current_extobject        |       1         |
      |stg_current_fullversionfamily|       1         |
    #  |stg_current_originatoraddress|       1         | Removed for GDOR
      |stg_current_originators      |       1         |
      |stg_current_pricing          |       1         |
      |stg_current_product          |       1         |
      |stg_current_production       |       1         |
      |stg_current_relations        |       1         |
      |stg_current_responsibilities |       1         |
      |stg_current_sublocation      |       1         |
      |stg_current_text             |       1         |
      |stg_current_versionfamily    |       1         |
      |stg_current_originatornotes  |       1         |



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
      |stg_current_originatornotes              |stg_history_originatornotes_part |     1           |
    #  |stg_current_originatoraddress          |stg_history_originatoraddress_part |     1           | Remove for GdPR



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


  @BCSInbound
  Scenario Outline: Verify Data and count for BCS stg_Current_tables for book series transferred from Source Table initial_ingest
    Given Get the total count of BCS Book series Data from initial_ingest <targetTable>
    Then  Get total count of BCS Book series Current table <targetTable>
    And Compare count of initial ingest series with current book series table <targetTable>
    Given Get <countOfRandomIds> random ids for initial ingest book series <targetTable>
    When Get the data records from initial ingest book series for <targetTable>
    Then Get the records for current tables for the book series <targetTable>
    And Compare the records for the initial ingest book series and current table book series <targetTable>
     Examples:
      | targetTable                        | countOfRandomIds|
    #  |stg_current_classification_series   |       1         | removed
      |stg_current_content_series          |       1         |
   #   |stg_current_originatoraddress_series|       1         |removed
      |stg_current_originatornotes_series  |       1         |
      |stg_current_originators_series      |       1         |
   #   |stg_current_product_series          |       1         |removed
      |stg_current_text_series             |       1         |


  @BCSInbound
  Scenario Outline: Verify Count & Data for BCS stg_history series tables are transferred from stg_Current_tables series
    Given Get total count of BCS Book series Current table <SourceTableName>
    Then Get the count for the BCS stg_history series <TargetTableName> for current comparision
    And Check count for the current table <SourceTableName> and history table series <TargetTableName> are identical
    Given Get the <countOfRandomIds> random ids from the current book series <SourceTableName>
    When Get the records for current tables for the book series <SourceTableName>
    Then Get the records for current tables for the staging history book series <TargetTableName>
    And Compare the records for the current book series and history table book series <TargetTableName>
    Examples:
      | SourceTableName                               | TargetTableName                         |countOfRandomIds |
  #    |stg_current_classification_series             |stg_history_classification_series_part    |     1           |removed
      |stg_current_content_series                    |stg_history_content_series_part           |     1           |
   #   |stg_current_originatoraddress_series          |stg_history_originatoraddress_series_part |     1           |removed
      |stg_current_originatornotes_series            |stg_history_originatornotes_series_part   |     1           |
      |stg_current_originators_series                |stg_history_originators_series_part       |     1           |
    #  |stg_current_product_series                    |stg_history_product_series_part           |     1           |removed
      |stg_current_text_series                       |stg_history_text_series_part              |     1           |


