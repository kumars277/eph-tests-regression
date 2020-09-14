#Created by Nishant @ 04 Aug 2020
Feature:Validate data count for BCS tables in Data Lake

  @BCS
  Scenario Outline: Verify Data Count for BCS stg_Current_tables transferred from Source Table initial_ingest
    Given Get the total count of BCS Data from initial_ingest <tableName>
    Then  Get total count of BCS Current table <tableName>
    And Compare count of initial ingest with current table <tableName>
    Examples:
      | tableName                              |
      | stg_current_classification             |
      | stg_current_content                    |
|                                        stg_current_extobject|

