Feature:Validate data count for JRBI Work,Manifestation and Person tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data Count for JRBI transform_Current_tables is transferred from Source Table
      Given Get the total count of JRBI Data from Full Load <tableName>
      Then  We know the total count of JRBI data from <tableName>
      And The count of source table and <tableName> table are identical
    Examples:
      | tableName                                 |
     |jrbi_transform_current_work               |
     |jrbi_transform_current_manifestation       |
     |jrbi_transform_current_person              |

      
      @JRBI
      Scenario Outline: Verify Data count for JRBI transform_current_history tables are transferred from transformed_current tables
        Given We know the total count of JRBI data from <SourceTableName>
        Then Get the JRBI <TargettableName> history data count
        And Compare count <SourceTableName> table and <TargettableName> are identical
        Examples:
          |SourceTableName            |TargettableName                                 |
          |jrbi_transform_current_work|jrbi_temp_transform_current_work_history_part   |
          |jrbi_transform_current_manifestation|jrbi_temp_transform_current_manifestation_history_part   |
          |jrbi_transform_current_person|jrbi_temp_transform_current_person_history_part   |

  @JRBI
  Scenario Outline: Verify Data count for JRBI transform_previous_history tables are transferred from transformed_previous tables
    Given We know the total count of JRBI data from <SourceTableName>
    Then We Get the JRBI <TargettableName> previous history data count
    And Compare count <SourceTableName> table and <TargettableName> are identical
    Examples:
      |SourceTableName            |TargettableName                                 |
      |jrbi_transform_previous_work|jrbi_temp_transform_current_work_history_part   |
      |jrbi_transform_previous_manifestation|jrbi_temp_transform_current_manifestation_history_part   |
      |jrbi_transform_previous_person|jrbi_temp_transform_current_person_history_part   |

  @JRBI
  Scenario Outline: Verify Data count for JRBI delta_current_work_history tables are transferred from delta_current_work tables
    Given We know the total count of JRBI data from <SourceTableName>
    Then Get the JRBI <TargettableName> history data count
    And Compare count <SourceTableName> table and <TargettableName> are identical
    Examples:
      |SourceTableName            |TargettableName                                 |
      |jrbi_delta_current_work|jrbi_transform_delta_work_history_part   |
      |jrbi_delta_current_manifestation|jrbi_transform_delta_manifestation_history_part   |
      |jrbi_delta_current_person|jrbi_transform_delta_person_history_part   |




