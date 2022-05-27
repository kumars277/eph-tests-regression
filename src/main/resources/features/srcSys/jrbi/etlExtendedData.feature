Feature:Validate data for JRBI Extended
  #confluence Version: v.3
  #Confluence LinK: https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45468777561/JRBI+ETL

  @JRBIETLExtended
  Scenario Outline: Verify Data Count for JRBI transform_Current_tables is transferred from Source Table
    Given Get the total count of JRBI Data from Full Load <tableName>
    Then we get the total count of Current JRBI data from <tableName>
    And we Compare count of Full load with current <tableName> table are identical
    Given We get the<countOfRandomIds> random EPR ids from full load<tableName>
    When We get the records from data jrbi_journal_data_full <tableName>
    Then We get the records from transform <tableName>
    And compare the records of <SourceTableName> and <tableName>
    Examples:
     |SourceTableName        | tableName                                |    countOfRandomIds  |
     |jrbi_journal_data_full |jrbi_transform_current_work               | 10                   |
     |jrbi_journal_data_full |jrbi_transform_current_manifestation      | 10                   |
     |jrbi_journal_data_full |jrbi_transform_current_person             | 10                   |

  @JRBIETLExtended
  Scenario Outline: Verify Data count for JRBI transform file and history are transferred from current tables
    Given we get the total count of Current JRBI data from <SourceTableName>
    Then Get the count of transform_current_history <TargettableName>
    And Check count of current table <SourceTableName> and current history <TargettableName> are identical
    Given We get the <countOfRandomIds> random EPR ids from <SourceTableName>
    When We get the records from transform <SourceTableName>
    Then we get the records from transform history <TargettableName>
    And compare the records of <SourceTableName> and <TargettableName>
    Examples:
      |SourceTableName                      |TargettableName                                  | countOfRandomIds   |
      |jrbi_transform_current_work          |jrbi_transform_current_work_history_part         |   10                |
      |jrbi_transform_current_manifestation |jrbi_transform_current_manifestation_history_part|   10                  |
      |jrbi_transform_current_person        |jrbi_transform_current_person_history_part       |   10                |
      |jrbi_transform_current_work          |jrbi_transform_work_file_history_part         |   10                |
      |jrbi_transform_current_manifestation |jrbi_transform_manifestation_file_history_part|   10                  |
      |jrbi_transform_current_person        |jrbi_transform_person_file_history_part       |   10                |


  @JRBIETLExtended
  Scenario Outline: Verify Data count for JRBI delta_latest tables are transferred from delta_current and Current_Exclude tables
    Given Get the sum of total count between delta current and and Current_Exclude Table <TargetTable>
    Then Get the JRBI <TargetTable> latest data count
    And Compare latest counts of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Given We get the <countOfRandomIds> random EPR ids from <TargetTable>
    When we get the records from the addition of delta current and exclude <TargetTable>
    Then we get the records from latest table <TargetTable>
    And Compare the records for Latest tables <TargetTable>
    Examples:
      |FirstSourceTable                 |SecondSourceTable                               |TargetTable                            |countOfRandomIds |
      |jrbi_delta_current_work          |jrbi_transform_history_work_excl_delta          |jrbi_transform_latest_work             |1              |
      |jrbi_delta_current_manifestation |jrbi_transform_history_manifestation_excl_delta |jrbi_transform_latest_manifestation    |1              |
      |jrbi_delta_current_person        |jrbi_transform_history_person_excl_delta        |jrbi_transform_latest_person           |1              |

    @JRBIETLExtended
    Scenario Outline: Verify Duplicate Entry for JRBI in transform latest tables
      Given Get the Duplicate count in <SourceTableName> table
      Then Check the count should be equal to Zero <SourceTableName>
      Examples:
        |SourceTableName                      |
        |jrbi_transform_latest_work           |
        |jrbi_transform_latest_manifestation  |
        |jrbi_transform_latest_person         |

  @JRBIETLExtended
  Scenario Outline: Get the delta current table counts for verification new dag
    Given Get the count of delta current <SourceTableName> table
    Examples:
      |SourceTableName                      |
      |jrbi_delta_current_work           |
      |jrbi_delta_current_manifestation  |
      |jrbi_delta_current_person         |
    |jrbi_delta_person_history_part    |
    |jrbi_delta_work_history_part      |
    |jrbi_delta_manifestation_history_part|


      ###################################3

#  @notUsed
#  Scenario Outline: Verify Data count for JRBI transform_previous_history tables are transferred from transformed_previous tables
#    Given We know the total count of Previous tables from <SourceTableName>
#    Then We Get the JRBI <TargettableName> previous history data count
#    And Check count of previous table <SourceTableName> and previous history <TargettableName> are identical
#    Given We get the <countOfRandomIds> random EPR ids from <SourceTableName>
#    When  We get the records from transform <SourceTableName>
#    Then Get the records from transform history <TargettableName>
#    And compare the records of <SourceTableName> and <TargettableName>
#    Examples:
#      |SourceTableName                      |TargettableName                                    |  countOfRandomIds         |
#      |jrbi_transform_previous_work         |jrbi_transform_previous_work_history_part          |     10                    |
#      |jrbi_transform_previous_manifestation|jrbi_transform_previous_manifestation_history_part |     10                    |
#      |jrbi_transform_previous_person       |jrbi_transform_previous_person_history_part        |     10                    |

  @notUsed
  Scenario Outline: Verify Data count for JRBI delta_current tables are transferred from Current and Previous tables
    Given Get the difference of total count between current and previous Table <TargetTable>
    Then We know the delta current count for tables <TargetTable>
    And Compare delta count of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Given We get the <countOfRandomIds> random EPR ids from <TargetTable>
    When Get the records from the difference of current and previous tables <TargetTable>
    Then We get the records from transform <TargetTable>
    And  commpare records of <TargetTable> with difference of<FirstSourceTable> and <SecondSourceTable>
    Examples:
      |FirstSourceTable                     |SecondSourceTable                     |TargetTable                     |countOfRandomIds  |
      |jrbi_transform_current_work          |jrbi_transform_previous_work          |jrbi_delta_current_work         |   10              |
      | jrbi_transform_current_manifestation|jrbi_transform_previous_manifestation |jrbi_delta_current_manifestation|   10              |
      |jrbi_transform_current_person        |jrbi_transform_previous_person        |jrbi_delta_current_person       |   10              |

  @notUsed
  Scenario Outline: Verify Data count for JRBI delta_current_exclude are transferred from delta_current and current_history tables
    Given Get the total count difference between delta current and transform current history Table <TargetTable>
    Then Get the JRBI <TargetTable> exclude data count
    And Compare exclude count of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Given We get the <countOfRandomIds> random EPR ids from <TargetTable>
    When Get the records from the difference of <FirstSourceTable> and <SecondSourceTable>
    And We get the records from transform <TargetTable>
    And  Compare the records of Exclude with difference of Delta_current and current_history<TargetTable>
    Examples:
      |FirstSourceTable                 |SecondSourceTable                                 |TargetTable                               |countOfRandomIds  |
      |jrbi_delta_current_work          |jrbi_transform_current_work_history_part          |jrbi_transform_history_work_excl_delta    |10                |
      |jrbi_delta_current_manifestation |jrbi_transform_current_manifestation_history_part |jrbi_transform_history_manifestation_excl_delta|10        |
      |jrbi_delta_current_person        |jrbi_transform_current_person_history_part        |jrbi_transform_history_person_excl_delta|10               |

  @notUsed
  Scenario Outline: Verify Data count for JRBI delta_current_history tables are transferred from delta_current_work tables
    Given We know the delta current count for tables <SourceTableName>
    Then Get the count of delta current history <TargettableName> table
    And Compare delta current <SourceTableName> table and delta history <TargettableName> are identical
    Examples:
      |SourceTableName                   |TargettableName                                 |
      |jrbi_delta_current_work           |jrbi_transform_delta_work_history_part   |
      |jrbi_delta_current_manifestation  |jrbi_transform_delta_manifestation_history_part   |
      |jrbi_delta_current_person         |jrbi_transform_delta_person_history_part   |
