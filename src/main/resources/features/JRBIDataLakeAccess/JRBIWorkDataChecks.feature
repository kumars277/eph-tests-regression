Feature:Validate data for JRBI Work tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_work is transferred from journal_data_full
      Given We get the <countOfRandomIds> random EPR ids <sourceTable>
      When We get the records from data jrbi_journal_data_full
      Then We get the records from transform current work
      And Compare the records of jrbi_journal_data_full and transform current work
    Examples:
     | sourceTable         | countOfRandomIds|
    |jrbi_journal_data_full|50                 |

  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_work_history is transferred from Current Work
    Given We get the <countOfRandomIds> random EPR ids <sourceTable>
    When We get the records from transform current work <sourceTable>
    Then Get the records from transform current work history <targetTable>
    And Compare the records of current work and current work history <targetTable>
    Examples:
      | sourceTable               | targetTable                               | countOfRandomIds|
      |jrbi_transform_current_work| jrbi_transform_current_work_history_part  |10                 |

  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Previous_work_history is transferred from Previous Work
    Given We get the <countOfRandomIds> random EPR ids <sourceTable>
    When We get the records from transform previous work <sourceTable>
    Then Get the records from transform previous work history <targetTable>
    And Compare the records of previous work and previous work history <targetTable>
    Examples:
      | sourceTable               | targetTable                               | countOfRandomIds|
      |jrbi_transform_previous_work| jrbi_transform_current_work_history_part  |10                 |


  @JRBI
  Scenario Outline: Verify Data for JRBI delta work history is transferred from delta current Work
    Given We get the <countOfRandomIds> random EPR ids <sourceTable>
    When We get the records from transform delta current work
    Then Get the records from transform delta work history
    And Compare the records of delta work and delta work history <targetTable>
    Examples:
      | sourceTable               | targetTable                               | countOfRandomIds|
      |jrbi_delta_current_work| jrbi_transform_delta_work_history_part        |10                 |


  @JRBI
  Scenario Outline: Verify Data from the difference of Delta_work and work_history is transferred to work exclude table
    Given We get the <countOfRandomIds> random EPR ids <tableName>
    When Get the records from the difference of Delta_current_work and work_history
    Then Get the records from work exclude table
    And  Compare the records of Work Exclude with difference of Delta_current_work and work_history
    Examples:
      |tableName                                | countOfRandomIds|
      |jrbi_transform_history_work_excl_delta   |1                 |

  @JRBI
  Scenario Outline: Verify Data from the addition of Delta_current_work and work_Exclude is transferred to work Latest table
    Given We get the <countOfRandomIds> random EPR ids <tableName>
    When Get the records from the addition of Delta_current_work and work_Exclude
    Then Get the records from work latest table
    And  Compare the records of Work Latest with addition of Delta_current_work and work_Exclude
    Examples:
      |tableName                                | countOfRandomIds|
      |jrbi_transform_latest_work               |1                 |

  @JRBI
  Scenario Outline: Verify Data from the difference of current_work and previous_work is transferred to delta current work table
    Given We get the <countOfRandomIds> delta random EPR ids
    When Get the records from the difference of current_work and previous_work
    Then We get the records from transform delta current work
    And  Compare the records of Delta Current work with difference of current and previous work
    Examples:
     | countOfRandomIds|
     |1                 |
