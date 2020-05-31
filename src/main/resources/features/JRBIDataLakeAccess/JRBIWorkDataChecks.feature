Feature:Validate data for JRBI Work tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_work is transferred from journal_data_full
      Given We get the <countOfRandomIds> random EPR ids <sourceTable>
      When We get the records from data full load <sourceTable>
      Then We get the records from transform current work <targetTable>
      And Compare the records of data full load and transform current work <targetTable>
    Examples:
     | sourceTable         | targetTable                  | countOfRandomIds|
    |jrbi_journal_data_full|jrbi_transform_current_work   |1                 |

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
  Scenario Outline: Verify Data for JRBI delta work history is transferred from delta current Work
    Given We get the <countOfRandomIds> random EPR ids <sourceTable>
    When We get the records from the difference of transform delta current work
