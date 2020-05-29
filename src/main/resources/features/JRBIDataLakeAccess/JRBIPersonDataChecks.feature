Feature:Validate data for JRBI Work tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data for JRBI transform_current_person is transferred from person_data_full
      Given We get the <countOfRandomIds> random Person EPR ids <sourceTable>
      When Get the records from data full load for Person <sourceTable>
      Then Get the records from transform current person <targetTable>
      And Compare the records of person full load and current person
    Examples:
     | sourceTable| targetTable                                 | countOfRandomIds|
    |jrbi_journal_data_full|jrbi_transform_current_person               |10                 |


  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_person_history is transferred from Current PErson
    Given We get the <countOfRandomIds> random Person EPR ids <sourceTable>
    When Get the records from transform current person <sourceTable>
    Then We Get the records from transform current person History <targetTable>
    And Compare the records of current person and current person history
    Examples:
      | sourceTable               | targetTable                               | countOfRandomIds|
      |jrbi_transform_current_person| jrbi_transform_current_person_history_part  |10                 |


  @JRBI
  Scenario Outline: Verify Data for JRBI transform_previous_person_history is transferred from Previous Person
    Given We get the <countOfRandomIds> random Person EPR ids <sourceTable>
    When Get the records from transform previous person <sourceTable>
    Then We Get the records from transform previous person History <targetTable>
    And Compare the records of previous person and previous person history
    Examples:
      | sourceTable                  | targetTable                                 | countOfRandomIds|
      |jrbi_transform_previous_person| jrbi_transform_current_person_history_part  |10                 |

