Feature:Validate data for JRBI Person tables in Data Lake

#  Created by Dinesh on 26/05/2020
  #  Created by Dinesh on 26/05/2020
  #updated Dinesh on 07/04/2021
  #confluence Version: v.3
  #Confluence LinK: https://confluence.cbsels.com/pages/viewpage.action?pageId=168466078


  @JRBI
  Scenario Outline: Verify Data for JRBI transform_current_person is transferred from person_data_full
    Given We get the <countOfRandomIds> random Person EPR ids <tableName>
    When  Get the records from data full load for Person
    Then  Get the records from transform current person
    And   Compare the records of person full load and current person
    Examples:
      | tableName                        |    countOfRandomIds|
      |jrbi_journal_data_full             |10            |


  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_person_history is transferred from Current PErson
    Given We get the <countOfRandomIds> random Person EPR ids <sourceTable>
    When Get the records from transform current person
    Then We Get the records from transform current person History
    And Compare the records of current person and current person history
    Examples:
      | sourceTable                 | countOfRandomIds      |
      |jrbi_transform_current_person|   10                 |


  @JRBIP
  Scenario Outline: Verify Data for JRBI transform_previous_person_history is transferred from Previous Person
    Given We get the <countOfRandomIds> random Person EPR ids <sourceTable>
    When Get the records from transform previous person
    Then We Get the records from transform previous person History
    And Compare the records of previous person and previous person history
    Examples:
      | sourceTable                  | countOfRandomIds|
      |jrbi_transform_previous_person|   10                 |


  @JRBI
  Scenario Outline: Verify Data from the difference of current_person and previous_person is transferred to delta current person table
    Given We get the <countOfRandomIds> random Person EPR ids <tableReference>
    When Get the records from the difference of current_person and previous_person
    Then Get the records from transform Delta Current person
    And  Compare the records of Delta Current person with difference of current and previous person
    Examples:
      | tableReference                  | countOfRandomIds|
      |jrbi_current_previous_person       |10                 |


  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Delta_person_history is transferred from Delta Person
    Given We get the <countOfRandomIds> random Person EPR ids <sourceTable>
    When Get the records from transform Delta Current person
    Then We Get the records from transform Delta person History
    And Compare the records of delta person and delta person history
    Examples:
      | sourceTable              | countOfRandomIds|
      |jrbi_delta_current_person| 10                 |


  @JRBI
  Scenario Outline: Verify Data from the difference of Delta_person and person_history is transferred to person exclude table
    Given We get the <countOfRandomIds> random Person EPR ids <tableName>
    When Get the records from the difference of Delta_current_person and person_history
    And Get the records from person exclude jrbi ext table
    And  Compare the records of Person Exclude with difference of Delta_current_person and person_history
    Examples:
      |tableName                                | countOfRandomIds|
      |jrbi_transform_history_person_excl_delta|10                |

  @JRBI
  Scenario Outline: Verify Data from the addition of Delta_current_person and person_Exclude is transferred to person Latest table
    Given We get the <countOfRandomIds> random Person EPR ids <tableName>
    When Get the records from the addition of Delta_Person and Person_Exclude
    Then Get the records from Person latest table
    And  Compare the records of Person Latest with addition of Delta_current_Person and Person_Exclude
    Examples:
      |tableName                                | countOfRandomIds|
      |jrbi_transform_latest_person               |10                 |
