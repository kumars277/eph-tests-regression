Feature:Validate data for JRBI Work tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data for JRBI transform_current_person is transferred from person_data_full
      Given We get the <countOfRandomIds> random EPR ids for Person from data full load <sourceTable>
      When Get the records from data full load for Person <sourceTable>
      Then Get the records from transform current person <targetTable>
      And Compare the records of person full load and current person <targetTable>
    Examples:
     | sourceTable| targetTable                                 | countOfRandomIds|
    |jrbi_journal_data_full|jrbi_transform_current_person               |10                 |


      
