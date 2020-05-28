Feature:Validate data for JRBI Work tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_manifestation is transferred from data_full_manifestation
      Given We get the <countOfRandomIds> random EPR ids for Manifestation from data full load <sourceTable>
      When Get the records from data full load for Manifestation <sourceTable>
      Then Get the records from transform current manifestation <targetTable>
      And Compare the records of manifestation full load and current manifestation <targetTable>
    Examples:
     | sourceTable         | targetTable                               | countOfRandomIds|
    |jrbi_journal_data_full|jrbi_transform_current_manifestation       |10                 |


      
