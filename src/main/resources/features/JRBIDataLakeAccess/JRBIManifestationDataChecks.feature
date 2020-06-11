Feature:Validate data for JRBI Work tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_manifestation is transferred from data_full_manifestation
      Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
      When Get the records from data full load for Manifestation
      Then Get the records from transform current manifestation
      And Compare the records of manifestation full load and current manifestation
    Examples:
     | sourceTable         | countOfRandomIds|
    |jrbi_journal_data_full|       1        |


  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_manifestation_history is transferred from Current manifestation
    Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
    When Get the records from transform current manifestation
    Then We Get the records from transform current manifestation History
    And Compare the records of current manifestation and current manifestation history
    Examples:
      | sourceTable                        | countOfRandomIds|
      |jrbi_transform_current_manifestation| 50                 |

  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Previous_manifestation_history is transferred from manifestation
    Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
    When We get the records from transform previous manifestation
    Then Get the records from transform previous manifestation history
    And Compare the records of previous manifestation and previous manifestation history
    Examples:
      | sourceTable                         | countOfRandomIds|
      |jrbi_transform_previous_manifestation|   50                 |


  @JRBI
  Scenario Outline: Verify Data from the difference of current_manif and previous_manif is transferred to delta current manif table
    Given We get the <countOfRandomIds> random manifestation EPR ids <tableRef>
    When Get the records from the difference of current_manifestation and previous_manifestation
    Then We get the records from transform delta manifestation
    And  Compare the records of Delta Current manifestation with difference of current and previous manifestation
    Examples:
      | tableRef                                 | countOfRandomIds|
      |jrbi_current_previous_manifestation       |50                 |


  @JRBI
  Scenario Outline: Verify Data for JRBI delta_manifestation_history is transferred from delta manifestation
    Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
    When We get the records from transform delta manifestation
    Then Get the records from transform delta manifestation history
    And Compare the records of delta manifestation and delta manifestation history
    Examples:
      | sourceTable                      | countOfRandomIds|
      |jrbi_delta_current_manifestation  |50                 |


  @JRBI
  Scenario Outline: Verify Data from the difference of Delta_manif and manif_history is transferred to manif exclude table
    Given We get the <countOfRandomIds> random manifestation EPR ids <tableName>
    When Get the records from the difference of Delta_current_manif and manif_history
    Then Get the records from manifestation exclude table
    And  Compare the records of Manif Exclude with difference of Delta_current_manif and manif_history
    Examples:
      |tableName                                      | countOfRandomIds|
      |jrbi_transform_history_manifestation_excl_delta|50                 |

  @JRBI
  Scenario Outline: Verify Data from the addition of Delta_current_manifestation and manifestation_Exclude is transferred to manifestation Latest table
    Given We get the <countOfRandomIds> random manifestation EPR ids <tableName>
    When Get the records from the addition of Delta_manifestation_work and manifestation_Exclude
    Then Get the records from manifestation latest table
    And  Compare the records of Manifestation Latest with addition of Delta_current_Manifestation and Manifestation_Exclude
    Examples:
      |tableName                                | countOfRandomIds|
      |jrbi_transform_latest_manifestation               |50                 |




      
