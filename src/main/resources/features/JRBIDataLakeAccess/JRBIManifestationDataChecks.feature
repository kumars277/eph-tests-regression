Feature:Validate data for JRBI Work tables in Data Lake

#  Created by Dinesh on 26/05/2020

    @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_manifestation is transferred from data_full_manifestation
      Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
      When Get the records from data full load for Manifestation
      Then Get the records from transform current manifestation
      And Compare the records of manifestation full load and current manifestation
    Examples:
     | sourceTable         | targetTable                               | countOfRandomIds|
    |jrbi_journal_data_full|jrbi_transform_current_manifestation       |50              |


  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Current_manifestation_history is transferred from Current manifestation
    Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
    When Get the records from transform current manifestation
    Then We Get the records from transform current manifestation History
    And Compare the records of current manifestation and current manifestation history
    Examples:
      | sourceTable               | targetTable                               | countOfRandomIds|
      |jrbi_transform_current_manifestation| jrbi_transform_current_manifestation_history_part  |50                 |


  @JRBI
  Scenario Outline: Verify Data for JRBI transform_Previous_manifestation_history is transferred from manifestation
    Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
    When We get the records from transform previous manifestation
    Then Get the records from transform previous manifestation history
    And Compare the records of previous manifestation and previous manifestation history
    Examples:
      | sourceTable               | targetTable                               | countOfRandomIds|
      |jrbi_transform_previous_manifestation| jrbi_transform_current_manifestation_history_part  |50                 |

  @JRBI
  Scenario Outline: Verify Data for JRBI delta_manifestation_history is transferred from delta manifestation
    Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
    When We get the records from transform delta manifestation
    Then Get the records from transform delta manifestation history
    And Compare the records of delta manifestation and delta manifestation history
    Examples:
      | sourceTable                    | targetTable                               | countOfRandomIds|
      |jrbi_delta_current_manifestation| jrbi_transform_delta_manifestation_history_part  |50                 |


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
      |jrbi_transform_latest_manifestation      |50               |

  @JRBI
  Scenario Outline: Verify Data for product manifestation extended is transferred from jrbi_transform_latest_manifestation
    Given We get the <countOfRandomIds> random manifestation EPR ids <sourceTable>
    When We get the records from transform latest manifestation <sourceTable>
    Then Get the records from productDB manifestation extended <targetTable>
    And Compare the records of transform latest manifestation and manifestation extended
    Examples:
      | sourceTable                       | targetTable             | countOfRandomIds|
      |jrbi_transform_latest_manifestation| manifestation_extended  |50               |

