Feature:Validate data for JRBI Work tables in Data Lake

#  Created by Dinesh on 26/05/2020
  #updated Dinesh on 07/04/2021
  #confluence Version: v.3
  #Confluence LinK: https://confluence.cbsels.com/pages/viewpage.action?pageId=168466078



  @JRBIETLExtended
  Scenario Outline: Verify Data for JRBI transform_Current_work_history is transferred from Current Work
    Given We get the <countOfRandomIds> random EPR ids <sourceTable>
    When We get the records from transform current work
    Then Get the records from transform current work history
    And Compare the records of current work and current work history
    Examples:
      | sourceTable               | countOfRandomIds|
      |jrbi_transform_current_work| 10                |

  @JRBIETLExtended
  Scenario Outline: Verify Data for JRBI transform_Previous_work_history is transferred from Previous Work
    Given We get the <countOfRandomIds> random EPR ids <sourceTable>
    When We get the records from transform previous work
    Then Get the records from transform previous work history
    And Compare the records of previous work and previous work history
    Examples:
      | sourceTable                  | countOfRandomIds|
      |jrbi_transform_previous_work  |10                 |

  @JRBIETLExtended
  Scenario Outline: Verify Data from the difference of current_work and previous_work is transferred to delta current work table
    Given We get the <countOfRandomIds> random EPR ids <tableReference>
    When Get the records from the difference of current_work and previous_work
    Then We get the records from transform delta current work
    And  Compare the records of Delta Current work with difference of current and previous work
    Examples:
      |tableReference                       | countOfRandomIds|
      |jrbi_transform_previous_current_work| 10                 |


  @JRBIETLExtended
  Scenario Outline: Verify Data for JRBI delta_current_work_history is transferred from delta current Work
    Given We get the <countOfRandomIds> random EPR ids <sourceTable>
    When We get the records from transform delta current work
    Then Get the records from transform delta work history
    And Compare the records of delta work and delta work history
    Examples:
      | sourceTable               | countOfRandomIds|
      |jrbi_delta_current_work|         10                 |


  @JRBIETLExtended
  Scenario Outline: Verify Data from the difference of Delta_current_work and work_history is transferred to work exclude table
    Given We get the <countOfRandomIds> random EPR ids <tableName>
    When Get the records from the difference of Delta_current_work and work_history
    And Get the records from work jrbi ext exclude table for data check
    And  Compare the records of Work Exclude with difference of Delta_current_work and work_history
    Examples:
      |tableName                                | countOfRandomIds|
      |jrbi_transform_history_work_excl_delta   |10                 |

  @JRBIETLExtended
  Scenario Outline: Verify Data from the addition of Delta_current_work and work_Exclude is transferred to work Latest table
    Given We get the <countOfRandomIds> random EPR ids <tableName>
    When Get the records from the addition of Delta_current_work and work_Exclude
    Then Get the records from work latest table
    And  Compare the records of Work Latest with addition of Delta_current_work and work_Exclude
    Examples:
      |tableName                                | countOfRandomIds|
      |jrbi_transform_latest_work               |10                 |

