Feature:Validate data for SDBooks in Data Lake Access Layer

  #confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45487295732/SD+Books+Inbound

  @r12
  Scenario Outline: Verify Data for r12 Consumer Application extracted from EPH
    Given Get the total count of Full set <tableName>
    Then  We know the total count of <tableName>
    And  Compare count of Full load with <tableName>
    Given We get the <countOfRandomIds> random ids <tableName>
    When We Get the records from full set data <tableName>
    Then We Get the records from the views of <tableName>
    And  we compare records of full set and <tableName>
    Examples:
      | tableName                   |    countOfRandomIds |
      |r12_full_data_v              |500                   |
      |drm_action_scripts_v         |500                   |


