Feature:Validate data for R12 App in Data Lake

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
      |r12_full_data_v              |1                   |
      |drm_action_scripts_v         |1                   |


