Feature:Validate data for Exari App in Data Lake

  @exari
  Scenario Outline: Verify Data for exari view consumer application
    Given Get the total count of Full set <tableName>
    Then  We know the total count of <tableName>
    And  Compare count of Full load with <tableName>
    Given We get the <countOfRandomIds> random ids <tableName>
    When We Get the records from full set data <tableName>
    Then We Get the records from the views of <tableName>
    And  we compare records of full set and <tableName>
    Examples:
      | tableName                   |    countOfRandomIds |
      |exari_view                   |500                   |



