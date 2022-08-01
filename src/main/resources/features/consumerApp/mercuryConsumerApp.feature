Feature:Validate data for Mercury Print in Data Lake

  @mercury
  Scenario Outline: Verify Data for mercury print Consumer Application extracted from EPH
    Given Get the total count of Full set <tableName>
    Then  We know the total count of <tableName>
    And  Compare count of Full load with <tableName>
    Given We get the <countOfRandomIds> random ids <tableName>
    When We Get the records from full set data <tableName>
    Then We Get the records from the views of <tableName>
    And  we compare records of full set and <tableName>
    Examples:
      | tableName                   |    countOfRandomIds |
      |extract_mercury_print_v      |1                   |



