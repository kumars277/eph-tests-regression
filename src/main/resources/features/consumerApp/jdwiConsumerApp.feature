Feature:Validate data for JDW App in Data Lake

  @jdw
  Scenario Outline: Verify Data for jdw view consumer application
    Given Get the total count of Full set <tableName>
    Then  We know the total count of <tableName>
    And  Compare count of Full load with <tableName>
    Given We get the <countOfRandomIds> random ids <tableName>
    When We Get the records from full set data <tableName>
    Then We Get the records from the views of <tableName>
    And  we compare records of full set and <tableName>
    Examples:
      | tableName                   |    countOfRandomIds |
      |jdw_journal_lov_vw                   |500                   |



