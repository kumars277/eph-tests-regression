Feature:Validate data for SDBooks in Data Lake Access Layer

  #confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45487295732/SD+Books+Inbound

  @mercury
  Scenario Outline: Verify Data for mercury print Consumer Application extracted from EPH
    Given Get the total count from EPH <tableName>
    Then  We know the total count from mercury view <tableName>
    And  Compare counts of EPH and MercuryPrint view <tableName>
    Given Get the <countOfRandomIds> random ids from EPH <tableName>
    When We Get the records from the source EPH <tableName>
    Then We Get the records from the Mercury Print <tableName>
    And  compare the rec of both EPH and Mercury Print views <tableName>
    Examples:
      | tableName                   |    countOfRandomIds |
      |extract_mercury_print_v      |500                   |



