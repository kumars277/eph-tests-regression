Feature: Products data mapping checks

  @Regression
  Scenario Outline: Validate data is transferred from PMX to EPH STG
    Given We get <countOfRandomIds> random ids for <type>
    When We get the corresponding records from PMX
    Then We get the data from EPH STG
    And Compare the records in PMX and EPH STG for <type>
    Examples:
      | countOfRandomIds | type    |
      | 10               | journal |
      | 10               | book    |

  @Regression
  Scenario Outline: Validate data is transferred from EPH STG and EPH SA for books
    Given We get <countOfRandomIds> random ids for <type>
    Then We get the data from EPH STG
    And We get the data from EPH SA
    And We check that mandatory columns are populated
    And Compare the records in EPH STG and EPH SA for books
    And We get the data from EPH GD
    And Compare the products data between EPH SA and EPH GD
    Examples:
      | countOfRandomIds | type |
      | 10               | book |

  @Regression
  Scenario Outline: Validate data is transferred from EPH STG and EPH SA for journals
    Given We get <countOfRandomIds> ids of journals for <type> with <open_access>
    When We get the data from EPH STG
    Then We get the data from EPH SA for journals
    And We check that mandatory columns are populated
    And Depends on the flags of every record from Staging check if we have the expected number of records in SA
    And Compare the records in EPH STG and EPH SA for journals with <type>
    And We get the data from EPH GD for journals
    And Compare the products data between EPH SA and EPH GD

    Examples:
      | countOfRandomIds | type                            | open_access |
      | 2                | print_journal                   | N           |
      | 2                | print_journal                   | Y           |
      | 2                | electronic_journal              | N           |
      | 2                | electronic_journal              | Y           |
      | 2                | non_print_or_electronic_journal | N           |


