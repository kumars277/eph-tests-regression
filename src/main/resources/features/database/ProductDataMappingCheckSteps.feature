Feature: Products data mapping checks

  @WIP
  Scenario Outline: Validate data is transferred from PMX to EPH STG
    Given We get <countOfRandomIds> random ids for <type>
    When We get the corresponding records from PMX
    Then We get the data from EPH STG
    And Compare the records in PMX and EPH STG for <type>
    Examples:
      | countOfRandomIds | type    |
      | 10               | journal |
      | 10               | book    |

  @WIP
  Scenario Outline: Validate data is transferred from EPH STG and EPH SA for books
    Given We get <countOfRandomIds> random ids for <type>
    When we get the corresponding pmx_source_reference ids for the random ids for <type>
    Then We get the data from EPH STG
    And We get the data from EPH SA
    And Compare the records in EPH STG and EPH SA for <type>
    Examples:
      | countOfRandomIds | type |
      | 10               | book |

  @WIP
  Scenario Outline: Validate data is transferred from EPH STG and EPH SA for journals
    Given We get <countOfRandomIds> random ids for <type>
    When we get the corresponding pmx_source_reference ids for the random ids for <type>
    Then We get the data from EPH STG
    And We get the data from EPH SA
    And Compare the records in EPH STG and EPH SA for <type>
    Examples:
      | countOfRandomIds | type |
      | 2                | print_journal |
      | 2                | electronic_journal |
      | 2                | non_print_or_electronic_journal |

    @WIP
    Scenario: Validate data mapping between EPH STG and EPH SA for journals with OAA
