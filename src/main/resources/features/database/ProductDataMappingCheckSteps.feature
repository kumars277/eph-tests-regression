Feature: Entity - PRODUCT - Data Mapping Check - Validate data between PMX and EPH - Talend Full Load


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
      | 10               | package |

    
  @Regression
  Scenario Outline: Validate data is transferred from EPH STG to EPH STG Canonical
    Given We get <countOfRandomIds> random ids for <type>
    When We get the data from EPH STG
    Then We get the data from EPH STG Canonical for <type>
    And Compare the records in EPH STG and EPH STG Canonical for <type>
    Examples:
      | countOfRandomIds | type    |
      | 10               | book    |
      | 10               | package |


  @Regression
  Scenario Outline: Validate data is transferred from EPH STG to EPH STG Canonical for journals
    Given We get <countOfRandomIds> ids of journals for <type> with <open_access>
    When We get the data from EPH STG
    Then We get the data from EPH STG Canonical for <type>
    And Depends on the flags of every record from Staging check if we have the expected number of records in EPH STG Canonical
    And Compare the records in EPH STG and EPH STG Canonical for <type>
    Examples:
      | countOfRandomIds | type                            | open_access |
      | 10               | print_journal                   | N           |
      | 10               | print_journal                   | Y           |
      | 10               | electronic_journal              | N           |
      | 10               | electronic_journal              | Y           |
#      | 10              | non_print_or_electronic_journal | N           |  ##covered from packages scenario


  @Regression
  Scenario Outline: Validate data is transferred from EPH STG Canonical and EPH SA for books
    Given We get <countOfRandomIds> random ids for <type>
    Then We get the data from EPH STG Canonical for <type>
    And We get the data from EPH SA for <type>
    And We check that mandatory columns are populated
    And Compare the records in EPH STG Canonical and EPH SA for <type>
    And We get the data from EPH GD for <type>
    And Compare the products data between EPH SA and EPH GD for <type>
    Examples:
      | countOfRandomIds | type |
      | 10               | book |
      | 10               | package |

  @Regression
  Scenario Outline: Validate data is transferred from EPH STG Canonical and EPH SA for journals
    Given We get <countOfRandomIds> ids of journals for <type> with <open_access>
    When  We get the data from EPH STG Canonical for <type>
    Then We get the data from EPH SA for <type>
    And We check that mandatory columns are populated
    And Compare the records in EPH STG Canonical and EPH SA for <type>
    And We get the data from EPH GD for <type>
    And Compare the products data between EPH SA and EPH GD for <type>

    Examples:
      | countOfRandomIds | type                            | open_access |
      | 10               | print_journal                   | N           |
      | 10               | print_journal                   | Y           |
      | 10               | electronic_journal              | N           |
      | 10               | electronic_journal              | Y           |
#      | 10               | non_print_or_electronic_journal | N           |  ##covered from packages scenario


