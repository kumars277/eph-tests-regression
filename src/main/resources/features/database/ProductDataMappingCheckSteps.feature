Feature: Entity - PRODUCT - Data Mapping Check - Validate data between PMX and EPH - Talend Load


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
  Scenario Outline: Validate data is transferred from EPH STG to EPH STG DQ
    Given We get <countOfRandomIds> random ids for <type>
    When We get the data from EPH STG
    Then We get the data from EPH STG DQ for <type>
    And Compare the records in EPH STG and EPH STG DQ for <type>
    Examples:
      | countOfRandomIds | type    |
      | 10               | book    |
      | 10               | package |


  @Regression @WP
  Scenario Outline: Validate data is transferred from EPH STG to EPH STG DQ for journals
    Given We get <countOfRandomIds> ids of journals for <type> with <open_access> and <author_charges>
    When We get the data from EPH STG
    Then We get the data from EPH STG DQ for <type>
    And Depends on the flags of every record from Staging check if we have the expected number of records in EPH STG DQ
    And Compare the records in EPH STG and EPH STG DQ for <type>
    Examples:
      | countOfRandomIds | type                            | open_access | author_charges  |
     # | 10               | print_journal                   | N           |       Y         |
       | 10               | print_journal                   | N           |       N         |
       | 10               | print_journal                   | Y           |       Y         |
#      | 10               | electronic_journal              | N           |       Y         |
       | 10               | electronic_journal              | N           |       N         |
       | 10               | electronic_journal              | Y           |       Y         |
#      | 10              | non_print_or_electronic_journal  | N           |  ##covered from packages scenario


  @Regression @WP
  Scenario Outline: Validate data is transferred from EPH STG DQ and EPH SA and GD for books
    Given We get <countOfRandomIds> random ids for <type>
    When We get the data from EPH STG
    Then We get the data from EPH STG DQ for <type>
    And We get the data from EPH SA for <type>
    And We check that mandatory columns are populated
    And Compare the records in EPH STG DQ and EPH SA for <type>
    And We get the data from EPH GD for <type>
    And Compare the products data between EPH SA and EPH GD for <type>
    Examples:
      | countOfRandomIds | type |
      | 10               | book |
      | 10               | package |

  @Regression @WP
  Scenario Outline: Validate data is transferred from EPH STG DQ and EPH SA and GD for journals
    Given We get <countOfRandomIds> ids of journals for <type> with <open_access> and <author_charges>
    When We get the data from EPH STG
    When  We get the data from EPH STG DQ for <type>
    Then We get the data from EPH SA for <type>
    And We check that mandatory columns are populated
    And Compare the records in EPH STG DQ and EPH SA for <type>
    And We get the data from EPH GD for <type>
    And Compare the products data between EPH SA and EPH GD for <type>

    Examples:
      | countOfRandomIds | type                            | open_access | author_charges  |
#     | 10               | print_journal                   | N           |       Y         |
      | 10               | print_journal                   | N           |       N         |
      | 30               | print_journal                   | Y           |       Y         |
#     | 10               | electronic_journal              | N           |       Y         |
      | 10               | electronic_journal              | N           |       N         |
      | 10               | electronic_journal              | Y           |       Y         |
#     | 10               | non_print_or_electronic_journal | N           |  ##covered from packages scenario




#  @Regression
#  Scenario: Check if a product is linked to different works and manifestations (duplicate product with different product id and manifestation ids)
#  Given Check the db for duplicate products