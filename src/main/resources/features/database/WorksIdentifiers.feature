Feature: Checking the data in the Works Identifier table

  @Regression
  Scenario Outline: Check if all of the identifiers are stored in the identifiers table
    Given We have a work from type <type> to check by <random value>
    When We get the data from Staging, SA and Work Identifiers using <random value>
    Then All of the identifiers are stored
    And The identifiers data is correct
    And The identifiers data between SA and GD is identical

    Examples:
      |type       |random value|
      |STAB       |PRIMARY_ISBN|
      |SERMEM     |PRIMARY_ISBN|
      |BOOKSET    |PRIMARY_ISBN|
      |BOOKSERIES |ISSN_L|
      |CONT       |ISSN_L|
      |JNL        |ISSN_L|
      |CABS       |ISSN_L|