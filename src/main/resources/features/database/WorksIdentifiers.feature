Feature: Entity - Work Identifier - Count & Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario Outline: Check if all of the identifiers are stored in the identifiers table
    Given We have a work from type <type> to check
    When We get the data from Staging, SA and Work Identifiers
    Then All of the identifiers are stored
    And The identifiers data is correct
    And The identifiers data between SA and GD is identical

    Examples:
      |type       |
      |STAB       |
      |SERMEM     |
      |BOOKSET    |
      |BOOKSERIES |
      |CONT       |
      |JNL        |
      |CABS       |