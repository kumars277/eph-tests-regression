Feature: Entity - WORK - Data Mapping Check - Validate data between PMX and EPH - Talend Full Load

  @Regression
  Scenario Outline: Checking the Works data between PMX and EPH
    Given We have a work with type <type> to check
    When We get the product data from PMX, EPH Staging and EPH
    Then The work data between PMX and PMX STG is identical
    And The work data between PMX STG and STG DQ is identical
    And The work data between STG DQ and SA is identical
    And The work data between EPH SA and EPH GD is identical


    Examples:
      |type       |
      |STAB       |
      |SERMEM     |
      |BOOKSET    |
      |BOOKSERIES |
      |CONT       |
      |JNL        |
      |CABS       |