Feature: Checking the data between PMX works and EPH works

  Scenario Outline: Checking the Works data between PMX and EPH
    Given We have a work with <random_value> id and type <type> to check
    When We get the product data from PMX, EPH Staging and EPH using <random_value>
    Then The work data between PMX and PMX STG is identical
    #And The work data between PMX STG and EPH is identical

    Examples:
      | random_value |type       |
      | PRIMARY_ISBN |STAB       |
      | PRIMARY_ISBN |SERMEM     |
      | PRIMARY_ISBN |BOOKSET    |
      #| PRIMARY_ISBN |BOOKSERIES |
      | ISSN_L       |CONT       |
      | ISSN_L       |JNL        |
      | ISSN_L       |CABS       |

  #Scenario: Checking the data between PMX and EPH by ISSN
   # Given We have a work with ISSN_L id to check
   # When We get the product data from PMX, EPH Staging and EPH using issn
   # Then The work data between PMX and PMX STG is identical
    #And The work data between PMX STG and EPH is identical

   # Examples:
    #  | issn          |
     # | 2531-0488     |
