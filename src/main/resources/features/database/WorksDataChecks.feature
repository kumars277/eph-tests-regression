Feature: Checking the data between PMX works and EPH works

  Scenario Outline: Checking the Works data between PMX and EPH
    Given We have a work with <random_value> id to check
    When We get the product data from PMX, EPH Staging and EPH using <parameter>
    Then The work data between PMX and PMX STG is identical
    #And The work data between PMX STG and EPH is identical

    Examples:
      | random_value |parameter|
      | PRIMARY_ISBN |isbn     |
      | PRIMARY_ISBN |isbn     |
      | PRIMARY_ISBN |isbn     |
      | ISSN_L       |issn     |
      | ISSN_L       |issn     |
      | ISSN_L       |issn     |

  #Scenario: Checking the data between PMX and EPH by ISSN
   # Given We have a work with ISSN_L id to check
   # When We get the product data from PMX, EPH Staging and EPH using issn
   # Then The work data between PMX and PMX STG is identical
    #And The work data between PMX STG and EPH is identical

   # Examples:
    #  | issn          |
     # | 2531-0488     |
