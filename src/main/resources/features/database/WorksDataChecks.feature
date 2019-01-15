Feature: Checking the data between PMX works and EPH works

  Scenario Outline: Checking the data between PMX and EPH Staging works
    Given We have a work with isbn <isbn> id to check
    When We get the product data from PMX, EPH Staging and EPH using isbn
    Then The work data between PMX and PMX STG is identical
    #And The work data between PMX STG and EPH is identical

    Examples:
      | isbn          |
      | 9782294704864 |
      #| 9783437462108 |
      #| 9780128039724 |
      #| 9780723610755 |

  Scenario Outline: Checking the data between PMX and EPH Staging works
    Given We have a work with issn <issn> id to check
    When We get the product data from PMX, EPH Staging and EPH using issn
    Then The work data between PMX and PMX STG is identical
    #And The work data between PMX STG and EPH is identical

    Examples:
      | issn          |
      | 1812-7676     |
