Feature: Products data comparison

  @WIP
  Scenario: Validate data is transferred from PMX to PMX Stg
    Given We have a work with  id and type  to check
    When We get the product data from PMX, EPH Staging and EPH using
    Then The work data between PMX and PMX STG is identical
    And The work data between PMX STG and EPH SA is identical
    And The work data between EPH SA and EPH GD is identical
