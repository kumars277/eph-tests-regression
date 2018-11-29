Feature: Validate Product details against DQ library Tier 1 checks

 # DQ Rules details are available in Confluence
#  https://confluence.cbsels.com/display/DQH/Data+Quality+Rule+Overview

  Scenario Outline: Validate Product details against DQ library Tier 1 checks
    Given the data is loaded from PMX to EPH
    When the product from PMX Source fails a <rule> Tier 1 DQ rule
    Then the product is not loaded into the golden layer in EPH
    And the product is written to the Data Quality output table with all attribute
    And the product is listed as Tier 1

    Examples:
      |                     rule                          |
      |   Availability status for product manifestation   |
      |   duplicates ISSN   |
