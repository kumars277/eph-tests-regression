Feature: Data Quality Checks

  As a EPH System User
  I Would like to validate the  EPH Entities Data Qualities and Business Rules
  So that the data in the EPH Systems are valid and meeting the expected data quality requirements.

  @ZERO-DEPLOYMENT
  Scenario: Validate Product details against DQ library Tier 1 checks
    Given The data is loaded from PMX to EPH
    When  The product from PMX Source fails a Tier 1 DQ rule
    Then  The product is not loaded into the golden layer in EPH
    And The product is written to the Data Quality output table with all attributes
    And The product is listed as a Tier 1 DQ rule failure
