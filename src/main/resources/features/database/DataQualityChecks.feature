Feature: Data Quality Checks

  As a EPH System User
  I Would like to validate the  EPH Entities Data Qualities and Business Rules
  So that the data in the EPH Systems are valid and meeting the expected data quality requirements.

  @ZERO-DEPLOYMENT
  Scenario: Validate Product details between PMX and EPH
    Given A product loaded into EPH from PMX
    When  checking the loaded product in EPH
    Then  the product details are consistent between EPH and PMX


