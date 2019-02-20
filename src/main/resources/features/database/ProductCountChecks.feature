Feature: Entity - PRODUCT - Count Check - Validate data count between PMX and EPH - Talend Full Load

  @Regression
  Scenario: Count check between PMX and PMX Staging
    Given We know the products count in PMX
    When The products are in PMX Staging
    Then The number of products between PMX and Staging is equal

  @Regression
  Scenario: Count check between PMX Staging and EPH SA
    Given The products are in PMX Staging
    When We know the number of Products in EPH
    Then The number of products between Staging and SA is equal

  @Regression
  Scenario: Count check between EPH SA and EPH GD
    Given The products are in PMX Staging
    When We know the number of Products in EPH
    Then The number of products between SA and GD is equal