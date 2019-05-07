Feature: EMS - Customer Service - Customer Search API
  As an EIP-MS Integration User
  I would like to search products from Enterprise Customer Hub using EIP Search
  So that I can use the details to validate business needs

  @API
  Scenario: search product by ID
    Given We get 1 random search ids for products
    And We get the search data from EPH GD for products
    Then the product details are retrieved and compared


  @API @new
  Scenario Outline: search product by title
    Given We get 1 random search ids for products
    And We get the search data from EPH GD for products
    Then the product details are retrieved when searched by <title> and compared
    Examples:
      | title                            |
      | PRODUCT_PRODUCT_TITLE            |
      | WORK_MANIFESTATION_TITLE         |
      | PRODUCT_MANIFESTATION_WORK_TITLE |

  @Api @new
  Scenario Outline: search product by identifier
    Given We get 1 random search ids for products
    And We get the search data from EPH GD for products
    Then the product details are retrieved and compared when searched by <idType>
    Examples:
      | idType                           |
      | PRODUCT_ID                       |
      | PRODUCT_WORK_IDENTIFIER          |
      | PRODUCT_MANIFESTATION_IDENTIFIER |
      | PRODUCT_WORK_ID                  |
      | PRODUCT_MANIFESTATION_ID         |

  @Api @new
  Scenario Outline: search product by type and identifier
    Given We get 1 random search ids for products
    And We get the search data from EPH GD for products
    Then the products detail are retrieved and compared when searched by type and <identifier>
    Examples:
      | identifier                       |
      | PRODUCT_WORK_IDENTIFIER          |
      | PRODUCT_MANIFESTATION_IDENTIFIER |

  @Api @new
  Scenario Outline: search product with search option
    Given We get 1 random search ids for products
    And We get the search data from EPH GD for products
    Then the product details are retrieved and compared when search option is used with <idType>
    Examples:
      | idType                           |
      | PRODUCT_ID                       |
      | PRODUCT_WORK_IDENTIFIER          |
      | PRODUCT_MANIFESTATION_IDENTIFIER |
      | PRODUCT_WORK_ID                  |
      | PRODUCT_MANIFESTATION_ID         |
#      | PRODUCT_PRODUCT_TITLE            |
#      | WORK_MANIFESTATION_TITLE         |
#      | PRODUCT_MANIFESTATION_WORK_TITLE |