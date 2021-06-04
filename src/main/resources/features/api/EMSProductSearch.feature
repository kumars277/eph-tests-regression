Feature: Search API: Products
  As an EIP-MS Integration User
  I would like to search products from Enterprise Customer Hub using EIP Search
  So that I can use the details to validate business needs

  @searchAPI @productSearchAPI @searchAPIdebug
  Scenario: search product by ID
    Given We get 1 random search ids for products default
    And We get the search data from EPH GD for products
    Then the product details are retrieved and compared

  @searchAPI @productSearchAPI
  Scenario Outline: search product by title
    Given We get 1 random search ids for products <title>
    And We get the search data from EPH GD for products
    Then the product details are retrieved when searched by <title> and compared
    Examples:
      | title                            |
      | PRODUCT_PRODUCT_TITLE            |
      | WORK_MANIFESTATION_TITLE         |
      | PRODUCT_MANIFESTATION_WORK_TITLE |

  @searchAPI @productSearchAPI
  Scenario Outline: search product by identifier
    Given We get 1 random search ids for products <idType>
    And We get the search data from EPH GD for products
    Then the product details are retrieved and compared when searched by <idType>
    Examples:
      | idType                           |
  #    |PRODUCT_IDENTIFIER                |
      |PRODUCT_WORK_IDENTIFIER           |
      |PRODUCT_MANIFESTATION_IDENTIFIER  |
      |PRODUCT_MANIFESTATION_WORK_IDENTIFIER|
      |PRODUCT_ID                           |
      |PRODUCT_WORK_ID                      |
      |PRODUCT_MANIFESTATION_ID             |
      |PRODUCT_MANIFESTATION_WORK_ID        |

  @searchAPI @productSearchAPI
  Scenario Outline: search product by type and identifier
    Given We get 1 random search ids for products <identifier>
    And We get the search data from EPH GD for products
    Then the products detail are retrieved and compared when searched by type and <identifier>
    Examples:
      | identifier                       |
  #    |PRODUCT_IDENTIFIER                |
      |PRODUCT_WORK_IDENTIFIER          |
      |PRODUCT_MANIFESTATION_IDENTIFIER |
      |PRODUCT_MANIFESTATION_WORK_IDENTIFIER|

  @searchAPI @productSearchAPI
  Scenario Outline: search product with search option
    Given We get 1 random search ids for products <idType>
    And We get the search data from EPH GD for products
    Then the product details are retrieved and compared when search option is used with <idType>
    Examples:
      | idType                                     |
      | PRODUCT_ID                                 |
  #    | PRODUCT_IDENTIFIER                         |
      | PRODUCT_WORK_IDENTIFIER                    |
      | PRODUCT_WORK_ID                            |
      | PRODUCT_MANIFESTATION_IDENTIFIER           |
      | PRODUCT_MANIFESTATION_ID                   |
      | PRODUCT_MANIFESTATION_WORK_IDENTIFIER      |
      | PRODUCT_MANIFESTATION_WORK_ID              |
      | PRODUCT_TITLE                              |
      | PRODUCT_WORK_TITLE                         |
      | PRODUCT_MANIFESTATION_TITLE                |
      | PRODUCT_MANIFESTATION_WORK_TITLE           |
      | PRODUCT_PERSONS_FULLNAME                   |
      | PRODUCT_WORK_PERSONS_FULLNAME              |
      | PRODUCT_MANIFESTATION_WORK_PERSONS_FULLNAME|

  @searchAPI @productSearchAPI
  Scenario: search product by person ID
    Given We get 1 search ids from the db for person roles of products
    Then the product response returned when searched by personID is verified

  @searchAPI @productSearchAPI
  Scenario: search product by PMC
    Given We get 1 random search ids for products default
    And We get the search data from EPH GD for products
    Then the product details are retrieved by PMC Code and compared

  @searchAPI @productSearchAPI
  Scenario: search product by PMG
    Given We get 1 random search ids for products default
    And We get the search data from EPH GD for products
    Then the product details are retrieved by PMG Code and compared
   ##created by Nishant

  @searchAPI @productSearchAPI
  Scenario: search products in Package - IsInPackage
    Given We get 1 random package id
    Then the product response returned when searched by packages is verified

  @searchAPI @productSearchAPI
  Scenario: search packages by products - hasComponents
    Given We get 1 random package id
    And We get 1 random search ids from package
    Then the product response returned when searched by components is verified

  @searchAPI @productSearchAPI
  Scenario Outline: search product by accountableProduct
    Given We get 1 random search ids for products <type>
    And We get the search data from EPH GD for products
    Then the product details are retrieved and compared by <type>
    Examples:
    |type|
    |PRODUCT_WORK_ACCOUNTABLE_PRODUCT|
    |PRODUCT_MANIFESTATION_WORK_ACCOUNTABLE_PRODUCT|
  #  |PRODUCT_ACCOUNTABLE_PRODUCT| NA as per Lujiang on 13 May 2020

  @searchAPI @productSearchAPI
  Scenario Outline: search product by parameters
    Given We get 1 random search ids for products <paramKey>
    And We get the search data from EPH GD for products
    Then the product count are retrieved by <paramKey> compared
    Examples:
      | paramKey         |
      | productStatus    |
      | productType      |
      | workType         |
      | manifestationType|
      | pmcCode          |
      | pmgCode          |

  @searchAPI @productSearchAPI
  Scenario Outline: Product search E2E
    Given We get product by ID <id>
    And We get the search data from EPH GD for products
    Then the product details are retrieved and compared
    Examples:
      | id                                   |
      | EPR-11BBFJ                           |
      | EPR-11BBFK                           |
      | EPR-11BBFM                           |
      | EPR-11BBFN                           |
      | EPR-11BBFR                           |


