Feature: Search API: Products
  As an EIP-MS Integration User
  I would like to search products from Enterprise Product Hub using EIP Search
  So that I can use the details to validate business needs
#confluence page
#https://elsevier.atlassian.net/wiki/spaces/IN/pages/88417842132/EPHMS001+-+Enterprise+Product+Search+V2+retired?pageId=88417842132
#https://elsevier.atlassian.net/wiki/spaces/IN/pages/88442085624/Search+API+Automation


  @searchAPI @productSearchAPI
  Scenario: search product by ID
    Given   We get 1 random search ids for products default
    And     We get the search data from EPH GD for products
    Then    the product details are retrieved and compared

  @searchAPI @productSearchAPI
  Scenario Outline: search product by title
    Given We get 1 random search ids for products <title>
    And We get the search data from EPH GD for products
    Then the product details are retrieved when searched by <title> and compared
    Examples:
      | title                              |
      | PRODUCT_TITLE                      |
      | PRODUCT_MANIFESTATION_TITLE        |
      | PRODUCT_MANIFESTATION_WORK_TITLE   |

  @searchAPI @productSearchAPI
  Scenario Outline: search product by identifier
    Given  We get 1 random search ids for products <idType>
    And    We get the search data from EPH GD for products
    Then   the product details are retrieved and compared when searched by <idType>
    Examples:
      | idType                                |
      | PRODUCT_WORK_ID                       |
      | PRODUCT_ID                            |
     #| PRODUCT_IDENTIFIER                    |
      | PRODUCT_WORK_IDENTIFIER               |
      | PRODUCT_MANIFESTATION_ID              |
      | PRODUCT_MANIFESTATION_IDENTIFIER      |
      | PRODUCT_MANIFESTATION_WORK_ID         |
      | PRODUCT_MANIFESTATION_WORK_IDENTIFIER |

  @searchAPI @productSearchAPI
  Scenario Outline: search product by type and identifier
    Given  We get 1 random search ids for products <identifier>
    And    We get the search data from EPH GD for products
    Then   the products detail are retrieved and compared when searched by type and <identifier>
    Examples:
      | identifier                            |
  #   | PRODUCT_IDENTIFIER                    |
      | PRODUCT_MANIFESTATION_IDENTIFIER      |
      | PRODUCT_WORK_IDENTIFIER               |
      | PRODUCT_MANIFESTATION_WORK_IDENTIFIER |

  @searchAPI @productSearchAPI
  Scenario Outline: search product with search option
    Given  We get 1 random search ids for products <idType>
    And    We get the search data from EPH GD for products
    Then   the product details are retrieved and compared when search option is used with <idType>
    Examples:
      | idType                                      |
      | PRODUCT_PERSONS_FULLNAME                    |
      | PRODUCT_MANIFESTATION_ID                    |
      | PRODUCT_TITLE                               |
      | PRODUCT_MANIFESTATION_WORK_PERSONS_FULLNAME |
      | PRODUCT_ID                                  |
 #    | PRODUCT_IDENTIFIER                          |
      | PRODUCT_WORK_ID                             |
      | PRODUCT_WORK_TITLE                          |
      | PRODUCT_WORK_IDENTIFIER                     |
      | PRODUCT_MANIFESTATION_TITLE                 |
      | PRODUCT_MANIFESTATION_IDENTIFIER            |
      | PRODUCT_MANIFESTATION_WORK_ID               |
      | PRODUCT_MANIFESTATION_WORK_TITLE            |
      | PRODUCT_MANIFESTATION_WORK_IDENTIFIER       |

      | PRODUCT_WORK_PERSONS_FULLNAME               |

  @searchAPI @productSearchAPI
  Scenario: search product by person ID
    Given   We get 1 search ids from the db for person roles of products
    Then    the product response returned when searched by personID is verified

  @searchAPI @productSearchAPI
  Scenario: search product by PMC
    Given   We get 1 random search ids for products default
    And     We get the search data from EPH GD for products
    Then    the product details are retrieved by PMC Code and compared

  @searchAPI @productSearchAPI
  Scenario: search product by PMG
    Given   We get 1 random search ids for products default
    And     We get the search data from EPH GD for products
    Then    the product details are retrieved by PMG Code and compared

#created by Nishant
  @searchAPI @productSearchAPI @CkAPI
  Scenario: search products in Package - IsInPackage
    Given   We get 1 random package id
    Then    the product response returned when searched by packages is verified

  @searchAPI @productSearchAPI @CkAPI
  Scenario: search packages by products - hasComponents
    Given   We get 1 random package id
    And     We get 1 random search ids from package
    Then    the product response returned when searched by components is verified

  @searchAPI @productSearchAPI
  Scenario Outline: search product by accountableProduct
    Given  We get 1 random search ids for products <type>
    And    We get the search data from EPH GD for products
    Then   the product details are retrieved and compared by <type>
    Examples:
      | type                                           |
      | PRODUCT_MANIFESTATION_WORK_ACCOUNTABLE_PRODUCT |
      | PRODUCT_WORK_ACCOUNTABLE_PRODUCT               |
      #|PRODUCT_ACCOUNTABLE_PRODUCT| NA as per Lujiang on 13 May 2020

  @searchAPI @productSearchAPI
  Scenario Outline: search product by parameters
    Given  We get 1 random search ids for products <paramKey>
    And    We get the search data from EPH GD for products
    Then   the product count are retrieved by <paramKey> compared
    Examples:
      | paramKey      |
      | productType   |
      | productStatus |
      | workType      |
      | pmcCode       |
      | pmgCode       |

  @searchAPI @productSearchAPI
  Scenario: Product search E2E
    Given  We set specific product ids for search
    And    We get the search data from EPH GD for products
    Then   the product details are retrieved and compared

  @searchAPI @productSearchAPI
  Scenario Outline: search product and verify title contains searchKey
    Given  We get 1 random search ids for products <paramKey>
    And    We get the search data from EPH GD for products
    Then   the product title are retrieved by <paramKey> compared
    Examples:
      | paramKey      |
      | productStatus |

  @notInUse
  Scenario Outline: verify search API pagination productSearch
    Given verify pagination duplicate ids retried for product <Keyword> with <scroll>
    Examples:
      |Keyword        | scroll  |
      |science            |30s      |
      |test               |40s      |
      |technolog          |50s      |
      |research           |60s      |
      |development        |80s      |
      |people             |90s      |
      | doctor            |10s      |
      | maths             |20s      |
      | bio               |30s      |
      | cell              |30s      |
      | engineer          |30s      |
      | blood             |30s      |
      | medicine          |30s      |