Feature: Customer Search API: Works
  As an EIP-MS Integration User
  I would like to search works from Enterprise Customer Hub using EIP Search
  So that I can use the details to validate business needs

  @searchAPI
  Scenario: search work by ID
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved and compared

  @searchAPI
  Scenario Outline: search work by title
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by title <tType> and compared
    Examples:
    | tType |
    |WORK_PRODUCT_SUMMARY_NAME|
    |WORK_TITLE |
    |WORK_MANIFESTATION_TITLE|
    |WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME|

  @searchAPI
  Scenario Outline: Search works by identifier
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the works search by identifier <idType> details are retrieved and compared
    Examples:
      | idType                        |
      | WORK_IDENTIFIER               |
      | WORK_MANIFESTATION_IDENTIFIER |
      | WORK_ID                       |
      | WORK_MANIFESTATION_ID         |

  @searchAPI
  Scenario Outline: Search works by identifier and Type
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work search by identifier <idType> and type details are retrieved and compared
    Examples:
      | idType                        |
      | WORK_IDENTIFIER          |
      | WORK_MANIFESTATION_IDENTIFIER |

  @searchAPI
  Scenario Outline: Search works by search option
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the works retrieved by search <option> details are retrieved and compared
    Examples:
      | option                                 |
      | WORK_IDENTIFIER                        |
      | WORK_ID                                |
      | WORK_MANIFESTATION_ID                  |
      | WORK_MANIFESTATION_IDENTIFIER          |
      | WORK_PRODUCT_ID                        |
      | WORK_MANIFESTATION_PRODUCT_ID          |
      | WORK_TITLE                             |
      | WORK_MANIFESTATION_TITLE               |
      | WORK_PERSONS_FULLNAME                  |
      | WORK_PRODUCT_SUMMARY_NAME              |
      | WORK_MANIFESTATION_PRODUCT_SUMMARY_NAME|

  @searchAPI
  Scenario: search work by PMC Code
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by PMC Code and compared

  @searchAPI
  Scenario: search work by PMG Code
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by PMG Code and compared

  @searchAPI
  Scenario: search work by Person id
    Given We get 1 random search ids for person roles
    Then the work response count is compared with the count in the DB for person ID

  @searchAPI
  Scenario Outline: Search E2E
    Given We get id for work search <id>
    And We get the work search data from EPH GD
    Then the work details are retrieved and compared
    Examples:
      | id                        |
      | EPR-W-10C6N8              |

##created by Nishant

  @searchAPI
  Scenario: search work by accountableProduct
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by accountableProduct and compared

  @searchAPI
  Scenario: search work by workStatus
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by workStatus and compared

  @searchAPI
  Scenario: search work by workType
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by workType and compared

  @searchAPI
  Scenario: search work by manifestationType
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by manifestationType and compared

  @searchAPI
  Scenario: search work by Search with PMCCode
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by search with PMC code and compared

  @searchAPI
  Scenario: search work by Search with PMGCode
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by search with PMG code and compared

