Feature: EMS - Customer Service - Customer Search API
  As an EIP-MS Integration User
  I would like to search works from Enterprise Customer Hub using EIP Search
  So that I can use the details to validate business needs

  @API
  Scenario: search work by ID
    Given We get 10 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved and compared

  @API
  Scenario: search work by title
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by title and compared


  @API
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

  @API
  Scenario Outline: Search works by identifier and Type
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work search by identifier <idType> and type details are retrieved and compared
    Examples:
      | idType                        |
      | WORK_IDENTIFIER               |
      | WORK_MANIFESTATION_IDENTIFIER |


  @API
  Scenario Outline: Search works by search option
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the works retrieved by search <option> details are retrieved and compared
    Examples:
      | option                        |
      | WORK_IDENTIFIER               |
      | WORK_ID                       |
      | WORK_MANIFESTATION_ID         |
      | WORK_MANIFESTATION_IDENTIFIER |
      | WORK_PRODUCT_ID               |
      | WORK_MANIFESTATION_TITLE      |
      | WORK_TITLE                    |


  @API
  Scenario: search work by PMC Code
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by PMC Code and compared

  @API
  Scenario: search work by PMG Code
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by PMG Code and compared

  @API
  Scenario: search work by Person roles
    Given We get 1 random search ids for person roles
    Then the work response count is compared with the count in the DB for person ID
