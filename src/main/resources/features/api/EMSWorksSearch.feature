Feature: EMS - Customer Service - Customer Search API
  As an EIP-MS Integration User
  I would like to search works from Enterprise Customer Hub using EIP Search
  So that I can use the details to validate business needs

  @API @new
  Scenario Outline: search work by ID
    Given We get <countOfRandomIds> random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved and compared
    Examples:
      | countOfRandomIds |
      | 1               |


    @API @new
  Scenario Outline: search work by title
    Given We get <countOfRandomIds> random search ids for works
    And We get the work search data from EPH GD
    Then the work details are retrieved by title and compared
    Examples:
      | countOfRandomIds |
      | 10               |
#

  @API @new
  Scenario Outline: Search works by identifier
    Given We get <countOfRandomIds> random search ids for works
    And We get the work search data from EPH GD
    Then the works search by identifier <idType> details are retrieved and compared
    Examples:
      | countOfRandomIds | idType                        |
      | 1                | WORK_IDENTIFIER               |
      | 1                | WORK_MANIFESTATION_IDENTIFIER |
      | 1                | WORK_ID                       |
      | 1                | WORK_MANIFESTATION_ID         |

  @API @new
  Scenario Outline: Search works by identifier and Type
    Given We get <countOfRandomIds> random search ids for works
    And We get the work search data from EPH GD
    Then the work search by identifier <idType> and type details are retrieved and compared
    Examples:
      | countOfRandomIds | idType                        |
      | 1                | WORK_IDENTIFIER               |
      | 1                | WORK_MANIFESTATION_IDENTIFIER |


  @API @new
  Scenario Outline: Search works by search option
    Given We get <countOfRandomIds> random search ids for works
    And We get the work search data from EPH GD
    Then the works retrieved by search <option> details are retrieved and compared
    Examples:
      | countOfRandomIds | option                        |
      | 1                | WORK_IDENTIFIER               |
      | 1                | WORK_ID                       |
      | 1                | WORK_MANIFESTATION_ID         |
      | 1                | WORK_MANIFESTATION_IDENTIFIER |
      | 1                | WORK_PRODUCT_ID               |
#      | 1               | WORK_MANIFESTATION_TITLE      |
#      | 1               | WORK_TITLE                    |











