Feature: EMS - Customer Service - Customer Search API
  As an EIP-MS Integration User
  I would like to search works from Enterprise Customer Hub using EIP Search
  So that I can use the details to validate business needs

  @API
  Scenario Outline: search work by ID
    Given We get <countOfRandomIds> random search id for works
    And We get the work search data from EPH GD
    Then the work details are retrieved and compared
    Examples:
      | countOfRandomIds |
      | 10               |
















