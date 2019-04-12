Feature: EMS - Customer Service - Customer Search API
  As an EIP-MS Integration User
  I would like to search products from Enterprise Customer Hub using EIP Search
  So that I can use the details to validate business needs

  @API
  Scenario Outline: search product by ID
    Given We get <countOfRandomIds> random search ids for <type>
    And We get the search data from EPH GD for <type>
    Then the product details are retrieved and compared
    Examples:
      | countOfRandomIds | type |
      | 10               | book |















