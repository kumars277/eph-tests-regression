Feature: Product Finder Selenium tests

  @UI
  Scenario: search work by ID
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    When user is on search page
    And wants to search for "Works"
    Then Search for works by ID
    And Analyze results

  @UI
  Scenario: search work by title
    Given We get 1 random search ids for works
    And We get the work search data from EPH GD
    When user is on search page
    And wants to search for "Works"
    Then Search for works by title
    And Analyze results

#  @UI
#  Scenario: search work by keyword
#    Given We get 1 random search ids for works
#    And We get the work search data from EPH GD
#    When user is on search page
#    And wants to search for "Works"
#    Then Search for works by title
#    And Analyze results


  @Search
  Scenario: When no product is found "There are no results that match your search" is displayed
    Given user is on search page
    And wants to search for "Works"
    When user is searching for "abcdefg1234567890"
    Then No results message is displayed