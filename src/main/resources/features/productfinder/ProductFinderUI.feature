Feature: Product Finder Selenium tests

  @PF
  Scenario Outline: Search the Work by Id
    Given We get the id for work search <id>
    And We get the work search data from the EPH GD
    When user is on search page
    And Searches for works by ID
    Then The searched item is listed and clicked
    And User is forwarded to the searched works page
    Examples:
      | id                        |
      | EPR-W-TSTW01              |

  @PF
  Scenario Outline: Search the work by title
    Given We get the id for work search <id>
    And We get the work search data from the EPH GD
    And user is on search page
    When Searches for works by title
    Then The searched item is listed and clicked
    And User is forwarded to the searched works page
    Examples:
      | id                        |
      | EPR-W-TSTW01              |

  @PF
  Scenario Outline: Search the work by keyword
    Given We get the id for work search <id>
    And We get the work search data from the EPH GD
    When user is on search page
    And Searches for works by keyword "<keyword>"
    Then The searched item is listed and clicked
    And User is forwarded to the searched works page
    Examples:
      | id                        | keyword     |
      | EPR-W-TSTW01              | Tetrahedron |


  @PF
  Scenario Outline: When no product is found "There are no results that match your search" is displayed
    Given user is on search page
    When user is searching for "<keyword>"
    Then No results message is displayed for "<keyword>"
    Examples:
    |keyword      |
    |abcdefg1234567890|