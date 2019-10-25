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

    @PF
    Scenario Outline: Search the work and filter them with Work Types
      Given user is on search page
      And   Searches for works by given "<keyword>"
      And   Filter the Search Result by "<workType>"
      Then  Search items are listed and click the result based on "<id>"
      And   Verify user is forwarded to the searched works page "<id>"
      Then  Verify the Work "<id>" Type is "<workType>"
      Examples:
      |keyword        |   workType          |  id          |
      |Cell           |   Book              |EPR-W-10D7K9  |
      |neuro           |   Book              |EPR-W-10CK4C  |


      @PF
      Scenario Outline: Search the work and filter them with Work Statuses
        Given user is on search page
        And Searches for works by given "<keyword>"
        And Filter the Search Result by work status "<workStatus>"
        Then Search items are listed and click the result based on "<id>"
        And  Verify user is forwarded to the searched works page "<id>"
        Then Verify the Work "<id>" Status is "<workStatus>"
        Examples:
          |keyword        |        workStatus          |    id      |
          |Cell           |       Launched             |     EPR-W-10BW9B       |
          |neuro           |       Planned             |    EPR-W-109BWM         |

      @PF
      Scenario Outline: Search the work and filter them with Work Statuses and Types
        Given user is on search page
        And Searches for works by keyword "<keyword>"
        And Filter the Search Result by "<workStatus>" and "<workType>"
        Then The searched item is listed and clicked
        And  User is forwarded to the searched works page
        Then Verify the Work Status and Type is matching with the "<workStatus>" and "<workType>"
        Examples:
        |keyword        |        workStatus          |  workType  |
        |Cell           |       Being Published      |  Book      |
        |Cell           |       Launched             |  Journal   |
        |Cell           |       Planned              |   Book      |





