Feature: Product Finder Selenium tests

  @UI @PFRegressionSuite
  Scenario Outline: search work by multiple options
    Given get 1 random work id from DB
    And We get the work search data from EPH GD
    When user is on Product Finder search page
    And Search works by <options>
    Then The searched work is listed and clicked
    And User is forwarded to the searched works page from DB
    Examples:
    |options|
    |Id     |
    |Title  |
    |Keyword|

  @UI @PFRegressionSuite
  Scenario Outline: search product by multiple options
    Given get 1 random product id from DB
    And We get the product search data from DB
    When user is on Product Finder search page
    And Search product by <options>
    Then The searched product is listed and clicked
    And User is forwarded to the searched product page from DB
    Examples:
      |options|
      |Id     |
      |Title  |
      |Keyword|

  @UI @PFRegressionSuite
  Scenario Outline: search manifestation by multiple options
    Given get 1 random manifestation id
    And We get the manifestation data from DB
    When user is on Product Finder search page
    And Search manifestation by <options>
    Then The searched manifestation is listed and clicked
    And User is forwarded to the searched manifestation page from DB
    Examples:
      |options|
      |Id     |
      |Title  |
   # |Keyword|  EPH-1909 created for issue – “Product finder is not searching ‘Manifestation by keyword’.”


  @UI @PFRegressionSuite @apiDebug
  Scenario Outline: Search the work and filter them with one Work Type
    Given Get the available Work Types from the DB "<workType>"
    Then  Get a Work Id for each Work Types available in the DB
    Given user is on Product Finder search page
    And   Search for the Work by Work Ids Filter workType and verify the work Type is "<workType>"
    Examples:
      |workType |
      |Book     |
      |Journal  |

  @UI @PFRegressionSuite @PFDMC @apiDebug
  Scenario Outline: Search the Product Finder and verify all 3 tabs
    Given get 1 random work id from DB
    And We get the work search data from EPH GD
    When user is on Product Finder search page
    And   Searches work by id
    Then  Search items are listed and click the workid from result
    And   Verify user is forwarded to the searched work page
    And   get Extended Data from DB
    Then  Verify PF/JF UI work overview values
    Examples:
    |iterator|
    |1|
    |2|
    |3|
    |4|
    |5|

  @UI @PFRegressionSuite @PFDMC @JFUI @apiDebug
  Scenario Outline: Search the Journal Finder and verify all 5 tabs
    Given We get 1 random journal ids for search
    And   We get the work search data from EPH GD
    And   user is on Journal Finder search page
    And   Searches work by id
    Then  Search items are listed and click the workid from result
    And   Verify user is forwarded to the searched work page
    And   get Extended Data from DB
    Then  Verify PF/JF UI work overview values
Examples:
    |iterator|
    |1|
    |2|
    |3|
    |4|
    |5|

  @UI @PFRegressionSuite @JFUI @apiDebug
  Scenario Outline: Search the Journal by person
    Given We get 5 random search ids for person roles
    And get person data from EPH DB
    And   user is on Journal Finder search page
    And   Searches journal work by person <option>
    Examples:
      |option               |
      |personFullNameCurrent|
   #   |personIdCurrent      |
   #   |personName           |
   #   |personId             |

  @UI @PFRegressionSuite @JFUI @apiDebug
  Scenario Outline: Search the Journal by PMC
    Given We get 5 random journal ids for search
    And   We get the work search data from EPH GD
    And   user is on Journal Finder search page
    And   Searches journal by pmc <option>
    Examples:
      |option |
      |pmgCode|
      |pmcCode|


  #below scenarios can be ignored, already covered in one of the above

  @PFDMC
  Scenario Outline: Search the work and verify data model changes
    Given user is on Product Finder search page
    And   We get the work search data from EPH GD for <id>
    And   Searches for given <id>
    Then  Search items are listed and click specific work <id> from the result
    And   Verify user is forwarded to the searched work page of <id>
    Then  Verify PF/JF UI work overview values
    Examples:
      |id           |
      |EPR-W-10104W |
   #   |EPR-W-101055 |
