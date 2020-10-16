Feature: Product Finder Selenium tests

  @PFUI
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

  @PFUI
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

  @PFUI
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

  @PFUI
  Scenario Outline: When no product is found "There are no results that match your search" is displayed
    Given user is on Product Finder search page
    When user is searching for "<keyword>"
    Then No results message is displayed for "<keyword>"
    Examples:
      |keyword      |
      |abcdefg1234567890|
      |invalidSearch|
      |1234567890|
   #   |ab.#$12%|
   #   |a@b.com|

  @PFUI
  Scenario Outline: Search the work and filter them with one Work Type
    Given Get the available Work Types from the DB "<workType>"
    Then  Get a Work Id for each Work Types available in the DB
    Given user is on Product Finder search page
    And   Search for the Work by Work Ids Filter workType and verify the work Type is "<workType>"
    Examples:
      |workType |
      |Book     |
      |Journal  |

  @PFUI
  Scenario Outline: Search the work and filter them with Work Types
    Given user is on Product Finder search page
    And   Searches for works by given <keyword>
    And   Filter the Search Result by "<workType>"
    Then  Search items are listed and click a work id from the result
    And   Verify user is forwarded to the searched work page from Search Result
    Then  Verify the Work id Type is "<workType>"
    Examples:
      |keyword   |   workType  |
      |Cell      |   Book      |
      |neuro     |   Journal   |

  @PFUI
  Scenario Outline: Search the work and filter them with Work Status
    Given user is on Product Finder search page
    And Searches for works by given <keyword>
    And Filter the Search Result by "<workStatus>"
    Then Search items are listed and click a work id from the result
    And  Verify user is forwarded to the searched work page from Search Result
    Then Verify the Work Status is "<workStatus>"
    Examples:
      |keyword        |workStatus          |
      |Physics        |Launched            |
      |Chemistry      |Planned             |
      |math           |No Longer Published |

  @PFUI
  Scenario Outline: Search the work and filter them with Work Statuses and Types
    Given user is on Product Finder search page
    And Searches for works by given <keyword>
    And Filter the Search Result by "<workStatus>"
    And Filter the Search Result by "<workType>"
    Then Search items are listed and click a work id from the result
    And  Verify user is forwarded to the searched work page from Search Result
    Then  Verify the Work id Type is "<workType>"
    Then Verify the Work Status is "<workStatus>"
    Examples:
      |keyword        |workStatus          |workType  |
      |clinical       |Launched            |Journal   |
      |surgical       |Planned             |Book      |
      |nurse          |No Longer Published |Book      |

  @PFDMC @PFUI
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

  @PFDMC @JFUI @PFUI
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

  @JFUI @PFUI
  Scenario Outline: Search the Journal by person
    Given We get 1 random search ids for person roles
    And get person data from EPH DB
    And   user is on Journal Finder search page
    And   Searches journal work by person <option>
    Examples:
      |option               |
      |personFullNameCurrent|
   #   |personIdCurrent      |
   #   |personName           |
   #   |personId             |

  @JFUI
  Scenario Outline: Search the Journal by PMC
    Given We get 2 random journal ids for search
    And   We get the work search data from EPH GD
    And   user is on Journal Finder search page
    And   Searches journal by pmc <option>
    Examples:
      |option |
      |pmgCode|
      |pmcCode|


  #below scenarios can be ignored, already covered in one of the above

  @PFDMC
  Scenario: Search the Journal and verify link tab
    Given We get 2 random journal ids for search
    Then  search work and verify links

  @PFDMC
  Scenario: Search specific Journal and verify link tab
    Given We set specific journal ids for search
    Then  search work and verify links

  @PFDMC
  Scenario Outline: Search the work and verify data model changes
    Given user is on Product Finder search page
    And   We get the work search data from EPH GD for <id>
    And   Searches for works by given <id>
    Then  Search items are listed and click specific work <id> from the result
    And   Verify user is forwarded to the searched work page of <id>
    Then  Verify PF/JF UI work overview values
    Examples:
      |id           |
      |EPR-W-10104W |
   #   |EPR-W-101055 |
