Feature: Product Finder Selenium tests

  @PFUI
  Scenario Outline: search work by multiple options
    Given get 1 random work id from DB
    And We get the work search data from the EPH GD
    When user is on search page
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
    When user is on search page
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
    When user is on search page
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
    Given user is on search page
    When user is searching for "<keyword>"
    Then No results message is displayed for "<keyword>"
    Examples:
      |keyword      |
      |abcdefg1234567890|
      |invalidSearch|
      |1234567890|
      |ab.#$12%|
      |a@b.com|

  @PFUI
  Scenario Outline: Search the work and filter them with one Work Type
    Given Get the available Work Types from the DB "<workType>"
    Then  Get a Work Id for each Work Types available in the DB
    Given user is on search page
    And   Search for the Work by Work Ids Filter workType and verify the work Type is "<workType>"
    Examples:
      |workType |
      |Book     |
      |Journal  |

  @PFUI
  Scenario Outline: Search the work and filter them with Work Types
    Given user is on search page
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
    Given user is on search page
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
    Given user is on search page
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

  #for data model changes
  #https://sit.productfinder.elsevier.net/work/EPR-W-10104W/overview
  #https://sit.productfinder.elsevier.net/work/EPR-W-101055/overview
  @PFDMC
  Scenario Outline: Search the work and verify data model changes
    Given user is on search page
    And   We get the work search data from EPH GD for <id>
    And   Searches for works by given <id>
    Then  Search items are listed and click specific work <id> from the result
    And   Verify user is forwarded to the searched work page of <id>
    Then  Verify work Overview Information for <id>
    And   Verify work Finanfial records
    Examples:
      |id           |
      |EPR-W-10104W |
   #   |EPR-W-101055 |
