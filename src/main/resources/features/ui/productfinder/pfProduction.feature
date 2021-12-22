Feature: Product Finder production smoke tests

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: When no product is found "There are no results that match your search" is displayed
    Given user is on Product/Journal Finder search page <ui>
    When user is searching for "<keyword>"
    Then No results message is displayed for "<keyword>"
    Examples:
      |ui|keyword      |
      |PF|abcdefg1234567890|
      |PF|invalidSearch|
      |PF|1234567890|
      |JF|abcdefg1234567890|
      |JF|invalidSearch|
      |JF|1234567890|

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: verify search Suggestion displayed
    Given user is on Product/Journal Finder search page <ui>
    When user is searching for "<keyword>"
    Then capture search suggestion for "<keyword>" and validate with "<ExpSuggestion>"
    Examples:
      |ui |keyword                    |ExpSuggestion              |
      |JF |Euroopean Joournal         |european journal           |
      |JF|Eropean Joournal            |european journal           |
      |JF|Americaan                   |american                   |
      |JF|Aerican                     |american                   |
      |JF|Amercan                     |american                   |
      |JF|Neveer                      |never                      |
      |PF|Actualitées pharmaceutiques |actualites pharmaceutiques |
      |PF|amrica                      |america                    |
      |PF|africa                      |africa                     |
      |PF|america                     |america                    |
      |PF|anerica                     |america                    |
      |PF|anrica                      |africa                     |

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: Search and filter with Work Types
    Given user is on Product Finder search page
    And   Searches for given <keyword>
    And   Filter the Search Result by "<workType>"
    Then  Search items are listed and click an id from the result
    And   Verify user is forwarded to the searched work page from Search Result
    Then  Verify the Work id Type is "<workType>"
    Examples:
      |keyword   |   workType  |
      |Cell      |   Book      |
      |neuro     |   Journal   |

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: Search and filter them with Work Statuses and Types
    Given user is on Product Finder search page
    And Searches for given <keyword>
    And Filter the Search Result by "<workStatus>"
    And Filter the Search Result by "<workType>"
    Then Search items are listed and click an id from the result
    And  Verify user is forwarded to the searched work page from Search Result
    Then  Verify the Work id Type is "<workType>"
    Then Verify the Work Status is "<workStatus>"
    Examples:
      |keyword        |workStatus          |workType  |
      |clinical       |Launched            |Journal   |
      |surgical       |Planned             |Book      |

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: Search and filter with Work Status
    Given user is on Product/Journal Finder search page <ui>
    And Searches for given <keyword>
    And Filter the Search Result by "<workStatus>"
    Then Search items are listed and click an id from the result
    And  Verify user is forwarded to the searched work page from Search Result
    Then Verify the Work Status is "<workStatus>"
    Examples:
      |ui |keyword        |workStatus          |
      |JF |math           |Launched            |
      |PF |math           |Launched            |
      |JF |clinic         |Planned             |
      |PF |clinic         |No Longer Published |

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: Search and filter with Product Status or Product Type
    Given user is on Product/Journal Finder search page <ui>
    When Search for given <keyword> and switch to Products and Packages tab
    And Filter the Search Result by filter "<filterType>" and value "<filterValue>" and click first id
    Then Verify the Product filter is "<filterType>"
    Examples:
      |ui   |keyword       |filterType        |filterValue    |
      |PF   |Physics       |Product Status    |Available      |
      |PF   |Chemistry     |Product Type      |Subscription   |
      |JF   |clinic        |Product Type      |Open Access    |

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: Search and filter with Product Status and Product Type
    Given user is on Product/Journal Finder search page <ui>
    When Search for given <keyword> and switch to Products and Packages tab
    And Filter the Search Result by filter "<filterType1>" and value "<filterValue1>"
    And Filter the Search Result by filter "<filterType2>" and value "<filterValue2>" and click first id
    Then Verify the Product filter is "<filterType1>"
    Then Verify the Product filter is "<filterType2>"
    Examples:
      |ui   |keyword    |filterType1       |filterValue1    |filterType2    |filterValue2        |
      |PF   |math        |Product Status    |Available       |Product Type   |Open Access         |
      |PF   |clinic      |Product Type      |Package         |Product Status |No Longer Published |

  @PFProd @UI @PFRegressionSuit
    Scenario Outline: Verify on search result page Works, Products&Packages, Manifestation tab
      Given user is on Product/Journal Finder search page <ui>
      And Searches for given <keyword>
      Then verify Works, Products&Packages, Manifestation tab
      Examples:
        |ui   |keyword    |    |
        |JF   |math        |    |
        |PF   |clinic      |     |

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: Verify on work overview page Core, People, Financial, Editorial and Links tabs are available
  Given user is on Product/Journal Finder search page <ui>
  And Searches for given <keyword>
    And Search items are listed and click an id from the result
    And  Verify user is forwarded to the searched work page from Search Result
    Then Verify overview tabs
    Examples:
    |ui   |keyword    |
    |PF   |math       |
    |JF   |clinic     |

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: Verify latest works,Help and previous search on landing page
    Given user is on Product/Journal Finder search page <ui>
    And verify latest work,previous search and help
    Examples:
      |ui   |keyword    |
      |PF   |math       |
      |JF   |clinic     |

  @PFProd @UI @PFRegressionSuite
  Scenario Outline: Verify any broken link on PF
    Given user is on Product/Journal Finder search page <ui>
    And verify links on home page
    When Searches for given <keyword>
    And verify links on Search Result page - works
    Then Search items are listed and click an id from the result
    And validate links on work overview page
    Given user is on Product/Journal Finder search page <ui>
    When Searches for given <keyword>
    When switch to Manifestations tab
    And verify links on Search Result page - manifestation
    Then Search items are listed and click an id from the result
    And validate links on manifestation overview page
    Given user is on Product/Journal Finder search page <ui>
    When Searches for given <keyword>
    When switch to Products and Packages tab
    And verify links on Search Result page - Product and Packages
    Then Search items are listed and click an id from the result
    And validate links on product overview page
    Examples:
      |ui   |keyword    |
      |PF   |math       |
      |JF   |clinic     |


#below 3 scenario clubbed in 1 as above
@notInUse
    Scenario Outline: Verify any broken link on PF - work
      Given user is on Product/Journal Finder search page <ui>
      And verify links on home page
      When Searches for given <keyword>
    And verify links on Search Result page - works
    Then Search items are listed and click an id from the result
    And validate links on work overview page
    Examples:
      |ui   |keyword    |
      |PF   |math       |
   #     |JF   |clinic     |
  @notInUse
  Scenario Outline: Verify any broken link on PF - manifestation
    Given user is on Product/Journal Finder search page <ui>
    When Searches for given <keyword>
    When switch to Manifestations tab
    And verify links on Search Result page - manifestation
    Then Search items are listed and click an id from the result
    And validate links on manifestation overview page
    Examples:
      |ui   |keyword    |
      |PF   |math       |
   #     |JF   |clinic     |
  @notInUse
  Scenario Outline: Verify any broken link on PF - Product
    Given user is on Product/Journal Finder search page <ui>
    When Searches for given <keyword>
    When switch to Products and Packages tab
    And verify links on Search Result page - Product and Packages
    Then Search items are listed and click an id from the result
    And validate links on product overview page
    Examples:
      |ui   |keyword    |
      |PF   |math       |
      |JF   |clinic     |