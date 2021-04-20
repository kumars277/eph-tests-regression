Feature: Product Finder production smoke tests

  @PFProd
  Scenario Outline: When no product is found "There are no results that match your search" is displayed
    Given user is on Product/Journal Finder search page <ui>
    When user is searching for "<keyword>"
    Then No results message is displayed for "<keyword>"
    Examples:
      |ui|keyword      |
      |PF|abcdefg1234567890|
      |PF|invalidSearch|
 #     |PF|1234567890|
  #    |JF|abcdefg1234567890|
  #    |JF|invalidSearch|
  #    |JF|1234567890|

  @PFProd
  Scenario Outline: verify search Suggestion displayed
    Given user is on Product/Journal Finder search page <ui>
    When user is searching for "<keyword>"
    Then capture search suggestion for "<keyword>" and validate with "<ExpSuggestion>"
    Examples:
      |ui |keyword            |ExpSuggestion    |
      |JF |Euroopean Joournal |	european journal|
      |JF|Eropean Joournal   |european journal |
      |JF|Americaan          |american         |
      |JF|Aerican            |american         |
      |JF|Amercan            |american         |
      |PF|Neveer             |never            |
      |PF|Actualitées pharmaceutiques|actualites pharmaceutiques|
      |PF|amrica                    |america                   |
      |PF|africa                    |africa                    |
      |PF|america                   |america                   |
      |PF|anerica                   |america                   |
      |PF|anrica                    |africa                    |

  @PFProd
  Scenario Outline: Search the work and filter them with Work Status
    Given user is on Product/Journal Finder search page <ui>
    And Searches for works by given <keyword>
    And Filter the Search Result by "<workStatus>"
    Then Search items are listed and click a work id from the result
    And  Verify user is forwarded to the searched work page from Search Result
    Then Verify the Work Status is "<workStatus>"
    Examples:
     |ui |keyword        |workStatus          |
      |PF|Physics        |Launched            |
      |PF|Chemistry      |Planned             |
   #   |Chemistry      |Approved             |
    #  |math           |No Longer Published |

  @PFProd
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

  @PFProd
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
   #   |nurse          |No Longer Published |Book      |
