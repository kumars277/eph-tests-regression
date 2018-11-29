Feature: Talend InBound Integration

  Scenario: validate the inbound loading of product work details from PMX Source into EPH
    Given I have details of PMX product details to load
    When  I load complete a full reload of PMX data for Books and Journals
    Then  The staging tables for PMX is populated successfully
    And I wait till PMX batch is completed
    And all the necessary entities are loaded in golden layer for Work
