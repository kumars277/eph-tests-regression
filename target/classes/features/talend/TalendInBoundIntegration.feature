Feature: Talend InBound Integration

   Scenario: Example SScenario
     Given I have existing but no change in ringgold customer details to load into ECH
     When I run the weekly Ringgold Job
     And I wait till Ringgold batch is completed

  Scenario: validate the inbound loading of product work details from PMX Source into EPH
    Given I have details of PMX product details to load
    When  I load complete a full reload of PMX data for Books and Journals
    Then  The staging tables for PMX is populated successfully
    And I wait till PMX batch is completed
    And all the necessary entities are loaded in golden layer for Work


