Feature: MDM Integration Checks


  Scenario: verify that when a new work is created in EPH it is assigned a unique reference
    Given A work is moved from PMX to EPH
    When  A new work is created that has not existed previously
    Then  A unique reference is assigned that is clearly identifiable as a work identifier