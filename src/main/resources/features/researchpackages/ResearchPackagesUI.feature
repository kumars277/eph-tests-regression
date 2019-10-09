Feature: Research Packages UI Selenium Tests

  @SJC
  Scenario Outline: Add a Journal into the SJC (Special Journal Collection) - Clinical Collections
    Given User logged into the application
    Then  Enter into the SJC Clinical Collections
    And   Clicking on to the Add Journal
    When  User searching a journal with "<ISSN>"
    Then  Select a Journal from the Result
    And   Adding the journal to the collections by including and verify in DB "<ISSN>"
    Examples:
      | ISSN                        |
      | 1873-2143       |

    @SJC
    Scenario Outline: Excluding a Journal from SJC - Clinical Collections
      Given User logged into the application
      Then  Enter into the SJC Clinical Collections
      Then  Search the journal with "<ISSN>" to Exclude
      Then  Exclude the Journal from the Clinical Collections
      And   Verify the status of the journal "<ISSN>" is "<STATUS>" in DB
      Examples:
      |ISSN               | |STATUS |
      | 1558-0512        |  |E      |

    @SJC
    Scenario Outline: Changing the Status of the Journal to Pending from SJC - Clinical Collections
      Given User logged into the application
      Then  Enter into the SJC Clinical Collections
      Then  Search the journal with "<ISSN>" to Change into Pending
      Then  Change the Journal to Pending from the Clinical Collections
      And   Verify the status of the journal "<ISSN>" is "<STATUS>" in DB
      Examples:
        |ISSN               |   |STATUS |
        | 1558-1950       |    |P      |




