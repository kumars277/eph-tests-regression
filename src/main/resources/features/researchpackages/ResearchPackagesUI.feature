Feature: Research Packages UI Selenium Tests

  @SJC
  Scenario Outline: Add a Journal with Pending Status into the SJC - Clinical Collections
    Given User logged into the application as a Product Owner
    Then  Enter into the SJC Clinical Collections
    When  Clicking on to the Add Journal
    Then  Search for the journal with "<ISSN>" inside Popup
    Then  Change the status of journal "<ISSN>" to pending and Add
    And   Save the Collections
    Then  Verify the journal "<ISSN>" is added to the collection
    And   Verify the status of the journal "<ISSN>" is "<status>" in DB
    Examples:
      | ISSN             |        status  |
      | 1873-2143       |             P|


    @SJC
    Scenario Outline: Excluding a Journal from SJC - Clinical Collections
      Given User logged into the application as a Product Owner
      Then  Enter into the SJC Clinical Collections
      Then  Search for the journal with "<ISSN>" given
      And   Exclude from the collection
      And   Save the Collections
      And   Verify the status of the journal "<ISSN>" is "<STATUS>" in DB
      Examples:
      |ISSN               | STATUS |
      | 1558-0512        |  E      |

    @SJC
    Scenario Outline: Changing the Status of the Journal to Pending from SJC - Clinical Collections
      Given User logged into the application as a Product Owner
      Then  Enter into the SJC Clinical Collections
      Then  Search for the journal with "<ISSN>" given
      And  change to Pending from the collection
      And   Save the Collections
      And   Verify the status of the journal "<ISSN>" is "<STATUS>" in DB
      Examples:
        |ISSN             |    STATUS |
        | 1558-1950       |    P      |



      @MCC
      Scenario Outline: Search Journal with ISSN
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Search for the journal with "<ISSN>" given
        And   Verify the journal with "<ISSN>" is displayed
        Examples:
          |ISSN            |
          | 1096-1658      |

      @MCC
      Scenario Outline: Search Journal with Publisher
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Search publisher of the journal with "<publisher>" given
        And   Verify the publisher of the journal with "<publisher>" is displayed
        Examples:
          |publisher            |
          | Demerges Enquired      |

      @MCC
      Scenario Outline: Search Journal with title
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Search title keyword of the journal with "<title>" given
        And   Verify the title of the journal with "<title>" is displayed
        Examples:
          |title            |
          | Journal      |

      @MCC
      Scenario Outline: Search Journal with Journal No
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Search journalno of the journal with "<journalno>" given
        And   Verify the journalno of the journal with "<journalno>" is displayed
        Examples:
          |journalno            |
          | 12003      |

      @MCC
      Scenario Outline: Filter Journal with Publishing director
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Choose the "<publishing director>" filter
        And   Verify the result displayed for the "<publishing director>"
        Examples:
          |publishing director            |
          | Rapes Relaxed      |


        @MCC
        Scenario Outline: Filter Journal with Ownership Type
          Given User logged into the application as a Product Owner
          Then  Check for MCC availability and Click the same
          Then  Choose the Ownership Type "<ownership>" filter
          And   Verify the result displayed for the Ownership Type "<ownership>"
          Examples:
            |ownership            |
            | ELSOWN     |

          @MCC
          Scenario Outline: Filter Journal with PMG
            Given User logged into the application as a Product Owner
            Then  Check for MCC availability and Click the same
            Then  Choose the PMG "<pmg>" filter
            And   Verify the result displayed for the PMG "<pmg>"
            Examples:
              |pmg            |
              | 600     |

            @MCC
            Scenario Outline: Adding Comment to the Journal
              Given User logged into the application as a Product Owner
              Then  Check for MCC availability and Click the same
              Then  Search journalno of the journal with "<journalno>" given
              And   Add "<comments>" to the "<journalno>" given
              Then Save the Collections
              Then  Search journalno of the journal with "<journalno>" given
              Then  Verify the "<comments>" added in the history
              Examples:
                |journalno            |comments                    |
                | 	12061               |Sample content for the comment|

     @MCC
      Scenario: Filter Journal by Has Remarks
          Given User logged into the application as a Product Owner
          Then  Check for MCC availability and Click the same
          Then  Choose the filter with Remarks
          And   Verify the result displayed for the Remarks


  @MCC
        Scenario Outline: Change Status to Pending for a Journal in Math Core Collections (MCC)
          Given User logged into the application as a Product Owner
          Then  Check for MCC availability and Click the same
          Then  Search for the journal with "<ISSN>" given
          And   change to Pending from the collection
          And   Save the Collections
          And   Verify the status of the journal "<ISSN>" is "<STATUS>" in DB
          Examples:
          |ISSN            | STATUS   |
          | 1096-0813      |  P       |

        @MCC
        Scenario: Filter Journal by Pending
          Given User logged into the application as a Product Owner
          Then  Check for MCC availability and Click the same
          Then  Choose the filter with Pending Status
          And   Verify the result displayed for the Pending Status

      @MCC
      Scenario Outline: Filter the Unsaved Changes
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Search for the journal with "<ISSN>" given
        And   change to Pending from the collection
        Then  Filter the journal with unsaved Status
        And   Verify the result displayed for the Unsaved Changes
        Examples:
          |ISSN            |
          |1095-9971 |

     @MCC
      Scenario Outline: Excluding a Journal in Math Core Collections (MCC)
         Given User logged into the application as a Product Owner
          Then  Check for MCC availability and Click the same
          Then  Search for the journal with "<ISSN>" given
          And   Exclude from the collection
          And   Save the Collections
          And   Verify the status of the journal "<ISSN>" is "<STATUS>" in DB
          Examples:
            |ISSN            | STATUS   |
            | 1873-1856      |  E       |

    @MCC
      Scenario: Filter Journal by Exclude
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Choose the filter with Excluded Status
        And   Verify the result displayed for the Excluded Status

    @MCC
      Scenario: Filter Journal by Included
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Choose the filter with Included Status
        And   Verify the result displayed for the Included Status

  @MCC
      Scenario Outline: Add a Journal with Pending Status in Math Core Collections (MCC)
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        When  Clicking on to the Add Journal
        Then  Search for the journal with "<ISSN>" inside Popup
        Then  Change the status of journal "<ISSN>" to pending and Add
        And   Save the Collections
        Then  Verify the journal "<ISSN>" is added to the collection
        And   Verify the status of the journal "<ISSN>" is "<status>" in DB
        Examples:
          |ISSN               |       status    |
          | 1522-4732         |       P         |


      @MCC
      Scenario Outline: Add a Journal with Included Status in Math Core Collections (MCC)
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        When  Clicking on to the Add Journal
        Then  Search for the journal with "<ISSN>" inside Popup
        Then  Change the status of journal "<ISSN>" to include and Add
        And   Save the Collections
        Then  Verify the journal "<ISSN>" is added to the collection
        And   Verify the status of the journal "<ISSN>" is "<status>" in DB
        Examples:
         |ISSN                 |       status   |
         | 1934-6069           |       I        |


    @MCC
    Scenario Outline: Change the Status to Include for a Journal in Math Core Collections (MCC)
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        Then  Search for the journal with "<ISSN>" given
        And   change to Pending from the collection
        And   Save the Collections
        And   Verify the status of the journal before include "<ISSN>" in DB
        Then  Search for the journal with "<ISSN>" given
        And   change to Include from the collection
        And   Save the Collections
        And   Verify the status of the journal "<ISSN>" is "<STATUS>" in DB
        Examples:
          |ISSN            | STATUS   |
          |	1873-1376      |  I       |


    @MCC
    Scenario Outline: Filtering the Journal with Multiple Filters
      Given User logged into the application as a Product Owner
      Then  Check for MCC availability and Click the same
      Then   Choose the filter with Included Status
      And   Choose the "<publishing director>" filter
      And   Choose the PMG "<pmg>" filter
      And   Choose the Ownership Type "<ownership>" filter
      Then  Verify the result displayed from multiple filters
      Examples:
      |publishing director |pmg            |ownership            |
        | Rapes Relaxed      |600            |ELSOWN             |


  @MCC
      Scenario: Submitting the Prospective Lists
        Given User logged into the application as a Product Owner
        Then  Check for MCC availability and Click the same
        And   Submit the Prospective First Lists
        And   Verify the Message displayed for the Prospective List
        Then  Submit the Second Prospective First Lists
        And   Verify the Message displayed for the Prospective List










