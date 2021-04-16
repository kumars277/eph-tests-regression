Feature: Add/update Journals on JM UI

  @JMUI
  Scenario Outline: Add a new Journal
    Given user is on JM home page as <userRole>
    Examples:
    |userRole|
    |publisher |