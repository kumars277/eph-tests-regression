Feature: Validating JM_Extended data (Count and Data Checks) between JM and product Extended DB

  @JMExtended
  Scenario Outline: Validate Count Check between JM and Product Extended DB
    Given We know the count of JM Extended data in JM
    Then Get the count for Product Extended DB
    And Compare the count for JM Extended Data between JM and Product Extended
    Given We get the <numberOfRecords> random JM Extended ids
    When We get the JM Extended DB records
    Then We get the ProductExtended records
    And Compare JM records in between JM Extended DB and Product Extended DB
    Examples:
      | numberOfRecords |
      | 5               |