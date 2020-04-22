Feature:Validate data for Event tables between EPH and Data Lake - Outbound


  @DLD
  Scenario Outline: Validate data is transferred from gd_event EPH to DL Outbound
    Given We get <countOfRandomIds> random event ids of <table>
    When We get the gd event records from EPH
    Then We get the gd event records from DL
    And Compare gd event records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 100              | gd_event|


  @DLD
  Scenario Outline: Validate data is transferred from subject_area EPH to DL Outbound
    Given We get <countOfRandomIds> random event ids of <table>
    When We get the subject area records from EPH of <table>
    Then We get the subject area records from DL of <table>
    And Compare subject area records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100              | gd_subject_area|
      | 100              | gh_subject_area|


  @DLD
  Scenario Outline: Validate data is transferred from copyright_owner EPH to DL Outbound
    Given We get <countOfRandomIds> random event ids of <table>
    When We get the copyright records from EPH of <table>
    Then We get the copyright records from DL of <table>
    And Compare copyright records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100              | gd_copyright_owner|
      | 100              | gh_copyright_owner|



