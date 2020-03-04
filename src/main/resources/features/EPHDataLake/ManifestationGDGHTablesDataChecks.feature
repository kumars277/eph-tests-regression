Feature:Validate data for manifestation tables between EPH and Data Lake - Outbound


  @DL
  Scenario Outline: Validate data is transferred from gd_manifestation EPH to DL Outbound
    Given We get <countOfRandomIds> random manifestation ids of <table>
    When We get the gd manifestation records from EPH
    Then We get the gd manifestation records from DL
    And Compare gd manifestation records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_manifestation|

  @DL
  Scenario Outline: Validate data is transferred from gh_manifestation EPH to DL Outbound
    Given We get <countOfRandomIds> random manifestation ids of <table>
    When We get the gh manifestation records from EPH
    Then We get the gh manifestation records from DL
    And Compare gh manifestation records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gh_manifestation|

  @DL
  Scenario Outline: Validate data is transferred from gh_manifestation_identifier EPH to DL Outbound
    Given We get <countOfRandomIds> random manifestation ids of <table>
    When We get the gh manifestation identifier records from EPH
    Then We get the gh manifestation identifier records from DL
    And Compare gh manifestation identifier records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gh_manifestation_identifier|

  @DL
  Scenario Outline: Validate data is transferred from gd_manifestation_identifier EPH to DL Outbound
    Given We get <countOfRandomIds> random manifestation ids of <table>
    When We get the gd manifestation identifier records from EPH
    Then We get the gd manifestation identifier records from DL
    And Compare gd manifestation identifier records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_manifestation_identifier|





