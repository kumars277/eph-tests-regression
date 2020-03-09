Feature:Validate data for manifestation tables between EPH and Data Lake - Outbound


  @DLD
  Scenario Outline: Validate data is transferred from manifestation EPH to DL Outbound
    Given We get <countOfRandomIds> random manifestation ids of <table>
    When We get the manifestation records from EPH of <table>
    Then We get the manifestation records from DL of <table>
    And Compare manifestation records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_manifestation|
      | 10              | gh_manifestation|




  @DLD
  Scenario Outline: Validate data is transferred from gd_manifestation_identifier EPH to DL Outbound
    Given We get <countOfRandomIds> random manifestation ids of <table>
    When We get the manifestation identifier records from EPH of <table>
    Then We get the manifestation identifier records from DL of <table>
    And Compare manifestation identifier records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_manifestation_identifier|
      | 10              | gh_manifestation_identifier|





