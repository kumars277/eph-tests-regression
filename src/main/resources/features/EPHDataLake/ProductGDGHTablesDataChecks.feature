Feature:Validate data for Product tables between EPH and Data Lake - Outbound


  @DL
  Scenario Outline: Validate data is transferred from gd_product EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the gd product records from EPH
    Then We get the gd product records from DL
    And Compare gd product records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10             | gd_product|

  @DL
  Scenario Outline: Validate data is transferred from gh_product EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the gh product records from EPH
    Then We get the gh product records from DL
    And Compare gh product records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gh_product|

    @DL
    Scenario Outline: Validate data is transferred from gd_accountable_product EPH to DL Outbound
      Given We get <countOfRandomIds> random product ids of <table>
      When We get the gd accountable product records from EPH
      Then We get the gd accountable product records from DL
      And Compare gd accountable product records in EPH and DL
      Examples:
        | countOfRandomIds | table  |
        | 10              | gd_accountable_product|

  @DL
  Scenario Outline: Validate data is transferred from gd_product_rel_package EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the gd product package records from EPH
    Then We get the gd product package records from DL
    And Compare gd product package records in EPH and DL
    Examples:
      | countOfRandomIds | table  |
      | 10              | gd_product_rel_package|


