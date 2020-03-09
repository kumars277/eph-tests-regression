Feature:Validate data for Product tables between EPH and Data Lake - Outbound


  @DLD
  Scenario Outline: Validate data is transferred from product EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the product records from EPH of <table>
    Then We get the product records from DL of <table>
    And Compare product records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1             | gd_product|
      | 1             | gh_product|


  @DLD
  Scenario Outline: Validate data is transferred from accountable_product EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the accountable product records from EPH of <table>
    Then We get the accountable product records from DL of <table>
    And Compare accountable product records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1              | gd_accountable_product|
      | 1              | gh_accountable_product|


  @DLD
  Scenario Outline: Validate data is transferred from product_rel_package EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the product package records from EPH of <table>
    Then We get the product package records from DL of <table>
    And Compare product package records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1              | gd_product_rel_package|
      | 1              | gh_product_rel_package|


  @DLD
  Scenario Outline: Validate data is transferred from product_financial_attribs EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the product financial records from EPH of <table>
    Then We get the product financial records from DL of <table>
    And Compare product financial records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1              | gd_product_financial_attribs|
      | 1              | gh_product_financial_attribs|


  @DLD
  Scenario Outline: Validate data is transferred from product_identifier EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the product identifier records from EPH of <table>
    Then We get the product identifier records from DL of <table>
    And Compare product identifier records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1              | gd_product_identifier|
      | 1                 | gh_product_identifier|


  @DLD
  Scenario Outline: Validate data is transferred from product_relationship EPH to DL Outbound
    Given We get <countOfRandomIds> random product ids of <table>
    When We get the product relationship records from EPH of <table>
    Then We get the product relationship records from DL of <table>
    And Compare product relationship records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 1              | gd_product_relationship|
      | 1              | gh_product_relationship|




