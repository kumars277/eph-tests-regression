Feature:Validate data for person tables between EPH and Data Lake - Outbound

  @DLDP
  Scenario Outline: Validate data is transferred from person EPH to DL Outbound
    Given We get <countOfRandomIds> random person ids of <table>
    When We get the person records from EPH of <table>
    Then We get the person records from DL of <table>
    And Compare person records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100              | gd_person|
      | 100              | gh_person|

  @DLDP
  Scenario Outline: Validate data is transferred from product_person_role EPH to DL Outbound
    Given We get <countOfRandomIds> random person ids of <table>
    When We get the product person records from EPH of <table>
    Then We get the product person records from DL of <table>
    And Compare product person records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100             | gd_product_person_role|
      | 100             | gh_product_person_role|


  @DLDP
  Scenario Outline: Validate data is transferred from work_person_role EPH to DL Outbound
    Given We get <countOfRandomIds> random person ids of <table>
    When We get the work person role records from EPH of <table>
    Then We get the work person role records from DL of <table>
    And Compare work person role records in EPH and DL of <table>
    Examples:
      | countOfRandomIds | table  |
      | 100             | gd_work_person_role|
      | 100             | gh_work_person_role|









