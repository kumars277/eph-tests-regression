Feature:Validate data for gd_x_Lov tables between EPH and Data Lake - Outbound

  @DLD
  Scenario Outline: Validate Lov Table data is transferred from EPH to DL Outbound
    Given We get <countOfRandomIds> random Lov Codes of <tableName>
    When We get the Lov <tableName> records from EPH
    Then We get the Lov <tableName> records from DL
    And Compare Lov <tableName> records in EPH and DL
    Examples:
      | countOfRandomIds |   tableName                 |
      | 50                | gd_x_lov_etax_product_code |

      #     Tables with specific columns

      | 50                | gd_x_lov_event_type        |
      | 50                | gd_x_lov_identifier_type   |
      | 50                | gd_x_lov_manif_status      |
      | 50                | gd_x_lov_manif_type        |
      | 50                | gd_x_lov_pmc               |
      | 50                | gd_x_lov_product_status    |
      | 50                | gd_x_lov_relationship_type |
      | 50                | gd_x_lov_society_ownership |
      | 50                | gd_x_lov_work_status       |
      | 50                | gd_x_lov_work_type         |


#  Scripts untested due to no table in Athena
#      | 1                | gd_x_lov_manif_format      |
#      | 1                | gd_x_lov_product_line_type |








