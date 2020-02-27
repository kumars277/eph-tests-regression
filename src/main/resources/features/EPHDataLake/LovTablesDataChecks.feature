Feature:Validate data for gd_x_Lov tables between EPH and Data Lake - Outbound

  @DL
  Scenario Outline: Validate Lov Table data is transferred from EPH to DL Outbound
    Given We get <countOfRandomIds> random Lov Codes of <tableName>
    When We get the Lov <tableName> records from EPH
    Then We get the Lov <tableName> records from DL
    And Compare Lov <tableName> records in EPH and DL
    Examples:
      | countOfRandomIds |   tableName                 |
      | 50                | gd_x_lov_etax_product_code |
      | 50                | gd_x_lov_oa_type           |
      | 50                | gd_x_lov_gl_company        |
      | 50                | gd_x_lov_gl_prod_seg_parent|
      | 50                | gd_x_lov_gl_resp_centre    |
      | 50                | gd_x_lov_imprint           |
      | 50                | gd_x_lov_language          |
      | 50                | gd_x_lov_oa_type           |
      | 50                | gd_x_lov_origin            |
      | 50                | gd_x_lov_person_role       |
      | 50                | gd_x_lov_pmg               |
      | 50                | gd_x_lov_product_type      |
      | 50                | gd_x_lov_revenue_model     |
      | 50                | gd_x_lov_subject_area_type |
      | 50                | gd_x_lov_workflow_source   |

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








