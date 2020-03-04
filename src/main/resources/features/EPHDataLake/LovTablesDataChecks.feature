Feature:Validate data for gd_x_Lov tables between EPH and Data Lake - Outbound

  @DL
  Scenario Outline: Validate Lov Table data is transferred from EPH to DL Outbound
    Given We get <countOfRandomIds> random Lov Codes of <tableName>
    When We get the Lov <tableName> records from EPH
    Then We get the Lov <tableName> records from DL
    And Compare Lov <tableName> records in EPH and DL
    Examples:
      | countOfRandomIds |   tableName                |
      | 5                | gd_x_lov_etax_product_code |
      | 5                | gd_x_lov_oa_type           |
      | 5                | gd_x_lov_event_type        |
      | 5                | gd_x_lov_gl_company        |
      | 5                | gd_x_lov_gl_prod_seg_parent|
      | 5                | gd_x_lov_gl_resp_centre    |
      | 5                | gd_x_lov_identifier_type   |
      | 5                | gd_x_lov_imprint           |
      | 5                | gd_x_lov_language          |
#      | 1                | gd_x_lov_manif_format      |
      | 5                | gd_x_lov_manif_status      |
      | 5                | gd_x_lov_manif_type        |
      | 5                | gd_x_lov_oa_type           |
      | 5                | gd_x_lov_origin            |
      | 5                | gd_x_lov_person_role       |
      | 5                | gd_x_lov_pmc               |
      | 5                | gd_x_lov_pmg               |
#      | 1                | gd_x_lov_product_line_type |
      | 5                | gd_x_lov_product_status    |
      | 5                | gd_x_lov_product_type      |
      | 5                | gd_x_lov_relationship_type |
      | 5                | gd_x_lov_revenue_model     |
      | 5                | gd_x_lov_society_ownership |
      | 5                | gd_x_lov_subject_area_type |
      | 5                | gd_x_lov_work_status       |
      | 5                | gd_x_lov_work_type         |
      | 5                | gd_x_lov_workflow_source   |


