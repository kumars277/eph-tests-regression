Feature:Validate data for GD Tables in DataLake from PostgreSQL

  @gdDL
  Scenario Outline: Verify that GD tables in the PostgreSQL and DL

    Given We get the count <SourceTable> from postgreSQL
    Then Get the count of <SourceTable> from the DataLake
    And we compare both the counts are identical for the table <SourceTable>
    Given Get <numberOfRecords> random ids of <SourceTable> from the postgreSQL
    When We get the records for <SourceTable> from postgreSQL
    Then Get the records for <SourceTable> from the DataLake
    And Compare the records of <SourceTable> from postgreSQL and DataLake
    Examples:
      |numberOfRecords  |SourceTable                 |
      | 5               |gd_accountable_product      |
      | 5               |gd_event                    |
      | 5               |gd_legal_owner              |
      | 5               |gd_manifestation            |
      | 5               |gd_manifestation_identifier |
      | 5               |gd_person                   |
      | 5               |gd_product                   |
      | 5               |gd_product_financial_attribs |
      | 5               |gd_product_identifier      |
      | 5               |gd_product_person_role      |
      | 5               |gd_product_rel_package      |
      | 5               |gd_subject_area      |
      | 5               |gd_work_access_model      |
      | 5               |gd_work_business_model      |
      | 5               |gd_work_financial_attribs      |
      | 5               |gd_work_hierarchy      |
      | 5               |gd_work_identifier      |
      | 5               |gd_work_legal_owner      |
      | 5               |gd_work_person_role      |
      | 5               |gd_work_rel_package      |
      | 5               |gd_work_relationship      |
      | 5               |gd_work_subject_area_link      |
      | 5               |gd_work_work_hchy_link      |
      | 5               |gd_wwork                   |


  @gdDL
  Scenario Outline: Verify that GD LOV tables in the PostgreSQL and DL
    Given We get the count <SourceTable> from postgreSQL
    Then Get the count of <SourceTable> from the DataLake
    And we compare both the counts are identical for the table <SourceTable>
    #Given Get <numberOfRecords> random ids of <SourceTable> from the postgreSQL
    #When We get the records for <SourceTable> from postgreSQL
    #Then Get the records for <SourceTable> from the DataLake
    #And Compare the records of <SourceTable> from postgreSQL and DataLake
    Examples:
      |numberOfRecords  |SourceTable                 |
      | 5               |gd_x_lov_access_model      |
      | 5               |gd_x_lov_business_model                    |
      | 5               |gd_x_lov_currency              |
      | 5               |gd_x_lov_etax_product_code            |
      | 5               |gd_x_lov_event_type |
      | 5               |gd_x_lov_gl_company                   |
      | 5               |gd_x_lov_gl_prod_seg_parent |
      | 5               |gd_x_lov_gl_resp_centre      |
      | 5               |gd_x_lov_identifier_type      |
      | 5               |gd_x_lov_imprint      |
      | 5               |gd_x_lov_language      |
      | 5               |gd_x_lov_legal_ownership      |
      | 5               |gd_x_lov_manif_status      |
      | 5               |gd_x_lov_manif_type      |
      | 5               |gd_x_lov_metric_type      |
      | 5               |gd_x_lov_origin      |
      | 5               |gd_x_lov_owner_description      |
      | 5               |gd_x_lov_person_role      |
      | 5               |gd_x_lov_pmc      |
      | 5               |gd_x_lov_pmg      |
      | 5               |gd_x_lov_product_status      |
      | 5               |gd_x_lov_product_type      |
      | 5               |gd_x_lov_relationship_type      |
      | 5               |gd_x_lov_revenue_account      |
      | 5               |gd_x_lov_revenue_model      |
      | 5               |gd_x_lov_subject_area_type      |
      | 5               |gd_x_lov_subscription_type      |
      | 5               |gd_x_lov_work_hchy_type      |
      | 5               |gd_x_lov_work_status      |
      | 5               |gd_x_lov_work_type      |
      | 5               |gd_x_lov_workflow_source      |





