Feature: Validate data count for all tables between EPH and Data Lake - Outbound


  @DL
  Scenario: Verify that all the work data is transferred from EPH to DL
    Given We know the number of Works in EPH
    When The wwork data is in the DL
    Then The count between EPH_gd_wwork and DL_gd_wwork are identical

  @DL
  Scenario: Verify that all the Accountable product data is transferred from EPH to DL
    Given We know the number of Accountable products in EPH
    When The Accountable product data is in the DL
    Then The count between EPH_gd_accountable_product and DL_gd_accountable_product are identical

  @DL
  Scenario: Verify that all the event data is transferred from EPH to DL
    Given We know the number of event data in EPH
    When The event data is in the DL
    Then The count between EPH_gd_event and DL_gd_event are identical


  @DL
  Scenario: Verify that all the manifestation data is transferred from EPH to DL
    Given We know the number of manifestation data in EPH
    When The manifestation data is in the DL
    Then The count between EPH_gd_manifestation and DL_gd_manifestation are identical

  @DL
  Scenario: Verify that all the manifestation_identifier data is transferred from EPH to DL
    Given We know the number of manifestation_identifier data in EPH
    When The manifestation_identifier data is in the DL
    Then The count between EPH_gd_manifestation_identifier and DL_gd_manifestation_identifier are identical

  @DL
  Scenario: Verify that all the person data is transferred from EPH to DL
    Given We know the number of person data in EPH
    When The person data is in the DL
    Then The count between EPH_gd_person and DL_gd_person are identical

  @DL
  Scenario: Verify that all the product data is transferred from EPH to DL
    Given We know the number of product data in EPH
    When The product data is in the DL
    Then The count between EPH_gd_product and DL_gd_product are identical

  @DL
  Scenario: Verify that all the product person role data is transferred from EPH to DL
    Given We know the number of product person role data in EPH
    When The product person role data is in the DL
    Then The count between EPH_gd_product_person_role and DL_gd_product_person_role are identical


  @DL
  Scenario: Verify that all the product rel package data is transferred from EPH to DL
    Given We know the number of product rel package data in EPH
    When The product rel package data is in the DL
    Then The count between EPH_gd_product_rel_package and DL_gd_product_rel_package are identical

  @DL
  Scenario: Verify that all the subject area data is transferred from EPH to DL
    Given We know the number of subject area data in EPH
    When The subject area data is in the DL
    Then The count between EPH_gd_work_subject_area and DL_gd_work_subject_area are identical

  @DL
  Scenario: Verify that all the work financial attribs data is transferred from EPH to DL
    Given We know the number of work financial attribs data in EPH
    When The work financial attribs data is in the DL
    Then The count between EPH_gd_work_financial_attribs and DL_gd_work_financial_attribs are identical


  @DL
  Scenario: Verify that all the work identifier data is transferred from EPH to DL
    Given We know the number of work identifier data in EPH
    When The work identifier data is in the DL
    Then The count between EPH_gd_work_identifier and DL_gd_work_identifier are identical

  @DL
  Scenario: Verify that all the work person role data is transferred from EPH to DL
    Given We know the number of work person role data in EPH
    When The work person role data is in the DL
    Then The count between EPH_gd_work_person_role and DL_gd_work_person_role are identical

  @DL
  Scenario: Verify that all the work relationship data is transferred from EPH to DL
    Given We know the number of work relationship data in EPH
    When The work relationship data is in the DL
    Then The count between EPH_gd_work_relationship and DL_gd_work_relationship are identical

  @DL
  Scenario: Verify that all the work subject area link data is transferred from EPH to DL
    Given We know the number of work subject area link data in EPH
    When The work subject area link data is in the DL
    Then The count between EPH_gd_work_subject_area_link and DL_gd_work_subject_area_link are identical

  @DL
  Scenario: Verify that all the x lov etax product code data is transferred from EPH to DL
    Given We know the number of x lov etax product code data in EPH
    When The x lov etax product code data is in the DL
    Then The count between EPH_gd_x_lov_etax_product_code and DL_gd_x_lov_etax_product_code are identical

  @DL
  Scenario: Verify that all the x lov event type data is transferred from EPH to DL
    Given We know the number of x lov event type data in EPH
    When The x lov event type data is in the DL
    Then The count between EPH_gd_x_lov_event_type and DL_gd_x_lov_event_type are identical

  @DL
  Scenario: Verify that all the x lov gl company data is transferred from EPH to DL
    Given We know the number of x lov gl company data in EPH
    When The x lov gl company data is in the DL
    Then The count between EPH_gd_x_lov_gl_company and DL_gd_x_lov_gl_company are identical

  @DL
  Scenario: Verify that all the x lov gl prod seg parent data is transferred from EPH to DL
    Given We know the number of x lov gl prod seg parent data in EPH
    When The x lov gl prod seg parent data is in the DL
    Then The count between EPH_gd_x_lov_gl_prod_seg_parent and DL_gd_x_lov_gl_prod_seg_parent are identical

  @DL
  Scenario: Verify that all the x lov gl resp centre data is transferred from EPH to DL
    Given We know the number of x lov gl resp centre data in EPH
    When The x lov gl resp centre data is in the DL
    Then The count between EPH_gd_x_lov_gl_resp_centre and DL_gd_x_lov_gl_resp_centre are identical

  @DL
  Scenario: Verify that all the x lov identifier type data is transferred from EPH to DL
    Given We know the number of x lov identifier type data in EPH
    When The x lov identifier type data is in the DL
    Then The count between EPH_gd_x_lov_identifier_type and DL_gd_x_lov_identifier_type are identical

  @DL
  Scenario: Verify that all the x lov imprint data is transferred from EPH to DL
    Given We know the number of x lov imprint data in EPH
    When The x lov imprint data is in the DL
    Then The count between EPH_gd_x_lov_imprint and DL_gd_x_lov_imprint are identical

  @DL
  Scenario: Verify that all the x lov language data is transferred from EPH to DL
    Given We know the number of x lov language data in EPH
    When The x lov language data is in the DL
    Then The count between EPH_gd_x_lov_language and DL_gd_x_lov_language are identical

  @DL
  Scenario: Verify that all the x lov manif status data is transferred from EPH to DL
    Given We know the number of x lov manif status data in EPH
    When The x lov manif status data is in the DL
    Then The count between EPH_gd_x_lov_manif_status and DL_gd_x_lov_manif_status are identical

  @DL
  Scenario: Verify that all the x lov manif type data is transferred from EPH to DL
    Given We know the number of x lov manif type data in EPH
    When The x lov manif type data is in the DL
    Then The count between EPH_gd_x_lov_manif_type and DL_gd_x_lov_manif_type are identical

  @DL
  Scenario: Verify that all the x lov oa type data is transferred from EPH to DL
    Given We know the number of x lov oa type data in EPH
    When The x lov oa type data is in the DL
    Then The count between EPH_gd_x_lov_oa_type and DL_gd_x_lov_oa_type are identical

  @DL
  Scenario: Verify that all the x lov origin data is transferred from EPH to DL
    Given We know the number of x lov origin data in EPH
    When The x lov origin data is in the DL
    Then The count between EPH_gd_x_lov_origin and DL_gd_x_lov_origin are identical

  @DL
  Scenario: Verify that all the x lov person role data is transferred from EPH to DL
    Given We know the number of x lov person role data in EPH
    When The x lov person role data is in the DL
    Then The count between EPH_gd_x_lov_person_role and DL_gd_x_lov_person_role are identical

  @DL
  Scenario: Verify that all the x lov pmc data is transferred from EPH to DL
    Given We know the number of x lov pmc data in EPH
    When The x lov pmc data is in the DL
    Then The count between EPH_gd_x_lov_pmc and DL_gd_x_lov_pmc are identical


  @DL
  Scenario: Verify that all the x lov pmg data is transferred from EPH to DL
    Given We know the number of x lov pmg data in EPH
    When The x lov pmg data is in the DL
    Then The count between EPH_gd_x_lov_pmg and DL_gd_x_lov_pmg are identical

  @DL
  Scenario: Verify that all the x lov product status data is transferred from EPH to DL
    Given We know the number of x lov product status data in EPH
    When The x lov product status data is in the DL
    Then The count between EPH_gd_x_lov_product_status and DL_gd_x_lov_product_status are identical

  @DL
  Scenario: Verify that all the x lov product type data is transferred from EPH to DL
    Given We know the number of x lov product type data in EPH
    When The x lov product type data is in the DL
    Then The count between EPH_gd_x_lov_product_type and DL_gd_x_lov_product_type are identical

  @DL
  Scenario: Verify that all the x lov relationship type data is transferred from EPH to DL
    Given We know the number of x lov relationship type data in EPH
    When The x lov relationship type data is in the DL
    Then The count between EPH_gd_x_lov_product_relationship and DL_gd_x_lov_product_relationship are identical

  @DL
  Scenario: Verify that all the x lov revenue model data is transferred from EPH to DL
    Given We know the number of x lov revenue model data in EPH
    When The x lov revenue model data is in the DL
    Then The count between EPH_gd_x_lov_revenue_model and DL_gd_x_lov_revenue_model are identical

  @DL
  Scenario: Verify that all the x lov society ownership data is transferred from EPH to DL
    Given We know the number of x lov society ownership data in EPH
    When The x lov society ownership data is in the DL
    Then The count between EPH_gd_x_lov_society_ownership and DL_gd_x_lov_society_ownership are identical

  @DL
  Scenario: Verify that all the x lov subject area type data is transferred from EPH to DL
    Given We know the number of x lov subject area type data in EPH
    When The x lov subject area type data is in the DL
    Then The count between EPH_gd_x_lov_subject_area_type and DL_gd_x_lov_subject_area_type are identical

  @DL
  Scenario: Verify that all the x lov subscription type data is transferred from EPH to DL
    Given We know the number of x lov subscription type data in EPH
    When The x lov subscription type data is in the DL
    Then The count between EPH_gd_x_lov_subscription_type and DL_gd_x_lov_subscription_type are identical

  @DL
  Scenario: Verify that all the x lov work status data is transferred from EPH to DL
    Given We know the number of x lov work status data in EPH
    When The x lov work status data is in the DL
    Then The count between EPH_gd_x_lov_work_status and DL_gd_x_lov_work_status are identical

  @DL
  Scenario: Verify that all the x lov work type data is transferred from EPH to DL
    Given We know the number of x lov work type data in EPH
    When The x lov work type data is in the DL
    Then The count between EPH_gd_x_lov_work_type and DL_gd_x_lov_work_type are identical

  @DL
  Scenario: Verify that all the x lov workflow source data is transferred from EPH to DL
    Given We know the number of x lov workflow source data in EPH
    When The x lov workflow source data is in the DL
    Then The count between EPH_gd_x_lov_workflow_source and DL_gd_x_lov_workflow_source are identical

  @DL
  Scenario: Verify that all the gh accountable product data is transferred from EPH to DL
    Given We know the number of gh accountable product data in EPH
    When The gh accountable product data is in the DL
    Then The count between EPH_gh_accountable_product and DL_gh_accountable_product are identical

  @DL
  Scenario: Verify that all the gh manifestation data is transferred from EPH to DL
    Given We know the number of gh manifestation data in EPH
    When The gh manifestation data is in the DL
    Then The count between EPH_gh_manifestation and DL_gh_manifestation are identical

  @DL
  Scenario: Verify that all the gh manifestation identifier data is transferred from EPH to DL
    Given We know the number of gh manifestation identifier data in EPH
    When The gh manifestation identifier data is in the DL
    Then The count between EPH_gh_manifestation_identifier and DL_gh_manifestation_identifier are identical

  @DL
  Scenario: Verify that all the gh person data is transferred from EPH to DL
    Given We know the number of gh person data in EPH
    When The gh person data is in the DL
    Then The count between EPH_gh_person and DL_gh_person are identical

  @DL
  Scenario: Verify that all the gh product data is transferred from EPH to DL
    Given We know the number of gh product data in EPH
    When The gh product data is in the DL
    Then The count between EPH_gh_product and DL_gh_product are identical

  @DL
  Scenario: Verify that all the gh product person role data is transferred from EPH to DL
    Given We know the number of gh product person role data in EPH
    When The gh product person role data is in the DL
    Then The count between EPH_gh_product_person_role and DL_gh_product_person_role are identical

  @DL
  Scenario: Verify that all the gh product rel package data is transferred from EPH to DL
    Given We know the number of gh product rel package data in EPH
    When The gh product rel package data is in the DL
    Then The count between EPH_gh_product_rel_package and DL_gh_product_rel_package are identical

  @DL
  Scenario: Verify that all the gh work financial attribs data is transferred from EPH to DL
    Given We know the number of gh work financial attribs data in EPH
    When The gh work financial attribs data is in the DL
    Then The count between EPH_gh_work_financial_attribs and DL_gh_work_financial_attribs are identical

  @DL
  Scenario: Verify that all the gh work identifier data is transferred from EPH to DL
    Given We know the number of gh work identifier data in EPH
    When The gh work identifier data is in the DL
    Then The count between EPH_gh_work_identifier and DL_gh_work_identifier are identical

  @DL
  Scenario: Verify that all the gh work person role data is transferred from EPH to DL
    Given We know the number of gh work person role data in EPH
    When The gh work person role data is in the DL
    Then The count between EPH_gh_work_person_role and DL_gh_work_person_role are identical

  @DL
  Scenario: Verify that all the gh work relationship data is transferred from EPH to DL
    Given We know the number of gh work relationship data in EPH
    When The gh work relationship data is in the DL
    Then The count between EPH_gh_work_relationship and DL_gh_work_relationship are identical

  @DL
  Scenario: Verify that all the gh work subject area link data is transferred from EPH to DL
    Given We know the number of gh work subject area link data in EPH
    When The gh work subject area link data is in the DL
    Then The count between EPH_gh_work_subject_area_link and DL_gh_work_subject_area_link are identical

  @DL
  Scenario: Verify that all the gh wwork data is transferred from EPH to DL
    Given We know the number of gh wwork data in EPH
    When The gh wwork data is in the DL
    Then The count between EPH_gh_wwork and DL_gh_wwork are identical



