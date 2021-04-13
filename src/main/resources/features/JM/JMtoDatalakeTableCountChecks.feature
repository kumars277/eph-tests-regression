Feature:Validate data for JM tables between MYSQL and Data Lake

#  Created by Thomas Kruck on 20/03/20

    @JMDL
  Scenario Outline: Verify that all JM data is transferred from MYSQL to DL
    Given We know the number of JM <tableName> data in MYSQL
    When The JM <tableName> data is in the DL
    Then The JM count for <tableName> table between MYSQL and DL are identical
    Examples:
      | tableName                            |
#      | jmf_approval_request                 |
#      | jmf_chronicle_scenario               |
#      | jmf_chronicle_status                 |
#      | jmf_family_resource_details          |
#      | jmf_financial_information            |
#      | jmf_legal_information                |
#      | jmf_manifestation_electronic_details |
#      | jmf_manifestation_print_details      |
#      | jmf_party_in_product                 |
#      | jmf_product_availability             |
#      | jmf_product_chronicle                |
#      | jmf_product_family                   |
#      | jmf_product_manifestation            |
#      | jmf_product_subject_area             |
#      | jmf_product_work                     |
#      | jmf_production_information           |
#      | jmf_review_comment                   |
      |jmf_work_business_model               |
      |jmf_work_access_model                 |
      |jmf_work_product_group                |
      |jmf_product_group                     |
      |jmf_pricing_option                    |
      |jmf_bm_pg_options                     |

  Scenario Outline: Verify that all JM data is transferred from transformed Inbound data is transferred to Access tables
    Given We know the number of JM <tableName> data in Inbound
    When The JM <tableName> data is in the Access Table
    Then The JM count for <tableName> table between transformed Inbound data is transferred to Access tables are identical
    Examples:
      | tableName                 |
      |jmf_work_business_model    |
      |jmf_work_access_model      |
      |jmf_work_product_group     |
      |jmf_product_group          |
      |jmf_pricing_option         |
      |jmf_bm_pg_options          |










