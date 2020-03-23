Feature:Validate data for JM tables between EPH and Data Lake

#  Created by Thomas Kruck on 20/03/20

    @DL
  Scenario Outline: Verify that all JM data is transferred from EPH to DL
    Given We know the number of JM <tableName> data in EPH
    When The JM <tableName> data is in the DL
    Then The JM count for <tableName> table between EPH and DL are identical
    Examples:
      | tableName                           |
      | jmf_allocation_change               |
      | jmf_application_properties          |
      | jmf_approval_attachment             |
      | jmf_approval_request                |
      | jmf_chronicle_scenario              |
      | jmf_chronicle_status                |
      | jmf_family_resource_details         |
      | jmf_financial_information           |
      | jmf_legal_information               |
      |jmf_manifestation_electronic_details |
      |jmf_manifestation_print_details      |
      |jmf_party_in_product                 |
      |jmf_product_availability             |
      |jmf_product_chronicle                |
      |jmf_product_family                   |
      |jmf_product_manifestation            |
      |jmf_product_subject_area             |
      |jmf_product_work                     |
      |jmf_production_information           |
      |jmf_review_comment                   |









