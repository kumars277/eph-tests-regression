Feature:Validate data of DL All Core Views where data comes from BCS and JM Core

#  Created by Dinesh on 09/12/2020



  @DLCoreView
  Scenario Outline: Verify Data Count for DL core views is transferred from BCS and JM core Tables
    Given Get the total count of DL Core views <tableName>
    Then  We know the total count of BCS And JM Core tables <tableName>
    And Compare count of BCS and JM Core with <tableName> views are identical
    Examples:
      | tableName                                 |
#      |all_accountable_product_v                  |
#     |all_manifestation_identifiers_v            |
#      |all_manifestation_v                        |
#     |all_person_v                               |
#      |all_product_v                              |
#        |all_product_rel_package_v                  |
#     |all_work_identifier_v                      |
#     |all_work_person_role_v                     |
#      |all_work_relationship_v                    |
  #   |all_work_subject_areas_v                   |
#      |all_work_v                                 |
#  |all_work_legal_owner_v                     |
#    |all_work_access_model_v                    |
#|all_work_business_model_v                  |

  @DLCoreView
  Scenario Outline: Verify Data for DL core views is transferred from BCS and JM core Tables
    Given Get the <countOfRandomIds> from JM and BCS Core Tables <tableName>
    Then  Get the Records from the JM and BCS Core Tables <tableName>
    And   Get the Records from the DL core views <tableName>
    Then  Compare data of BCS and JM Core with DL Core views <tableName> are identical
    Examples:
      | tableName                         |countOfRandomIds  |
#    |all_accountable_product_v            |1000                |
#    |all_manifestation_identifiers_v     |1000                |
#    |all_manifestation_v                 |1000                |
#    |all_person_v                        |1000               |
#     |all_product_v                      |1000                |
#      |all_product_rel_package_v         |1000               |
#     |all_work_identifier_v              |1000                |
#     |all_work_person_role_v             |1000                |
 #   |all_work_relationship_v             |1000               |
  |all_work_subject_areas_v            |100                |
#      |all_work_v                        |1000                |
#     |all_work_legal_owner_v             |10                |
#    |all_work_access_model_v             |10                |
#      |all_work_business_model_v    |       10               |