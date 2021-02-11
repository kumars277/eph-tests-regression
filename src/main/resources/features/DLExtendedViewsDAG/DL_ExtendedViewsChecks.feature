Feature:Validate data of DL Extended Views where data comes from All Extended Views

#  Created by Dinesh on 03/02/2021

  @DLExtView
  Scenario Outline: Verify Data Count for DL extended views is transferred from All Extended views
    Given Get the total count of DL Extended views <tableName>
    Then  We know the total count of All Extended views <tableName>
    And Compare count of All Ext Views with <tableName> views are identical
    Examples:
      | tableName                                                         |
      |product_extended_availability                                      |
      |product_extended_pricing                                           |
      |manifestation_extended_restriction                                 |
      |manifestation_extended_page_count                                  |
      |manifestation_extended                                             |
      |work_extended                                                      |
#     |work_extended_editorial_board                                      |
      |work_extended_metric                                               |
      |work_extended_person_role                                          |
      |work_extended_relationship_sibling                                 |
      |work_extended_subject_area                                         |
      |work_extended_url                                                  |


#  @DLCoreView
#  Scenario Outline: Verify Data for DL core views is transferred from BCS and JM core Tables
#    Given Get the <countOfRandomIds> from JM and BCS Core Tables <tableName>
#    Then  Get the Records from the JM and BCS Core Tables <tableName>
#    And   Get the Records from the DL core views <tableName>
#    Then  Compare data of BCS and JM Core with DL Core views <tableName> are identical
#    Examples:
#      | tableName                        |countOfRandomIds  |
#    |all_accountable_product_v         |10                |
#     |all_manifestation_identifiers_v   |10                |
#     |all_manifestation_v               |10                |
#      |all_person_v                      |10                |
#      |all_product_v                     |10                |
#      |all_work_identifier_v             |10                |
#      |all_work_person_role_v            |10                |
#      |all_work_relationship_v           |10                |
#      |all_work_subject_areas_v          |10                |
#      |all_work_v                        |10                |
#
