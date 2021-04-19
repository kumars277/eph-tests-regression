Feature:Validate data of DL Extended Views where data comes from All Extended Views

#  Created by Dinesh on 17/02/2021

  @DLExtViews
  Scenario Outline: Verify Data Count for DL extended tables is transferred from All Extended views
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
      |work_extended_editorial_board                                      |
      |work_extended_metric                                               |
      |work_extended_person_role                                          |
      |work_extended_relationship_sibling                                 |
      |work_extended_subject_area                                         |
      |work_extended_url                                                  |


 @DLExtViews
 Scenario Outline: Verify Data for DL Ext tables is transferred from All Extended Views
    Given Get the <countOfRandomIds> from All Extended views <tableName>
    Then  Get the Records from the All Extended views <tableName>
    And   Get the Records from the Extended Tables <tableName>
   Then  Compare data of All Extended views with Extended Tables <tableName> are identical
    Examples:
      | tableName                                                         |countOfRandomIds|
     |product_extended_availability                                      |50              |
     |product_extended_pricing                                           |50              |
   |manifestation_extended_restriction                                 |50              |
     |manifestation_extended_page_count                                  |50              |
     |manifestation_extended                                             |50              |
    |work_extended                                                      |50              |
      |work_extended_editorial_board                                      |50              |
     |work_extended_metric                                               |50              |
    |work_extended_person_role                                          |50              |
     |work_extended_relationship_sibling                                 |50              |
    |work_extended_subject_area                                         |50              |
     |work_extended_url                                                  |50              |
##
