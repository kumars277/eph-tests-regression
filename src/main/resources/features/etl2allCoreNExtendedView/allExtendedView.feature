Feature:Validate data of DL Extended Views and Extended Tables

#  Created by Dinesh on 17/05/2021

  #Confluence Link: https://elsevier.atlassian.net/wiki/spaces/EPH/history/45506168424/Extended+Data+Topic+Mappings

  @DLExtViews
  Scenario Outline: Verify Data Count for All Extended views is transferred from Source Ingestion
    Given Get the total count of All Extended views <tableName>
    Then  We know the total count of Source Ingestion <tableName>
    And Compare count of All Extended Views with Source Ingestion <tableName> are identical
    Given Get the <countOfRandomIds> from source ingestion Tables <tableName>
    Then  Get the Records from the source ingestion Tables <tableName>
    And   Get the Records from the DL all source views <tableName>
    Then  Compare data of source ingestion with all source extended views <tableName> are identical
    Examples:
      | tableName                                                                 |countOfRandomIds  |
      |product_availability_extended_allsource_v                                  |10                |
      |product_extended_pricing_allsource_v                                       |10                |
      |manifestation_extended_allsource_v                                         |10                |
      |manifestation_extended_page_count_allsource_v                              |10                |
      |manifestation_extended_restriction_allsource_v                             |10                |
      |work_extended_allsource_v                                                  |10                |
      |work_extended_metric_allsource_v                                           |10                |
      |work_extended_person_role_allsource_v                                      |10                 |
      |work_extended_relationship_sibling_allsource_v                             |10                |
      |work_extended_subject_area_allsource_v                                     |10                |
      |work_extended_url_allsource_v                                              |10                |
      |work_extended_editorial_board_allsource_v                                  |10                |


  @DLExtViews
  Scenario Outline: Verify Data Count for DL extended tables is transferred from All Extended views
    Given Get the total count of DL Extended views <tableName>
    Then  We know the total count of All Extended views <tableName>
    And   Compare count of All Ext Views with <tableName> views are identical
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



