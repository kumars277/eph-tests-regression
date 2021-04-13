Feature:Validate data of DL Extended Views where data comes from BCS,SDRM,JM Ext to All Extended Views

#  Created by Dinesh on 03/02/2021

  @DLExtViews
  Scenario Outline: Verify Data Count for All Extended views is transferred from Source Ingestion
    Given Get the total count of All Extended views <tableName>
    Then  We know the total count of Source Ingestion <tableName>
    And Compare count of All Extended Views with Source Ingestion <tableName> are identical
    Examples:
      | tableName                                                                 |
      |product_availability_extended_allsource_v                                  |
      |product_extended_pricing_allsource_v                                       |
      |manifestation_extended_allsource_v                                         |
      |manifestation_extended_page_count_allsource_v                              |
      |manifestation_extended_restriction_allsource_v                             |
      |work_extended_allsource_v                                                  |
      |work_extended_metric_allsource_v                                          |
      |work_extended_person_role_allsource_v                                     |
      |work_extended_relationship_sibling_allsource_v                            |
      |work_extended_subject_area_allsource_v                                    |
      |work_extended_url_allsource_v                                             |
      |work_extended_editorial_board_allsource_v                                  |


  @DLExtView
  Scenario Outline: Verify Data for DL all source extended views is transferred from Source  Ingestion Tables
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

