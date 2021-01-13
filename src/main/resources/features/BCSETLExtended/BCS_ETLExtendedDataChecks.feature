Feature:Validate data checks for BCS ETL Extended in Data Lake Access Layer

#  Created by Dinesh on 06/10/2020

  @BCSExtended
  Scenario Outline: Verify Data for BCS_ETL Extended tables is transferred from Inbound Tables
    Given Get the <countOfRandomIds> of BCS Extende data from Inbound Tables <tableName>
    Then  Get the Data from the BCS Extended Inbound Tables <tableName>
    And   Data from the BCS Extended Current Tables to compare Inbound Check <tableName>
   Then  Compare data of BCS Inbound and BCS Extended <tableName> tables are identical
    Examples:
      | tableName                                |       countOfRandomIds |
     | etl_availability_extended_current_v      |        10              |
    | etl_manifestation_extended_current_v     |        10              |
     | etl_page_count_extended_current_v        |        10              |
     | etl_url_extended_current_v                    |        10              |
      | etl_work_extended_current_v                   |10                      |
      | etl_work_subject_area_extended_current_v      |10                        |
      | etl_manifestation_restrictions_extended_current_v      |10               |
      | etl_product_prices_extended_current_v      |10                           |
      | etl_work_person_role_extended_current_v      |10|