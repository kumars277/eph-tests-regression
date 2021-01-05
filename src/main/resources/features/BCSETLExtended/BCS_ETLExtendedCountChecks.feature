Feature:Validate data count for BCS Extended tables

#  Created by Dinesh S on 05/01/2020



  @BCSExtended
  Scenario Outline: Verify Data Count for BCS Extended tables is transferred from Inbound Tables
    Given Get the total count of BCS Extended from Current Tables <tableName>
    When We know the total count of BCS Extended Inbound tables <tableName>
    Then Compare count of BCS Inbound and BCS Extended <tableName> tables are identical
    Examples:
      | tableName                                |
      | etl_availability_extended_current_v      |
      | etl_manifestation_extended_current_v      |
      | etl_page_count_extended_current_v      |
      | etl_url_extended_current_v      |
      | etl_work_extended_current_v      |
     | etl_work_subject_area_extended_current_v      |
      | etl_manifestation_restrictions_extended_current_v      |
      | etl_product_prices_extended_current_v      |
      #| etl_work_person_role_extended_current_v      |