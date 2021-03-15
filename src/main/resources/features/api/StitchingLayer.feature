Feature:data check between stitching layer and PF API's



  @stitchingData @extendedData
  Scenario Outline: data check for stch_table json with PF APIs
    Given we get 10 random ids from <stcTable>
    And call PF search API for ids and compare with json for <stcTable>
    Examples:
    |stcTable|
    |  stch_product_ext_json_byAvailability      |
 #   |  stch_product_ext_json_byPricing           |
 #   |  stch_product_core_json                    |
 #   |  stch_manifestation_ext_json               |
 #   |  stch_work_ext_json                        |
 #   |  stch_work_core_json                       |

