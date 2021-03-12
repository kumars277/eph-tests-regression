Feature:Validate data for  Product Ext Stitching tables in EPH

#  Created by Dinesh on 22/02/2020

  @ExtStitching
  Scenario Outline: Verify Data from the Product extended availability tables transferred to Product Extended Stitching JSON
    Given We get the <countOfRandomIds> random Prod Ext Availability EPR ids <tableName>
    And Get the records from Prod extended Availability table
    Then Compare Product Extended Availability and Product Extended Stitching JSON
    Examples:
      |tableName                                   |countOfRandomIds|
      |product_extended_availability               |10                 |




