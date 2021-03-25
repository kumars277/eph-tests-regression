Feature:Validate data for  Manifestation Ext Stitching tables in EPH

#  Created by Dinesh on 22/02/2020

  @ExtStitching
  Scenario Outline: Verify Data from the Manif_extended tables transferred to Manif Extended Stitching table
    Given We get the <countOfRandomIds> random manifestation Ext EPR ids <tableName>
    And Get the records from Manifestation extended table
    Then Compare Manif Extended and Manif Extended Stitching Table
    Examples:
      |tableName                   |countOfRandomIds|
      |manifestation_extended      |20                 |

  @ExtStitching
  Scenario Outline: Verify Data from the Manif_extended page count tables transferred to Manif Extended Stitching table
    Given We get the <countOfRandomIds> random manifestation Ext page count EPR ids <tableName>
    And Get the records from Manifestation extended page count table
    Then Compare Manif Extended page count and Manif Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |manifestation_extended_page_count               |10             |


  @ExtStitching
  Scenario Outline: Verify Data from the Manif_extended restrictions tables transferred to Manif Extended Stitching table
    Given We get the <countOfRandomIds> random manifestation Ext restrictions EPR ids <tableName>
    And   Get the records from Manifestation extended restrictions table
    Then  Compare Manif Extended restrictions and Manif Extended Stitching Table
    Examples:
      |tableName                                        |countOfRandomIds|
      |manifestation_extended_restriction               |10            |

