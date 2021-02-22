Feature:Validate data for  Manifestation Ext Stitching tables in EPH

#  Created by Dinesh on 22/02/2020

  @ExtStitching
  Scenario Outline: Verify Data from the Manif_extended tables transferred to Manif Extended Stitching table
    Given We get the <countOfRandomIds> random manifestation Ext EPR ids <tableName>
    And Get the records from Manifestation extended table
    Then Compare Manif Extended and Manif Extended Stitching Table
    Examples:
      |tableName                   |countOfRandomIds|
      |manifestation_extended               |1                 |

