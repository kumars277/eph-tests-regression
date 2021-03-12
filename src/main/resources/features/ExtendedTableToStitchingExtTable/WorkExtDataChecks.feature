Feature:Validate data for  Work Ext Stitching tables in EPH

#  Created by Dinesh on 22/02/2020

  @ExtStitching
  Scenario Outline: Verify Data from the Work_extended tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work Ext EPR ids <tableName>
    And Get the records from Work extended table
    Then Compare work Extended and work Extended Stitching Table
    Examples:
      |tableName                   |countOfRandomIds|
      |work_extended               |1                 |


  Scenario: Verify count between work extended and work stitching table
    Given We get the counts from the WorkExtended Table
    And Get the counts from Work Stitching Table
    Then Compare work Extended and work Extended Stitching Table Counts



