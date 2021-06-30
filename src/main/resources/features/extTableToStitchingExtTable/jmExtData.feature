Feature: Validate JM Extended data to stitch

  @JMETLExtendedStitch
  Scenario Outline: Verify Data for JM_ETL Extended tables is transferred to stitching Tables
    Given Get the total count of JM ETL Extended Table
    When We know the total count of Stitching table
    Then Compare count of JM Stitch data and JM ETL Extended table are identical
    Given Get the IDs <countOfRandomIds> of JM ETL Extended data
    Then  Get the Data from JMETL Extended Tables
    And   Data from the Stitching table
    Then  Compare data of JM ETL Extended Tables and Stitching tables are identical
    Examples:
      |countOfRandomIds|
      |2               |
