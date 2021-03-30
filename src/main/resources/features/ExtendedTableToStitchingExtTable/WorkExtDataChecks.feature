Feature:Validate data for  Work Ext Stitching tables in EPH

#  Created by Dinesh on 18/03/2020

  @ExtStitching
  Scenario Outline: Verify Data from the Work_extended tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from Work extended table
    Then Compare work Extended and work Extended Stitching Table
    Examples:
      |tableName                   |countOfRandomIds|
      |work_extended               |50                 |

  @ExtStitching
  Scenario Outline: Verify Data from the work_extended metric tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended metric table
    Then Compare work Extended metric and work Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |work_extended_metric                           |10             |

  @ExtStitching
  Scenario Outline: Verify Data from the work_extended url tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended url table
    Then Compare work Extended url and work Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |work_extended_url                           |50             |

  @ExtStitching
  Scenario Outline: Verify Data from the work_extended_subj_area tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended subj area table
    Then Compare work Extended subj area and work Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |work_extended_subject_area                           |50            |

  @ExtStitching
  Scenario Outline: Verify Data from the work_extended_person_role tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended person role table
    Then Compare work Extended person role and work Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |work_extended_person_role                           |50           |


  @ExtStitching
  Scenario Outline: Verify Data from the work_extended_relationship tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended relationship sibling table
    Then Compare work Extended Relationships and work Extended Stitching Table
    Examples:
      |tableName                                                    |countOfRandomIds|
      |work_extended_relationship_sibling                           |50           |

  Scenario: Verify count between work extended and work stitching table
    Given We get the counts from the WorkExtended Table
    And Get the counts from Work Stitching Table
    Then Compare work Extended and work Extended Stitching Table Counts



