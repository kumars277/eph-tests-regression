Feature:Validate data for EPH Cross Reference PostgreSQL

  @crossRef
  Scenario Outline: Verify that GD tables in the PostgreSQL and CrossRef DL

    Given We get the count <SourceTable> from semarchy_eph_mdm
    Then Get the count of <SourceTable> from the CrossRef Schema from DL
    And we compare crossRef Gd and Semarchy GD counts are identical <SourceTable>
    Given Get <numberOfRecords> random ids of <SourceTable> from the Semarchy
    When We get the records for <SourceTable> from semarchy
    Then Get the records for <SourceTable> from the crossRef DL
    And Compare the records of <SourceTable> from Semarchy and crossRef DL
    Examples:
      |numberOfRecords  |SourceTable                 |
      | 5               |gd_manifestation            |
      | 5               |gd_manifestation_identifier |
      | 5               |gd_product                   |
      | 5               |gd_product_identifier      |
      | 5               |gd_work_identifier      |
      | 5               |gd_wwork                   |


  @crossRef
  Scenario Outline: Verify that CrossRef identifier in DL
    Given We get the count from the work,manif and product identifiers
    Then Get the count from eph_identifier_cross_reference
    And we compare work,manifestation and product identifiers counts with eph_identifier_cross_reference
    Given Get <numberOfRecords> random ids of <SourceTable> from the Semarchy
    When We get the records from the work,manif and product identifiers
    Then Get the records from <SourceTable>
   And Compare the records of work,manif and product identifiers with <SourceTable>
    Examples:
      |numberOfRecords  |SourceTable                 |
      | 1000               |eph_identifier_cross_reference            |




