Feature:Validate data for Stitching Core Tables

  @GdToStitching
  Scenario Outline: Verify that all core data is transferred between Semarchy and Stitching DB
    Given We get the <numberOfRecords> random Stitching ids of <SemarchyTable>
    And Compare Core records from <SemarchyTable> with Work or Product stitching db
    Examples:
      |numberOfRecords  |SemarchyTable                   |
      | 10              |gd_wwork                        |
      | 10              |gd_work_identifier              |
      | 5               |gd_product                      |
      | 5               |gd_work_relationship            |
      | 5               |gd_work_person_role             |
      | 5               |gd_person                       |
      | 5               |gd_subject_area                 |
      | 5               |gd_manifestation                |
      | 5               |gd_manifestation_identifier     |
      | 5               |gd_accountable_product          |





























