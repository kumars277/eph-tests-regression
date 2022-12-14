Feature:Validate data for GD Tables

  @coreToGd
  Scenario Outline: Verify that  all core view is transferred to Semarchy/GD tables

    Given We get the count of all core views <SourceTable>
    Then Get the count of Gd tables <SemarchyTable>
    And we compare count of All core Views <SourceTable> and gd tables <SemarchyTable> are identical
    Given We get the <numberOfRecords> random JM Staging ids of <SourceTable>
    When We get the JM Staging records from <SourceTable>
    Then We get the JM Semarchy records from <SemarchyTable>
    And Compare JM records in Staging and Semarchy of <SourceTable>
    Examples:
      |numberOfRecords  |SourceTable                        |SemarchyTable               |
      | 5               |all_manifestation_identifiers_v    |gd_manifestation_identifier |
      | 5               |all_manifestation_v                |gd_manifestation            |
      | 5               |all_person_v                       |gd_person                   |
      | 5               |all_product_v                      |gd_product                  |
      | 5               |all_work_identifier_v              |gd_work_identifier          |
      | 5               |all_work_v                         |gd_wwork                    |
      | 5               |all_work_relationship_v            |gd_work_relationship        |
      | 5               |all_work_person_role_v             |gd_work_person_role         |
      | 5               |all_work_subject_areas_v           |gd_subject_area             |
      | 5               |all_work_access_model_v            |gd_work_access_model        |
      | 5               |all_work_business_model_v          |gd_work_business_model      |
      | 5               |all_accountable_product_v          |gd_accountable_product      |

    @coreToGd
    Scenario Outline: Check for the Duplicates in the GD tables
      Given We get the duplicate count from the <SemarchyTable>
      Then Verify the duplicate count from the <SemarchyTable> is 0.
      Examples:
      |SemarchyTable                |
      |gd_manifestation_identifier  |































