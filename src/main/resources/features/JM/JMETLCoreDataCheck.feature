Feature: JM ETL Core tests

  @JMETL
  Scenario Outline: Verify that all JM Transformed ETL data is transferred to ETL tables
    Given We get the <numberOfRecords> random JM ETL ids of <ETLtable>
    When We get the JM Transformed ETL Query records from <ETLtable>
    Then We get the JM ETL records from <ETLtable>
    And Compare JM records in Transformed ETL and ETL of <ETLtable>
    Examples:
      |numberOfRecords  |ETLtable                              |
#      | 10               |etl_accountable_product_dq_v          |
#      | 10               |etl_wwork_dq_v                        |
#      | 10               |etl_work_identifier_dq_v              |
#      | 10               |etl_work_legal_owner_dq_V             |
#      | 10               |etl_work_subject_area_dq_v            |
#      | 10               |etl_work_person_role_dq_v             |
#      | 10               |etl_work_business_model_dq_v          |
#      | 10               |etl_work_access_model_dq_v            |
      | 10               |etl_manifestation_dq_v                |
#      | 10               |etl_manifestation_updates1_v          |
#      | 10               |etl_manifestation_identifier_dq_v     |
#      | 10               |etl_product_part1_v                   |
#      | 10               |etl_product_inserts_v                 |
#      | 10               |etl_product_updates_v                 |
#      | 10               |etl_product_dq_v                      |
#      | 10               |etl_product_person_role_dq_v          |
#      | 10               |sd_subject_areas_v                    |


#  works_attrs_roles_people_v was left out of these tests due to the view Query causing stack overflow errors
