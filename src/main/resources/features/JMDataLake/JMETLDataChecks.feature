Feature:Validate data for JM between Transform Tables

  @JMETL
  Scenario Outline: Verify that all JM Transformed Inbound data is transferred to Current tables
    Given We get the <numberOfRecords> random JM ids of <Currenttable>
    When We get the JM Transformed Inbound records from <Currenttable>
    Then We get the JM Staging Query records from <CurrenttableQuery>
    And Compare JM records in Transformed Inbound and Current of <Currenttable>
    Examples:
      |numberOfRecords  |CurrenttableQuery           |Currenttable               |
      | 5               |work                        |jmf_work                   |
#      | 5               |manifestation               |jmf_manifestation          |
#      | 5               |work_person_role            |jmf_work_person_role       |
#      | 5               |work_subject_area           |jmf_work_subject_area      |
#      | 5               |work_chronicle              |jmf_work_chronicle         |
#      | 5               |pmg_pubdir_allocation       |jmf_pmg_pubdir_allocation  |
#      | 5               |approval_attachment         |jmf_approval_attachment    |
#      | 5               |review_comment              |jmf_review_comment         |
#      | 5               |approval_request            | jmf_approval_request      |
#      | 5               |application_properties      |jmf_application_properties |
#      | 5               |chronicle_scenario          |jmf_chronicle_scenario     |
#      | 5               |work_ownership              |jmf_work_ownership         |

  @JMETL
  Scenario Outline: Verify that all JM Transformed ETL data is transferred to ETL tables
    Given We get the <numberOfRecords> random JM ETL ids of <ETLtable>
#    When We get the JM Transformed Inbound records from <Currenttable>
#    Then We get the JM Staging Query records from <CurrenttableQuery>
#    And Compare JM records in Transformed Inbound and Current of <Currenttable>
    Examples:
      |numberOfRecords  |ETLtable                              |
#      | 5               |etl_accountable_product_dq_v          |
#      | 5               |etl_wwork_dq_v                        |
#      | 5               |etl_work_identifier_dq_v              |
#      | 5               |etl_work_subject_area_dq_v            |
#      | 5               |etl_work_person_role_dq_v             |
#      | 5               |etl_manifestation_updates1_v          |
#      | 5               |etl_manifestation_identifier_dq_v     |
#      | 5               |etl_product_part1_v                   |
#      | 5               |etl_product_inserts_v                 |
#      | 5               |etl_product_updates_v                 |
#      | 5               |etl_product_dq_v                      |
#      | 5               |etl_product_person_role_dq_v          |
      | 5               |sd_subject_areas_v                    |
#      | 5               |works_attrs_roles_people_v            |
#
#      | 5               |etl_manifestation_dq_v                |
#      | 5               |jm_imprint_data_v                     |

#  manifestation_dq is under development



















