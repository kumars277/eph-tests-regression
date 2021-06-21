Feature:Validate data for JM between Transform Tables

  @JMETLJM
  Scenario Outline: Verify that all JM DB data is transferred to Inbound Table
    Given We get the <numberOfRecords> random JMDB ids of <JMDBtable>
    When We get the JM DB records from <JMDBtable>
    Then We get the JMInbound records from <InboundTable>
    And Compare JM records in between JMDB and Inbound of <JMDBtable>
    Examples:
      |numberOfRecords  |JMDBtable                   |InboundTable               |
      | 5               |work_business_model         |jmf_work_business_model    |
      | 5               |work_access_model           |jmf_work_access_model      |
      | 5               |work_product_group          |jmf_work_product_group     |
      | 5               |product_group               |jmf_product_group          |
      | 5               |pricing_option              |jmf_pricing_option         |
      | 5               |bm_pg_options               |jmf_bm_pg_options          |

  @JMETLJM
  Scenario Outline: Verify that all transformed Inbound data is transferred to Access tables
    Given We get the <numberOfRecords> random JM ids of <Currenttable>
    When We get the JM Transformed Inbound records from <Currenttable>
    Then We get the JM Staging Query records from <CurrenttableQuery>
    And Compare JM records in Transformed Inbound and Current of <Currenttable>
    Examples:
      |numberOfRecords  |CurrenttableQuery           |Currenttable               |
      | 5               |work                        |jmf_work                   |
      | 5               |manifestation               |jmf_manifestation          |
      | 5               |work_person_role            |jmf_work_person_role       |
      | 5               |work_subject_area           |jmf_work_subject_area      |
      | 5               |work_chronicle              |jmf_work_chronicle         |
      | 5               |pmg_pubdir_allocation       |jmf_pmg_pubdir_allocation  |
      | 5               |approval_attachment         |jmf_approval_attachment    |
      | 5               |review_comment              |jmf_review_comment         |
      | 5               |approval_request            | jmf_approval_request      |
      | 5               |application_properties      |jmf_application_properties |
      | 5               |chronicle_scenario          |jmf_chronicle_scenario     |
      | 5               |work_ownership              |jmf_work_ownership         |
      | 5               |work_business_model         |jmf_work_business_model    |
      | 5               |work_access_model           |jmf_work_access_model      |
      | 5               |work_product_group          |jmf_work_product_group     |
      | 5               |product_group               |jmf_product_group          |
      | 5               |pricing_option              |jmf_pricing_option         |
      | 5               |bm_pg_options               |jmf_bm_pg_options          |

  @JMETL
  Scenario Outline: Verify that all core data is transferred between Semarchy and Stitching DB
    Given We get the <numberOfRecords> random Stitching ids of <SemarchyTable>
    And Compare Core records from <SemarchyTable> with Work or Product stitching db
    Examples:
      |numberOfRecords  |SemarchyTable                   |
      | 5               |gd_wwork                        |
      | 5               |gd_work_identifier              |
      | 5               |gd_product                      |
      | 5               |gd_work_relationship            |
      | 5               |gd_work_person_role             |
      | 5               |gd_person                       |
      | 5               |gd_subject_area                 |
      | 5               |gd_manifestation                |
      | 5               |gd_manifestation_identifier     |
      | 5               |gd_accountable_product          |

  @JMETL
  Scenario Outline: Verify that  all JM Staging data is transferred to Semarchy tables
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































