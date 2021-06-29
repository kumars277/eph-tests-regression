Feature:Validate data for JM between Transform Tables
  #git hub: https://github.com/elsevier-bts/eph-datalabs-dag/tree/master_v1/src/dag/resources/common/journalmaestro_inbound

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
    #need to add count checks as well.
  Scenario Outline: Verify that all transformed Inbound data is transferred to Access tables
    Given We know the number of JM <Currenttable> data in inbound
    When The JM <Currenttable> data is in the current
    Then The JM count for <Currenttable> table between inbound and current are identical
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
     #- | 5               |work_chronicle              |jmf_work_chronicle         | missing in UAT
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






























