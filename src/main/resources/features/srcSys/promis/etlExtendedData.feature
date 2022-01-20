#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45487295035/PROMIS+Inbound
  #Queries:https://github.com/elsevier-bts/eph-datalabs-dag/tree/master_v1/src/dag/resources/common/promis_inbound

Feature:Validate data for Promis between transform tables


  @PROMISETL
  Scenario Outline: Verify that all Promis data is transferred between Transform mapping and Current tables
    Given We know the number of Promis <tablename> data for Transform mapping
    Then Get the count for Promis <Currenttablename> Current
    And Compare the Promis count for <tablename> table between Transform Mapping and Current
    Given We get the <numberOfRecords> random Promis transform mapping ids of <Currenttablename>
    When We get Promis transform mapping records from <tablename>
    Then We get the Promis Transform mapping current records from <Currenttablename>
    And Compare Promis records for transform mapping and current of <tablename>
    Examples:
      |numberOfRecords  |tablename      |Currenttablename                        |
      | 50                |subject_areas  |promis_transform_current_subject_areas |
      | 50               |pricing        |promis_transform_current_pricing        |
      | 50               |person_roles   |promis_transform_current_person_roles   |
      | 50               |works          |promis_transform_current_works          |
      | 50               |metrics        |promis_transform_current_metrics        |
      | 50               |urls           |promis_transform_current_urls           |
      | 50               |work_rels      |promis_transform_current_work_rels      |

  @PROMISETL
  Scenario Outline: Verify that Promis is correct between Current and transform History tables
   Given We know the number of Promis <PromisCurrentTable> data for the current
   Then Get the count for Promis <TransformHistoryTable> Transform Hisory with the latest timestamp
    And Compare the Promis count for <TransformHistoryTable> table between Current and Transform Hisory with the latest timestamp
    Given We get the <numberOfRecords> random Promis current ids of <PromisCurrentTable>
    Then We get the Promis Transform mapping current records from <PromisCurrentTable>
    Then We get the Promis transform file history records from <TransformHistoryTable>
    And Compare Promis records for current and transform file of <TransformHistoryTable>
    Examples:
      |numberOfRecords  |PromisCurrentTable                    |TransformHistoryTable                             |
      | 50               |promis_transform_current_subject_areas|promis_transform_file_history_subject_areas_part  |
      | 50               |promis_transform_current_pricing      |promis_transform_file_history_pricing_part        |
      | 50               |promis_transform_current_person_roles |promis_transform_file_history_person_roles_part   |
      | 50               |promis_transform_current_works        |promis_transform_file_history_works_part          |
      | 50               |promis_transform_current_metrics      |promis_transform_file_history_metrics_part        |
      | 50               |promis_transform_current_urls         |promis_transform_file_history_urls_part           |
      | 50               |promis_transform_current_work_rels    |promis_transform_file_history_work_rels_part      |

@notUsed
  Scenario Outline: Verify that Promis is correct between Delta Query and Delta tables
    Given We know the number of Promis <DeltaQueryTable> data for the Delta Query
    Then Get the count for Promis <Deltatablename> Delta
    And Compare the Promis count for <Deltatablename> table between Current minus Previous and Delta
    Given We get the <numberOfRecords> random Promis DeltaQuery ids of <Deltatablename>
    When We get Promis Delta Query records from <DeltaQueryTable>
    Then We get the Promis Delta records from <Deltatablename>
    And Compare Promis records for delta query and delta tables of <Deltatablename>
    Examples:
      |numberOfRecords  |DeltaQueryTable                                   |Deltatablename                     |
      | 5               |promis_transform_file_history_subject_areas_part  |promis_delta_current_subject_areas |
      | 5               |promis_transform_file_history_pricing_part        |promis_delta_current_pricing       |
      | 5               |promis_transform_file_history_person_roles_part   |promis_delta_current_person_roles  |
      | 5               |promis_transform_file_history_works_part          |promis_delta_current_works         |
      | 5               |promis_transform_file_history_metrics_part        |promis_delta_current_metrics       |
      | 5               |promis_transform_file_history_urls_part           |promis_delta_current_urls          |
      | 5               |promis_transform_file_history_work_rels_part      |promis_delta_current_work_rels     |


@notUsed
  Scenario Outline: Verify that all Promis data is transferred between History excluding query and History exl tables
    Given We know the number of Promis <tablename> data for History excluding query
    Then Get the count for Promis <HistExcltablename> History excluding
    And Compare the Promis count for <HistExcltablename> table between History excluding query and History excluding
    Given We get the <numberOfRecords> random Promis History Excluding Query ids of <HistExcltablename>
    When We get Promis History Excluding Query records from <tablename>
    Then We get the Promis History Excluding records from <HistExcltablename>
    And Compare Promis records for History Excluding query and History Excluding tables of <tablename>
    Examples:
      |numberOfRecords  |tablename      |HistExcltablename                                   |
      | 5               |subject_areas  |promis_transform_history_subject_areas_excl_delta_v |
      | 5               |pricing        |promis_transform_history_pricing_excl_delta_v       |
      | 5               |person_roles   |promis_transform_history_person_roles_excl_delta_v  |
      | 5               |works          |promis_transform_history_works_excl_delta_v         |
      | 5               |metrics        |promis_transform_history_metrics_excl_delta_v       |
      | 5               |urls           |promis_transform_history_urls_excl_delta_v          |
      | 5               |work_rels      |promis_transform_history_work_rels_excl_delta_v     |

  @PROMISETL
  Scenario Outline: Verify that all Promis data is transferred between Latest query and Latest tables
    Given We know the number of Promis <tablename> data for latest query
    Then Get the count for Promis <Latesttablename> latest
    And Compare the Promis count for <Latesttablename> table between Latest query and Latest
    Given We get the <numberOfRecords> random Promis Latest Query ids of <Latesttablename>
    When We get Promis Latest Query records from <tablename>
    Then We get the Promis Latest records from <Latesttablename>
    And Compare Promis records for Latest query and Latest tables of <tablename>
    Examples:
      |numberOfRecords  |tablename      |Latesttablename                     |
      | 50               |subject_areas  |promis_transform_latest_subject_areas |
      | 50               |pricing        |promis_transform_latest_pricing       |
      | 50               |person_roles   |promis_transform_latest_person_roles  |
      | 50               |works          |promis_transform_latest_works         |
      | 50               |metrics        |promis_transform_latest_metrics       |
      | 50               |urls           |promis_transform_latest_urls          |
      | 50               |work_rels      |promis_transform_latest_work_rels     |
















