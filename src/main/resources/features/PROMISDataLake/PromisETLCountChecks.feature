Feature:Validate data count for Promis between transform tables

  @PROMISETL
  Scenario Outline: Verify that all Promis data is transferred between inbound and current tables
    Given We know the number of Promis <InboundtableName> data in inbound
    Then Get the count for Promis <Currenttablename> current
    And Compare the Promis count for <InboundtableName> table between inbound and current
    Examples:
      |InboundtableName        |Currenttablename           |
      |promis_prmautpubt_part  |promis_prmautpubt_current  |
      |promis_prmclscodt_part  |promis_prmclscodt_current  |
      |promis_prmclst_part     |promis_prmclst_current     |
      |promis_prmlondest_part  |promis_prmlondest_current  |
      |promis_prmpricest_part  |promis_prmpricest_current  |
      |promis_prmpubinft_part  |promis_prmpubinft_current  |
      |promis_prmpubrelt_part  |promis_prmpubrelt_current  |
      |promis_prmincpmct_part  |promis_prmincpmct_current  |
      |promis_prmpmccodt_part  |promis_prmpmccodt_current  |

  @PROMISETL
  Scenario Outline: Verify that Promis count is correct between Delta Query and Delta tables
    Given We know the number of Promis <DeltaQueryTable> data for the Delta Query
    Then Get the count for Promis <Deltatablename> Delta
    And Compare the Promis count for <Deltatablename> table between Current minus Previous and Delta
    Examples:
      |DeltaQueryTable                                   |Deltatablename                     |
      |promis_transform_file_history_subject_areas_part  |promis_delta_current_subject_areas |
      |promis_transform_file_history_pricing_part        |promis_delta_current_pricing       |
      |promis_transform_file_history_person_roles_part   |promis_delta_current_person_roles  |
      |promis_transform_file_history_works_part          |promis_delta_current_works         |
      |promis_transform_file_history_metrics_part        |promis_delta_current_metrics       |
      |promis_transform_file_history_urls_part           |promis_delta_current_urls          |
      |promis_transform_file_history_work_rels_part      |promis_delta_current_work_rels     |

  @PROMISETL
  Scenario Outline: Verify that all Promis data is transferred between History excluding query and History exl tables
    Given We know the number of Promis <tablename> data for History excluding query
    Then Get the count for Promis <HistExcltablename> History excluding
    And Compare the Promis count for <HistExcltablename> table between History excluding query and History excluding
    Examples:
      |tablename      |HistExcltablename                                 |
      |subject_areas  |promis_transform_history_subject_areas_excl_delta |
      |pricing        |promis_transform_history_pricing_excl_delta       |
      |person_roles   |promis_transform_history_person_roles_excl_delta  |
      |works          |promis_transform_history_works_excl_delta         |
      |metrics        |promis_transform_history_metrics_excl_delta       |
      |urls           |promis_transform_history_urls_excl_delta          |
      |work_rels      |promis_transform_history_work_rels_excl_delta     |

  @PROMISETL
  Scenario Outline: Verify that all Promis data is transferred between Latest query and Latest tables
    Given We know the number of Promis <tablename> data for latest query
    Then Get the count for Promis <Latesttablename> latest
    And Compare the Promis count for <Latesttablename> table between Latest query and Latest
    Examples:
      |tablename      |Latesttablename                     |
      |subject_areas  |promis_transform_latest_subject_areas |
      |pricing        |promis_transform_latest_pricing       |
      |person_roles   |promis_transform_latest_person_roles  |
      |works          |promis_transform_latest_works         |
      |metrics        | promis_transform_latest_metrics      |
      |urls           |promis_transform_latest_urls          |
      |work_rels      |promis_transform_latest_work_rels     |

  @PROMISETL
  Scenario Outline: Verify that all Promis data is transferred between Transform mapping and Current tables
    Given We know the number of Promis <tablename> data for Transform mapping
    Then Get the count for Promis <Currenttablename> Current
    And Compare the Promis count for <tablename> table between Transform Mapping and Current
    Examples:
      |tablename      |Currenttablename                       |
      |subject_areas  |promis_transform_current_subject_areas |
      |pricing        |promis_transform_current_pricing       |
      |person_roles   |promis_transform_current_person_roles  |
      |works          |promis_transform_current_works         |
      |metrics        |promis_transform_current_metrics       |
      |urls           |promis_transform_current_urls          |
      |work_rels      |promis_transform_current_work_rels     |

  @PROMISETL
  Scenario Outline: Verify that all Promis data is transferred between Latest and All_source tables
    Given We know the number of Promis <latesttablename> data for Latest
    Then Get the count for Promis <allsourcetablename> Promis All_source
    And Compare the Promis count for <latesttablename> table between Latest and All_source
    Examples:
      |latesttablename                       |allsourcetablename                                  |
      |promis_transform_latest_pricing       |product_extended_pricing_allsource_v                |
      |promis_transform_latest_works         |work_extended_allsource_v                           |
      |promis_transform_latest_metrics       |work_extended_metric_allsource_v                    |
      |promis_transform_latest_person_roles  |work_extended_editorial_board_allsource_v           |
      |promis_transform_latest_work_rels     |work_extended_relationship_sibling_allsource_v      |
      |promis_transform_latest_subject_areas |work_extended_subject_area_allsource_v              |
      |promis_transform_latest_urls          |work_extended_url_allsource_v                       |














