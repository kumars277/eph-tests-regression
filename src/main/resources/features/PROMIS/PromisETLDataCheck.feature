Feature:Validate data checks for Promis between transform tables

  @PROMISETL
  Scenario Outline: Verify that all Promis Inbound data is transferred to Current tables
    Given We get the <numberOfRecords> random Promis ids of <Currenttablename>
    When We get the Promis Inbound records from <Inboundtablename>
    Then We get the Promis Current records from <Currenttablename>
    And Compare Promis records in Inbound and Current of <Inboundtablename>
    Examples:
      |numberOfRecords |Inboundtablename       |Currenttablename             |
      | 5               |promis_prmautpubt_part  |promis_prmautpubt_current  |
      | 5               |promis_prmclscodt_part  |promis_prmclscodt_current  |
      | 5               |promis_prmclst_part     |promis_prmclst_current     |
      | 5               |promis_prmlondest_part  |promis_prmlondest_current  |
      | 5               |promis_prmpricest_part  |promis_prmpricest_current  |
      | 5               |promis_prmpubinft_part  |promis_prmpubinft_current  |
      | 5               |promis_prmincpmct_part  |promis_prmincpmct_current  |
      | 5               |promis_prmpmccodt_part  |promis_prmpmccodt_current  |

#      | 5               |promis_prmpubrelt_part  |promis_prmpubrelt_current  |

  @PROMISETL
  Scenario Outline: Verify that all Promis Delta data is transferred
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

  @PROMISETL
  Scenario Outline: Verify that all Promis History Excluding data is transferred
    Given We get the <numberOfRecords> random Promis History Excluding Query ids of <HistExcltablename>
    When We get Promis History Excluding Query records from <tablename>
    Then We get the Promis History Excluding records from <HistExcltablename>
   And Compare Promis records for History Excluding query and History Excluding tables of <tablename>
    Examples:
      |numberOfRecords  |tablename      |HistExcltablename                                 |
      | 5               |subject_areas  |promis_transform_history_subject_areas_excl_delta |
      | 5               |pricing        |promis_transform_history_pricing_excl_delta       |
      | 5               |person_roles   |promis_transform_history_person_roles_excl_delta  |
      | 5               |works          |promis_transform_history_works_excl_delta         |
      | 5               |metrics        |promis_transform_history_metrics_excl_delta       |
      | 5               |urls           |promis_transform_history_urls_excl_delta          |
      | 5               |work_rels      |promis_transform_history_work_rels_excl_delta     |

  @PROMISETL
  Scenario Outline: Verify that all Promis Latest data is transferred
    Given We get the <numberOfRecords> random Promis Latest Query ids of <Latesttablename>
    When We get Promis Latest Query records from <tablename>
    Then We get the Promis Latest records from <Latesttablename>
    And Compare Promis records for Latest query and Latest tables of <tablename>
    Examples:
      |numberOfRecords  |tablename      |Latesttablename                     |
      | 5               |subject_areas  |promis_transform_latest_subject_areas |
      | 5               |pricing        |promis_transform_latest_pricing       |
      | 5               |person_roles   |promis_transform_latest_person_roles  |
      | 5               |works          |promis_transform_latest_works         |
      | 5               |metrics        |promis_transform_latest_metrics       |
      | 5               |urls           |promis_transform_latest_urls          |
      | 5               |work_rels      |promis_transform_latest_work_rels     |

  @PROMISETL
  Scenario Outline: Verify that all Promis transform mapping data is transferred to current tables
    Given We get the <numberOfRecords> random Promis transform mapping ids of <Currenttablename>
    When We get Promis transform mapping records from <tablename>
    Then We get the Promis Transform mapping current records from <Currenttablename>
    And Compare Promis records for transform mapping and current of <tablename>
    Examples:
      |numberOfRecords  |tablename      |Currenttablename                       |
      | 5               |subject_areas  |promis_transform_current_subject_areas |
      | 5               |pricing        |promis_transform_current_pricing       |
      | 5               |person_roles   |promis_transform_current_person_roles  |
      | 5               |works          |promis_transform_current_works         |
      | 5               |metrics        |promis_transform_current_metrics       |
      | 5               |urls           |promis_transform_current_urls          |
      | 5               |work_rels      |promis_transform_current_work_rels     |

  @PROMISETL
  Scenario Outline: Verify that all Promis data is transferred between Latest and All_source tables
    Given We get the <numberOfRecords> random Promis Latest ids of <latesttablename>
    When We get Promis allsource records from <allsourcetablename>
    Then We get the Latest records from <latesttablename>
    And Compare Promis records for Latest and AllSource of <allsourcetablename>
    Examples:
      |numberOfRecords  |latesttablename                       |allsourcetablename                                  |
#      | 5               |promis_transform_latest_pricing       |product_extended_pricing_allsource_v                |
#
#      | 5               |promis_transform_latest_works         |work_extended_allsource_v                           |
#      | 5               |promis_transform_latest_metrics       |work_extended_metric_allsource_v                    |
#      | 5               |promis_transform_latest_person_roles  |work_extended_editorial_board_allsource_v           |
#      | 5               |promis_transform_latest_work_rels     |work_extended_relationship_sibling_allsource_v      |
#      | 5               |promis_transform_latest_subject_areas |work_extended_subject_area_allsource_v              |
#      | 5               |promis_transform_latest_urls          |work_extended_url_allsource_v                       |


