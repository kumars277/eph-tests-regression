Feature:Validate data for BCS ETL Extended tables

  #confluence link: https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45477370785/BCS+Extended+Transformed+View+Mappings
  #confluence version: v83

  @BCSExtended
  Scenario Outline: Verify Data for BCS Extended tables is transferred from Inbound Tables
    Given Get the total count of BCS Extended from Current Tables <tableName>
    When We know the total count of BCS Extended Inbound tables <tableName>
    Then Compare count of BCS Inbound and BCS Extended <tableName> tables are identical
    Given Get the <countOfRandomIds> of BCS Extended data from Inbound Tables <tableName>
    Then  Get the Data from the BCS Extended Inbound Tables <tableName>
    And   Data from the BCS Extended Current Tables <tableName>
    Then  Compare data of BCS Inbound and BCS Extended <tableName> tables are identical
    Examples:
      | tableName                                               |countOfRandomIds |
      | etl_availability_extended_current_v                     |10            |
      | etl_manifestation_extended_current_v                    |10             |
      | etl_page_count_extended_current_v                       |10             |
      | etl_url_extended_current_v                              |10             |
      | etl_work_extended_current_v                             |10             |
      | etl_work_subject_area_extended_current_v                |10             |
      | etl_manifestation_restrictions_extended_current_v       |10             |
      | etl_product_prices_extended_current_v                   |10             |
      | etl_work_person_role_extended_current_v                 |10             |

  @BCSExtended
  Scenario Outline: Verify Data for BCS Extended transform_file tables are transferred from current tables
    Given Get the total count of BCS Extended from Current Tables <sourceTable>
    Then Get the count of BCS Extended transform_file <targetTable>
   And Compare BCS Extended count of current <sourceTable> and tranform_file <targetTable> are identical
    Given Get the <countOfRandomIds> of BCS Extended data from Current Tables <sourceTable>
    When Data from the BCS Extended Current Tables <sourceTable>
    Then We Get the records from transform BCS Ext Transform_File <targetTable>
    And Compare the records of BCS Extended current and BCS Transform_File <targetTable>
    Examples:
      | sourceTable                                      |  targetTable                                                       |countOfRandomIds     |
      |etl_availability_extended_current_v               |etl_availability_extended_transform_file_history_part               |50                   |
      |etl_manifestation_extended_current_v              |etl_manifestation_extended_transform_file_history_part              |50                   |
      |etl_page_count_extended_current_v                 |etl_page_count_extended_transform_file_history_part                 |50                   |
      |etl_url_extended_current_v                        |etl_url_extended_transform_file_history_part                        |50                   |
      |etl_work_extended_current_v                       |etl_work_extended_transform_file_history_part                       |50                   |
      |etl_work_subject_area_extended_current_v          |etl_work_subject_area_extended_transform_file_history_part          |50                   |
      |etl_manifestation_restrictions_extended_current_v |etl_manifestation_restrictions_extended_transform_file_history_part |50                   |
      |etl_product_prices_extended_current_v             |etl_product_prices_extended_transform_file_history_part             |50                   |
      |etl_work_person_role_extended_current_v           |etl_work_person_role_extended_transform_file_history_part           |50                   |

  @BCSExtended
  Scenario Outline: Verify Data for BCS Extended delta_latest tables are transferred from delta_current and Current_Exclude tables
    Given Get the sum of total count between BCS Extended delta current and Current_Exclude Table <targetTable>
    Then Get the BCS Extended <targetTable> latest data count
    And Compare BCS Extended latest counts of are identical <targetTable>
    Given Get the <countOfRandomIds> from sum of delta_current and exclude_delta for BCS Extended <targetTable>
    When Get the records from the sum of delta_current and exclude_delta for BCS Extended <targetTable>
    Then Get the records from <targetTable> BCS Extended latest table
    And  Compare the records of Latest with sum of delta_current and Exclude_Delta for BCS Extended <targetTable>
    Examples:
      | targetTable                                                     |  countOfRandomIds     |
      |etl_transform_history_extended_availability_latest               |50                     |
      |etl_transform_history_extended_manifestation_latest              |50                     |
      |etl_transform_history_extended_page_count_latest                 |50                     |
      |etl_transform_history_extended_url_latest                        |50                     |
      |etl_transform_history_extended_work_latest                       |50                     |
      |etl_transform_history_extended_work_subject_area_latest          |50                     |
      |etl_transform_history_extended_manifestation_restrictions_latest |50                     |
      |etl_transform_history_extended_product_prices_latest             |50                     |
      |etl_transform_history_extended_work_person_role_latest           |50                     |

  @BCSExtended
  Scenario Outline: Verify Duplicate Entry for BCS Extended in transform latest tables
    Given Get the BCS Extended duplicate count in <SourceTableName> table
    Then Check the BCS Extended count should be equal to Zero <SourceTableName>
    Examples:
      |SourceTableName                                                      |
      |etl_transform_history_extended_availability_latest                   |
      |etl_transform_history_extended_manifestation_latest                  |
      |etl_transform_history_extended_page_count_latest                     |
      |etl_transform_history_extended_url_latest                            |
      |etl_transform_history_extended_work_latest                           |
      |etl_transform_history_extended_work_subject_area_latest              |
      |etl_transform_history_extended_manifestation_restrictions_latest     |
      |etl_transform_history_extended_product_prices_latest                 |
      |etl_transform_history_extended_work_person_role_latest               |

    ##########################
  @notUsed
  Scenario Outline: Verify Data for BCS Extended history tables are transferred from current tables
    Given Get the total count of BCS Extended from Current Tables <sourceTable>
    Then Get the count of BCS Extended history <targetTable>
    And Compare count of BCS Extended current <sourceTable> and history <targetTable> are identical
    Given Get the <countOfRandomIds> of BCS Extended data from Current Tables <sourceTable>
    When Data from the BCS Extended Current Tables <sourceTable>
    Then We Get the records from transform BCS Ext Current History <targetTable>
    And Compare the records of BCS Extended current and BCS Current_History <targetTable>
    Examples:
      | sourceTable                                      |  targetTable                                                 |countOfRandomIds     |
      |etl_availability_extended_current_v               |etl_transform_history_extended_availability_part              | 50                  |
      |etl_manifestation_extended_current_v              |etl_transform_history_extended_manifestation_part             |50                   |
      |etl_page_count_extended_current_v                 |etl_transform_history_extended_page_count_part                |50                   |
      |etl_url_extended_current_v                        |etl_transform_history_extended_url_part                       |50                   |
      |etl_work_extended_current_v                       |etl_transform_history_extended_work_part                      |50                   |
      |etl_work_subject_area_extended_current_v          |etl_transform_history_extended_work_subject_area_part         |50                   |
      |etl_manifestation_restrictions_extended_current_v |etl_transform_history_extended_manifestation_restrictions_part|50                   |
      |etl_product_prices_extended_current_v             |etl_transform_history_extended_product_prices_part            |50                   |
      |etl_work_person_role_extended_current_v           |etl_transform_history_extended_work_person_role_part          |50                   |


  @notUsed
  Scenario Outline: Verify Data for BCS Extended delta_current tables are transferred from transform_file tables
    Given Get the total count of BCS Extended transform_file by diff of current and previous timestamp <sourceTable>
    Then We know the total count of BCS Extended delta current <targetTable>
    And Compare count of BCS Ext tranform_file <sourceTable> and delta current <targetTable> are identical
    Given Get the <countOfRandomIds> of BCS Extended data from transform_file Tables <sourceTable>
    When Get the Data from the Difference of Current and Previous timestamp from transform_file Tables <sourceTable>
    Then Get the Data from the BCS Extended delta current <targetTable>
    And Compare the records of BCS Extended delta current and BCS diff of Transform_File <targetTable>

    Examples:
      |targetTable                                            |sourceTable                                                        |countOfRandomIds     |
      |etl_delta_current_extended_availability                |etl_availability_extended_transform_file_history_part              |50                   |
      |etl_delta_current_extended_manifestation               |etl_manifestation_extended_transform_file_history_part             |50                   |
      |etl_delta_current_extended_page_count                  |etl_page_count_extended_transform_file_history_part                |50                   |
      |etl_delta_current_extended_url                         |etl_url_extended_transform_file_history_part                       |50                   |
      |etl_delta_current_extended_work                        |etl_work_extended_transform_file_history_part                      |50                   |
      |etl_delta_current_extended_work_subject_area           |etl_work_subject_area_extended_transform_file_history_part         |50                   |
      |etl_delta_current_extended_manifestation_restrictions  |etl_manifestation_restrictions_extended_transform_file_history_part|50                   |
      |etl_delta_current_extended_product_prices              |etl_product_prices_extended_transform_file_history_part            |50                   |
      |etl_delta_current_extended_work_person_role            |etl_work_person_role_extended_transform_file_history_part          |50                   |


  @notUsed
  Scenario Outline: Verify Data from the difference of BCS Extended Delta_Current and Current_history is transferred to BCS Extended exclude table
    Given Get the BCS Extended Ext total count difference between delta current and transform current history Table <targetTable>
    Then Get the BCS Extended <targetTable> exclude data count
    And Compare BCS Extended exclude count are identical <targetTable>
    Given Get the <countOfRandomIds> from diff of BCS Ext delta_current and current_hist tables <targetTable>
    When Get the BCS Ext records from the diff of delta_current and current_hist tables <targetTable>
    Then Get the BCS Ext records from <targetTable> exclude table
    And  Compare the BCS Ext records of Exclude with diff of delta_current and current_hist tables <targetTable>
    Examples:
      | targetTable                                                             |  countOfRandomIds |
      | etl_transform_history_extended_availability_excl_delta                  | 50                |
      | etl_transform_history_extended_manifestation_excl_delta                 | 50                |
      | etl_transform_history_extended_page_count_excl_delta                    | 50                |
      | etl_transform_history_extended_url_excl_delta                           | 50                |
      | etl_transform_history_extended_work_excl_delta                          | 50                |
      | etl_transform_history_extended_work_subject_area_excl_delta             | 50                |
      | etl_transform_history_extended_manifestation_restrictions_excl_delta    | 50                |
      | etl_transform_history_extended_product_prices_excl_delta                | 50                |
      | etl_transform_history_extended_work_person_role_excl_delta              | 50                |





