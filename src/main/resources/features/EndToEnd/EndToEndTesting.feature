Feature:Validate EndToEnd Testing

#  Created by Dinesh S on 05/05/2021


    #Phase-1 Scenarios
   #To check the Data Flowing from BCS Source System(parquet file) to BCS-Initial Ingest Tables
   #DAG: eph_bcs_inbound_dag
   #Confluence: https://confluence.cbsels.com/pages/viewpage.action?spaceKey=EPH&title=BCS+Inbound

  #Count & Data Checks make sure All the Data is transffered from Parquet File to Initial Ingest based on the Business Logic
    Scenario Outline: Verify Data Count for BCS stg_Current_tables transferred from Source Table initial_ingest
    Given Get the total count of BCS Data from initial_ingest <tableName>
    Then  Get total count of BCS Current table <tableName>
    And Compare count of initial ingest with current table <tableName>
    Examples:
      | tableName                    |
      | stg_current_classification   |
      | stg_current_content          |
      | stg_current_extobject        |
      | stg_current_fullversionfamily|
      | stg_current_originatoraddress|
      | stg_current_originators      |
      | stg_current_pricing          |
      | stg_current_product          |
      | stg_current_production       |
      | stg_current_relations        |
      | stg_current_responsibilities |
      | stg_current_sublocation      |
      | stg_current_text             |
      | stg_current_versionfamily    |

    Scenario Outline: Verify Data for BCS is transferred from initial ingest
    Given We get the <countOfRandomIds> random ids from initial ingest <targetTable>
    When Get the data records from initial ingest for <targetTable>
    Then Get the records from current tables <targetTable>
    And Compare the records of initial ingest and current table <targetTable>
    Examples:
      | targetTable                  | countOfRandomIds|
      |stg_current_classification   |       1         |
      |stg_current_content          |       2         |
      |stg_current_extobject        |       1         |
      |stg_current_fullversionfamily|       1         |
      |stg_current_originatoraddress|       1         |
      |stg_current_originators      |       1         |
      |stg_current_pricing          |       1         |
      |stg_current_product          |       1         |
      |stg_current_production       |       1         |
      |stg_current_relations        |       1         |
      |stg_current_responsibilities |       1         |
      |stg_current_sublocation      |       1         |
      |stg_current_text             |       1         |
      |stg_current_versionfamily    |       1         |

      #Data Quality Check for the Phase-1 through Data Driven Approach
  Scenario Outline: Verify Data Quality for BCS initial ingest
    Given We read the Expected Data from the CSV File from each <table>
    Then  We get the Actual Data from the Initial Ingest Table
    And  Compare the Expected and Actual Data
    Examples:
      | table                        |
      | stg_current_classification   |
      | stg_current_content          |
      | stg_current_extobject        |
      | stg_current_fullversionfamily|
      | stg_current_originatoraddress|
      | stg_current_originators      |
      | stg_current_pricing          |
      | stg_current_product          |
      | stg_current_production       |
      | stg_current_relations        |
      | stg_current_responsibilities |
      | stg_current_sublocation      |
      | stg_current_text             |
      | stg_current_versionfamily    |

    #Phase-2 Scenarios
    #To check the Data Flowing from BCS-Initial Ingest Tables to BCS ETL Extended Tables
   #Confluence: https://confluence.cbsels.com/display/EPH/BCS+Extended+Transformed+View+Mappings
   #DAG: eph_bcs_etl_extended_dag

  #Below Count and Data confirms whether the data is Flowing into ETL Phase
  Scenario Outline: Verify Data Count for BCS Extended tables is transferred from Inbound Tables
    Given Get the total count of BCS Extended from Current Tables <tableName>
    When We know the total count of BCS Extended Inbound tables <tableName>
    Then Compare count of BCS Inbound and BCS Extended <tableName> tables are identical
    Examples:
      | tableName                                           |
      | etl_availability_extended_current_v                 |
      | etl_manifestation_extended_current_v                |
      | etl_page_count_extended_current_v                   |
      | etl_url_extended_current_v                          |
      | etl_work_extended_current_v                         |
      | etl_work_subject_area_extended_current_v            |
      | etl_manifestation_restrictions_extended_current_v   |
      | etl_product_prices_extended_current_v               |
      | etl_work_person_role_extended_current_v             |

  Scenario Outline: Verify Data count for BCS Extended delta_latest tables are transferred from delta_current and Current_Exclude tables
    Given Get the sum of total count between BCS Extended delta current and Current_Exclude Table <TargetTable>
    Then Get the BCS Extended <TargetTable> latest data count
    And Compare BCS Extended latest counts of <FirstSourceTable> and <SecondSourceTable> with <TargetTable> are identical
    Examples:
      |FirstSourceTable                                       |SecondSourceTable                                                          |TargetTable                                                      |
      |etl_delta_current_extended_availability                |etl_transform_history_extended_availability_excl_delta                     |etl_transform_history_extended_availability_latest               |
      |etl_delta_current_extended_manifestation               |etl_transform_history_extended_manifestation_excl_delta                    |etl_transform_history_extended_manifestation_latest              |
      |etl_delta_current_extended_page_count                  |etl_transform_history_extended_page_count_excl_delta                       |etl_transform_history_extended_page_count_latest                 |
      |etl_delta_current_extended_url                         |etl_transform_history_extended_url_excl_delta                              |etl_transform_history_extended_url_latest                        |
      |etl_delta_current_extended_work                        | etl_transform_history_extended_work_excl_delta                            |etl_transform_history_extended_work_latest                       |
      |etl_delta_current_extended_work_subject_area           |etl_transform_history_extended_work_subject_area_excl_delta                |etl_transform_history_extended_work_subject_area_latest          |
      |etl_delta_current_extended_manifestation_restrictions  |etl_transform_history_extended_manifestation_restrictions_excl_delta       |etl_transform_history_extended_manifestation_restrictions_latest |
      |etl_delta_current_extended_product_prices              |etl_transform_history_extended_product_prices_excl_delta                   |etl_transform_history_extended_product_prices_latest             |
      |etl_delta_current_extended_work_person_role            |etl_transform_history_extended_work_person_role_excl_delta                 |etl_transform_history_extended_work_person_role_latest           |


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



  Scenario Outline: Verify Data for BCS_ETL Extended tables is transferred from Inbound Tables
    Given Get the <countOfRandomIds> of BCS Extende data from Inbound Tables <tableName>
    Then  Get the Data from the BCS Extended Inbound Tables <tableName>
    And   Data from the BCS Extended Current Tables <tableName>
    Then  Compare data of BCS Inbound and BCS Extended <tableName> tables are identical
    Examples:
      | tableName                                               |countOfRandomIds |
      | etl_availability_extended_current_v                     |50            |
      | etl_manifestation_extended_current_v                    |50             |
      | etl_page_count_extended_current_v                       |50             |
      | etl_url_extended_current_v                              |50             |
      | etl_work_extended_current_v                             |50             |
      | etl_work_subject_area_extended_current_v                |50             |
      | etl_manifestation_restrictions_extended_current_v       |50             |
      | etl_product_prices_extended_current_v                   |50             |
      | etl_work_person_role_extended_current_v                 |50             |

  Scenario Outline: Verify Data from the sum of BCS Extended Delta_Current and Exclude is transferred to BCS Extended Latest table
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

    #Below Scenario to check the data quality for phase2, whether the mapping done as per the business logic
  Scenario Outline: Verify Data Quality for BCS ETL phase
    Given We read the Expected Data from the CSV File for table <tableName>
    Then  We get the Actual Data from the ETL current <tableName>
    And   Compare the Expected and Actual Data and veify data equal
    Examples:
      | tableName                                           |
      | etl_availability_extended_current_v                 |
      | etl_manifestation_extended_current_v                |
      | etl_page_count_extended_current_v                   |
      | etl_url_extended_current_v                          |
      | etl_work_extended_current_v                         |
      | etl_work_subject_area_extended_current_v            |
      | etl_manifestation_restrictions_extended_current_v   |
      | etl_product_prices_extended_current_v               |
      | etl_work_person_role_extended_current_v             |

    #Phase-3 Scenarios
    #To check the data flowing from ETL Phase to Extended Tables (Holds extended records from all source System)
    #DAG: eph_extended_product_availability_dag, eph_extended_product_pricing_dag, eph_extended_work_dag,
    #      eph_extended_manifestation_dag
   #Confluence Link: https://confluence.cbsels.com/display/EPH/Source+To+Extended+Topic+Table+Mappings

  Scenario Outline: Verify Data Count for DL extended tables is transferred from All Extended views
    Given Get the total count of DL Extended views <tableName>
    Then  We know the total count of All Extended views <tableName>
    And Compare count of All Ext Views with <tableName> views are identical
    Examples:
      | tableName                                                         |
      |product_extended_availability                                      |
      |product_extended_pricing                                           |
      |manifestation_extended_restriction                                 |
      |manifestation_extended_page_count                                  |
      |manifestation_extended                                             |
      |work_extended                                                      |
      |work_extended_editorial_board                                      |
      |work_extended_metric                                               |
      |work_extended_person_role                                          |
      |work_extended_relationship_sibling                                 |
      |work_extended_subject_area                                         |
      |work_extended_url                                                  |

    #Below Scenario to check the data quality for phase3, whether the mapping done as per the business logic
  Scenario Outline: Verify Data Quality for eXTENDED Tables
    Given We read the Expected Data from the CSV File for table <tableName>
    Then  We get the Actual Data from the Extended tables <tableName>
    And   Compare the Expected and Actual Data and veify data equal for extended tables
    Examples:
      | tableName                                                         |
      |product_extended_availability                                      |
      |product_extended_pricing                                           |
      |manifestation_extended_restriction                                 |
      |manifestation_extended_page_count                                  |
      |manifestation_extended                                             |
      |work_extended                                                      |
      |work_extended_editorial_board                                      |
      |work_extended_metric                                               |
      |work_extended_person_role                                          |
      |work_extended_relationship_sibling                                 |
      |work_extended_subject_area                                         |
      |work_extended_url                                                  |

    #Phase-4 Scenarios
  #To check the data flowing from Extended Tables to Stitching Tables
  Scenario Outline: Verify Data from the Manif_extended tables transferred to Manif Extended Stitching table
    Given We get the <countOfRandomIds> random manifestation Ext EPR ids <tableName>
    And Get the records from Manifestation extended table
    Then Compare Manif Extended and Manif Extended Stitching Table
    Examples:
      |tableName                   |countOfRandomIds|
      |manifestation_extended      |10                 |
  Scenario Outline: Verify Data from the Manif_extended page count tables transferred to Manif Extended Stitching table
    Given We get the <countOfRandomIds> random manifestation Ext page count EPR ids <tableName>
    And Get the records from Manifestation extended page count table
    Then Compare Manif Extended page count and Manif Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |manifestation_extended_page_count               |10             |
  Scenario Outline: Verify Data from the Manif_extended restrictions tables transferred to Manif Extended Stitching table
    Given We get the <countOfRandomIds> random manifestation Ext restrictions EPR ids <tableName>
    And   Get the records from Manifestation extended restrictions table
    Then  Compare Manif Extended restrictions and Manif Extended Stitching Table
    Examples:
      |tableName                                        |countOfRandomIds|
      |manifestation_extended_restriction               |10            |
  Scenario Outline: Verify Data from the Product extended availability tables transferred to Product Extended Stitching JSON
    Given We get the <countOfRandomIds> random Prod Ext Availability EPR ids <tableName>
    And Get the records from Prod extended Availability table
    Then Compare Product Extended Availability and Product Extended Stitching JSON
    Examples:
      |tableName                                   |countOfRandomIds|
      |product_extended_availability               |10                 |
  Scenario Outline: Verify Data from the Product extended Pricing tables transferred to Product Extended Stitching JSON
    Given We get the <countOfRandomIds> random Prod Ext Pricing EPR ids from Pricing Extended Table <tableName>
    And Get the records from Prod extended Pricing table
    Then Compare Product Extended Pricing and Product Extended Stitching JSON
    Examples:
      |tableName                                   |countOfRandomIds|
      |product_extended_pricing                    |10                |
  Scenario Outline: Verify Data from the Work_extended tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from Work extended table
    Then Compare work Extended and work Extended Stitching Table
    Examples:
      |tableName                   |countOfRandomIds|
      |work_extended               |10                 |
  Scenario Outline: Verify Data from the work_extended metric tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended metric table
    Then Compare work Extended metric and work Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |work_extended_metric                           |10             |
  Scenario Outline: Verify Data from the work_extended url tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended url table
    Then Compare work Extended url and work Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |work_extended_url                           |10             |
  Scenario Outline: Verify Data from the work_extended_subj_area tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended subj area table
    Then Compare work Extended subj area and work Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |work_extended_subject_area                           |10            |
  Scenario Outline: Verify Data from the work_extended_person_role tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended person role table
    Then Compare work Extended person role and work Extended Stitching Table
    Examples:
      |tableName                                       |countOfRandomIds|
      |work_extended_person_role                           |10           |
  Scenario Outline: Verify Data from the work_extended_relationship tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended relationship sibling table
    Then Compare work Extended Relationships and work Extended Stitching Table
    Examples:
      |tableName                                                    |countOfRandomIds|
      |work_extended_relationship_sibling                           |10           |
  Scenario Outline: Verify Data from the work_extended_editorial_board tables transferred to Work Extended Stitching table
    Given We get the <countOfRandomIds> random work EPR ids <tableName>
    And Get the records from work extended editorial board table
    Then Compare work Extended editorial and work Extended Stitching Table
    Examples:
      |tableName                                                    |countOfRandomIds|
      |work_extended_editorial_board                                | 1      |




