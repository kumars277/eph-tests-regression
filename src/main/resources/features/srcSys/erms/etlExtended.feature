Feature:Validate data between ERMS ETL Tables

  #Confluence:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45509983346/ERMS+Inbound
  #Git for Query: https://github.com/elsevier-bts/eph-datalabs-dag/tree/master_v1/src/dag/resources/property_substituted/erms_inbound

  @ERMS
  Scenario Outline: Check between ERMS Inbound and transform current tables
    Given Get the total count of ERMS Data from Inbound Load <tableName>
    Then  We know the total count of Current ERMS ETL data <tableName>
    And Compare count of ERMS Inbound load with current ERMS ETL table are identical <tableName>
    Given Get the <countOfRandomIds> random EPR ids from the table ERMS inbound <tableName>
    When Get the data from the ERMS inbound tables <tableName>
    Then Get the data from the ERMS transform current tables <tableName>
    And  we compare the records of ERMS Inbound and ERMS current tables <tableName>
    Examples:
      | tableName                                                                    |countOfRandomIds |
      |erms_transform_current_work_identifier                                        |10               |
      |erms_transform_current_work_person_role                                       |20                |

  @ERMS
  Scenario Outline: Check between ERMS Current and transform file tables
    Given We know the total count of Current ERMS ETL data <SrctableName>
    Then  We know the total count of erms transform file <trgtTable>
    And Compare count of ERMS current and the ERMS transform file table are identical <SrctableName><trgtTable>
    Given Get the <countOfRandomIds> random EPR ids from the current table <SrctableName>
    When  Get the data from the ERMS transform current tables <SrctableName>
    Then  Get the data from the ERMS transform files tables <trgtTable>
    And   we compare the records of ERMS Current and ERMS tranform file tables <SrctableName> and <trgtTable>
    Examples:
      | SrctableName                            |trgtTable                                        |countOfRandomIds |
      |erms_transform_current_work_identifier   |erms_transform_file_history_work_identifier_part | 50               |
      |erms_transform_current_work_person_role  |erms_transform_file_history_work_person_role_part|50                |

  @ERMS
  Scenario Outline: Check between ERMS Current and transform partition history tables
    Given We know the total count of Current ERMS ETL data <SrctableName>
    Then  We know the total count of erms transform partition history <trgtTable>
    And Compare count of ERMS current and the ERMS transform partition history table are identical <SrctableName><trgtTable>
    Given Get the <countOfRandomIds> random EPR ids from the current table <SrctableName>
    When  Get the data from the ERMS transform current tables <SrctableName>
    Then  Get the data from the ERMS transform history partition tables <trgtTable>
    And   we compare the records of ERMS Current and ERMS tranform history partition tables <SrctableName> and <trgtTable>
    Examples:
      | SrctableName                            |trgtTable                                        |countOfRandomIds |
      |erms_transform_current_work_identifier   |erms_transform_history_work_identifier_part      | 50               |
      |erms_transform_current_work_person_role  |erms_transform_history_work_person_role_part     |50                |

  @ERMS
    Scenario Outline: Check ERMS latest tables data transffered from delta current and exclude tables
    Given We know the total count of latest ERMS ETL data <SrctableName>
    Then  We know the total count of erms delta current and exclude tables <SrctableName>
    And Compare count of ERMS latest with the delta current and exclude tables are identical <SrctableName>
    Given Get the <countOfRandomIds> random EPR ids from the latest table <SrctableName>
    When  Get the data from the ERMS erms delta current and exclude tables <SrctableName>
    Then  Get the data from the ERMS transform latest tables <SrctableName>
     And   we compare the records for ERMS latest tables <SrctableName>
    Examples:
      | SrctableName                           |countOfRandomIds |
     |erms_transform_latest_work_identifier   | 50               |
      |erms_transform_latest_work_person_role  |50                |

    @ERMS
    Scenario Outline: Verify Duplicate Entry for ERMS COre in transform latest tables
      Given Get the ERMS Duplicate count in latest tables <SrctableName>
      Then  Check the ERMS latest count should be equal to Zero <SrctableName>
      Examples:
        | SrctableName                           |
        |erms_transform_latest_work_identifier   |
        |erms_transform_latest_work_person_role  |
