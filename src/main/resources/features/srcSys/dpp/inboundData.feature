Feature:Validate data for ck Inbound tables

#confluence link:https://elsevier.atlassian.net/wiki/spaces/EPH/pages/90784826471/CK+Inbound

  @CKINBOUND
  Scenario Outline: Verify that all CK data is transferred between inbound Source and current tables
    Given We know the number of CK <InboundSourcetablename> data in inbound Source
    Then Get the count for CK <Currenttablename> current
    And Compare the CK count for <InboundSourcetablename> table between inbound Source and current
    Given We get the <numberOfRecords> random CK ids of <Currenttablename>
    When We get the CK Current records from <Currenttablename>
    Then We get the CK Inbound Source records from <InboundSourcetablename>
    And Compare CK records in Inbound Source and Current of <InboundSourcetablename>

    Examples:
      | numberOfRecords | InboundSourcetablename            | Currenttablename                       |
      | 5               | ck_subject_area_transform_v       | ck_transform_current_subject_area      |
      | 5               | ck_work_transform_v               | ck_transform_current_work              |
      | 5               | ck_package_work_transform_v       | ck_transform_current_package_work      |
      | 5               | ck_work_subject_area_transform_v  | ck_transform_current_work_subject_area |

    #tables missing refer AirflowDAG