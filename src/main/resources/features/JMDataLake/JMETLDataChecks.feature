Feature:Validate data for JM between Transform Tables

  @JMETL
  Scenario Outline: Verify that all JM Transformed Inbound data is transferred to Current tables
    Given We get the <numberOfRecords> random JM ids of <Currenttable>
#    When We get the Promis Inbound records from <CurrenttableQuery>
#    Then We get the Promis Current records from <Currenttable>
#    And Compare Promis records in Inbound and Current of <Currenttable>
    Examples:
      |numberOfRecords  |CurrenttableQuery           |Currenttable               |
#      | 5               |work                        |jmf_work                   |
#      | 5               |manifestation               |jmf_manifestation          |
#      | 5               |work_person_role            |jmf_work_person_role       |
#      | 5               |work_subject_area           |jmf_work_subject_area      |
#      | 5               |work_chronicle              |jmf_work_chronicle         |
#      | 5               |pmg_pubdir_allocation       |jmf_pmg_pubdir_allocation  |
#      | 5               |approval_attachment         |jmf_approval_attachment    |
#      | 5               |review_comment              |jmf_review_comment         |
#      | 5               |approval_request            | jmf_approval_request      |
      | 5               |application_properties      |jmf_application_properties |











