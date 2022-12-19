Feature:Validate data of DL All Core Views where data comes from BCS and JM Core

    #confluence v.92
    #confluenc link: https://elsevier.atlassian.net/wiki/spaces/EPH/pages/45483691971/All+Views+Combining+Data+Sources

  @DLCoreView
  Scenario Outline: Verify Data for DL core views is transferred from BCS and JM core Tables
    Given Get the total count of DL Core views <tableName>
    Then  The total count of BCS And JM Core tables <tableName>
    And Compare count of BCS and JM Core with <tableName> views are identical
    Then Check whether externalReference field not holding any null value <tableName>
    Given Get the <countOfRandomIds> from JM and BCS Core Tables <tableName>
    Then  Get the Records from the JM and BCS Core Tables <tableName>
    And   Get the Records from the DL core views <tableName>
    Then  Compare data of BCS and JM Core with DL Core views <tableName> are identical
    And   Compare count of externalReference field null value count is 0 <tableName>
    Examples:
      | tableName                           |countOfRandomIds   |
      |all_accountable_product_v            |5                 |
      |all_manifestation_identifiers_v      |5                 |
      |all_manifestation_v                  |5                 |
      |all_person_v                         |5                 |
      |all_product_v                        |5                 |
   #  |all_product_rel_package_v            |50                 | not required, this is for analysis purpose created in SIT
      |all_work_identifier_v                |5                 |
      |all_work_person_role_v               |5                 |
      |all_work_relationship_v              |5                 |
      |all_work_subject_areas_v             |5                |
      |all_work_v                           |5                 |
      |all_work_legal_owner_v               |1                 |
      |all_work_access_model_v              |5                 |
      |all_work_business_model_v            |5                 |

    #EPHD-3362
  @DLCoreView
    Scenario: Verify the leadIndicator count in the all core view
      Given Get the leadIndicator count from the sourcesystem table
      Then  Get the count of leadIndicator from the all_manifestation_identifiers_v
      And   Compare the counts to verify their counts are equal

    #new scenario scope allworkperson role shouldn't have duplicate workId if the effective end date is Null or deleteflag=true for both recs.
  # new scenario leadindicator should be only one for one workId
  #new scenario people hubId is null if so familyname and givename concat to find duplicates (do group By)
  #new scenario all_manifestation_identifiers_v check for duplicate issn
  #new scenario all_work_identifier_v check identifier duplicate if effective end date is null




