Feature:Validate data count for BCS ETL Core in Data Lake Access Layer

#  Created by Dinesh on 30/09/2020



  @DLCore
  Scenario Outline: Verify Data Count for BCS_ETL Core tables is transferred from Inbound Tables
    Given Get the total count of DL Core views <tableName>
    Then  We know the total count of BCS And JM Core tables <tableName>
    And Compare count of BCS and JM Core with <tableName> views are identical
    Examples:
      | tableName                                |
      |all_accountable_product_v         |
      |all_manifestation_identifiers_v               |
      |all_manifestation_v                      |
    |all_person_v                     |
      |all_product_v            |
      |all_work_identifier_v           |
      |all_work_person_role_v                        |
      |all_work_relationship_v             |
      |all_work_subject_areas_v    |
      |all_work_v               |

