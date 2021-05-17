Feature:Validate data for JM tables between JM ETL Core tables and JM Staging

#  Created by Ranjithkumar S on 11/12/2020
#    Updated by Thomas Kruck on 21/04/2021

    @JMETLCore
    Scenario Outline: Verify that all JM data is transferred from Staging to JM ETL Core tables
        Given We know the total count of JM ETL Core <tableName> data
        When Get the total count of JM <tableName> data is in the JM Staging
        Then Compare count of the JM count for <tableName> table between JM ETL Core and DL are identical

        Examples:

            | tableName                           |
#            | etl_accountable_product_dq_v        |
#            | etl_wwork_dq_v                      |
#            | etl_work_identifier_dq_v            |
#            | etl_work_subject_area_dq_v          |
#            | etl_work_person_role_dq_v           |
#            | etl_manifestation_updates1_v        |
            | etl_manifestation_dq_v              |
            | etl_manifestation_identifier_dq_v   |
            | etl_product_part1_v                 |
            | etl_product_inserts_v               |
            | etl_product_updates_v               |
#            | etl_product_dq_v                    |
#            | etl_product_person_role_dq_v        |
#            | jm_imprint_data_v                   |
#            | sd_subject_areas_v                  |
#            | works_attrs_roles_people_v          |