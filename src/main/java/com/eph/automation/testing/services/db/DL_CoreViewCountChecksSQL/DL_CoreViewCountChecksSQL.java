package com.eph.automation.testing.services.db.DL_CoreViewCountChecksSQL;


import com.eph.automation.testing.services.db.BCS_ETLCoreSQL.GetBCS_ETLCoreDLDBUser;

public class DL_CoreViewCountChecksSQL {


    public static String GET_DL_CORE_ALL_ACC_PROD_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_accountable_product_v";

    public static String GET_DL_CORE_ALL_MANIF_IDENT_VIEW_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_manifestation_identifiers_v";

    public static String GET_DL_CORE_ALL_MANIF_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_manifestation_v";

     public static String GET_DL_CORE_ALL_PERSON_VIEW_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_person_v";

    public static String GET_DL_CORE_ALL_PRODUCT_VIEW_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_product_v";


    public static String GET_DL_CORE_ALL_WRK_IDENT_VIEW_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_work_identifier_v";

    public static String GET_DL_CORE_ALL_WRK_RELT_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_work_relationship_v";


    public static String GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_work_person_role_v";


    public static String GET_DL_CORE_ALL_WORK_SUB_AREA_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_work_subject_areas_v";

    public static String GET_DL_CORE_ALL_WORK_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".all_work_v";

    public static String GET_BCS_JM_CORE_ACC_PROD_COUNT =
            "WITH source as (\n" +
                    "SELECT\n" +
                    "  u_key external_reference \n" +
                    ", sourceref work_reference \n" +
                    ", accountableproduct gl_product_segment_code \n" +
                    ", accountablename gl_product_segment_name \n" +
                    ", accountableparent f_gl_product_segment_parent \n" +
                    ", dq_err \n" +
                    ", last_updated_date last_updated_date \n" +
                    ", sourceref work_source_reference \n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_latest_v\n" +
                    "UNION ALL SELECT \n" +
                    "  jm_source_reference external_reference \n" +
                    ", acc_prod_id work_reference \n" +
                    ", acc_prod_id gl_product_segment_code \n" +
                    ", work_title gl_product_segment_name \n" +
                    ", hfm_hierarchy_position_code f_gl_product_segment_parent \n" +
                    ", dq_err \n" +
                    ", notified_date last_updated_date \n" +
                    ", acc_prod_id work_source_reference \n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".etl_accountable_product_dq)\n" +
                    "select count(*) as Source_Count from source\n" +
                    "where external_reference in (select external_reference from source group by external_reference having count(1) < 2) \n";

    public static String GET_BCS_JM_CORE_MANIF_COUNT =
            "WITH source as (\n" +
                    "SELECT\n" +
                    "  sourceref external_reference\n" +
                    ", CAST(Null as varchar) eph_manifestation_id\n" +
                    ", title manifestation_key_title\n" +
                    ", intereditionflag inter_edition_flag\n" +
                    ", firstpublisheddate first_published_date\n" +
                    ", last_pub_date last_pub_date\n" +
                    ", coalesce(manifestation_type,'UNK') f_type\n" +
                    ", coalesce(status,'UNK') f_status\n" +
                    ", CAST(null AS varchar) format_type\n" +
                    ", work_id work_source_ref\n" +
                    ", CAST(Null as varchar) eph_work_id\n" +
                    ", dq_err dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", CAST(Null as varchar) update_type\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_latest_v\n" +
                    "UNION ALL SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", eph_manifestation_id eph_manifestation_id\n" +
                    ", manifestaton_key_title manifestation_key_title\n" +
                    ", null inter_edition_flag\n" +
                    ", (CASE WHEN (f_type = 'JEL') THEN online_launch_date ELSE CAST(effective_start_date AS date) END) first_published_date\n" +
                    ", null last_pub_date\n" +
                    ", coalesce(f_type,'UNK') f_type\n" +
                    ", coalesce(f_status,'UNK') f_status\n" +
                    ", CAST(null AS varchar) format_type\n" +
                    ", split_part(jm_source_reference, '-', 2) work_source_ref\n" +
                    ", eph_work_id eph_work_id\n" +
                    ", dq_err dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", upsert update_type\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".etl_manifestation_dq)\n" +
                    "select count(*) as Source_Count from source\n" +
                    "where external_reference in (select external_reference from source group by external_reference having count(1) < 2)\n";

    public static String GET_BCS_JM_CORE_MANIF_IDENT_COUNT =
            "WITH source as (\n" +
                    "SELECT\n" +
                    "  concat(jm_source_reference, '-', identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", effective_start_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", f_type f_type\n" +
                    ", eph_manifestation_id f_manifestation\n" +
                    ", manifestation_source_reference manifestation_source_reference\n" +
                    ", notified_date last_updated_date\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".etl_manifestation_identifier_dq\n" +
                    " UNION ALL SELECT\n" +
                    "  concat(sourceref, '-', identifier_type, '-', identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", identifier_type f_type\n" +
                    ", null f_manifestation\n" +
                    ", sourceref manifestation_source_reference\n" +
                    ", last_updated_date last_updated_date\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest_v)\n" +
                    "select count(*) as Source_Count from source\n" +
                    "where external_reference in (select external_reference from source group by external_reference having count(1) < 2) \n";

        public static String GET_BCS_JM_CORE_PERSON_COUNT =
            "WITH\n" +
                    "  source AS (\n" +
                    "   SELECT\n" +
                    "     CAST(null AS integer) person_id\n" +
                    "   , CAST(sourceref AS varchar) external_reference\n" +
                    "   , firstname given_name\n" +
                    "   , familyname family_name\n" +
                    "   , peoplehub_id peoplehub_id\n" +
                    "   , email_address email\n" +
                    "   , dq_err dq_err\n" +
                    "   , last_updated_date last_updated_date\n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_latest_v\n" +
                    "UNION ALL    SELECT\n" +
                    "     CAST(null AS integer) person_id\n" +
                    "   , CAST(emplid AS varchar) external_reference\n" +
                    "   , first_name given_name\n" +
                    "   , last_name family_name\n" +
                    "   , emplid peoplehub_id\n" +
                    "   , business_email email\n" +
                    "   , 'N' dq_err\n" +
                    "   , CAST(current_timestamp AS timestamp) last_updated_date\n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".workday_data_orgdata\n" +
                    ") \n" +
                    "SELECT count(*) as Source_Count\n" +
                    "FROM\n" +
                    "  source\n" +
                    "WHERE (external_reference IN (SELECT external_reference\n" +
                    "FROM\n" +
                    "  source\n" +
                    "GROUP BY external_reference\n" +
                    "HAVING (count(1) < 2)\n" +
                    "))\n";

    public static String GET_BCS_JM_CORE_PRODUCT_COUNT =
            "WITH source as (\n" +
                    "SELECT\n" +
                    "  eph_product_id product_id\n" +
                    ", jm_source_reference external_reference\n" +
                    ", name name\n" +
                    ", null short_name\n" +
                    ", separately_saleable_ind separately_sale_indicator\n" +
                    ", trial_allowed_ind trial_allowed_indicator\n" +
                    ", CAST(null AS boolean) restricted_sale_indicator\n" +
                    ", launch_date launch_date\n" +
                    ", CAST(null AS date) content_from_date\n" +
                    ", CAST(null AS date) content_to_date\n" +
                    ", CAST(null AS integer) content_date_offset\n" +
                    ", coalesce(f_type,'UNK') f_type\n" +
                    ", coalesce(f_status,'UNK') f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", tax_code f_tax_code\n" +
                    ", f_revenue_model f_revenue_model\n" +
                    ", CAST(null AS varchar) f_revenue_account\n" +
                    ", eph_work_id f_wwork\n" +
                    ", null work_reference\n" +
                    ", eph_manifestation_id f_manifestation\n" +
                    ", null manifestation_reference\n" +
                    ", notified_date last_updated_date\n" +
                    ", 'N' dq_err\n" +
                    ", upsert update_type\n" +
                    ", 'JOURNAL' work_roll_up_type\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".etl_product_dq\n" +
                    "UNION ALL SELECT\n" +
                    "  null product_id\n" +
                    ", concat(u_key, '-', product_type) external_reference\n" +
                    ", name name \n" +
                    ", shorttitle short_name\n" +
                    ", separately_sale_indicator separately_sale_indicator\n" +
                    ", trial_allowed_indicator trial_allowed_indicator\n" +
                    ", restricted_sale_indicator restricted_sale_indicator\n" +
                    ", launchdate launch_date\n" +
                    ", CAST(null AS date) content_from_date\n" +
                    ", CAST(null AS date) content_to_date\n" +
                    ", CAST(null AS integer) content_date_offset\n" +
                    ", coalesce(product_type,'UNK') f_type\n" +
                    ", coalesce(status,'UNK') f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", taxcode f_tax_code\n" +
                    ", f_revenue_model f_revenue_model\n" +
                    ", CAST(null AS varchar) f_revenue_account\n" +
                    ", null f_wwork\n" +
                    ", worksource work_reference \n" +
                    ", null f_manifestation\n" +
                    ", manifestationref manifestation_reference\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", 'N' dq_err\n" +
                    ", CAST(Null as varchar) update_type\n" +
                    ", 'BOOK' work_roll_up_type\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_latest_v)\n" +
                    "select count(*) as Source_Count from source\n" +
                    "where external_reference in (select external_reference from source group by external_reference having count(1) < 2) \n";

    public static String GET_BCS_JM_CORE_WORK_IDENT_COUNT =
            "WITH source as (\n" +
                    "SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", identifier identifier\n" +
                    ", f_type f_type\n" +
                    ", eph_work_id f_wwork\n" +
                    ", split_part(jm_source_reference, '-', 1) work_source_ref\n" +
                    ", effective_start_date effective_start_date\n" +
                    ", CAST(Null as date) effective_end_date\n" +
                    ", notified_date last_updated_date\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".etl_work_identifier_dq\n" +
                    "UNION ALL SELECT\n" +
                    "  concat(sourceref, identifier_type, identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", identifier_type f_type\n" +
                    ", null f_wwork\n" +
                    ", sourceref work_source_ref\n" +
                    ", current_date effective_start_date\n" +
                    ", CAST(Null as date) effective_end_date\n" +
                    ", last_updated_date last_updated_date\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_latest)\n" +
                    "select count(*) as Source_Count from source\n" +
                    "where external_reference in (select external_reference from source group by external_reference having count(1) < 2)\n";


    public static String GET_BCS_JM_CORE_WORK_PERS_ROLE_COUNT =
            "WITH source as (\n" +
                    "SELECT\n" +
                    "  u_key external_reference\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(null AS timestamp) effective_end_date\n" +
                    ", roletype f_role\n" +
                    ", CAST(null AS varchar) f_wwork\n" +
                    ", worksourceref work_source_reference\n" +
                    ", personsourceref person_source_reference\n" +
                    ", CAST(null AS varchar) person_email\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_latest_v\n" +
                    "UNION ALL SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", start_date effective_start_date\n" +
                    ", end_date effective_end_date\n" +
                    ", f_role f_role\n" +
                    ", eph_work_id f_wwork\n" +
                    ", split_part(jm_source_reference, '-', 1) work_source_reference\n" +
                    ", employee_number person_source_reference\n" +
                    ", internal_email person_email\n" +
                    ", dq_err dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".etl_work_person_role_dq)\n" +
                    "select count(*) as Source_Count from source\n" +
                    "where external_reference in (select external_reference from source group by external_reference having count(1) < 2)\n";

    public static String GET_BCS_JM_CORE_WORK_RELATION_COUNT =
            "WITH source as (\n" +
                    "SELECT\n" +
                    "  u_key external_reference\n" +
                    ", parentref parent_work_source_ref\n" +
                    ", childref child_work_source_ref\n" +
                    ", relationtyperef f_relationship_type\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(Null as date) effective_end_date\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_latest_v)\n" +
                    " select count(*) as Source_Count from source\n" +
                    " where external_reference in (select external_reference from source group by external_reference having count(1) < 2)\n";


    public static String GET_BCS_JM_CORE_WORK_SUBJ_AREA_COUNT =
            "WITH source as (\n" +
                    "SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", start_date effective_start_date\n" +
                    ", end_date effective_end_date\n" +
                    ", f_subject_area f_subject_area\n" +
                    ", eph_work_id f_wwork\n" +
                    ", split_part(jm_source_reference, '-', 1) work_source_reference\n" +
                    ", subject_area_code subject_area_reference\n" +
                    ", dq_err dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".etl_work_subject_area_dq\n" +
                    "WHERE subject_area_type = 'SD')\n" +
                    "select count(*) as Source_Count from source\n" +
                    "where external_reference in (select external_reference from source group by external_reference having count(1) < 2)\n";

    public static String GET_BCS_JM_CORE_WORK_COUNT =
            "WITH\n" +
                    "  source AS (\n" +
                    "   SELECT\n" +
                    "     eph_work_id epr\n" +
                    "   , jm_source_reference external_reference\n" +
                    "   , work_title work_title\n" +
                    "   , work_subtitle work_subtitle\n" +
                    "   , electro_rights_indicator electro_rights_indicator\n" +
                    "   , volume volume\n" +
                    "   , CAST(copyright_year AS integer) copyright_year\n" +
                    "   , edition_number edition_number\n" +
                    "   , CAST(f_pmc AS varchar) f_pmc\n" +
                    "   , f_oa_type f_oa_journal_type\n" +
                    "   , COALESCE(f_type, 'UNK') f_type\n" +
                    "   , COALESCE(f_status, 'UNK') f_status\n" +
                    "   , f_imprint f_imprint\n" +
                    "   , o.ephcode f_society_ownership\n" +
                    "   , resp_centre resp_centre\n" +
                    "   , opco opco\n" +
                    "   , language_code language_code\n" +
                    "   , subscription_type subscription_type\n" +
                    "   , planned_launch_date planned_launch_date\n" +
                    "   , CAST(null AS date) actual_launch_date\n" +
                    "   , planned_termination_date planned_termination_date\n" +
                    "   , CAST(null AS date) actual_termination_date\n" +
                    "   , dq_err dq_err\n" +
                    "   , notified_date last_updated_date\n" +
                    "   , upsert update_type\n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".etl_wwork_dq j\n" +
                    "   left join "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".jmownershipmapping o on split_part(j.f_society_ownership,'-',2) = o.jmcode\n" +
                    "UNION ALL SELECT\n" +
                    "  CAST(null AS varchar) epr\n" +
                    ", sourceref external_reference\n" +
                    ", title work_title\n" +
                    ", subtitle work_subtitle\n" +
                    ", electro_rights_indicator electro_rights_indicator\n" +
                    "-- , CAST(volumeno AS integer) volume\n" +
                    ", 0 volume\n" +
                    ", CAST(copyrightyear AS integer) copyright_year\n" +
                    ", editionno edition_number\n" +
                    ", CAST(pmc AS varchar) f_pmc\n" +
                    ", f_oa_journal_type f_oa_journal_type\n" +
                    ", coalesce(work_type,'UNK') f_type\n" +
                    ", coalesce(statuscode,'UNK') f_status\n" +
                    ", imprintcode f_imprint\n" +
                    ", f_society_ownership f_society_ownership\n" +
                    ", resp_centre resp_centre\n" +
                    ", opco opco\n" +
                    ", languagecode language_code\n" +
                    ", subscription_type subscription_type\n" +
                    ", planned_pubdate planned_launch_date\n" +
                    ", actual_pubdate actual_launch_date\n" +
                    ", CAST(Null as date) planned_termination_date\n" +
                    ", CAST(Null as date) actual_termination_date\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", CAST(Null as varchar) update_type\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_latest_v)\n" +
                    "select count(*) as Source_Count from source\n" +
                    "where external_reference in (select external_reference from source group by external_reference having count(1) < 2)\n";


}




