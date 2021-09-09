package com.eph.automation.testing.services.db.DL_CoreViewSQL;


import com.eph.automation.testing.services.db.bcsetlcoresql.GetBcsEtlCoreDLDBUser;

public class DL_CoreViewChecksSQL {


    public static String GET_DL_CORE_ALL_ACC_PROD_VIEW_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_accountable_product_v";

    public static String GET_DL_CORE_ALL_MANIF_IDENT_VIEW_COUNT =
               "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_manifestation_identifiers_v";

    public static String GET_LEAD_INDICATOR_ALL_CORE_VIEW_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_manifestation_identifiers_v where lead_indicator=true";

    public static String GET_DL_CORE_ALL_MANIF_VIEW_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_manifestation_v";

     public static String GET_DL_CORE_ALL_PERSON_VIEW_COUNT =
               "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_person_v";

    public static String GET_DL_CORE_ALL_PRODUCT_VIEW_COUNT =
               "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_product_v";

    public static String GET_DL_CORE_ALL_PRODUCT_REL_PKG_VIEW_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_product_rel_package_v";

    public static String GET_DL_CORE_ALL_WRK_IDENT_VIEW_COUNT =
               "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_identifier_v";

    public static String GET_DL_CORE_ALL_WRK_RELT_VIEW_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_relationship_v";


    public static String GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_person_role_v";

    public static String GET_DL_CORE_ALL_WORK_SUB_AREA_VIEW_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_subject_areas_v";

    public static String GET_DL_CORE_ALL_WORK_VIEW_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_v";

    public static String GET_DL_CORE_ALL_WORK_LEGAL_OWNER_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_legal_owner_v";

    public static String GET_DL_CORE_ALL_WORK_ACCESS_MODEL_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_access_model_v";

    public static String GET_DL_CORE_ALL_WORK_BUSINESS_MODEL_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_business_model_v";


    public static String GET_BCS_JM_CORE_ACC_PROD_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  concat(accountableproduct, accountableparent) external_reference\n" +
                    ", sourceref work_reference\n" +
                    ", accountableproduct gl_product_segment_code\n" +
                    ", accountablename gl_product_segment_name\n" +
                    ", accountableparent f_gl_product_segment_parent\n" +
                    ", dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", sourceref work_source_reference\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_latest_v\n" +
                    " UNION ALL SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", acc_prod_id work_reference\n" +
                    ", acc_prod_id gl_product_segment_code\n" +
                    ", work_title gl_product_segment_name\n" +
                    ", hfm_hierarchy_position_code f_gl_product_segment_parent\n" +
                    ", dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", acc_prod_id work_source_reference\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_accountable_product_dq)\n";

    public static String GET_BCS_JM_CORE_ACC_PROD_RAND_ID =
            "select work_reference as id from(\n" +
                    "SELECT\n" +
                    "  concat(accountableproduct, accountableparent) external_reference\n" +
                    ", sourceref work_reference\n" +
                    ", accountableproduct gl_product_segment_code\n" +
                    ", accountablename gl_product_segment_name\n" +
                    ", accountableparent f_gl_product_segment_parent\n" +
                    ", dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", sourceref work_source_reference\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_latest_v\n" +
                    " UNION ALL SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", acc_prod_id work_reference\n" +
                    ", acc_prod_id gl_product_segment_code\n" +
                    ", work_title gl_product_segment_name\n" +
                    ", hfm_hierarchy_position_code f_gl_product_segment_parent\n" +
                    ", dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", acc_prod_id work_source_reference\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_accountable_product_dq) order by rand() limit %s \n";

    public static String GET_BCS_JM_CORE_ACC_PROD_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",work_reference as WORKREFERENCE" +
                    ",gl_product_segment_code as GL_PRODUCT_SEGMENT_CODE" +
                    ",gl_product_segment_name as GL_PRODUCT_SEGMENT_NAME" +
                    ",f_gl_product_segment_parent as GL_PRODUCT_SEGMENT_PARENT" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from(" +
                    "SELECT\n" +
                    "  concat(accountableproduct, accountableparent) external_reference\n" +
                    ", sourceref work_reference\n" +
                    ", accountableproduct gl_product_segment_code\n" +
                    ", accountablename gl_product_segment_name\n" +
                    ", accountableparent f_gl_product_segment_parent\n" +
                    ", dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", sourceref work_source_reference\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'BCS' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_latest_v\n" +
                    " UNION ALL SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", acc_prod_id work_reference\n" +
                    ", acc_prod_id gl_product_segment_code\n" +
                    ", work_title gl_product_segment_name\n" +
                    ", hfm_hierarchy_position_code f_gl_product_segment_parent\n" +
                    ", dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", acc_prod_id work_source_reference\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_accountable_product_dq) where work_reference in ('%s')\n" +
                    "order by external_reference,work_reference,last_updated_date,work_source_reference,delete_flag desc \n";

    public static String GET_DL_CORE_ALL_ACC_PROD_VIEW_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",work_reference as WORKREFERENCE" +
                    ",gl_product_segment_code as GL_PRODUCT_SEGMENT_CODE" +
                    ",gl_product_segment_name as GL_PRODUCT_SEGMENT_NAME" +
                    ",f_gl_product_segment_parent as GL_PRODUCT_SEGMENT_PARENT" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_accountable_product_v" +
                    " where work_reference in ('%s') \n" +
                    "order by external_reference,work_reference,last_updated_date,work_source_reference,delete_flag desc \n";

    public static String GET_BCS_JM_CORE_MANIF_COUNT =
          "select count(*) as Source_Count from(\n" +
                  "SELECT\n" +
                  "  sourceref external_reference\n" +
                  ", CAST(null AS varchar) eph_manifestation_id\n" +
                  ", title manifestation_key_title\n" +
                  ", intereditionflag inter_edition_flag\n" +
                  ", firstpublisheddate first_published_date\n" +
                  ", last_pub_date last_pub_date\n" +
                  ", COALESCE(manifestation_type, 'UNK') f_type\n" +
                  ", COALESCE(status, 'UNK') f_status\n" +
                  ", CAST(null AS varchar) format_type\n" +
                  ", work_id work_source_ref\n" +
                  ", CAST(null AS varchar) eph_work_id\n" +
                  ", dq_err dq_err\n" +
                  ", last_updated_date last_updated_date\n" +
                  ", CAST(null AS varchar) update_type\n" +
                  ", delete_flag delete_flag\n" +
                  ", 'bcs' source_system\n" +
                  ", CAST(null AS varchar) scenario_code\n" +
                  ", CAST(null AS varchar) scenario_name\n" +
                  "FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_latest_v\n" +
                  "UNION ALL SELECT\n" +
                  "  m.jm_source_reference external_reference\n" +
                  ", m.eph_manifestation_id eph_manifestation_id\n" +
                  ", m.manifestaton_key_title manifestation_key_title\n" +
                  ", null inter_edition_flag\n" +
                  ", (CASE WHEN (m.f_type = 'JEL') THEN m.online_launch_date ELSE CAST(m.effective_start_date AS date) END) first_published_date\n" +
                  ", null last_pub_date\n" +
                  ", COALESCE(m.f_type, 'UNK') f_type\n" +
                  ", COALESCE(m.f_status, 'UNK') f_status\n" +
                  ", CAST(null AS varchar) format_type\n" +
                  ", split_part(m.jm_source_reference, '-', 2) work_source_ref\n" +
                  ", m.eph_work_id eph_work_id\n" +
                  ", m.dq_err dq_err\n" +
                  ", m.notified_date last_updated_date\n" +
                  ", m.upsert update_type\n" +
                  ", false delete_flag\n" +
                  ", 'jm' source_system\n" +
                  ", m.scenario_code scenario_code\n" +
                  ", m.scenario_name scenario_name\n" +
                  " FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq m\n" +
                  "INNER JOIN (\n" +
                  "   SELECT\n" +
                  "     scenario_code\n" +
                  "   , jm_source_reference\n" +
                  "   , max(notified_date) max_notified_date\n" +
                  "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                  "   GROUP BY scenario_code, jm_source_reference\n" +
                  ")  maxm ON (((maxm.max_notified_date = m.notified_date) AND (maxm.scenario_code = m.scenario_code)) AND (maxm.jm_source_reference = m.jm_source_reference))))\n";

    public static String GET_BCS_JM_CORE_MANIF_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "  sourceref external_reference\n" +
                    ", CAST(null AS varchar) eph_manifestation_id\n" +
                    ", title manifestation_key_title\n" +
                    ", intereditionflag inter_edition_flag\n" +
                    ", firstpublisheddate first_published_date\n" +
                    ", last_pub_date last_pub_date\n" +
                    ", COALESCE(manifestation_type, 'UNK') f_type\n" +
                    ", COALESCE(status, 'UNK') f_status\n" +
                    ", CAST(null AS varchar) format_type\n" +
                    ", work_id work_source_ref\n" +
                    ", CAST(null AS varchar) eph_work_id\n" +
                    ", dq_err dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", CAST(null AS varchar) update_type\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_latest_v\n" +
                    "UNION ALL SELECT\n" +
                    "  m.jm_source_reference external_reference\n" +
                    ", m.eph_manifestation_id eph_manifestation_id\n" +
                    ", m.manifestaton_key_title manifestation_key_title\n" +
                    ", null inter_edition_flag\n" +
                    ", (CASE WHEN (m.f_type = 'JEL') THEN m.online_launch_date ELSE CAST(m.effective_start_date AS date) END) first_published_date\n" +
                    ", null last_pub_date\n" +
                    ", COALESCE(m.f_type, 'UNK') f_type\n" +
                    ", COALESCE(m.f_status, 'UNK') f_status\n" +
                    ", CAST(null AS varchar) format_type\n" +
                    ", split_part(m.jm_source_reference, '-', 2) work_source_ref\n" +
                    ", m.eph_work_id eph_work_id\n" +
                    ", m.dq_err dq_err\n" +
                    ", m.notified_date last_updated_date\n" +
                    ", m.upsert update_type\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", m.scenario_code scenario_code\n" +
                    ", m.scenario_name scenario_name\n" +
                    " FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq m\n" +
                    " INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxm ON (((maxm.max_notified_date = m.notified_date) AND (maxm.scenario_code = m.scenario_code)) AND (maxm.jm_source_reference = m.jm_source_reference))))order by rand() limit %s\n";

    public static String GET_BCS_JM_CORE_MANIF_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",eph_manifestation_id as EPH_MANIF_ID" +
                    ",manifestation_key_title as MANIF_KEY_TITLE" +
                    ",inter_edition_flag as INTEREDITIONFLAG" +
                    ",first_published_date as FIRSTPUBLISHEDDATE" +
                    ",last_pub_date as LASTPUBDATE" +
                    ",f_type as F_TYPE" +
                    ",f_status as F_STATUS" +
                    ",format_type as FORMAT_TYPE" +
                    ",work_source_ref as WORKSOURCEREF" +
                    ",eph_work_id as EPH_WORK_ID" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",update_type as UPDATETYPE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    " from(\n" +
                    "SELECT\n" +
                    "  sourceref external_reference\n" +
                    ", CAST(null AS varchar) eph_manifestation_id\n" +
                    ", title manifestation_key_title\n" +
                    ", intereditionflag inter_edition_flag\n" +
                    ", firstpublisheddate first_published_date\n" +
                    ", last_pub_date last_pub_date\n" +
                    ", COALESCE(manifestation_type, 'UNK') f_type\n" +
                    ", COALESCE(status, 'UNK') f_status\n" +
                    ", CAST(null AS varchar) format_type\n" +
                    ", work_id work_source_ref\n" +
                    ", CAST(null AS varchar) eph_work_id\n" +
                    ", dq_err dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", CAST(null AS varchar) update_type\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_latest_v\n" +
                    "UNION ALL SELECT\n" +
                    "  m.jm_source_reference external_reference\n" +
                    ", m.eph_manifestation_id eph_manifestation_id\n" +
                    ", m.manifestaton_key_title manifestation_key_title\n" +
                    ", null inter_edition_flag\n" +
                    ", (CASE WHEN (m.f_type = 'JEL') THEN m.online_launch_date ELSE CAST(m.effective_start_date AS date) END) first_published_date\n" +
                    ", null last_pub_date\n" +
                    ", COALESCE(m.f_type, 'UNK') f_type\n" +
                    ", COALESCE(m.f_status, 'UNK') f_status\n" +
                    ", CAST(null AS varchar) format_type\n" +
                    ", split_part(m.jm_source_reference, '-', 2) work_source_ref\n" +
                    ", m.eph_work_id eph_work_id\n" +
                    ", m.dq_err dq_err\n" +
                    ", m.notified_date last_updated_date\n" +
                    ", m.upsert update_type\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", m.scenario_code scenario_code\n" +
                    ", m.scenario_name scenario_name\n" +
                    " FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq m\n" +
                    " INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxm ON (((maxm.max_notified_date = m.notified_date) AND (maxm.scenario_code = m.scenario_code)) AND (maxm.jm_source_reference = m.jm_source_reference))))" +
                    " where external_reference in ('%s') order by external_reference,manifestation_key_title desc \n";


    public static String GET_DL_CORE_ALL_MANIF_VIEW_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",eph_manifestation_id as EPH_MANIF_ID" +
                    ",manifestation_key_title as MANIF_KEY_TITLE" +
                    ",inter_edition_flag as INTEREDITIONFLAG" +
                    ",first_published_date as FIRSTPUBLISHEDDATE" +
                    ",last_pub_date as LASTPUBDATE" +
                    ",f_type as F_TYPE" +
                    ",f_status as F_STATUS" +
                    ",format_type as FORMAT_TYPE" +
                    ",work_source_ref as WORKSOURCEREF" +
                    ",eph_work_id as EPH_WORK_ID" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",update_type as UPDATETYPE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_manifestation_v" +
                    " where external_reference in ('%s') \n" +
                    "order by external_reference,manifestation_key_title desc \n" ;

    public static String GET_BCS_JM_CORE_MANIF_IDENT_COUNT =
            "select count(*) as Source_Count from(\n" +
                    " SELECT\n" +
                    "  concat(mi.jm_source_ref_new, '-', mi.identifier_new) external_reference\n" +
                    ", mi.identifier_new identifier\n" +
                    ", mi.effective_start_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", mi.f_type f_type\n" +
                    ", mi.eph_manifestation_id f_manifestation\n" +
                    ", mi.manifestation_source_reference manifestation_source_reference\n" +
                    ", mi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'JM' source_system\n" +
                    ", mi.scenario_code scenario_code\n" +
                    ", mi.scenario_name scenario_name\n" +
                    ", CAST(null AS boolean) lead_indicator\n" +
                    " FROM\n" +
                    "  ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_identifier_dq mi\n" +
                    " INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM\n" +
                    "      "+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxm ON (((maxm.max_notified_date = mi.notified_date) AND (maxm.scenario_code = mi.scenario_code)) AND (maxm.jm_source_reference = mi.manifestation_source_reference)))\n" +
                    " UNION ALL SELECT\n" +
                    "  concat(mi.jm_source_ref_old, '-', mi.identifier_old) external_reference\n" +
                    ", mi.identifier_old identifier\n" +
                    ", CAST(null AS date) effective_start_date\n" +
                    ", (mi.effective_start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                    ", mi.f_type f_type\n" +
                    ", mi.eph_manifestation_id f_manifestation\n" +
                    ", mi.manifestation_source_reference manifestation_source_reference\n" +
                    ", mi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'JM' source_system\n" +
                    ", mi.scenario_code scenario_code\n" +
                    ", mi.scenario_name scenario_name\n" +
                    ", CAST(null AS boolean) lead_indicator\n" +
                    " FROM\n" +
                    "  ("+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_identifier_dq mi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM\n" +
                    "     "+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxm ON (((maxm.max_notified_date = mi.notified_date) AND (maxm.scenario_code = mi.scenario_code)) AND (maxm.jm_source_reference = mi.manifestation_source_reference)))\n" +
                    " WHERE (mi.identifier_old IS NOT NULL)\n" +
                    " UNION ALL SELECT\n" +
                    "  concat(sourceref, '-', identifier_type, '-', identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", identifier_type f_type\n" +
                    ", null f_manifestation\n" +
                    ", sourceref manifestation_source_reference\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'BCS' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    ", lead_indicator lead_indicator\n" +
                    " FROM\n" +
                    "  "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest_v m)";

    public static String GET_SOURCE_LEAD_INDICATOR_COUNT =
            "select count(*) as Source_Count from(\n" +
                    " SELECT\n" +
                    "  concat(mi.jm_source_ref_new, '-', mi.identifier_new) external_reference\n" +
                    ", mi.identifier_new identifier\n" +
                    ", mi.effective_start_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", mi.f_type f_type\n" +
                    ", mi.eph_manifestation_id f_manifestation\n" +
                    ", mi.manifestation_source_reference manifestation_source_reference\n" +
                    ", mi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'JM' source_system\n" +
                    ", mi.scenario_code scenario_code\n" +
                    ", mi.scenario_name scenario_name\n" +
                    ", CAST(null AS boolean) lead_indicator\n" +
                    " FROM\n" +
                    "  ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_identifier_dq mi\n" +
                    " INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM\n" +
                    "      "+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxm ON (((maxm.max_notified_date = mi.notified_date) AND (maxm.scenario_code = mi.scenario_code)) AND (maxm.jm_source_reference = mi.manifestation_source_reference)))\n" +
                    " UNION ALL SELECT\n" +
                    "  concat(mi.jm_source_ref_old, '-', mi.identifier_old) external_reference\n" +
                    ", mi.identifier_old identifier\n" +
                    ", CAST(null AS date) effective_start_date\n" +
                    ", (mi.effective_start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                    ", mi.f_type f_type\n" +
                    ", mi.eph_manifestation_id f_manifestation\n" +
                    ", mi.manifestation_source_reference manifestation_source_reference\n" +
                    ", mi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'JM' source_system\n" +
                    ", mi.scenario_code scenario_code\n" +
                    ", mi.scenario_name scenario_name\n" +
                    ", CAST(null AS boolean) lead_indicator\n" +
                    " FROM\n" +
                    "  ("+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_identifier_dq mi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM\n" +
                    "     "+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxm ON (((maxm.max_notified_date = mi.notified_date) AND (maxm.scenario_code = mi.scenario_code)) AND (maxm.jm_source_reference = mi.manifestation_source_reference)))\n" +
                    " WHERE (mi.identifier_old IS NOT NULL)\n" +
                    " UNION ALL SELECT\n" +
                    "  concat(sourceref, '-', identifier_type, '-', identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", identifier_type f_type\n" +
                    ", null f_manifestation\n" +
                    ", sourceref manifestation_source_reference\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'BCS' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    ", lead_indicator lead_indicator\n" +
                    " FROM\n" +
                    "  "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest_v m) where lead_indicator=true";

    public static String GET_BCS_JM_CORE_MANIF_IDENT_RAND_ID =
            "select external_reference as id from(\n" +
                    " SELECT\n" +
                    "  concat(mi.jm_source_ref_new, '-', mi.identifier_new) external_reference\n" +
                    ", mi.identifier_new identifier\n" +
                    ", mi.effective_start_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", mi.f_type f_type\n" +
                    ", mi.eph_manifestation_id f_manifestation\n" +
                    ", mi.manifestation_source_reference manifestation_source_reference\n" +
                    ", mi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'JM' source_system\n" +
                    ", mi.scenario_code scenario_code\n" +
                    ", mi.scenario_name scenario_name\n" +
                    ", CAST(null AS boolean) lead_indicator\n" +
                    " FROM\n" +
                    "  ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_identifier_dq mi\n" +
                    " INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM\n" +
                    "      "+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxm ON (((maxm.max_notified_date = mi.notified_date) AND (maxm.scenario_code = mi.scenario_code)) AND (maxm.jm_source_reference = mi.manifestation_source_reference)))\n" +
                    " UNION ALL SELECT\n" +
                    "  concat(mi.jm_source_ref_old, '-', mi.identifier_old) external_reference\n" +
                    ", mi.identifier_old identifier\n" +
                    ", CAST(null AS date) effective_start_date\n" +
                    ", (mi.effective_start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                    ", mi.f_type f_type\n" +
                    ", mi.eph_manifestation_id f_manifestation\n" +
                    ", mi.manifestation_source_reference manifestation_source_reference\n" +
                    ", mi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'JM' source_system\n" +
                    ", mi.scenario_code scenario_code\n" +
                    ", mi.scenario_name scenario_name\n" +
                    ", CAST(null AS boolean) lead_indicator\n" +
                    " FROM\n" +
                    "  ("+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_identifier_dq mi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM\n" +
                    "     "+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxm ON (((maxm.max_notified_date = mi.notified_date) AND (maxm.scenario_code = mi.scenario_code)) AND (maxm.jm_source_reference = mi.manifestation_source_reference)))\n" +
                    " WHERE (mi.identifier_old IS NOT NULL)\n" +
                    " UNION ALL SELECT\n" +
                    "  concat(sourceref, '-', identifier_type, '-', identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", identifier_type f_type\n" +
                    ", null f_manifestation\n" +
                    ", sourceref manifestation_source_reference\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'BCS' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    ", lead_indicator lead_indicator\n" +
                    " FROM\n" +
                    "  "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest_v m) order by rand() limit %s \n";

    public static String GET_BCS_JM_CORE_MANIF_IDENT_REC =
                    "select external_reference as EXTERNALREFERENCE" +
                    ",identifier as IDENTIFIER" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_type as F_TYPE" +
                    ",f_manifestation as F_MANIFESTATION" +
                    ",manifestation_source_reference as MANIFESTATIONSOURCEREF" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    ",lead_indicator as leadIndicator" +
                    " from(\n" +
                            " SELECT\n" +
                            "  concat(mi.jm_source_ref_new, '-', mi.identifier_new) external_reference\n" +
                            ", mi.identifier_new identifier\n" +
                            ", mi.effective_start_date effective_start_date\n" +
                            ", CAST(null AS date) effective_end_date\n" +
                            ", mi.f_type f_type\n" +
                            ", mi.eph_manifestation_id f_manifestation\n" +
                            ", mi.manifestation_source_reference manifestation_source_reference\n" +
                            ", mi.notified_date last_updated_date\n" +
                            ", false delete_flag\n" +
                            ", 'JM' source_system\n" +
                            ", mi.scenario_code scenario_code\n" +
                            ", mi.scenario_name scenario_name\n" +
                            ", CAST(null AS boolean) lead_indicator\n" +
                            " FROM\n" +
                            "  ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_identifier_dq mi\n" +
                            " INNER JOIN (\n" +
                            "   SELECT\n" +
                            "     scenario_code\n" +
                            "   , jm_source_reference\n" +
                            "   , max(notified_date) max_notified_date\n" +
                            "   FROM\n" +
                            "      "+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                            "   GROUP BY scenario_code, jm_source_reference\n" +
                            ")  maxm ON (((maxm.max_notified_date = mi.notified_date) AND (maxm.scenario_code = mi.scenario_code)) AND (maxm.jm_source_reference = mi.manifestation_source_reference)))\n" +
                            " UNION ALL SELECT\n" +
                            "  concat(mi.jm_source_ref_old, '-', mi.identifier_old) external_reference\n" +
                            ", mi.identifier_old identifier\n" +
                            ", CAST(null AS date) effective_start_date\n" +
                            ", (mi.effective_start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                            ", mi.f_type f_type\n" +
                            ", mi.eph_manifestation_id f_manifestation\n" +
                            ", mi.manifestation_source_reference manifestation_source_reference\n" +
                            ", mi.notified_date last_updated_date\n" +
                            ", false delete_flag\n" +
                            ", 'JM' source_system\n" +
                            ", mi.scenario_code scenario_code\n" +
                            ", mi.scenario_name scenario_name\n" +
                            ", CAST(null AS boolean) lead_indicator\n" +
                            " FROM\n" +
                            "  ("+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_identifier_dq mi\n" +
                            "INNER JOIN (\n" +
                            "   SELECT\n" +
                            "     scenario_code\n" +
                            "   , jm_source_reference\n" +
                            "   , max(notified_date) max_notified_date\n" +
                            "   FROM\n" +
                            "     "+GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_manifestation_dq\n" +
                            "   GROUP BY scenario_code, jm_source_reference\n" +
                            ")  maxm ON (((maxm.max_notified_date = mi.notified_date) AND (maxm.scenario_code = mi.scenario_code)) AND (maxm.jm_source_reference = mi.manifestation_source_reference)))\n" +
                            " WHERE (mi.identifier_old IS NOT NULL)\n" +
                            " UNION ALL SELECT\n" +
                            "  concat(sourceref, '-', identifier_type, '-', identifier) external_reference\n" +
                            ", identifier identifier\n" +
                            ", last_updated_date effective_start_date\n" +
                            ", CAST(null AS date) effective_end_date\n" +
                            ", identifier_type f_type\n" +
                            ", null f_manifestation\n" +
                            ", sourceref manifestation_source_reference\n" +
                            ", last_updated_date last_updated_date\n" +
                            ", delete_flag delete_flag\n" +
                            ", 'BCS' source_system\n" +
                            ", CAST(null AS varchar) scenario_code\n" +
                            ", CAST(null AS varchar) scenario_name\n" +
                            ", lead_indicator lead_indicator\n" +
                            " FROM\n" +
                            "  "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest_v m)where external_reference in ('%s') order by external_reference desc \n";


    public static String GET_DL_CORE_ALL_MANIF_IDENT_VIEW_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",identifier as IDENTIFIER" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_type as F_TYPE" +
                    ",f_manifestation as F_MANIFESTATION" +
                    ",manifestation_source_reference as MANIFESTATIONSOURCEREF" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    ",lead_indicator as leadIndicator" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_manifestation_identifiers_v" +
                    " where external_reference in ('%s') \n" +
                    "order by external_reference desc \n";


    public static String GET_BCS_JM_CORE_PERSON_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  CAST(null AS integer) person_id\n" +
                    ", CAST(u_key AS varchar) external_reference\n" +
                    ", max(firstname) given_name\n" +
                    ", max(familyname) family_name\n" +
                    ", peoplehub_id peoplehub_id\n" +
                    ", email_address email\n" +
                    ", dq_err dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    "FROM\n" +
                    "  "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_latest_v\n" +
                    "GROUP BY u_key, peoplehub_id, email_address, dq_err, last_updated_date, delete_flag\n" +
                    "UNION ALL SELECT DISTINCT\n" +
                    "  CAST(null AS integer) person_id\n" +
                    ", COALESCE(p.external_reference, w.person_source_reference) external_reference\n" +
                    ", first_name given_name\n" +
                    ", last_name family_name\n" +
                    ", emplid peoplehub_id\n" +
                    ", business_email email\n" +
                    ", 'N' dq_err\n" +
                    ", CAST(current_timestamp AS timestamp) last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'Workday' source_system\n" +
                    "FROM\n" +
                    "  (("+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".workday_data_orgdata n\n" +
                    "LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBase()+".gd_person p ON (n.emplid = p.peoplehub_id))\n" +
                    "LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_person_role_v w ON (lower(to_hex(md5(to_utf8(CAST(n.emplid AS varchar))))) = w.person_source_reference))\n" +
                    "WHERE (COALESCE(p.peoplehub_id, w.person_source_reference) IS NOT NULL))\n";


    public static String GET_BCS_JM_CORE_PERSON_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "  CAST(null AS integer) person_id\n" +
                    ", CAST(u_key AS varchar) external_reference\n" +
                    ", max(firstname) given_name\n" +
                    ", max(familyname) family_name\n" +
                    ", peoplehub_id peoplehub_id\n" +
                    ", email_address email\n" +
                    ", dq_err dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    "FROM\n" +
                    "  "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_latest_v\n" +
                    "GROUP BY u_key, peoplehub_id, email_address, dq_err, last_updated_date, delete_flag\n" +
                    "UNION ALL SELECT DISTINCT\n" +
                    "  CAST(null AS integer) person_id\n" +
                    ", COALESCE(p.external_reference, w.person_source_reference) external_reference\n" +
                    ", first_name given_name\n" +
                    ", last_name family_name\n" +
                    ", emplid peoplehub_id\n" +
                    ", business_email email\n" +
                    ", 'N' dq_err\n" +
                    ", CAST(current_timestamp AS timestamp) last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'Workday' source_system\n" +
                    "FROM\n" +
                    "  (("+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".workday_data_orgdata n\n" +
                    "LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBase()+".gd_person p ON (n.emplid = p.peoplehub_id))\n" +
                    "LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_person_role_v w ON (lower(to_hex(md5(to_utf8(CAST(n.emplid AS varchar))))) = w.person_source_reference))\n" +
                    "WHERE (COALESCE(p.peoplehub_id, w.person_source_reference) IS NOT NULL))order by rand() limit %s\n";

    public static String GET_BCS_JM_CORE_PERSON_REC =
            "SELECT person_id as PERSONID" +
                    ",external_reference as EXTERNALREFERENCE \n" +
                    ",given_name as GIVENNAME \n" +
                    ",family_name as FAMILYNAME \n"+
                    ",peoplehub_id as PEOPLEHUBID \n" +
                    ",email as EMAIL \n" +
                    ",dq_err as DQ_ERR " +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM\n" +
                    " from(\n" +
                    "SELECT\n" +
                    "  CAST(null AS integer) person_id\n" +
                    ", CAST(u_key AS varchar) external_reference\n" +
                    ", max(firstname) given_name\n" +
                    ", max(familyname) family_name\n" +
                    ", peoplehub_id peoplehub_id\n" +
                    ", email_address email\n" +
                    ", dq_err dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    "FROM\n" +
                    "  "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_latest_v\n" +
                    "GROUP BY u_key, peoplehub_id, email_address, dq_err, last_updated_date, delete_flag\n" +
                    "UNION ALL SELECT DISTINCT\n" +
                    "  CAST(null AS integer) person_id\n" +
                    ", COALESCE(p.external_reference, w.person_source_reference) external_reference\n" +
                    ", first_name given_name\n" +
                    ", last_name family_name\n" +
                    ", emplid peoplehub_id\n" +
                    ", business_email email\n" +
                    ", 'N' dq_err\n" +
                    ", CAST(current_timestamp AS timestamp) last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'Workday' source_system\n" +
                    "FROM\n" +
                    "  (("+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".workday_data_orgdata n\n" +
                    "LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getProdDataBase()+".gd_person p ON (n.emplid = p.peoplehub_id))\n" +
                    "LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_person_role_v w ON (lower(to_hex(md5(to_utf8(CAST(n.emplid AS varchar))))) = w.person_source_reference))\n" +
                    "WHERE (COALESCE(p.peoplehub_id, w.person_source_reference) IS NOT NULL)) where external_reference in ('%s')" +
                    " order by person_id,external_reference,last_updated_date desc\n";


    public static String GET_DL_CORE_ALL_PERSON_VIEW_REC =
            "SELECT person_id as PERSONID" +
                    ",external_reference as EXTERNALREFERENCE \n" +
                    ",given_name as GIVENNAME \n" +
                    ",family_name as FAMILYNAME \n"+
                    ",peoplehub_id as PEOPLEHUBID \n" +
                    ",email as EMAIL \n" +
                    ",dq_err as DQ_ERR\n" +
                    ",last_updated_date as LASTUPDATEDDATE\n" +
                    ",delete_flag as DELETEFLAG\n" +
                    ",source_system as SOURCESYSTEM\n" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_person_v" +
                    " where external_reference in ('%s') \n" +
                    "order by person_id,external_reference,last_updated_date desc \n";


    public static String GET_BCS_JM_CORE_PRODUCT_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  p.eph_product_id product_id\n" +
                    ", p.jm_source_reference external_reference\n" +
                    ", p.name name\n" +
                    ", null short_name\n" +
                    ", p.separately_saleable_ind separately_sale_indicator\n" +
                    ", p.trial_allowed_ind trial_allowed_indicator\n" +
                    ", CAST(null AS boolean) restricted_sale_indicator\n" +
                    ", p.launch_date launch_date\n" +
                    ", CAST(null AS date) content_from_date\n" +
                    ", CAST(null AS date) content_to_date\n" +
                    ", CAST(null AS integer) content_date_offset\n" +
                    ", COALESCE(p.f_type, 'UNK') f_type\n" +
                    ", COALESCE(p.f_status, 'UNK') f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", p.tax_code f_tax_code\n" +
                    ", p.f_revenue_model f_revenue_model\n" +
                    ", CAST(null AS varchar) f_revenue_account\n" +
                    ", p.eph_work_id f_wwork\n" +
                    ", null work_reference\n" +
                    ", p.eph_manifestation_id f_manifestation\n" +
                    ", null manifestation_reference\n" +
                    ", p.notified_date last_updated_date\n" +
                    ", 'N' dq_err\n" +
                    ", p.upsert update_type\n" +
                    ", 'JOURNAL' work_roll_up_type\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", p.scenario_code scenario_code\n" +
                    ", p.scenario_name scenario_name\n" +
                    "FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_product_dq p\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_product_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = p.notified_date) AND (maxw.scenario_code = p.scenario_code)) AND (maxw.jm_source_reference = p.jm_source_reference)))\n" +
                    "UNION ALL SELECT\n" +
                    "  null product_id\n" +
                    ", concat(u_key, '-', product_type) external_reference\n" +
                    ", name name\n" +
                    ", shorttitle short_name\n" +
                    ", separately_sale_indicator separately_sale_indicator\n" +
                    ", trial_allowed_indicator trial_allowed_indicator\n" +
                    ", restricted_sale_indicator restricted_sale_indicator\n" +
                    ", launchdate launch_date\n" +
                    ", CAST(null AS date) content_from_date\n" +
                    ", CAST(null AS date) content_to_date\n" +
                    ", CAST(null AS integer) content_date_offset\n" +
                    ", COALESCE(product_type, 'UNK') f_type\n" +
                    ", COALESCE(status, 'UNK') f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", taxcode f_tax_code\n" +
                    ", f_revenue_model f_revenue_model\n" +
                    ", CAST(null AS varchar) f_revenue_account\n" +
                    ", null f_wwork\n" +
                    ", worksource work_reference\n" +
                    ", null f_manifestation\n" +
                    ", manifestationref manifestation_reference\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", 'N' dq_err\n" +
                    ", CAST(null AS varchar) update_type\n" +
                    ", 'BOOK' work_roll_up_type\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    "FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_latest_v)\n";

    public static String GET_BCS_JM_CORE_PRODUCT_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "  p.eph_product_id product_id\n" +
                    ", p.jm_source_reference external_reference\n" +
                    ", p.name name\n" +
                    ", null short_name\n" +
                    ", p.separately_saleable_ind separately_sale_indicator\n" +
                    ", p.trial_allowed_ind trial_allowed_indicator\n" +
                    ", CAST(null AS boolean) restricted_sale_indicator\n" +
                    ", p.launch_date launch_date\n" +
                    ", CAST(null AS date) content_from_date\n" +
                    ", CAST(null AS date) content_to_date\n" +
                    ", CAST(null AS integer) content_date_offset\n" +
                    ", COALESCE(p.f_type, 'UNK') f_type\n" +
                    ", COALESCE(p.f_status, 'UNK') f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", p.tax_code f_tax_code\n" +
                    ", p.f_revenue_model f_revenue_model\n" +
                    ", CAST(null AS varchar) f_revenue_account\n" +
                    ", p.eph_work_id f_wwork\n" +
                    ", null work_reference\n" +
                    ", p.eph_manifestation_id f_manifestation\n" +
                    ", null manifestation_reference\n" +
                    ", p.notified_date last_updated_date\n" +
                    ", 'N' dq_err\n" +
                    ", p.upsert update_type\n" +
                    ", 'JOURNAL' work_roll_up_type\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", p.scenario_code scenario_code\n" +
                    ", p.scenario_name scenario_name\n" +
                    "FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_product_dq p\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_product_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = p.notified_date) AND (maxw.scenario_code = p.scenario_code)) AND (maxw.jm_source_reference = p.jm_source_reference)))\n" +
                    "UNION ALL SELECT\n" +
                    "  null product_id\n" +
                    ", concat(u_key, '-', product_type) external_reference\n" +
                    ", name name\n" +
                    ", shorttitle short_name\n" +
                    ", separately_sale_indicator separately_sale_indicator\n" +
                    ", trial_allowed_indicator trial_allowed_indicator\n" +
                    ", restricted_sale_indicator restricted_sale_indicator\n" +
                    ", launchdate launch_date\n" +
                    ", CAST(null AS date) content_from_date\n" +
                    ", CAST(null AS date) content_to_date\n" +
                    ", CAST(null AS integer) content_date_offset\n" +
                    ", COALESCE(product_type, 'UNK') f_type\n" +
                    ", COALESCE(status, 'UNK') f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", taxcode f_tax_code\n" +
                    ", f_revenue_model f_revenue_model\n" +
                    ", CAST(null AS varchar) f_revenue_account\n" +
                    ", null f_wwork\n" +
                    ", worksource work_reference\n" +
                    ", null f_manifestation\n" +
                    ", manifestationref manifestation_reference\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", 'N' dq_err\n" +
                    ", CAST(null AS varchar) update_type\n" +
                    ", 'BOOK' work_roll_up_type\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    "FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_latest_v)order by rand() limit %s \n";


    public static String GET_BCS_JM_CORE_PRODUCT_REC =
            "select product_id as PRODUCTID" +
                    ",external_reference as EXTERNALREFERENCE" +
                    ",name as NAME" +
                    ",short_name as SHORTNAME" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR" +
                    ",restricted_sale_indicator as RESTRICTEDSALEINDICATOR" +
                    ",launch_date as LAUNCHDATE" +
                    ",content_from_date as CONTENTFROMDATE" +
                    ",content_to_date as CONTENTTODATE" +
                    ",content_date_offset as CONTENTDATEOFFSET" +
                    ",f_type as F_TYPE" +
                    ",f_status as F_STATUS" +
                    ",f_accountable_product as F_ACCOUNTABLEPRODUCT" +
                    ",f_tax_code as F_TAXCODE" +
                    ",f_revenue_model as F_REVENUEMODEL" +
                    ",f_revenue_account as F_REVENUEACC" +
                    ",f_wwork as F_WWORK" +
                    ",work_reference as WORKREFERENCE" +
                    ",f_manifestation as F_MANIFESTATION" +
                    ",manifestation_reference as MANIFESTATIONREF" +
                    ",dq_err as DQ_ERR" +
                    ",update_type as UPDATE_TYPE" +
                    ",work_roll_up_type as WORKROLLUPTYPE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    " from(\n" +
                    "SELECT\n" +
                    "  p.eph_product_id product_id\n" +
                    ", p.jm_source_reference external_reference\n" +
                    ", p.name name\n" +
                    ", null short_name\n" +
                    ", p.separately_saleable_ind separately_sale_indicator\n" +
                    ", p.trial_allowed_ind trial_allowed_indicator\n" +
                    ", CAST(null AS boolean) restricted_sale_indicator\n" +
                    ", p.launch_date launch_date\n" +
                    ", CAST(null AS date) content_from_date\n" +
                    ", CAST(null AS date) content_to_date\n" +
                    ", CAST(null AS integer) content_date_offset\n" +
                    ", COALESCE(p.f_type, 'UNK') f_type\n" +
                    ", COALESCE(p.f_status, 'UNK') f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", p.tax_code f_tax_code\n" +
                    ", p.f_revenue_model f_revenue_model\n" +
                    ", CAST(null AS varchar) f_revenue_account\n" +
                    ", p.eph_work_id f_wwork\n" +
                    ", null work_reference\n" +
                    ", p.eph_manifestation_id f_manifestation\n" +
                    ", null manifestation_reference\n" +
                    ", p.notified_date last_updated_date\n" +
                    ", 'N' dq_err\n" +
                    ", p.upsert update_type\n" +
                    ", 'JOURNAL' work_roll_up_type\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", p.scenario_code scenario_code\n" +
                    ", p.scenario_name scenario_name\n" +
                    "FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_product_dq p\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_product_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = p.notified_date) AND (maxw.scenario_code = p.scenario_code)) AND (maxw.jm_source_reference = p.jm_source_reference)))\n" +
                    "UNION ALL SELECT\n" +
                    "  null product_id\n" +
                    ", concat(u_key, '-', product_type) external_reference\n" +
                    ", name name\n" +
                    ", shorttitle short_name\n" +
                    ", separately_sale_indicator separately_sale_indicator\n" +
                    ", trial_allowed_indicator trial_allowed_indicator\n" +
                    ", restricted_sale_indicator restricted_sale_indicator\n" +
                    ", launchdate launch_date\n" +
                    ", CAST(null AS date) content_from_date\n" +
                    ", CAST(null AS date) content_to_date\n" +
                    ", CAST(null AS integer) content_date_offset\n" +
                    ", COALESCE(product_type, 'UNK') f_type\n" +
                    ", COALESCE(status, 'UNK') f_status\n" +
                    ", CAST(null AS integer) f_accountable_product\n" +
                    ", taxcode f_tax_code\n" +
                    ", f_revenue_model f_revenue_model\n" +
                    ", CAST(null AS varchar) f_revenue_account\n" +
                    ", null f_wwork\n" +
                    ", worksource work_reference\n" +
                    ", null f_manifestation\n" +
                    ", manifestationref manifestation_reference\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", 'N' dq_err\n" +
                    ", CAST(null AS varchar) update_type\n" +
                    ", 'BOOK' work_roll_up_type\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_latest_v)" +
                    "where external_reference in ('%s') order by external_reference desc \n";


    public static String GET_DL_CORE_ALL_PRODUCT_VIEW_REC =
            "select product_id as PRODUCTID" +
                    ",external_reference as EXTERNALREFERENCE" +
                    ",name as NAME" +
                    ",short_name as SHORTNAME" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR" +
                    ",restricted_sale_indicator as RESTRICTEDSALEINDICATOR" +
                    ",launch_date as LAUNCHDATE" +
                    ",content_from_date as CONTENTFROMDATE" +
                    ",content_to_date as CONTENTTODATE" +
                    ",content_date_offset as CONTENTDATEOFFSET" +
                    ",f_type as F_TYPE" +
                    ",f_status as F_STATUS" +
                    ",f_accountable_product as F_ACCOUNTABLEPRODUCT" +
                    ",f_tax_code as F_TAXCODE" +
                    ",f_revenue_model as F_REVENUEMODEL" +
                    ",f_revenue_account as F_REVENUEACC" +
                    ",f_wwork as F_WWORK" +
                    ",work_reference as WORKREFERENCE" +
                    ",f_manifestation as F_MANIFESTATION" +
                    ",manifestation_reference as MANIFESTATIONREF" +
                    ",dq_err as DQ_ERR" +
                    ",update_type as UPDATE_TYPE" +
                    ",work_roll_up_type as WORKROLLUPTYPE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_product_v" +
                    " where external_reference in ('%s') \n" +
                    "order by external_reference desc \n";

    public static String GET_BCS_JM_CORE_PRODUCT_REL_PKG_VIEW_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  external_reference external_reference\n" +
                    ", allocation allocation\n" +
                    ", effective_start_date effective_start_date\n" +
                    ", effective_end_date effective_end_date\n" +
                    ", package_epr_id f_package_owner\n" +
                    ", epr_id f_component\n" +
                    ", f_relationship_type f_relationship_type\n" +
                    ", insert_ts last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'DPP' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getdppDataBase()+".etl_dpp_product_rel_package)\n";

    public static String GET_BCS_JM_CORE_PROD_REL_PKG_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "  external_reference external_reference\n" +
                    ", allocation allocation\n" +
                    ", effective_start_date effective_start_date\n" +
                    ", effective_end_date effective_end_date\n" +
                    ", package_epr_id f_package_owner\n" +
                    ", epr_id f_component\n" +
                    ", f_relationship_type f_relationship_type\n" +
                    ", insert_ts last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'DPP' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getdppDataBase()+".etl_dpp_product_rel_package) order by rand() limit %s\n";

    public static String GET_BCS_JM_CORE_PROD_REL_PKG_REC =
            "select " +
                    "external_reference as EXTERNALREFERENCE" +
                        ",allocation as ALLOCATION" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_package_owner as FPACKAGEOWNER" +
                    ",f_component as FCOMPONENT" +
                    ",f_relationship_type as FRELATIONSHIPTYPE" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from(\n" +
                    "SELECT\n" +
                    "  external_reference external_reference\n" +
                    ", allocation allocation\n" +
                    ", effective_start_date effective_start_date\n" +
                    ", effective_end_date effective_end_date\n" +
                    ", package_epr_id f_package_owner\n" +
                    ", epr_id f_component\n" +
                    ", f_relationship_type f_relationship_type\n" +
                    ", insert_ts last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'DPP' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getdppDataBase()+".etl_dpp_product_rel_package) where external_reference in ('%s') order by external_reference desc \n";

    public static String GET_DL_CORE_ALL_PROD_REL_PKG_REC =
            "select " +
                    "external_reference as EXTERNALREFERENCE" +
                    ",allocation as ALLOCATION" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_package_owner as FPACKAGEOWNER" +
                    ",f_component as FCOMPONENT" +
                    ",f_relationship_type as FRELATIONSHIPTYPE" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_product_rel_package_v where external_reference in ('%s') order by external_reference desc\n";

    public static String GET_BCS_JM_CORE_WORK_IDENT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT\n" +
                    "  wi.jm_source_ref_new external_reference\n" +
                    ", wi.identifier_new identifier\n" +
                    ", wi.f_type f_type\n" +
                    ", wi.eph_work_id f_wwork\n" +
                    ", split_part(wi.jm_source_ref_new, '-', 1) work_source_ref\n" +
                    ", wi.effective_start_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", wi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", wi.scenario_name scenario_name\n" +
                    "FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_identifier_dq wi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = wi.notified_date) AND (maxw.scenario_code = wi.scenario_code)) AND (maxw.jm_source_reference = split_part(wi.jm_source_ref_new, '-', 1))))\n" +
                    "UNION ALL SELECT\n" +
                    "  wi.jm_source_ref_old external_reference\n" +
                    ", wi.identifier_old identifier\n" +
                    ", wi.f_type f_type\n" +
                    ", wi.eph_work_id f_wwork\n" +
                    ", split_part(wi.jm_source_ref_old, '-', 1) work_source_ref\n" +
                    ", CAST(null AS date) effective_start_date\n" +
                    ", (wi.effective_start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                    ", wi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", wi.scenario_code scenario_code\n" +
                    ", wi.scenario_name scenario_name\n" +
                    "FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_identifier_dq wi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = wi.notified_date) AND (maxw.scenario_code = wi.scenario_code)) AND (maxw.jm_source_reference = split_part(wi.jm_source_ref_old, '-', 1))))\n" +
                    "WHERE (wi.identifier_old IS NOT NULL)\n" +
                    "UNION ALL SELECT\n" +
                    "  concat(sourceref, identifier_type, identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", identifier_type f_type\n" +
                    ", null f_wwork\n" +
                    ", sourceref work_source_ref\n" +
                    ", current_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    "FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_latest)\n";

    public static String GET_BCS_JM_CORE_WORK_IDENT_RAND_ID =
            "select external_reference as id from (\n" +
                    "SELECT\n" +
                    "  wi.jm_source_ref_new external_reference\n" +
                    ", wi.identifier_new identifier\n" +
                    ", wi.f_type f_type\n" +
                    ", wi.eph_work_id f_wwork\n" +
                    ", split_part(wi.jm_source_ref_new, '-', 1) work_source_ref\n" +
                    ", wi.effective_start_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", wi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", wi.scenario_name scenario_name\n" +
                    " FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_identifier_dq wi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = wi.notified_date) AND (maxw.scenario_code = wi.scenario_code)) AND (maxw.jm_source_reference = split_part(wi.jm_source_ref_new, '-', 1))))\n" +
                    "UNION ALL SELECT\n" +
                    "  wi.jm_source_ref_old external_reference\n" +
                    ", wi.identifier_old identifier\n" +
                    ", wi.f_type f_type\n" +
                    ", wi.eph_work_id f_wwork\n" +
                    ", split_part(wi.jm_source_ref_old, '-', 1) work_source_ref\n" +
                    ", CAST(null AS date) effective_start_date\n" +
                    ", (wi.effective_start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                    ", wi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", wi.scenario_code scenario_code\n" +
                    ", wi.scenario_name scenario_name\n" +
                    " FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_identifier_dq wi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = wi.notified_date) AND (maxw.scenario_code = wi.scenario_code)) AND (maxw.jm_source_reference = split_part(wi.jm_source_ref_old, '-', 1))))\n" +
                    "WHERE (wi.identifier_old IS NOT NULL)\n" +
                    "UNION ALL SELECT\n" +
                    "  concat(sourceref, identifier_type, identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", identifier_type f_type\n" +
                    ", null f_wwork\n" +
                    ", sourceref work_source_ref\n" +
                    ", current_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_latest) order by rand() limit %s \n";


    public static String GET_BCS_JM_CORE_WORK_IDENT_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",identifier as IDENTIFIER" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_type as F_TYPE" +
                    ",f_wwork as F_WWORK" +
                    ",work_source_ref as WORKSOURCEREF" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    " from (\n" +
                    "SELECT\n" +
                    "  wi.jm_source_ref_new external_reference\n" +
                    ", wi.identifier_new identifier\n" +
                    ", wi.f_type f_type\n" +
                    ", wi.eph_work_id f_wwork\n" +
                    ", split_part(wi.jm_source_ref_new, '-', 1) work_source_ref\n" +
                    ", wi.effective_start_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", wi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", wi.scenario_name scenario_name\n" +
                    " FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_identifier_dq wi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = wi.notified_date) AND (maxw.scenario_code = wi.scenario_code)) AND (maxw.jm_source_reference = split_part(wi.jm_source_ref_new, '-', 1))))\n" +
                    "UNION ALL SELECT\n" +
                    "  wi.jm_source_ref_old external_reference\n" +
                    ", wi.identifier_old identifier\n" +
                    ", wi.f_type f_type\n" +
                    ", wi.eph_work_id f_wwork\n" +
                    ", split_part(wi.jm_source_ref_old, '-', 1) work_source_ref\n" +
                    ", CAST(null AS date) effective_start_date\n" +
                    ", (wi.effective_start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                    ", wi.notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", wi.scenario_code scenario_code\n" +
                    ", wi.scenario_name scenario_name\n" +
                    " FROM ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_identifier_dq wi\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = wi.notified_date) AND (maxw.scenario_code = wi.scenario_code)) AND (maxw.jm_source_reference = split_part(wi.jm_source_ref_old, '-', 1))))\n" +
                    "WHERE (wi.identifier_old IS NOT NULL)\n" +
                    "UNION ALL SELECT\n" +
                    "  concat(sourceref, identifier_type, identifier) external_reference\n" +
                    ", identifier identifier\n" +
                    ", identifier_type f_type\n" +
                    ", null f_wwork\n" +
                    ", sourceref work_source_ref\n" +
                    ", current_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_latest)" +
                    " where external_reference in ('%s') order by external_reference desc \n";

    public static String GET_DL_CORE_ALL_WRK_IDENT_VIEW_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",identifier as IDENTIFIER" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_type as F_TYPE" +
                    ",f_wwork as F_WWORK" +
                    ",work_source_ref as WORKSOURCEREF" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_identifier_v" +
                    " where external_reference in ('%s') \n" +
                    "order by external_reference desc \n";

    public static String GET_BCS_JM_CORE_WORK_PERS_ROLE_COUNT =
           "WITH\n" +
                   "  source AS (\n" +
                   "   SELECT\n" +
                   "     u_key external_reference\n" +
                   "   , last_updated_date effective_start_date\n" +
                   "   , CAST(null AS timestamp) effective_end_date\n" +
                   "   , roletype f_role\n" +
                   "   , CAST(null AS varchar) f_wwork\n" +
                   "   , worksourceref work_source_reference\n" +
                   "   , personsourceref person_source_reference\n" +
                   "   , CAST(null AS varchar) person_email\n" +
                   "   , 'N' dq_err\n" +
                   "   , last_updated_date last_updated_date\n" +
                   "   , delete_flag delete_flag\n" +
                   "   , 'bcs' source_system\n" +
                   "   , 'bcs' scenario_code\n" +
                   "   , 'bcs' scenario_name\n" +
                   "   FROM\n" +
                   "     "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_latest_v\n" +
                   "UNION ALL    SELECT\n" +
                   "     wpr.jm_source_ref_new external_reference\n" +
                   "   , wpr.start_date effective_start_date\n" +
                   "   , CAST(null AS date) effective_end_date\n" +
                   "   , wpr.f_role f_role\n" +
                   "   , wpr.eph_work_id f_wwork\n" +
                   "   , wpr.work_source_reference work_source_reference\n" +
                   "   , wpr.employee_number_new person_source_reference\n" +
                   "   , wpr.internal_email_new person_email\n" +
                   "   , wpr.dq_err dq_err\n" +
                   "   , wpr.notified_date last_updated_date\n" +
                   "   , false delete_flag\n" +
                   "   , 'jm' source_system\n" +
                   "   , wpr.scenario_code scenario_code\n" +
                   "   , wpr.scenario_name scenario_name\n" +
                   "   FROM\n" +
                   "     ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq wpr\n" +
                   "   INNER JOIN (\n" +
                   "      SELECT\n" +
                   "        scenario_code\n" +
                   "      , work_source_reference\n" +
                   "      , f_role\n" +
                   "      , max(notified_date) max_notified_date\n" +
                   "      FROM\n" +
                   "        "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq\n" +
                   "      GROUP BY scenario_code, work_source_reference, f_role\n" +
                   "   )  maxw ON ((((maxw.max_notified_date = wpr.notified_date) AND (maxw.scenario_code = wpr.scenario_code)) AND (maxw.work_source_reference = wpr.work_source_reference)) AND (maxw.f_role = wpr.f_role)))\n" +
                   "UNION ALL    SELECT\n" +
                   "     wpr.jm_source_ref_old external_reference\n" +
                   "   , CAST(null AS date) effective_start_date\n" +
                   "   , (wpr.start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                   "   , wpr.f_role f_role\n" +
                   "   , wpr.eph_work_id f_wwork\n" +
                   "   , wpr.work_source_reference work_source_reference\n" +
                   "   , wpr.employee_number_old person_source_reference\n" +
                   "   , wpr.internal_email_old person_email\n" +
                   "   , CAST(null AS varchar) dq_err\n" +
                   "   , wpr.notified_date last_updated_date\n" +
                   "   , false delete_flag\n" +
                   "   , 'jm' source_system\n" +
                   "   , wpr.scenario_code scenario_code\n" +
                   "   , wpr.scenario_name scenario_name\n" +
                   "   FROM\n" +
                   "     ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq wpr\n" +
                   "   INNER JOIN (\n" +
                   "      SELECT\n" +
                   "        scenario_code\n" +
                   "      , work_source_reference\n" +
                   "      , f_role\n" +
                   "      , max(notified_date) max_notified_date\n" +
                   "      FROM\n" +
                   "        "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq\n" +
                   "      GROUP BY scenario_code, work_source_reference, f_role\n" +
                   "   )  maxw ON ((((maxw.max_notified_date = wpr.notified_date) AND (maxw.scenario_code = wpr.scenario_code)) AND (maxw.work_source_reference = wpr.work_source_reference)) AND (maxw.f_role = wpr.f_role)))\n" +
                   "   WHERE (wpr.employee_number_old IS NOT NULL)\n" +
                   ") \n" +
                   "select count(*) as Source_Count from(\n" +
                   "SELECT s.*\n" +
                   "FROM\n" +
                   "  (source s\n" +
                   "INNER JOIN (\n" +
                   "   SELECT\n" +
                   "     external_reference\n" +
                   "   , scenario_code\n" +
                   "   FROM\n" +
                   "     source\n" +
                   "   GROUP BY external_reference, scenario_code\n" +
                   "   HAVING (count(1) < 2)\n" +
                   ")  si ON ((si.external_reference = s.external_reference) AND (si.scenario_code = s.scenario_code))))\n";

    public static String GET_BCS_JM_CORE_WORK_PERS_ROLE_RAND_ID =
        "WITH\n" +
             "  source AS (\n" +
             "   SELECT\n" +
             "     u_key external_reference\n" +
             "   , last_updated_date effective_start_date\n" +
             "   , CAST(null AS timestamp) effective_end_date\n" +
             "   , roletype f_role\n" +
             "   , CAST(null AS varchar) f_wwork\n" +
             "   , worksourceref work_source_reference\n" +
             "   , personsourceref person_source_reference\n" +
             "   , CAST(null AS varchar) person_email\n" +
             "   , 'N' dq_err\n" +
             "   , last_updated_date last_updated_date\n" +
             "   , delete_flag delete_flag\n" +
             "   , 'bcs' source_system\n" +
             "   , 'bcs' scenario_code\n" +
             "   , 'bcs' scenario_name\n" +
             "   FROM\n" +
             "     "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_latest_v\n" +
             "UNION ALL    SELECT\n" +
             "     wpr.jm_source_ref_new external_reference\n" +
             "   , wpr.start_date effective_start_date\n" +
             "   , CAST(null AS date) effective_end_date\n" +
             "   , wpr.f_role f_role\n" +
             "   , wpr.eph_work_id f_wwork\n" +
             "   , wpr.work_source_reference work_source_reference\n" +
             "   , wpr.employee_number_new person_source_reference\n" +
             "   , wpr.internal_email_new person_email\n" +
             "   , wpr.dq_err dq_err\n" +
             "   , wpr.notified_date last_updated_date\n" +
             "   , false delete_flag\n" +
             "   , 'jm' source_system\n" +
             "   , wpr.scenario_code scenario_code\n" +
             "   , wpr.scenario_name scenario_name\n" +
             "   FROM\n" +
             "     ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq wpr\n" +
             "   INNER JOIN (\n" +
             "      SELECT\n" +
             "        scenario_code\n" +
             "      , work_source_reference\n" +
             "      , f_role\n" +
             "      , max(notified_date) max_notified_date\n" +
             "      FROM\n" +
             "        "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq\n" +
             "      GROUP BY scenario_code, work_source_reference, f_role\n" +
             "   )  maxw ON ((((maxw.max_notified_date = wpr.notified_date) AND (maxw.scenario_code = wpr.scenario_code)) AND (maxw.work_source_reference = wpr.work_source_reference)) AND (maxw.f_role = wpr.f_role)))\n" +
             "UNION ALL    SELECT\n" +
             "     wpr.jm_source_ref_old external_reference\n" +
             "   , CAST(null AS date) effective_start_date\n" +
             "   , (wpr.start_date - INTERVAL  '1' DAY) effective_end_date\n" +
             "   , wpr.f_role f_role\n" +
             "   , wpr.eph_work_id f_wwork\n" +
             "   , wpr.work_source_reference work_source_reference\n" +
             "   , wpr.employee_number_old person_source_reference\n" +
             "   , wpr.internal_email_old person_email\n" +
             "   , CAST(null AS varchar) dq_err\n" +
             "   , wpr.notified_date last_updated_date\n" +
             "   , false delete_flag\n" +
             "   , 'jm' source_system\n" +
             "   , wpr.scenario_code scenario_code\n" +
             "   , wpr.scenario_name scenario_name\n" +
             "   FROM\n" +
             "     ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq wpr\n" +
             "   INNER JOIN (\n" +
             "      SELECT\n" +
             "        scenario_code\n" +
             "      , work_source_reference\n" +
             "      , f_role\n" +
             "      , max(notified_date) max_notified_date\n" +
             "      FROM\n" +
             "        "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq\n" +
             "      GROUP BY scenario_code, work_source_reference, f_role\n" +
             "   )  maxw ON ((((maxw.max_notified_date = wpr.notified_date) AND (maxw.scenario_code = wpr.scenario_code)) AND (maxw.work_source_reference = wpr.work_source_reference)) AND (maxw.f_role = wpr.f_role)))\n" +
             "   WHERE (wpr.employee_number_old IS NOT NULL)\n" +
             ") \n" +
             "select external_reference as id from(\n" +
             "SELECT s.*\n" +
             "FROM\n" +
             "  (source s\n" +
             "INNER JOIN (\n" +
             "   SELECT\n" +
             "     external_reference\n" +
             "   , scenario_code\n" +
             "   FROM\n" +
             "     source\n" +
             "   GROUP BY external_reference, scenario_code\n" +
             "   HAVING (count(1) < 2)\n" +
             ")  si ON ((si.external_reference = s.external_reference) AND (si.scenario_code = s.scenario_code))))order by rand() limit %s\n";

    public static String GET_BCS_JM_CORE_WORK_PERS_ROLE_REC =

            "WITH\n" +
                    "  source AS (\n" +
                    "   SELECT\n" +
                    "     u_key external_reference\n" +
                    "   , last_updated_date effective_start_date\n" +
                    "   , CAST(null AS timestamp) effective_end_date\n" +
                    "   , roletype f_role\n" +
                    "   , CAST(null AS varchar) f_wwork\n" +
                    "   , worksourceref work_source_reference\n" +
                    "   , personsourceref person_source_reference\n" +
                    "   , CAST(null AS varchar) person_email\n" +
                    "   , 'N' dq_err\n" +
                    "   , last_updated_date last_updated_date\n" +
                    "   , delete_flag delete_flag\n" +
                    "   , 'bcs' source_system\n" +
                    "   , 'bcs' scenario_code\n" +
                    "   , 'bcs' scenario_name\n" +
                    "   FROM\n" +
                    "     "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_latest_v\n" +
                    "UNION ALL    SELECT\n" +
                    "     wpr.jm_source_ref_new external_reference\n" +
                    "   , wpr.start_date effective_start_date\n" +
                    "   , CAST(null AS date) effective_end_date\n" +
                    "   , wpr.f_role f_role\n" +
                    "   , wpr.eph_work_id f_wwork\n" +
                    "   , wpr.work_source_reference work_source_reference\n" +
                    "   , wpr.employee_number_new person_source_reference\n" +
                    "   , wpr.internal_email_new person_email\n" +
                    "   , wpr.dq_err dq_err\n" +
                    "   , wpr.notified_date last_updated_date\n" +
                    "   , false delete_flag\n" +
                    "   , 'jm' source_system\n" +
                    "   , wpr.scenario_code scenario_code\n" +
                    "   , wpr.scenario_name scenario_name\n" +
                    "   FROM\n" +
                    "     ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq wpr\n" +
                    "   INNER JOIN (\n" +
                    "      SELECT\n" +
                    "        scenario_code\n" +
                    "      , work_source_reference\n" +
                    "      , f_role\n" +
                    "      , max(notified_date) max_notified_date\n" +
                    "      FROM\n" +
                    "        "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq\n" +
                    "      GROUP BY scenario_code, work_source_reference, f_role\n" +
                    "   )  maxw ON ((((maxw.max_notified_date = wpr.notified_date) AND (maxw.scenario_code = wpr.scenario_code)) AND (maxw.work_source_reference = wpr.work_source_reference)) AND (maxw.f_role = wpr.f_role)))\n" +
                    "UNION ALL    SELECT\n" +
                    "     wpr.jm_source_ref_old external_reference\n" +
                    "   , CAST(null AS date) effective_start_date\n" +
                    "   , (wpr.start_date - INTERVAL  '1' DAY) effective_end_date\n" +
                    "   , wpr.f_role f_role\n" +
                    "   , wpr.eph_work_id f_wwork\n" +
                    "   , wpr.work_source_reference work_source_reference\n" +
                    "   , wpr.employee_number_old person_source_reference\n" +
                    "   , wpr.internal_email_old person_email\n" +
                    "   , CAST(null AS varchar) dq_err\n" +
                    "   , wpr.notified_date last_updated_date\n" +
                    "   , false delete_flag\n" +
                    "   , 'jm' source_system\n" +
                    "   , wpr.scenario_code scenario_code\n" +
                    "   , wpr.scenario_name scenario_name\n" +
                    "   FROM\n" +
                    "     ("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq wpr\n" +
                    "   INNER JOIN (\n" +
                    "      SELECT\n" +
                    "        scenario_code\n" +
                    "      , work_source_reference\n" +
                    "      , f_role\n" +
                    "      , max(notified_date) max_notified_date\n" +
                    "      FROM\n" +
                    "        "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_person_role_dq\n" +
                    "      GROUP BY scenario_code, work_source_reference, f_role\n" +
                    "   )  maxw ON ((((maxw.max_notified_date = wpr.notified_date) AND (maxw.scenario_code = wpr.scenario_code)) AND (maxw.work_source_reference = wpr.work_source_reference)) AND (maxw.f_role = wpr.f_role)))\n" +
                    "   WHERE (wpr.employee_number_old IS NOT NULL)\n" +
                    ") \n" +
                    "select external_reference as EXTERNALREFERENCE" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_role as F_ROLE" +
                    ",f_wwork as F_WWORK" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",person_source_reference as PERSONSOURCEREF" +
                    ",person_email as PERSONEMAIL" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    " from(\n" +
                    "SELECT s.*\n" +
                    "FROM\n" +
                    "  (source s\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     external_reference\n" +
                    "   , scenario_code\n" +
                    "   FROM\n" +
                    "     source\n" +
                    "   GROUP BY external_reference, scenario_code\n" +
                    "   HAVING (count(1) < 2)\n" +
                    ")  si ON ((si.external_reference = s.external_reference) AND (si.scenario_code = s.scenario_code))))where external_reference in ('%s') \n" +
                    "order by external_reference desc \n";

    public static String GET_DL_CORE_ALL_WRK_PERS_ROLE_VIEW_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_role as F_ROLE" +
                    ",f_wwork as F_WWORK" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",person_source_reference as PERSONSOURCEREF" +
                    ",person_email as PERSONEMAIL" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_person_role_v" +
                    " where external_reference in ('%s') \n" +
                    " order by external_reference desc \n";

    public static String GET_BCS_JM_CORE_WORK_RELATION_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  u_key external_reference\n" +
                    ", parentref parent_work_source_ref\n" +
                    ", childref child_work_source_ref\n" +
                    ", relationtyperef f_relationship_type\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    "FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_latest_v)\n";

    public static String GET_BCS_JM_CORE_WORK_RELATION_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "  u_key external_reference\n" +
                    ", parentref parent_work_source_ref\n" +
                    ", childref child_work_source_ref\n" +
                    ", relationtyperef f_relationship_type\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    "FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_latest_v) order by rand() limit %s\n";

    public static String GET_BCS_JM_CORE_WORK_RELATION_REC =
                    " select external_reference as EXTERNALREFERENCE" +
                    ",parent_work_source_ref as PARENTWORKSOURCEREF" +
                    ",child_work_source_ref as CHILDWORKSOURCEREF" +
                    ",f_relationship_type as F_RELATIONTYPEREF" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from(\n" +
                    " SELECT\n" +
                    "  u_key external_reference\n" +
                    ", parentref parent_work_source_ref\n" +
                    ", childref child_work_source_ref\n" +
                    ", relationtyperef f_relationship_type\n" +
                    ", last_updated_date effective_start_date\n" +
                    ", CAST(null AS date) effective_end_date\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_latest_v) where external_reference in ('%s') order by external_reference desc \n";


    public static String GET_DL_CORE_ALL_WRK_RELT_VIEW_REC=
            " select external_reference as EXTERNALREFERENCE" +
                    ",parent_work_source_ref as PARENTWORKSOURCEREF" +
                    ",child_work_source_ref as CHILDWORKSOURCEREF" +
                    ",f_relationship_type as F_RELATIONTYPEREF" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_relationship_v" +
                    " where external_reference in ('%s') \n" +
                    "order by external_reference desc \n";

    public static String GET_BCS_JM_CORE_WORK_SUBJ_AREA_COUNT =
           "select count(*) as Source_Count" +
                   " from(\n" +
                   " SELECT\n" +
                   "  jm_source_reference external_reference\n" +
                   ", start_date effective_start_date\n" +
                   ", end_date effective_end_date\n" +
                   ", f_subject_area f_subject_area\n" +
                   ", eph_work_id f_wwork\n" +
                   ", split_part(jm_source_reference, '-', 1) work_source_reference\n" +
                   ", subject_area_code subject_area_reference\n" +
                   ", dq_err dq_err\n" +
                   ", notified_date last_updated_date\n" +
                   ", false delete_flag \n" +
                   ", 'jm' source_system \n" +
                   " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_subject_area_dq\n" +
                   " WHERE subject_area_type = 'SD')\n";


    public static String GET_BCS_JM_CORE_WORK_SUBJ_AREA_RAND_ID=
            "select external_reference as id" +
                    " from(\n" +
                    " SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", start_date effective_start_date\n" +
                    ", end_date effective_end_date\n" +
                    ", f_subject_area f_subject_area\n" +
                    ", eph_work_id f_wwork\n" +
                    ", split_part(jm_source_reference, '-', 1) work_source_reference\n" +
                    ", subject_area_code subject_area_reference\n" +
                    ", dq_err dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", false delete_flag \n" +
                    ", 'jm' source_system \n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_subject_area_dq\n" +
                    " WHERE subject_area_type = 'SD') order by rand() limit %s \n";

    public static String GET_BCS_JM_CORE_WORK_SUBJ_AREA_REC=
            "select external_reference as EXTERNALREFERENCE" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_subject_area as F_SUBJECTAREA" +
                    ",f_wwork as F_WWORK" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",subject_area_reference as SUBJECTAREAREF" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from(\n" +
                    " SELECT\n" +
                    "  jm_source_reference external_reference\n" +
                    ", start_date effective_start_date\n" +
                    ", end_date effective_end_date\n" +
                    ", f_subject_area f_subject_area\n" +
                    ", eph_work_id f_wwork\n" +
                    ", split_part(jm_source_reference, '-', 1) work_source_reference\n" +
                    ", subject_area_code subject_area_reference\n" +
                    ", dq_err dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", false delete_flag \n" +
                    ", 'jm' source_system \n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_subject_area_dq\n" +
                    " WHERE subject_area_type = 'SD')where external_reference in ('%s') order by external_reference desc \n";

    public static String GET_DL_CORE_ALL_WORK_SUB_AREA_VIEW_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",effective_start_date as EFFECTIVE_START_DATE" +
                    ",effective_end_date as EFFECTIVE_END_DATE" +
                    ",f_subject_area as F_SUBJECTAREA" +
                    ",f_wwork as F_WWORK" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",subject_area_reference as SUBJECTAREAREF" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_subject_areas_v" +
                    " where external_reference in ('%s') \n" +
                    "order by external_reference desc \n";


    public static String GET_BCS_JM_CORE_WORK_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  w.eph_work_id epr\n" +
                    ", w.jm_source_reference external_reference\n" +
                    ", w.work_title work_title\n" +
                    ", w.work_subtitle work_subtitle\n" +
                    ", w.electro_rights_indicator electro_rights_indicator\n" +
                    ", CAST(w.volume AS varchar) volume\n" +
                    ", CAST(copyright_year AS integer) copyright_year\n" +
                    ", w.edition_number edition_number\n" +
                    ", CAST(pmc_new AS varchar) f_pmc\n" +
                    ", w.f_oa_type f_oa_journal_type\n" +
                    ", COALESCE(f_type, 'UNK') f_type\n" +
                    ", COALESCE(f_status, 'UNK') f_status\n" +
                    ", w.f_imprint f_imprint\n" +
                    ", w.f_legal_ownership f_legal_ownership\n" +
                    ", w.resp_centre resp_centre\n" +
                    ", w.opco opco\n" +
                    ", w.language_code language_code\n" +
                    ", w.subscription_type subscription_type\n" +
                    ", w.planned_launch_date planned_launch_date\n" +
                    ", CAST(null AS date) actual_launch_date\n" +
                    ", w.planned_termination_date planned_termination_date\n" +
                    ", CAST(null AS date) actual_termination_date\n" +
                    ", w.dq_err dq_err\n" +
                    ", w.notified_date last_updated_date\n" +
                    ", w.upsert update_type\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", w.scenario_code scenario_code\n" +
                    ", w.scenario_name scenario_name\n" +
                    " FROM("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq w\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = w.notified_date) AND (maxw.scenario_code = w.scenario_code)) AND (maxw.jm_source_reference = w.jm_source_reference)))\n" +
                    "UNION ALL SELECT\n" +
                    "  CAST(null AS varchar) epr\n" +
                    ", sourceref external_reference\n" +
                    ", title work_title\n" +
                    ", subtitle work_subtitle\n" +
                    ", electro_rights_indicator electro_rights_indicator\n" +
                    ", volumeno volume\n" +
                    ", CAST(copyrightyear AS integer) copyright_year\n" +
                    ", editionno edition_number\n" +
                    ", CAST(pmc AS varchar) f_pmc\n" +
                    ", f_oa_journal_type f_oa_journal_type\n" +
                    ", COALESCE(work_type, 'UNK') f_type\n" +
                    ", COALESCE(statuscode, 'UNK') f_status\n" +
                    ", imprintcode f_imprint\n" +
                    ", f_society_ownership f_society_ownership\n" +
                    ", resp_centre resp_centre\n" +
                    ", opco opco\n" +
                    ", languagecode language_code\n" +
                    ", subscription_type subscription_type\n" +
                    ", planned_pubdate planned_launch_date\n" +
                    ", actual_pubdate actual_launch_date\n" +
                    ", CAST(null AS date) planned_termination_date\n" +
                    ", CAST(null AS date) actual_termination_date\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", CAST(null AS varchar) update_type\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_latest_v)\n";

    public static String GET_BCS_JM_CORE_WORK_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "  w.eph_work_id epr\n" +
                    ", w.jm_source_reference external_reference\n" +
                    ", w.work_title work_title\n" +
                    ", w.work_subtitle work_subtitle\n" +
                    ", w.electro_rights_indicator electro_rights_indicator\n" +
                    ", CAST(w.volume AS varchar) volume\n" +
                    ", CAST(copyright_year AS integer) copyright_year\n" +
                    ", w.edition_number edition_number\n" +
                    ", CAST(pmc_new AS varchar) f_pmc\n" +
                    ", w.f_oa_type f_oa_journal_type\n" +
                    ", COALESCE(f_type, 'UNK') f_type\n" +
                    ", COALESCE(f_status, 'UNK') f_status\n" +
                    ", w.f_imprint f_imprint\n" +
                    ", w.f_legal_ownership f_legal_ownership\n" +
                    ", w.resp_centre resp_centre\n" +
                    ", w.opco opco\n" +
                    ", w.language_code language_code\n" +
                    ", w.subscription_type subscription_type\n" +
                    ", w.planned_launch_date planned_launch_date\n" +
                    ", CAST(null AS date) actual_launch_date\n" +
                    ", w.planned_termination_date planned_termination_date\n" +
                    ", CAST(null AS date) actual_termination_date\n" +
                    ", w.dq_err dq_err\n" +
                    ", w.notified_date last_updated_date\n" +
                    ", w.upsert update_type\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", w.scenario_code scenario_code\n" +
                    ", w.scenario_name scenario_name\n" +
                    " FROM("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq w\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = w.notified_date) AND (maxw.scenario_code = w.scenario_code)) AND (maxw.jm_source_reference = w.jm_source_reference)))\n" +
                    "UNION ALL SELECT\n" +
                    "  CAST(null AS varchar) epr\n" +
                    ", sourceref external_reference\n" +
                    ", title work_title\n" +
                    ", subtitle work_subtitle\n" +
                    ", electro_rights_indicator electro_rights_indicator\n" +
                    ", volumeno volume\n" +
                    ", CAST(copyrightyear AS integer) copyright_year\n" +
                    ", editionno edition_number\n" +
                    ", CAST(pmc AS varchar) f_pmc\n" +
                    ", f_oa_journal_type f_oa_journal_type\n" +
                    ", COALESCE(work_type, 'UNK') f_type\n" +
                    ", COALESCE(statuscode, 'UNK') f_status\n" +
                    ", imprintcode f_imprint\n" +
                    ", f_society_ownership f_society_ownership\n" +
                    ", resp_centre resp_centre\n" +
                    ", opco opco\n" +
                    ", languagecode language_code\n" +
                    ", subscription_type subscription_type\n" +
                    ", planned_pubdate planned_launch_date\n" +
                    ", actual_pubdate actual_launch_date\n" +
                    ", CAST(null AS date) planned_termination_date\n" +
                    ", CAST(null AS date) actual_termination_date\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", CAST(null AS varchar) update_type\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_latest_v)order by rand() limit %s \n";

    public static String GET_BCS_JM_CORE_WORK_REC=
            "select external_reference as EXTERNALREFERENCE" +
                    ",epr as EPR" +
                    ",work_title as WORKTITLE" +
                    ",work_subtitle as WORKSUBTITLE" +
                    ",electro_rights_indicator as ELECTRORIGHTINDICATOR" +
                    ",volume as VOLUME" +
                    ",copyright_year as COPYRIGHTYEAR" +
                    ",edition_number as EDITIONNO" +
                    ",f_pmc as F_PMC" +
                    ",f_oa_journal_type as F_OA_JOURNAL_TYPE" +
                    ",f_type as F_TYPE" +
                    ",f_status as F_STATUS" +
                    ",f_imprint as F_IMPRINT" +
                    ",f_legal_ownership as f_legal_ownership" +
                    ",resp_centre as RESP_CENTRE" +
                    ",opco as OPCO" +
                    ",language_code as LANGUAGECODE" +
                    ",subscription_type as SUBSCRIPTIONTYPE" +
                    ",planned_launch_date as PLANNED_LAUNCH_DATE" +
                    ",actual_launch_date as ACTUAL_LAUNCH_DATE" +
                    ",planned_termination_date as PLANNED_TERMINATION_DATE" +
                    ",actual_termination_date as ACTUAL_TERMINATION_DATE" +
                    ",dq_err as DQ_ERR" +
                    ",update_type as UPDATE_TYPE" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
                   // ",external_reference as EXTERNALREFERENCE" +
                 //   ",parent_work_source_ref as PARENTWORKSOURCEREF" +
                  //  ",child_work_source_ref as CHILDWORKSOURCEREF" +
                   // ",f_relation_type as F_RELATIONTYPEREF" +
                    //",effective_start_date as EFFECTIVE_START_DATE" +
                    //",effective_end_date as EFFECTIVE_END_DATE"+
                    " from(\n" +
                    "SELECT\n" +
                    "  w.eph_work_id epr\n" +
                    ", w.jm_source_reference external_reference\n" +
                    ", w.work_title work_title\n" +
                    ", w.work_subtitle work_subtitle\n" +
                    ", w.electro_rights_indicator electro_rights_indicator\n" +
                    ", CAST(w.volume AS varchar) volume\n" +
                    ", CAST(copyright_year AS integer) copyright_year\n" +
                    ", w.edition_number edition_number\n" +
                    ", CAST(pmc_new AS varchar) f_pmc\n" +
                    ", w.f_oa_type f_oa_journal_type\n" +
                    ", COALESCE(f_type, 'UNK') f_type\n" +
                    ", COALESCE(f_status, 'UNK') f_status\n" +
                    ", w.f_imprint f_imprint\n" +
                    ", w.f_legal_ownership f_legal_ownership\n" +
                    ", w.resp_centre resp_centre\n" +
                    ", w.opco opco\n" +
                    ", w.language_code language_code\n" +
                    ", w.subscription_type subscription_type\n" +
                    ", w.planned_launch_date planned_launch_date\n" +
                    ", CAST(null AS date) actual_launch_date\n" +
                    ", w.planned_termination_date planned_termination_date\n" +
                    ", CAST(null AS date) actual_termination_date\n" +
                    ", w.dq_err dq_err\n" +
                    ", w.notified_date last_updated_date\n" +
                    ", w.upsert update_type\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    ", w.scenario_code scenario_code\n" +
                    ", w.scenario_name scenario_name\n" +
                    " FROM("+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq w\n" +
                    "INNER JOIN (\n" +
                    "   SELECT\n" +
                    "     scenario_code\n" +
                    "   , jm_source_reference\n" +
                    "   , max(notified_date) max_notified_date\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_wwork_dq\n" +
                    "   GROUP BY scenario_code, jm_source_reference\n" +
                    ")  maxw ON (((maxw.max_notified_date = w.notified_date) AND (maxw.scenario_code = w.scenario_code)) AND (maxw.jm_source_reference = w.jm_source_reference)))\n" +
                    "UNION ALL SELECT\n" +
                    "  CAST(null AS varchar) epr\n" +
                    ", sourceref external_reference\n" +
                    ", title work_title\n" +
                    ", subtitle work_subtitle\n" +
                    ", electro_rights_indicator electro_rights_indicator\n" +
                    ", volumeno volume\n" +
                    ", CAST(copyrightyear AS integer) copyright_year\n" +
                    ", editionno edition_number\n" +
                    ", CAST(pmc AS varchar) f_pmc\n" +
                    ", f_oa_journal_type f_oa_journal_type\n" +
                    ", COALESCE(work_type, 'UNK') f_type\n" +
                    ", COALESCE(statuscode, 'UNK') f_status\n" +
                    ", imprintcode f_imprint\n" +
                    ", f_society_ownership f_society_ownership\n" +
                    ", resp_centre resp_centre\n" +
                    ", opco opco\n" +
                    ", languagecode language_code\n" +
                    ", subscription_type subscription_type\n" +
                    ", planned_pubdate planned_launch_date\n" +
                    ", actual_pubdate actual_launch_date\n" +
                    ", CAST(null AS date) planned_termination_date\n" +
                    ", CAST(null AS date) actual_termination_date\n" +
                    ", 'N' dq_err\n" +
                    ", last_updated_date last_updated_date\n" +
                    ", CAST(null AS varchar) update_type\n" +
                    ", delete_flag delete_flag\n" +
                    ", 'bcs' source_system\n" +
                    ", CAST(null AS varchar) scenario_code\n" +
                    ", CAST(null AS varchar) scenario_name\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_latest_v)" +
                    " where external_reference in ('%s') order by external_reference desc \n";


    public static String GET_DL_CORE_ALL_WORK_VIEW_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",epr as EPR" +
                    ",work_title as WORKTITLE" +
                    ",work_subtitle as WORKSUBTITLE" +
                    ",electro_rights_indicator as ELECTRORIGHTINDICATOR" +
                    ",volume as VOLUME" +
                    ",copyright_year as COPYRIGHTYEAR" +
                    ",edition_number as EDITIONNO" +
                    ",f_pmc as F_PMC" +
                    ",f_legal_ownership as f_legal_ownership" +
                    ",f_type as F_TYPE" +
                    ",f_status as F_STATUS" +
                    ",f_imprint as F_IMPRINT" +
                    ",f_legal_ownership as f_legal_ownership" +
                    ",resp_centre as RESP_CENTRE" +
                    ",opco as OPCO" +
                    ",language_code as LANGUAGECODE" +
                    ",subscription_type as SUBSCRIPTIONTYPE" +
                    ",planned_launch_date as PLANNED_LAUNCH_DATE" +
                    ",actual_launch_date as ACTUAL_LAUNCH_DATE" +
                    ",planned_termination_date as PLANNED_TERMINATION_DATE" +
                    ",actual_termination_date as ACTUAL_TERMINATION_DATE" +
                    ",dq_err as DQ_ERR" +
                    ",update_type as UPDATE_TYPE" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    ",scenario_code as SCENARIOCODE" +
                    ",scenario_name as SCENARIONAME" +
//                    ",external_reference as EXTERNALREFERENCE" +
//                    ",parent_work_source_ref as PARENTWORKSOURCEREF" +
//                    ",child_work_source_ref as CHILDWORKSOURCEREF" +
//                    ",f_relation_type as F_RELATIONTYPEREF" +
//                    ",effective_start_date as EFFECTIVE_START_DATE" +
//                    ",effective_end_date as EFFECTIVE_END_DATE"+
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_v" +
                    " where external_reference in ('%s') \n" +
                    "order by external_reference desc \n";

    public static String GET_BCS_JM_CORE_WORK_LEGAL_OWNER_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "  work_external_ref work_source_reference\n" +
                    ", legalowner_external_ref legal_owner_reference\n" +
                    ", f_ownership_description\n" +
                    ", work_legalowner_external_ref external_reference\n" +
                    ", 'N' dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_legal_owner_dq)\n";

    public static String GET_BCS_JM_CORE_WORK_LEGAL_OWNER_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "  work_external_ref work_source_reference\n" +
                    ", legalowner_external_ref legal_owner_reference\n" +
                    ", f_ownership_description\n" +
                    ", work_legalowner_external_ref external_reference\n" +
                    ", 'N' dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_legal_owner_dq) order by rand() limit %s\n";

    public static String GET_BCS_JM_CORE_WORK_LEGAL_OWNER_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",legal_owner_reference as legal_owner_reference" +
                    ",f_ownership_description as f_ownership_description" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from(\n" +
                    "SELECT\n" +
                    "  work_external_ref work_source_reference\n" +
                    ", legalowner_external_ref legal_owner_reference\n" +
                    ", f_ownership_description\n" +
                    ", work_legalowner_external_ref external_reference\n" +
                    ", 'N' dq_err\n" +
                    ", notified_date last_updated_date\n" +
                    ", false delete_flag\n" +
                    ", 'jm' source_system\n" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_legal_owner_dq) where external_reference in ('%s') order by external_reference desc\n";

    public static String GET_DL_CORE_ALL_WORK_LEGAL_OWNER_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",legal_owner_reference as legal_owner_reference" +
                    ",f_ownership_description as f_ownership_description" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_legal_owner_v where external_reference in ('%s') order by external_reference desc\n";

    public static String GET_BCS_JM_CORE_WORK_ACCESS_MODEL_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "external_work_ref work_source_reference\n" +
                    ", external_reference\n" +
                    ", access_model_code\n" +
                    ", access_model_description\n" +
                    ", 'N' dq_err \n" +
                    ", false delete_flag\n" +
                    ", notified_date last_updated_date \n" +
                    ", 'jm' source_system \n" +
                    " from "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_access_model_dq_v)";

    public static String GET_BCS_JM_CORE_WORK_ACCESS_MODEL_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "external_work_ref work_source_reference\n" +
                    ", external_reference\n" +
                    ", access_model_code\n" +
                    ", access_model_description\n" +
                    ", 'N' dq_err \n" +
                    ", false delete_flag\n" +
                    ", notified_date last_updated_date \n" +
                    ", 'jm' source_system \n" +
                    " from "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_access_model_dq_v)order by rand() limit %s";

    public static String GET_BCS_JM_CORE_WORK_ACCESS_MODEL_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",access_model_code as access_model_code" +
                    ",access_model_description as access_model_description" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from(\n" +
                    "SELECT\n" +
                    "external_work_ref work_source_reference\n" +
                    ", external_reference\n" +
                    ", access_model_code\n" +
                    ", access_model_description\n" +
                    ", 'N' dq_err \n" +
                    ", false delete_flag\n" +
                    ", notified_date last_updated_date \n" +
                    ", 'jm' source_system \n" +
                    " from "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_access_model_dq_v)where external_reference in ('%s') order by external_reference desc ";

    public static String GET_DL_CORE_ALL_WORK_ACCESS_MODEL_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",access_model_code as access_model_code" +
                    ",access_model_description as access_model_description" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_access_model_v where external_reference in ('%s') order by external_reference desc\n";

    public static String GET_BCS_JM_CORE_WORK_BUSINESS_MODEL_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT\n" +
                    "external_work_ref work_source_reference\n" +
                    ", external_reference\n" +
                    ", business_model_code\n" +
                    ", business_model_description\n" +
                    ", 'N' dq_err \n" +
                    ", false delete_flag\n" +
                    ", notified_date last_updated_date \n" +
                    ", 'jm' source_system \n" +
                    " from "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_business_model_dq_v)";

    public static String GET_BCS_JM_CORE_WORK_BUSINESS_MODEL_RAND_ID =
            "select external_reference as id from(\n" +
                    "SELECT\n" +
                    "external_work_ref work_source_reference\n" +
                    ", external_reference\n" +
                    ", business_model_code\n" +
                    ", business_model_description\n" +
                    ", 'N' dq_err \n" +
                    ", false delete_flag\n" +
                    ", notified_date last_updated_date \n" +
                    ", 'jm' source_system \n" +
                    " from "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_business_model_dq_v)order by rand() limit %s";

    public static String GET_BCS_JM_CORE_WORK_BUSINESS_MODEL_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",business_model_code as business_model_code" +
                    ",business_model_description as business_model_description" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from(\n" +
                    "SELECT\n" +
                    "external_work_ref work_source_reference\n" +
                    ", external_reference\n" +
                    ", business_model_code\n" +
                    ", business_model_description\n" +
                    ", 'N' dq_err \n" +
                    ", false delete_flag\n" +
                    ", notified_date last_updated_date \n" +
                    ", 'jm' source_system \n" +
                    " from "+ GetBcsEtlCoreDLDBUser.getJmCoreDataBase()+".etl_work_business_model_dq_v)where external_reference in ('%s') order by external_reference desc\n";

    public static String GET_DL_CORE_ALL_WORK_BUSINESS_MODEL_REC =
            "select external_reference as EXTERNALREFERENCE" +
                    ",work_source_reference as WORKSOURCEREF" +
                    ",business_model_code as business_model_code" +
                    ",business_model_description as business_model_description" +
                    ",dq_err as DQ_ERR" +
                    ",last_updated_date as LASTUPDATEDDATE" +
                    ",delete_flag as DELETEFLAG" +
                    ",source_system as SOURCESYSTEM" +
                    " from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_business_model_v where external_reference in ('%s') order by external_reference desc\n";


    public static String GET_BCS_JM_CORE_Acc_Prod_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_accountable_product_v where external_reference is null";

    public static String GET_BCS_JM_CORE_manif_ident_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_manifestation_identifiers_v where external_reference is null";

    public static String GET_BCS_JM_CORE_manif_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_manifestation_v where external_reference is null";

    public static String GET_BCS_JM_CORE_Person_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_person_v where external_reference is null";

    public static String GET_BCS_JM_CORE_Product_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_product_v where external_reference is null";

    public static String GET_BCS_JM_CORE_Product_Rel_Pkg_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_product_rel_package_v where external_reference is null";

    public static String GET_BCS_JM_CORE_Work_identf_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_identifier_v where external_reference is null";

    public static String GET_BCS_JM_CORE_Work_PERS_role_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_person_role_v where external_reference is null";
    public static String GET_BCS_JM_CORE_Work_RELAtion_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_relationship_v where external_reference is null";
    public static String GET_BCS_JM_CORE_Work_sub_area_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_subject_areas_v where external_reference is null";
    public static String GET_BCS_JM_CORE_Work_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_v where external_reference is null";
    public static String GET_BCS_JM_CORE_Work_legal_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_legal_owner_v where external_reference is null";

    public static String GET_BCS_JM_CORE_Work_access_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_access_model_v where external_reference is null";

    public static String GET_BCS_JM_CORE_Work_business_Ext_Ref_Null_count =
            "select count(*) as Null_COunt from "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".all_work_business_model_v where external_reference is null";

}




