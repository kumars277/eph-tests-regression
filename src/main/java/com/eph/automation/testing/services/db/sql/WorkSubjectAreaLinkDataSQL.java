package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 18/04/2019
 */
public class WorkSubjectAreaLinkDataSQL {

    public static String SELECT_COUNT_WORK_SUBJECT_AREA_PMX = "SELECT count(*) as count FROM (\n" +
            "SELECT\n" +
            "     PSA.PRODUCT_SUBJECT_AREA_ID\n" +
            "    ,PSA.F_SUBJECT_AREA\n" +
            "    ,PSA.F_PRODUCT_WORK\n" +
            "    ,NVL(PSA.EFFFROM_DATE,TO_DATE('2016-01-01','YYYY-MM-DD')) AS START_DATE\n" +
            "    ,PSA.EFFTO_DATE AS END_DATE\n" +
            "    ,TO_CHAR(NVL(PSA.B_UPDDATE,PSA.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM GD_PRODUCT_SUBJECT_AREA PSA\n" +
            "JOIN GD_SUBJECT_AREA SA ON F_SUBJECT_AREA = SUBJECT_AREA_ID\n" +
            "WHERE F_PRODUCT_WORK IN \n" +
            "    (SELECT PRODUCT_WORK_ID FROM GD_PRODUCT_WORK WHERE F_PRODUCT_TYPE IN \n" +
            "        (SELECT PRODUCT_TYPE_ID FROM GD_PRODUCT_TYPE WHERE F_PRODUCT_GROUP_TYPE = 2\n" +
            "            )\n" +
            "        )\n" +
            "AND SA.F_SUBJECT_AREA_TYPE = 181\n" +
            "    )";


//   old public static String SELECT_COUNT_WORK_SUBJECT_AREA_SA = "select count(*) as count from semarchy_eph_mdm.sa_work_subject_area_link s"+
//            " where s.b_loadid =  (select max (e.b_loadid) from\n" +
//            "semarchy_eph_mdm.sa_event e\n" +
//            "where  e.f_event_type = 'PMX'\n" +
//            "and e.workflow_id = 'talend'\n" +
//            "AND e.f_event_type = 'PMX'\n" +
//            "and e.f_workflow_source = 'PMX' )";

    public static String SELECT_COUNT_WORK_SUBJECT_AREA_SA = "select count(*) as count from semarchy_eph_mdm.sa_work_subject_area_link s\n" +
            " where s.b_loadid =  (select  max(b_loadid) from semarchy_eph_mdm.sa_event sa2  where sa2.f_event_type = 'PMX'\n" +
            "and sa2.workflow_id = 'talend'\n" +
            "and sa2.f_workflow_source = 'PMX' )";

    public static String GET_COUNT_WORK_SUBJECT_AREA_EPHAE = "select count(*) as count from semarchy_eph_mdm.ae_work_subject_area_link where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";

    public static String SELECT_COUNT_WORK_SUBJECT_AREA_STG = "select count(*) as count from "+GetEPHDBUser.getDBUser()+".STG_10_PMX_WORK_SUBJECT_AREA ";

    public static String SELECT_COUNT_WORK_SUBJECT_AREA_STG_DQ = "select count(*) as count from "+GetEPHDBUser.getDBUser()+".STG_10_PMX_WORK_SUBJECT_AREA where \"F_SUBJECT_AREA\"  is not null and \n" +
            "\"F_PRODUCT_WORK\" in (select pmx_source_reference from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq where dq_err!= 'Y')";

    public static String SELECT_COUNT_WORK_SUBJECT_AREA_STG_DQ_Delta = "select count(*) as count from "+GetEPHDBUser.getDBUser()+".STG_10_PMX_WORK_SUBJECT_AREA where \"F_SUBJECT_AREA\"  is not null and \n" +
            "\"F_PRODUCT_WORK\" in (select pmx_source_reference from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq where dq_err!= 'Y')\n"+
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI')";
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')";


    public static String SELECT_COUNT_WORK_SUBJECT_AREA_GD = "select count(*) as count from semarchy_eph_mdm.gd_work_subject_area_link \n" +
            " where b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event e \n" +
            "where  e.f_event_type = 'PMX'\n" +
            "and e.workflow_id = 'talend'\n" +
            "AND e.f_event_type = 'PMX'\n" +
            "and e.f_workflow_source = 'PMX' )";

    public static String EXTRACT_DATA_WORK_SUBJECT_AREA_PMX = "SELECT * FROM (\n" +
            "SELECT\n" +
            "     PSA.PRODUCT_SUBJECT_AREA_ID\n" +
            "    ,PSA.F_SUBJECT_AREA\n" +
            "    ,PSA.F_PRODUCT_WORK\n" +
            "    ,NVL(PSA.EFFFROM_DATE,TO_DATE('2016-01-01','YYYY-MM-DD')) AS START_DATE\n" +
            "    ,PSA.EFFTO_DATE AS EFFTO_DATE\n" +
            "    ,TO_CHAR(NVL(PSA.B_UPDDATE,PSA.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM GD_PRODUCT_SUBJECT_AREA PSA\n" +
            "JOIN GD_SUBJECT_AREA SA ON F_SUBJECT_AREA = SUBJECT_AREA_ID\n" +
            "WHERE F_PRODUCT_WORK IN \n" +
            "    (SELECT PRODUCT_WORK_ID FROM GD_PRODUCT_WORK WHERE F_PRODUCT_TYPE IN \n" +
            "        (SELECT PRODUCT_TYPE_ID FROM GD_PRODUCT_TYPE WHERE F_PRODUCT_GROUP_TYPE = 2\n" +
            "            )\n" +
            "        )\n" +
            "--AND PSA.EFFTO_DATE IS NULL -- removing this filter as we need to include end dated records\n" +
            "AND SA.F_SUBJECT_AREA_TYPE = 181\n" +
            "    ) WHERE PRODUCT_SUBJECT_AREA_ID IN ('%s')";

    public static String EXTRACT_DATA_WORK_SUBJECT_AREA_STG = "select  \n" +
            "\"PRODUCT_SUBJECT_AREA_ID\" as PRODUCT_SUBJECT_AREA_ID\n" +
            ",\"F_SUBJECT_AREA\" as F_SUBJECT_AREA\n" +
            ",\"F_PRODUCT_WORK\" as F_PRODUCT_WORK\n" +
            ",\"START_DATE\" as START_DATE\n" +
            ",\"END_DATE\" as EFFTO_DATE\n" +
            ",\"UPDATED\" as UPDATED\n" +
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_subject_area\t\n" +
            "left join "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "left join semarchy_eph_mdm.sa_work_subject_area_link sa on sa.work_subject_area_link_id = mp.numeric_id\n" +
            "join (select distinct subject_area_id, external_reference from semarchy_eph_mdm.sa_subject_area) g on stg_10_pmx_work_subject_area.\"F_SUBJECT_AREA\"::varchar = g.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, work_subject_area_link_id from semarchy_eph_mdm.sa_work_subject_area_link) a on stg_10_pmx_work_subject_area.\"PRODUCT_SUBJECT_AREA_ID\"::varchar = a.external_reference\n" +
            "where sa.b_error_status is null " +
            "and \"PRODUCT_SUBJECT_AREA_ID\" in ('%s')\n";

    public static String EXTRACT_DATA_WORK_SUBJECT_AREA_SA = "select  \n" +
            "B_LOADID as B_LOADID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",WORK_SUBJECT_AREA_LINK_ID as WORK_SUBJECT_AREA_LINK_ID\n" +
            ",F_SUBJECT_AREA as F_SUBJECT_AREA\n" +
            ",F_WWORK as F_WWORK\n" +
            ",external_reference as external_reference "+
            "from semarchy_eph_mdm.sa_work_subject_area_link \n" +
            "where WORK_SUBJECT_AREA_LINK_ID in (\n" +
            "select mp.numeric_id from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp \n" +
            "left join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_subject_area on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "where  \"PRODUCT_SUBJECT_AREA_ID\" in ('%s')\n" +
            ")\n";

    public static String EXTRACT_DATA_WORK_SUBJECT_AREA_GD = "select  \n" +
            "B_CLASSNAME as B_CLASSNAME\n" +
            ",WORK_SUBJECT_AREA_LINK_ID as WORK_SUBJECT_AREA_LINK_ID\n" +
            ",F_SUBJECT_AREA as F_SUBJECT_AREA\n" +
            ",F_WWORK as F_WWORK\n" +
            ",external_reference as external_reference "+
            "from semarchy_eph_mdm.gd_work_subject_area_link \n" +
            "where WORK_SUBJECT_AREA_LINK_ID in (\n" +
            "select mp.numeric_id from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp \n" +
            "left join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_subject_area on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "where  \"PRODUCT_SUBJECT_AREA_ID\" in ('%s')\n" +
            ")\n" +
            " ";

    public static String SELECT_RANDOM_IDS = "select  \n" +
        "\"PRODUCT_SUBJECT_AREA_ID\" as PRODUCT_SUBJECT_AREA_ID\n" +
        "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_subject_area\t\n" +
        "left join "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
        "left join semarchy_eph_mdm.sa_work_subject_area_link sa on sa.work_subject_area_link_id = mp.numeric_id\n" +
        "where sa.b_error_status is null\n" +
        "order by random()\n" +
        "limit '%s'";

    public static String SELECT_RANDOM_IDS_DELTA = "select  \n" +
            "\"PRODUCT_SUBJECT_AREA_ID\" as PRODUCT_SUBJECT_AREA_ID\n" +
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_subject_area\t\n" +
            "left join "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "left join semarchy_eph_mdm.sa_work_subject_area_link sa on sa.work_subject_area_link_id = mp.numeric_id\n" +
//            "where sa.b_error_status is null and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')\n\n" +
            "where sa.b_error_status is null and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')\n\n" +
            "order by random()\n" +
            "limit '%s'";


    public static String SELECT_DATA_FROM_STG_FOR_CURRENT_RECORD_FROM_SA = "select \"PRODUCT_SUBJECT_AREA_ID\" as PRODUCT_SUBJECT_AREA_ID \n" +
            ",\"F_SUBJECT_AREA\" as F_SUBJECT_AREA  \n" +
            ",\"F_PRODUCT_WORK\" as F_PRODUCT_WORK \n " +
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_subject_area\t\n" +
            "left join "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "left join semarchy_eph_mdm.sa_work_subject_area_link sa on sa.work_subject_area_link_id = mp.numeric_id\n" +
            "where sa.work_subject_area_link_id = '%s'";


    public static String GET_F_SUBJECT_AREA = "select numeric_id as F_SUBJECT_AREA from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid where source_ref = concat('SUBJ_AREA-', '%s')";

    public static String GET_F_WWORK = "select eph_id as F_WWORK from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid where ref_type = 'WORK' and source_ref = '%s'";


}
