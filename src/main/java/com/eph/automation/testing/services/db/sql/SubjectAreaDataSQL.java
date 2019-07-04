package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 17/04/2019
 */
public class SubjectAreaDataSQL {

    public static String SELECT_COUNT_SUBJECT_AREA_PMX = "SELECT count(*) AS count FROM (\n" +
            "SELECT \n" +
            "     SUBJECT_AREA_ID AS PMX_SOURCE_REF\n" +
            "    ,SUBJECT_AREA_CODE\n" +
            "    ,SUBJECT_AREA_NAME\n" +
            "    ,F_PARENT_SUBJECT_AREA AS PARENT_SUBJECT_AREA_REF\n" +
            "    ,'SD' AS SUBJECT_AREA_TYPE\n" +
            "    ,TO_CHAR(NVL(B_UPDDATE,B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM GD_SUBJECT_AREA WHERE F_SUBJECT_AREA_TYPE = 181 --Science Direct Subject Areas\n" +
            "    )";

    public static String SELECT_COUNT_SUBJECT_AREA_STG = "select count(*) as count from "+GetEPHDBUser.getDBUser()+".STG_10_PMX_SUBJECT_AREA";

    public static String SELECT_COUNT_SUBJECT_AREA_STG_Delta = "select count(*) as count from "+GetEPHDBUser.getDBUser()+".STG_10_PMX_SUBJECT_AREA\n"+
            "where TO_DATE(\"UPDATED\",'YYYYMMDDHH24MI') > TO_DATE('PARAM1','YYYYMMDDHH24MI')";

//  old  public static String SELECT_COUNT_SUBJECT_AREA_SA = "select count(*) from semarchy_eph_mdm.sa_subject_area s\n" +
//            " where s.b_loadid =  (select max (s.b_loadid) from\n" +
//            "semarchy_eph_mdm.sa_subject_area s join \n" +
//            "semarchy_eph_mdm.sa_event e on e.b_loadid = s.b_loadid\n" +
//            "where  e.f_event_type = 'PMX'\n" +
//            "and e.workflow_id = 'talend'\n" +
//            "AND e.f_event_type = 'PMX'\n" +
//            "and e.f_workflow_source = 'PMX' )";

    public static String SELECT_COUNT_SUBJECT_AREA_SA =
            "select count(*) from semarchy_eph_mdm.gd_subject_area \n" +
            " where b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event \n" +
            "where  f_event_type = 'PMX'\n" +
            "and workflow_id = 'talend'\n" +
            "AND f_event_type = 'PMX'\n" +
            "and f_workflow_source = 'PMX' )";

    public static String SELECT_COUNT_SUBJECT_AREA_GD = "select count(*) from semarchy_eph_mdm.gd_subject_area s\n" +
            " where b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event e\n" +
            "where  e.f_event_type = 'PMX'\n" +
            "and e.workflow_id = 'talend'\n" +
            "AND e.f_event_type = 'PMX'\n" +
            "and e.f_workflow_source = 'PMX' )";


    public static String EXTRACT_DATA_SUBJECT_AREA_PMX = "SELECT * FROM (\n" +
            "SELECT \n" +
            "     SUBJECT_AREA_ID AS PMX_SOURCE_REF\n" +
            "    ,SUBJECT_AREA_CODE\n" +
            "    ,SUBJECT_AREA_NAME\n" +
            "    ,F_PARENT_SUBJECT_AREA AS PARENT_SUBJECT_AREA_REF\n" +
            "    ,'SD' AS SUBJECT_AREA_TYPE\n" +
            "    ,TO_CHAR(NVL(B_UPDDATE,B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM GD_SUBJECT_AREA WHERE F_SUBJECT_AREA_TYPE = 181 --Science Direct Subject Areas\n" +
            "AND SUBJECT_AREA_ID IN ('%s')\n" +
            "    )";

    public static String EXTRACT_DATA_SUBJECT_AREA_STG = "select \n" +
            "\"PMX_SOURCE_REF\" as PMX_SOURCE_REF\n" +
            ",\"SUBJECT_AREA_CODE\" as SUBJECT_AREA_CODE\n" +
            ",\"SUBJECT_AREA_NAME\" as SUBJECT_AREA_NAME\n" +
            ",\"PARENT_SUBJECT_AREA_REF\" as PARENT_SUBJECT_AREA_REF\n" +
            ",\"SUBJECT_AREA_TYPE\" as SUBJECT_AREA_TYPE\n" +
            ",\"UPDATED\" as UPDATED\n" +
            "from "+GetEPHDBUser.getDBUser()+".STG_10_PMX_SUBJECT_AREA\n" +
            "where \"PMX_SOURCE_REF\" in ('%s')\n";

    public static String EXTRACT_DATA_SUBJECT_AREA_SA = "select\n" +
            "B_LOADID as B_LOADID\n" +
            ",B_CLASSNAME as B_CLASSNAME \n" +
            ",SUBJECT_AREA_ID as SUBJECT_AREA_ID\n" +
            ",CODE as SUBJECT_AREA_CODE\n" +
            ",NAME as SUBJECT_AREA_NAME\n" +
            ",F_TYPE as SUBJECT_AREA_TYPE\n" +
            ",F_PARENT_SUBJECT_AREA as F_PARENT_SUBJECT_AREA\n" +
            "from semarchy_eph_mdm.sa_subject_area\n" +
            "where SUBJECT_AREA_ID in (\n" +
            "select mp.numeric_id from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp\n" +
            "--left join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_subject_area on mp.source_ref = concat('SUBJ_AREA-',\"PMX_SOURCE_REF\") \n" +
            "where \"external_reference\" in ('%s')\n" +
            " and b_loadid =  (select max (s.b_loadid) from\n" +
            "semarchy_eph_mdm.sa_subject_area s join \n" +
            "semarchy_eph_mdm.sa_event e on e.b_loadid = s.b_loadid\n" +
            "where  e.f_event_type = 'PMX'\n" +
            "and e.workflow_id = 'talend'\n" +
            "AND e.f_event_type = 'PMX'\n" +
            "and e.f_workflow_source = 'PMX' )" +
            ")";


    public static String EXTRACT_DATA_SUBJECT_AREA_GD = "select\n" +
            "B_CLASSNAME as B_CLASSNAME \n" +
            ",SUBJECT_AREA_ID as SUBJECT_AREA_ID\n" +
            ",CODE as SUBJECT_AREA_CODE\n" +
            ",NAME as SUBJECT_AREA_NAME\n" +
            ",F_TYPE as SUBJECT_AREA_TYPE\n" +
            ",F_PARENT_SUBJECT_AREA as F_PARENT_SUBJECT_AREA\n" +
            "from semarchy_eph_mdm.gd_subject_area\n" +
            "where SUBJECT_AREA_ID in (\n" +
            "select mp.numeric_id from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp\n" +
            "--left join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_subject_area on mp.source_ref = concat('SUBJ_AREA-',\"PMX_SOURCE_REF\") \n" +
            "where \"external_reference\" in ('%s')\n" +
            ")";

    public static String SELECT_RANDOM_SUBJECT_DATA = "select \n" +
            "\"PMX_SOURCE_REF\" as PMX_SOURCE_REF\n" +
            "from "+GetEPHDBUser.getDBUser()+".STG_10_PMX_SUBJECT_AREA\n" +
            "left join "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp on mp.source_ref = concat('SUBJ_AREA-',\"PMX_SOURCE_REF\")\n" +
            "left join semarchy_eph_mdm.SA_SUBJECT_AREA sa on mp.numeric_id = sa.subject_area_id\n" +
            "where sa.b_error_status is null\n" +
            "order by random()\n" +
            "limit '%s'";

    public static String SELECT_RANDOM_DATA_DELTA = "select \n" +
            "\"PMX_SOURCE_REF\" as PMX_SOURCE_REF\n" +
            "from "+GetEPHDBUser.getDBUser()+".STG_10_PMX_SUBJECT_AREA\n" +
            "left join "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid mp on mp.source_ref = concat('SUBJ_AREA-',\"PMX_SOURCE_REF\")\n" +
            "left join semarchy_eph_mdm.SA_SUBJECT_AREA sa on mp.numeric_id = sa.subject_area_id\n" +
            "where sa.b_error_status is null\n" +
            "and TO_DATE(\"UPDATED\",'YYYYMMDDHH24MI') > TO_DATE('%s','YYYYMMDDHH24MI')\n" +
            "order by random()\n" +
            "limit '%s'";


    public static String GET_F_PARENT_SUBJECT_AREA = "select numeric_id as F_PARENT_SUBJECT_AREA from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid where source_ref = '%s'";

}
