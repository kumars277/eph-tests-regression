package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 17/04/2019
 */
public class SubjectAreaDataSQL {

    public static String SELECT_COUNT_SUBJECT_AREA_PMX = "SELECT count(*) as count FROM (\n" +
            "SELECT \n" +
            "     SUBJECT_AREA_ID AS PMX_SOURCE_REF\n" +
            "    ,SUBJECT_AREA_CODE\n" +
            "    ,SUBJECT_AREA_NAME\n" +
            "    ,F_PARENT_SUBJECT_AREA AS PARENT_SUBJECT_AREA_REF\n" +
            "    ,'SD' AS SUBJECT_AREA_TYPE \n" +
            "FROM GD_SUBJECT_AREA WHERE F_SUBJECT_AREA_TYPE = 181 --Science Direct Subject Areas\n" +
            "    )\n";

    public static String SELECT_COUNT_SUBJECT_AREA_STG = "select count(*) as count from ephsit_talend_owner.STG_10_PMX_SUBJECT_AREA";

    public static String SELECT_COUNT_SUBJECT_AREA_SA = "select count(*) as count from semarchy_eph_mdm.sa_subject_area";

    public static String SELECT_COUNT_SUBJECT_AREA_GD = "select count(*) as count from semarchy_eph_mdm.gd_subject_area";


    public static String EXTRACT_DATA_SUBJECT_AREA_PMX = "SELECT * FROM (\n" +
            "SELECT \n" +
            "     SUBJECT_AREA_ID AS PMX_SOURCE_REF\n" +
            "    ,SUBJECT_AREA_CODE AS SUBJECT_AREA_CODE\n" +
            "    ,SUBJECT_AREA_NAME AS SUBJECT_AREA_NAME\n" +
            "    ,F_PARENT_SUBJECT_AREA AS PARENT_SUBJECT_AREA_REF\n" +
            "    ,'SD' AS SUBJECT_AREA_TYPE \n" +
            "FROM GD_SUBJECT_AREA WHERE F_SUBJECT_AREA_TYPE = 181 --Science Direct Subject Areas\n" +
            "AND SUBJECT_AREA_ID IN ('%s')\n" +
            "    )";

    public static String EXTRACT_DATA_SUBJECT_AREA_STG = "select \n" +
            "\"PMX_SOURCE_REF\" as PMX_SOURCE_REF\n" +
            ",\"SUBJECT_AREA_CODE\" as SUBJECT_AREA_CODE\n" +
            ",\"SUBJECT_AREA_NAME\" as SUBJECT_AREA_NAME\n" +
            ",\"PARENT_SUBJECT_AREA_REF\" as PARENT_SUBJECT_AREA_REF\n" +
            ",\"SUBJECT_AREA_TYPE\" as SUBJECT_AREA_TYPE\n" +
            "from ephsit_talend_owner.STG_10_PMX_SUBJECT_AREA\n" +
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
            "select mp.numeric_id from ephsit_talend_owner.map_sourceref_2_numericid mp\n" +
            "left join ephsit_talend_owner.stg_10_pmx_subject_area on mp.source_ref = concat('SUBJ_AREA-',\"PMX_SOURCE_REF\") \n" +
            "where \"PMX_SOURCE_REF\" in ('%s')\n" +
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
            "select mp.numeric_id from ephsit_talend_owner.map_sourceref_2_numericid mp\n" +
            "left join ephsit_talend_owner.stg_10_pmx_subject_area on mp.source_ref = concat('SUBJ_AREA-',\"PMX_SOURCE_REF\") \n" +
            "where \"PMX_SOURCE_REF\" in ('%s')\n" +
            ")";

    public static String SELECT_RANDOM_SUBJECT_DATA = "select \n" +
            "\"PMX_SOURCE_REF\" as PMX_SOURCE_REF\n" +
            "from ephsit_talend_owner.STG_10_PMX_SUBJECT_AREA\n" +
            "left join ephsit_talend_owner.map_sourceref_2_numericid mp on mp.source_ref = concat('SUBJ_AREA-',\"PMX_SOURCE_REF\")\n" +
            "left join semarchy_eph_mdm.SA_SUBJECT_AREA sa on mp.numeric_id = sa.subject_area_id\n" +
            "where sa.b_error_status is null\n" +
            "order by random()\n" +
            "limit '%s'";


    public static String GET_F_PARENT_SUBJECT_AREA = "select numeric_id as F_PARENT_SUBJECT_AREA from ephsit_talend_owner.map_sourceref_2_numericid where source_ref = concat('SUBJ_AREA-', '%s')";

}
