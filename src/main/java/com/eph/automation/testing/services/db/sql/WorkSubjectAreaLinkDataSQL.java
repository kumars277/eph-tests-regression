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
            "FROM GD_PRODUCT_SUBJECT_AREA PSA \n" +
            "JOIN GD_SUBJECT_AREA SA ON F_SUBJECT_AREA = SUBJECT_AREA_ID\n " +
            "WHERE PSA.F_PRODUCT_WORK IN \n" +
            "    (SELECT PRODUCT_WORK_ID FROM GD_PRODUCT_WORK WHERE F_PRODUCT_TYPE IN \n" +
            "        (SELECT PRODUCT_TYPE_ID FROM GD_PRODUCT_TYPE WHERE F_PRODUCT_GROUP_TYPE = 2\n" +
            "            )\n" +
            "        )\n" +
            "AND PSA.EFFTO_DATE IS NULL\n" +
            "AND SA.F_SUBJECT_AREA_TYPE = 181\n" +
            "    ) ";

    public static String SELECT_COUNT_WORK_SUBJECT_AREA_STG = "select count(*) as count from ephsit_talend_owner.STG_10_PMX_WORK_SUBJECT_AREA";

    public static String SELECT_COUNT_WORK_SUBJECT_AREA_SA = "select count(*) as count from semarchy_eph_mdm.sa_work_subject_area_link";

    public static String SELECT_COUNT_WORK_SUBJECT_AREA_GD = "select count(*) as count from semarchy_eph_mdm.gd_work_subject_area_link";

    public static String EXTRACT_DATA_WORK_SUBJECT_AREA_PMX = "SELECT * FROM (\n" +
            "SELECT\n" +
            "     PSA.PRODUCT_SUBJECT_AREA_ID\n" +
            "    ,PSA.F_SUBJECT_AREA\n" +
            "    ,PSA.F_PRODUCT_WORK\n" +
            "FROM GD_PRODUCT_SUBJECT_AREA  PSA\n" +
            "JOIN GD_SUBJECT_AREA SA ON F_SUBJECT_AREA = SUBJECT_AREA_ID\n" +
            "WHERE PSA.F_PRODUCT_WORK IN \n" +
            "    (SELECT PRODUCT_WORK_ID FROM GD_PRODUCT_WORK WHERE F_PRODUCT_TYPE IN \n" +
            "        (SELECT PRODUCT_TYPE_ID FROM GD_PRODUCT_TYPE WHERE F_PRODUCT_GROUP_TYPE = 2\n" +
            "            )\n" +
            "        )\n" +
            "AND PSA.EFFTO_DATE IS NULL\n" +
            "AND SA.F_SUBJECT_AREA_TYPE = 181\n" +
            "    ) WHERE PRODUCT_SUBJECT_AREA_ID IN ('%s')";

    public static String EXTRACT_DATA_WORK_SUBJECT_AREA_STG = "select  \n" +
            "\"PRODUCT_SUBJECT_AREA_ID\" as PRODUCT_SUBJECT_AREA_ID\n" +
            ",\"F_SUBJECT_AREA\" as F_SUBJECT_AREA\n" +
            ",\"F_PRODUCT_WORK\" as F_PRODUCT_WORK\n" +
            "from ephsit_talend_owner.stg_10_pmx_work_subject_area\t\n" +
            "left join ephsit_talend_owner.map_sourceref_2_numericid mp on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "left join semarchy_eph_mdm.sa_work_subject_area_link sa on sa.work_subject_area_link_id = mp.numeric_id\n" +
            "where sa.b_error_status is null\n" +
            "and \"PRODUCT_SUBJECT_AREA_ID\" in ('%s')\n";

    public static String EXTRACT_DATA_WORK_SUBJECT_AREA_SA = "select  \n" +
            "B_LOADID as B_LOADID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",WORK_SUBJECT_AREA_LINK_ID as WORK_SUBJECT_AREA_LINK_ID\n" +
            ",F_SUBJECT_AREA as F_SUBJECT_AREA\n" +
            ",F_WWORK as F_WWORK\n" +
            "from semarchy_eph_mdm.sa_work_subject_area_link \n" +
            "where WORK_SUBJECT_AREA_LINK_ID in (\n" +
            "select mp.numeric_id from ephsit_talend_owner.map_sourceref_2_numericid mp \n" +
            "left join ephsit_talend_owner.stg_10_pmx_work_subject_area on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "where  \"PRODUCT_SUBJECT_AREA_ID\" in ('%s')\n" +
            ")\n" +
            " ";

    public static String EXTRACT_DATA_WORK_SUBJECT_AREA_GD = "select  \n" +
            "B_LOADID as B_LOADID\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",WORK_SUBJECT_AREA_LINK_ID as WORK_SUBJECT_AREA_LINK_ID\n" +
            ",F_SUBJECT_AREA as F_SUBJECT_AREA\n" +
            ",F_WWORK as F_WWORK\n" +
            "from semarchy_eph_mdm.gd_work_subject_area_link \n" +
            "where WORK_SUBJECT_AREA_LINK_ID in (\n" +
            "select mp.numeric_id from ephsit_talend_owner.map_sourceref_2_numericid mp \n" +
            "left join ephsit_talend_owner.stg_10_pmx_work_subject_area on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "where  \"PRODUCT_SUBJECT_AREA_ID\" in ('%s')\n" +
            ")\n" +
            " ";

    public static String SELECT_RANDOM_IDS = "select  \n" +
        "\"PRODUCT_SUBJECT_AREA_ID\" as PRODUCT_SUBJECT_AREA_ID\n" +
        "from ephsit_talend_owner.stg_10_pmx_work_subject_area\t\n" +
        "left join ephsit_talend_owner.map_sourceref_2_numericid mp on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
        "left join semarchy_eph_mdm.sa_work_subject_area_link sa on sa.work_subject_area_link_id = mp.numeric_id\n" +
        "where sa.b_error_status is null\n" +
        "order by random()\n" +
        "limit '%s'";

    public static String SELECT_DATA_FROM_STG_FOR_CURRENT_RECORD_FROM_SA = "select *\n" +
            "from ephsit_talend_owner.stg_10_pmx_work_subject_area\t\n" +
            "left join ephsit_talend_owner.map_sourceref_2_numericid mp on mp.source_ref = concat('WORK_SUBJ_AREA-',\"PRODUCT_SUBJECT_AREA_ID\")\n" +
            "left join semarchy_eph_mdm.sa_work_subject_area_link sa on sa.work_subject_area_link_id = mp.numeric_id\n" +
            "where sa.work_subject_area_link_id = '%s'";


    public static String GET_F_SUBJECT_AREA = "select numeric_id as F_SUBJECT_AREA from ephsit_talend_owner.map_sourceref_2_numericid where source_ref = concat('SUBJ_AREA-', '%s')";

    public static String GET_F_WWORK = "select eph_id as F_WWORK from ephsit_talend_owner.map_sourceref_2_ephid where ref_type = 'WORK' and source_ref = '%s'";


}
