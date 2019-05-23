package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 03/04/2019
 */
public class AccountableProductSQL {
    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_PMX = "SELECT\n" +
            "    distinct count(*) as count\n" +
            "    FROM\n" +
            "        GD_PRODUCT_WORK W\n" +
            "    JOIN\n" +
            "        GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "    JOIN\n" +
            "        GD_PRODUCT_GROUP_TYPE G ON T.F_PRODUCT_GROUP_TYPE = G.PRODUCT_GROUP_TYPE_ID\n" +
            "    LEFT JOIN\n" +
            "        (SELECT DISTINCT AC.ELSEVIER_PRODUCT_ID, AC.ELSEVIER_PRODUCT_NAME, M.F_PRODUCT_WORK, M.ISBN\n" +
            "         FROM   GD_PRODUCT_MANIFESTATION M\n" +
            "         JOIN   GD_ACCOUNTING_CLASS AC ON M.F_ACCOUNTING_CLASS = AC.ACCOUNTING_CLASS_CODE) A ON W.PRODUCT_WORK_ID = A.F_PRODUCT_WORK AND W.MASTER_ISBN = A.ISBN\n" +
            "    WHERE\n" +
            "        T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES')";

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_THAT_WILL_BE_PROCESSED_TO_SA = "select count(*) from \n" +
            "(select distinct\n" +
            "\"ACC_PROD_ID\"\n" +
            ",\"ACC_PROD_NAME\"\n" +
            ",\"PARENT_ACC_PROD\"\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_accountable_product) A " ;

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_THAT_WILL_BE_PROCESSED_TO_SA_DELTA = "select count(*) from \n" +
            "(select distinct\n" +
            "\"ACC_PROD_ID\"\n" +
            ",\"ACC_PROD_NAME\"\n" +
            ",\"PARENT_ACC_PROD\"\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_accountable_product) A \n" +
            "where TO_DATE(\"UPDATED\",'DD-MON-YY HH.MI.SS') > TO_DATE('%s','YYYYMMDDHH24MI')";

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_FROM_PMX = "select count(*) from "+ GetEPHDBUser.getDBUser() +".stg_10_pmx_accountable_product";

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_FROM_PMX_DELTA = "select count(*) from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_accountable_product \n" +
            "where TO_DATE(\"UPDATED\",'DD-MON-YY HH.MI.SS') > TO_DATE('%s','YYYYMMDDHH24MI')";

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_SA = "select \n" +
            " distinct count(*) as count\n" +
            "from semarchy_eph_mdm.sa_accountable_product sa\n" +
            "where sa.b_loadid =  (\n" +
            "select max (sa1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_accountable_product sa1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa2 on sa2.b_loadid = sa1.b_loadid \n" +
            "where  sa2.f_event_type = 'PMX'\n" +
            "and sa2.workflow_id = 'talend'\n" +
            "and sa2.f_workflow_source = 'PMX' )";

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_GD = "select \n" +
            "distinct count(*)\n" +
            "from semarchy_eph_mdm.gd_accountable_product";

    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_PMX = "SELECT\n" +
            "         W.PRODUCT_WORK_ID\n" +
            "        ,W.ELSEVIER_PRODUCT_ID AS WORK_ELS_PROD_ID\n" +
            "        ,A.ELSEVIER_PRODUCT_ID AS AC_ELS_PROD_ID\n" +
            "        ,G.PRODUCT_GROUP_TYPE_NAME\n" +
            "        ,SUBSTR(A.ELSEVIER_PRODUCT_NAME,0,LENGTH(A.ELSEVIER_PRODUCT_NAME)-6) AS AC_PROD_NAME\n" +
            "        ,W.PRODUCT_WORK_TITLE\n" +
            "        ,W.F_ACC_PROD_HIERARCHY\n" +
            "        ,TO_CHAR(NVL(NVL(W.B_UPDDATE,W.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY'))) AS W_UPDATED\n" +
            "        ,TO_CHAR(NVL(NVL(A.B_UPDDATE,A.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY'))) AS A_UPDATED\n" +
            "    FROM\n" +
            "        GD_PRODUCT_WORK W\n" +
            "    JOIN\n" +
            "        GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "    JOIN\n" +
            "        GD_PRODUCT_GROUP_TYPE G ON T.F_PRODUCT_GROUP_TYPE = G.PRODUCT_GROUP_TYPE_ID\n" +
            "    LEFT JOIN\n" +
            "        (SELECT DISTINCT AC.ELSEVIER_PRODUCT_ID, AC.ELSEVIER_PRODUCT_NAME, M.F_PRODUCT_WORK, M.ISBN, M.B_UPDDATE, M.B_CREDATE\n" +
            "         FROM   GD_PRODUCT_MANIFESTATION M\n" +
            "         JOIN   GD_ACCOUNTING_CLASS AC ON M.F_ACCOUNTING_CLASS = AC.ACCOUNTING_CLASS_CODE) A ON W.PRODUCT_WORK_ID = A.F_PRODUCT_WORK AND W.MASTER_ISBN = A.ISBN\n" +
            "    WHERE\n" +
            "        T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES'))\n" +
            "         AND W.PRODUCT_WORK_ID IN ('%s')\n";



    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_STG = "select \n" +
            "\"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID\n" +
            ",\"ACC_PROD_ID\" as ACC_PROD_ID\n" +
            ",\"ACC_PROD_NAME\" as ACC_PROD_NAME\n" +
            ",\"PARENT_ACC_PROD\" as PARENT_ACC_PROD\n" +
            ",\"PRODUCT_GROUP_TYPE_NAME\" as PRODUCT_GROUP_TYPE_NAME\n" +
            ",\"UPDATED\" as UPDATED\n" +
            "from " + GetEPHDBUser.getDBUser() +".STG_10_PMX_ACCOUNTABLE_PRODUCT\n" +
            "where \"PRODUCT_WORK_ID\" in ('%s')";



    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_DQ = "select \n" +
            "PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE\n" +
            ",ACC_PROD_ID as ACC_PROD_ID\n" +
            ",ACC_PROD_NAME as ACC_PROD_NAME\n" +
            ",PARENT_ACC_PROD as PARENT_ACC_PROD\n" +
            ",DQ_ERR as DQ_ERR\n" +
            "from " + GetEPHDBUser.getDBUser() +".stg_10_pmx_accountable_product_dq\n" +
            "where PMX_SOURCE_REFERENCE in ('%s')";

    public static String SELECT_IDS_STG = "select \n" +
            "\"ACC_PROD_ID\" as ACC_PROD_ID\n" +
            "from " + GetEPHDBUser.getDBUser() +".STG_10_PMX_ACCOUNTABLE_PRODUCT\n" +
            "where \"PRODUCT_WORK_ID\" in ('%s')";

    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_SA = "select \n" +
            "B_LOADID as B_LOADID\n" +
            ",F_EVENT as F_EVENT\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",ACCOUNTABLE_PRODUCT_ID as ACCOUNTABLE_PRODUCT_ID\n" +
            ",GL_PRODUCT_SEGMENT_CODE as GL_PRODUCT_SEGMENT_CODE\n" +
            ",GL_PRODUCT_SEGMENT_NAME as GL_PRODUCT_SEGMENT_NAME\n" +
            ",F_GL_PRODUCT_SEGMENT_PARENT as F_GL_PRODUCT_SEGMENT_PARENT\n" +
            "from semarchy_eph_mdm.sa_accountable_product sa\n" +
            "where sa.b_loadid =  (\n" +
            "select max (sa1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_accountable_product sa1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa2 on sa2.b_loadid = sa1.b_loadid \n" +
            "where  sa2.f_event_type = 'PMX'\n" +
            "and sa2.workflow_id = 'talend'\n" +
            "and sa2.f_workflow_source = 'PMX' )\n" +
            "and ACCOUNTABLE_PRODUCT_ID in ('%s')";

    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_GD = "select \n" +
            "F_EVENT as F_EVENT\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",ACCOUNTABLE_PRODUCT_ID as ACCOUNTABLE_PRODUCT_ID\n" +
            ",GL_PRODUCT_SEGMENT_CODE as GL_PRODUCT_SEGMENT_CODE\n" +
            ",GL_PRODUCT_SEGMENT_NAME as GL_PRODUCT_SEGMENT_NAME\n" +
            ",F_GL_PRODUCT_SEGMENT_PARENT as F_GL_PRODUCT_SEGMENT_PARENT\n" +
            "from semarchy_eph_mdm.gd_accountable_product \n" +
            "where ACCOUNTABLE_PRODUCT_ID in ('%s')";


    public static String SELЕCT_UPDATED_VALUE = "SELECT\n" +
            "        GREATEST(TO_CHAR(NVL(NVL(W.B_UPDDATE,W.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY'))) \n" +
            "        ,TO_CHAR(NVL(NVL(A.B_UPDDATE,A.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY'))) ) AS UPDATED\n" +
            "    FROM\n" +
            "        GD_PRODUCT_WORK W\n" +
            "    JOIN\n" +
            "        GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "    JOIN\n" +
            "        GD_PRODUCT_GROUP_TYPE G ON T.F_PRODUCT_GROUP_TYPE = G.PRODUCT_GROUP_TYPE_ID\n" +
            "    LEFT JOIN\n" +
            "        (SELECT DISTINCT AC.ELSEVIER_PRODUCT_ID, AC.ELSEVIER_PRODUCT_NAME, M.F_PRODUCT_WORK, M.ISBN, M.B_UPDDATE, M.B_CREDATE\n" +
            "         FROM   GD_PRODUCT_MANIFESTATION M\n" +
            "         JOIN   GD_ACCOUNTING_CLASS AC ON M.F_ACCOUNTING_CLASS = AC.ACCOUNTING_CLASS_CODE) A ON W.PRODUCT_WORK_ID = A.F_PRODUCT_WORK AND W.MASTER_ISBN = A.ISBN\n" +
            "    WHERE\n" +
            "        T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES')\n" +
            "        and w.PRODUCT_WORK_ID IN ('%s')";

    public static String GET_RANDOM_ACCOUNTABLE_PRODUCT_IDS_FROM_SA = "select \n" +
            "ACCOUNTABLE_PRODUCT_ID as ACCOUNTABLE_PRODUCT_ID\n" +
            "from semarchy_eph_mdm.sa_accountable_product sa\n" +
            "where sa.b_loadid =  (\n" +
            "select max (sa1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_accountable_product sa1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa2 on sa2.b_loadid = sa1.b_loadid \n" +
            "where  sa2.f_event_type = 'PMX'\n" +
            "and sa2.workflow_id = 'talend'\n" +
            "and sa2.f_workflow_source = 'PMX')\n" +
            "and B_ERROR_STATUS is null\n" +
            "order by random() limit '%s'";

    public static String GET_RANDOM_IDS_FROM_STG = "select \n" +
            "\"ACC_PROD_ID\" as ACC_PROD_ID\n" +
            "from " + GetEPHDBUser.getDBUser() + ".STG_10_PMX_ACCOUNTABLE_PRODUCT pp\n" +
            "left join " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid mp on mp.source_ref = concat(pp.\"ACC_PROD_ID\", pp.\"PARENT_ACC_PROD\")\n" +
            "left join semarchy_eph_mdm.sa_accountable_product sa on sa.ACCOUNTABLE_PRODUCT_ID = mp.numeric_id\n" +
            "where sa.b_error_status is null \n" +
            "order by random() limit '%s'";

    public static String GET_RANDOM_WORK_IDS_FROM_STG = "select \n" +
            "s.\"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID\n" +
            "from " + GetEPHDBUser.getDBUser() +".stg_10_pmx_accountable_product s\n" +
            "left join " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid m on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = m.source_ref\n" +
            "left join semarchy_eph_mdm.sa_accountable_product g on m.numeric_id = g.accountable_product_id\n" +
            "where not (\n" +
            "        coalesce(g.gl_product_segment_code,'') = s.\"ACC_PROD_ID\" and\n" +
            "        coalesce(g.gl_product_segment_name,'') = s.\"ACC_PROD_NAME\" and\n" +
            "        coalesce(g.f_gl_product_segment_parent,'') = s.\"PARENT_ACC_PROD\")\n" +
            "and g.b_error_status is null \n" +
            "order by random() limit '%s'";

    public static String GET_NUMERIC_ID_FROM_LOOKUP_AP = "select numeric_id as NUMERIC_ID from " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid where source_ref in ('%s')";
}
