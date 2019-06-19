package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 03/04/2019
 */
public class AccountableProductSQL {
    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_PMX = "SELECT count(*) as count\n" +
            "FROM \n" +
            "(SELECT DISTINCT\n" +
            "     PRODUCT_WORK_ID\n" +
            "    ,CASE WHEN PRODUCT_GROUP_TYPE_NAME = 'Journals' THEN WORK_ELS_PROD_ID ELSE AC_ELS_PROD_ID END AS ACC_PROD_ID\n" +
            "    ,CASE WHEN PRODUCT_GROUP_TYPE_NAME = 'Journals' THEN PRODUCT_WORK_TITLE ELSE AC_PROD_NAME END AS ACC_PROD_NAME\n" +
            "    ,CASE WHEN PRODUCT_GROUP_TYPE_NAME = 'Journals' THEN F_ACC_PROD_HIERARCHY\n" +
            "    \t  WHEN AC_ELS_PROD_ID = '1000000' THEN 'PBKSOTH'\n" +
            "    \t  WHEN AC_ELS_PROD_ID = '1000001' THEN 'PBKSTEX'\n" +
            "    \t  WHEN AC_ELS_PROD_ID = '1000002' THEN 'PBKSSER'\n" +
            "    \t  WHEN AC_ELS_PROD_ID = '1000003' THEN 'PBKSMRW'\n" +
            "    \t  WHEN AC_ELS_PROD_ID = '1000004' THEN 'PBKSREF'\n" +
            "    \t  ELSE NULL END AS PARENT_ACC_PROD\n" +
            "    ,PRODUCT_GROUP_TYPE_NAME\n" +
            "    ,GREATEST(W_UPDATED, A_UPDATED) AS UPDATED\n" +
            "FROM  \n" +
            "(SELECT\n" +
            "         W.PRODUCT_WORK_ID\n" +
            "        ,W.ELSEVIER_PRODUCT_ID AS WORK_ELS_PROD_ID\n" +
            "        ,A.ELSEVIER_PRODUCT_ID AS AC_ELS_PROD_ID\n" +
            "        ,G.PRODUCT_GROUP_TYPE_NAME\n" +
            "        ,SUBSTR(A.ELSEVIER_PRODUCT_NAME,0,LENGTH(A.ELSEVIER_PRODUCT_NAME)-6) AS AC_PROD_NAME\n" +
            "        ,W.PRODUCT_WORK_TITLE\n" +
            "        ,W.F_ACC_PROD_HIERARCHY\n" +
            "        ,TO_CHAR(NVL(NVL(W.B_UPDDATE,W.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY')),'YYYYMMDDHH24MI') AS W_UPDATED\n" +
            "        ,TO_CHAR(NVL(NVL(A.B_UPDDATE,A.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY')),'YYYYMMDDHH24MI') AS A_UPDATED\n" +
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
            "        T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES') ) ACC_PROD) \n" +
            "        WHERE ACC_PROD_ID IS NOT NULL\n" +
            "AND   PARENT_ACC_PROD IS NOT NULL\n" +
            "AND   ACC_PROD_NAME IS NOT NULL\n";

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

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_FROM_PMX = "select count(*) as count from "+ GetEPHDBUser.getDBUser() +".stg_10_pmx_accountable_product";

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_GOING_TO_DQ = "select count(*) as count from  " + GetEPHDBUser.getDBUser() +".stg_10_pmx_accountable_product s \n" +
            "left join semarchy_eph_mdm.gd_accountable_product g on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = g.external_reference";


    /*
    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_GOING_TO_DQ = "select count(distinct s.\"PRODUCT_WORK_ID\" ) as count \n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_accountable_product s\n" +
            "left join " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid m on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = m.source_ref\n" +
            "left join semarchy_eph_mdm.gd_accountable_product g on m.numeric_id = g.accountable_product_id\n" +
            "where not (\n" +
            "        coalesce(g.gl_product_segment_code,'') = s.\"ACC_PROD_ID\" and\n" +
            "        coalesce(g.gl_product_segment_name,'') = s.\"ACC_PROD_NAME\" and\n" +
            "        coalesce(g.f_gl_product_segment_parent,'') = s.\"PARENT_ACC_PROD\")"; */   //old logic


    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_DQ = "select count(*) as count from "+ GetEPHDBUser.getDBUser() +".stg_10_pmx_accountable_product_dq";


    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_DQ_GOING_TO_SA = "select count(*)  as count from \n" +
            " (select distinct\n" +
            " s.pmx_source_reference\n" +
            ",s.acc_prod_id\n" +
            ",s.acc_prod_name\n" +
            ",s.parent_acc_prod\n" +
            "from  " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_accountable_product_dq s\n" +
            "where dq_err != 'Y') A";

    //public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_DQ_GOING_TO_SA = "select count(*) as count from "+ GetEPHDBUser.getDBUser() +".stg_10_pmx_accountable_product_dq where dq_err != 'Y'";

    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_FROM_PMX_DELTA = "select count(*) as count from  "+ GetEPHDBUser.getDBUser() +".stg_10_pmx_accountable_product s \n" +
            "left join semarchy_eph_mdm.gd_accountable_product g on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = g.external_reference \n" +
            "where  TO_DATE(\"UPDATED\",'YYYYMMDDHH24MI') > TO_DATE('%s','YYYYMMDDHH24MI')";

    /*
    public static String SELECT_COUNT_ACCOUNTABLE_PRODUCT_STG_FROM_PMX_DELTA = "select count(distinct s.\"PRODUCT_WORK_ID\" ) as count \n" +
            "from ephsit_talend_owner.stg_10_pmx_accountable_product s\n" +
            "left join ephsit_talend_owner.map_sourceref_2_numericid m on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = m.source_ref\n" +
            "left join semarchy_eph_mdm.gd_accountable_product g on m.numeric_id = g.accountable_product_id\n" +
            "where not (\n" +
            "        coalesce(g.gl_product_segment_code,'') = s.\"ACC_PROD_ID\" and\n" +
            "        coalesce(g.gl_product_segment_name,'') = s.\"ACC_PROD_NAME\" and\n" +
            "        coalesce(g.f_gl_product_segment_parent,'') = s.\"PARENT_ACC_PROD\")\n" +
            "where TO_DATE(\"UPDATED\",'YYYYMMDDHH24MI') >= TO_DATE('%s','YYYYMMDDHH24MI')";  */

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
            "count(*)\n" +
            "from semarchy_eph_mdm.gd_accountable_product where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX')";

    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_PMX = "SELECT\n" +
            "         W.PRODUCT_WORK_ID as PRODUCT_WORK_ID \n" +
            "        ,W.ELSEVIER_PRODUCT_ID AS WORK_ELS_PROD_ID\n" +
            "        ,A.ELSEVIER_PRODUCT_ID AS AC_ELS_PROD_ID\n" +
            "        ,G.PRODUCT_GROUP_TYPE_NAME\n" +
            "        ,SUBSTR(A.ELSEVIER_PRODUCT_NAME,0,LENGTH(A.ELSEVIER_PRODUCT_NAME)-6) AS ACC_PROD_NAME\n" +
            "        ,W.PRODUCT_WORK_TITLE\n" +
            "        ,W.F_ACC_PROD_HIERARCHY as ACC_PROD_HIERARHY\n" +
            "        ,TO_CHAR(NVL(NVL(W.B_UPDDATE,W.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY')),'YYYYMMDDHH24MI') AS W_UPDATED\n" +
            "        ,TO_CHAR(NVL(NVL(A.B_UPDDATE,A.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY')),'YYYYMMDDHH24MI') AS A_UPDATED\n" +
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
            " AND W.PRODUCT_WORK_ID IN ('%s')";



    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_STG = "select \n" +
            "\"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID\n" +
            ",\"ACC_PROD_ID\" as ACC_PROD_ID\n" +
            ",\"ACC_PROD_NAME\" as ACC_PROD_NAME\n" +
            ",\"PARENT_ACC_PROD\" as PARENT_ACC_PROD\n" +
            ",\"PRODUCT_GROUP_TYPE_NAME\" as PRODUCT_GROUP_TYPE_NAME\n" +
            ",\"UPDATED\" as UPDATED\n" +
            "from " + GetEPHDBUser.getDBUser() +".STG_10_PMX_ACCOUNTABLE_PRODUCT\n" +
            "where concat(\"ACC_PROD_ID\",\"PARENT_ACC_PROD\") in ('%s')";

//
//
    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_DQ = "select\n" +
            "distinct\n" +
            "concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") as pmx_source_reference\n" +
            ",s.\"ACC_PROD_ID\" as acc_prod_id\n" +
            ",s.\"ACC_PROD_NAME\" as acc_prod_name\n" +
            ",s.\"PARENT_ACC_PROD\" as parent_acc_prod\n" +
            ",'N' as dq_err\n" +
            ",\"PRODUCT_WORK_ID\" as product_work_id\n" +
            "from ephsit_talend_owner.stg_10_pmx_accountable_product s\n" +
            "left join semarchy_eph_mdm.gd_accountable_product g on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = g.external_reference\n" +
            "and not (\n" +
            "        coalesce(g.gl_product_segment_code,'') = s.\"ACC_PROD_ID\" and\n" +
            "        coalesce(g.gl_product_segment_name,'') = s.\"ACC_PROD_NAME\" and\n" +
            "        coalesce(g.f_gl_product_segment_parent,'') = s.\"PARENT_ACC_PROD\")\n" +
            "where concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") in ('%s')";

//    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_DQ ="select \n" +
//            "PRODUCT_WORK_ID as PRODUCT_WORK_ID\n" +
//            ",PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE\n" +
//            ",ACC_PROD_ID as ACC_PROD_ID\n" +
//            ",ACC_PROD_NAME as ACC_PROD_NAME\n" +
//            ",PARENT_ACC_PROD as PARENT_ACC_PROD\n" +
//            ",DQ_ERR as DQ_ERR\n" +
//            "from ephsit_talend_owner.stg_10_pmx_accountable_product_dq\n" +
//            "left join  (select distinct external_reference, accountable_product_id from semarchy_eph_mdm.sa_accountable_product) a\n" +
//            " on STG_10_PMX_ACCOUNTABLE_PRODUCT_DQ.pmx_source_reference = a.external_reference\n" +
//            "where dq_err != 'Y' and PRODUCT_WORK_ID in ('%s')";

    public static String SELECT_IDS_STG =    "select \n"+
            "\"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID\n"+
            "from " + GetEPHDBUser.getDBUser() +".STG_10_PMX_ACCOUNTABLE_PRODUCT\n"+
            "where concat(\"ACC_PROD_ID\",\"PARENT_ACC_PROD\") in ('%s')";

    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_SA = "select distinct \n" +
            "B_LOADID as B_LOADID\n" +
            ",F_EVENT as F_EVENT\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",ACCOUNTABLE_PRODUCT_ID as ACCOUNTABLE_PRODUCT_ID\n" +
            ",GL_PRODUCT_SEGMENT_CODE as GL_PRODUCT_SEGMENT_CODE\n" +
            ",GL_PRODUCT_SEGMENT_NAME as GL_PRODUCT_SEGMENT_NAME\n" +
            ",F_GL_PRODUCT_SEGMENT_PARENT as F_GL_PRODUCT_SEGMENT_PARENT\n" +
            ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
            "from semarchy_eph_mdm.sa_accountable_product sa\n" +
            "where sa.b_loadid =  (\n" +
            "select max (sa1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_accountable_product sa1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa2 on sa2.b_loadid = sa1.b_loadid \n" +
            "where  sa2.f_event_type = 'PMX'\n" +
            "and sa2.workflow_id = 'talend'\n" +
            "and sa2.f_workflow_source = 'PMX' )\n" +
            "and EXTERNAL_REFERENCE in ('%s')";

    public static String SELECT_DATA_ACCOUNTABLE_PRODUCT_GD = "select \n" +
            "F_EVENT as F_EVENT\n" +
            ",B_CLASSNAME as B_CLASSNAME\n" +
            ",ACCOUNTABLE_PRODUCT_ID as ACCOUNTABLE_PRODUCT_ID\n" +
            ",GL_PRODUCT_SEGMENT_CODE as GL_PRODUCT_SEGMENT_CODE\n" +
            ",GL_PRODUCT_SEGMENT_NAME as GL_PRODUCT_SEGMENT_NAME\n" +
            ",F_GL_PRODUCT_SEGMENT_PARENT as F_GL_PRODUCT_SEGMENT_PARENT\n" +
            ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
            "from semarchy_eph_mdm.gd_accountable_product \n" +
            "where EXTERNAL_REFERENCE in ('%s')";


    public static String SELЕCT_UPDATED_VALUE = "SELECT GREATEST(W_UPDATED,A_UPDATED) AS UPDATED from \n" +
            "(SELECT\n" +
            "        TO_CHAR(NVL(NVL(W.B_UPDDATE,W.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY')),'YYYYMMDDHH24MI') AS W_UPDATED \n" +
            "        ,TO_CHAR(NVL(NVL(A.B_UPDDATE,A.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY')),'YYYYMMDDHH24MI') AS A_UPDATED \n" +
            "        ,w.PRODUCT_WORK_ID AS PRODUCT_WORK_ID\n" +
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
            "        where PRODUCT_WORK_ID IN ('%s')";

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


//    public static String GET_RANDOM_WORK_IDS_FROM_STG = "select  \"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID,\n" +
//            "concat(STG_10_PMX_ACCOUNTABLE_PRODUCT.\"ACC_PROD_ID\",STG_10_PMX_ACCOUNTABLE_PRODUCT.\"PARENT_ACC_PROD\") as PMX_SOURCE_REFERENCE\n" +
//            "from ephsit_talend_owner.stg_10_pmx_accountable_product \n" +
//            " where  concat(\"ACC_PROD_ID\",\"PARENT_ACC_PROD\")  in \n" +
//            "(select distinct concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") from  ephsit_talend_owner.stg_10_pmx_accountable_product s\n" +
//            "join semarchy_eph_mdm.sa_accountable_product sa on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = sa.external_reference\n" +
//            "where sa.b_error_status is null)\n" +
//            "order by random() limit '%s'";

    public static String GET_RANDOM_WORK_IDS_FROM_STG = "select  \"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID,\n" +
            "concat(\"ACC_PROD_ID\",\"PARENT_ACC_PROD\") as PMX_SOURCE_REFERENCE\n" +
            "from ephsit_talend_owner.stg_10_pmx_accountable_product s\n" +
            "join ephsit_talend_owner.STG_10_PMX_ACCOUNTABLE_PRODUCT_DQ dq on \"PRODUCT_WORK_ID\" = dq.PRODUCT_WORK_ID\n" +
            "left join semarchy_eph_mdm.gd_accountable_product g on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = g.external_reference\n" +
            "and not (\n" +
            "        coalesce(g.gl_product_segment_code,'') = s.\"ACC_PROD_ID\" and\n" +
            "        coalesce(g.gl_product_segment_name,'') = s.\"ACC_PROD_NAME\" and\n" +
            "        coalesce(g.f_gl_product_segment_parent,'') = s.\"PARENT_ACC_PROD\")\n" +
            "left join  (select distinct external_reference, accountable_product_id from semarchy_eph_mdm.sa_accountable_product) a on dq.pmx_source_reference = a.external_reference\n" +
            " where dq.dq_err != 'Y'\n" +
            " order by random() limit '%s'";



    public static String GET_RANDOM_WORK_IDS_FROM_GD = "select external_reference as PMX_SOURCE_REFERENCE\n" +
            "from semarchy_eph_mdm.sa_accountable_product \n" +
            "order by random() \n" +
            "limit '%s'";

    /*
    public static String GET_RANDOM_WORK_IDS_FROM_STG = "select distinct\n" +
            "s.\"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID\n" +
            "from ephsit_talend_owner.stg_10_pmx_accountable_product s\n" +
            "left join ephsit_talend_owner.map_sourceref_2_numericid m on concat(s.\"ACC_PROD_ID\",s.\"PARENT_ACC_PROD\") = m.source_ref\n" +
            "left join semarchy_eph_mdm.gd_accountable_product g on m.numeric_id = g.accountable_product_id\n" +
            "where not (\n" +
            "        coalesce(g.gl_product_segment_code,'') = s.\"ACC_PROD_ID\" and\n" +
            "        coalesce(g.gl_product_segment_name,'') = s.\"ACC_PROD_NAME\" and\n" +
            "        coalesce(g.f_gl_product_segment_parent,'') = s.\"PARENT_ACC_PROD\")\n" +
            "order by random() limit '%s'";*/

    public static String GET_NUMERIC_ID_FROM_LOOKUP_AP = "select numeric_id as NUMERIC_ID from " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid where source_ref in ('%s')";
}
