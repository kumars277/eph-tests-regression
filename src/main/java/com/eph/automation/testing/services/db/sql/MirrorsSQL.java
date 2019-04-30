package com.eph.automation.testing.services.db.sql;

public class MirrorsSQL {

    public static String GET_STG_Mirrors_COUNT ="select count(*) as stgCount from ephsit_talend_owner.stg_10_pmx_work_rel " +
            "where \"F_RELATIONSHIP_TYPE\"='MIR'";

    public static String GET_SA_Mirrors_COUNT ="select count(*) as saCount from semarchy_eph_mdm.sa_work_relationship_mirror sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_relationship_mirror join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";

    public static String GET_GD_Mirrors_COUNT ="select count(*) as gdCount from semarchy_eph_mdm.gd_work_relationship_mirror gd\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_relationship_mirror join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";

    public static String GET_AE_Mirrors_COUNT ="select count(distinct work_rel_mirror_id) as aeCount from semarchy_eph_mdm.ae_work_relationship_mirror ae\n"+
            " where ae.b_batchid =  (select max (ae.b_batchid) from\n" +
            "semarchy_eph_mdm.ae_work_relationship_mirror ae join \n"+
            "semarchy_eph_mdm.gd_event e on ae.b_batchid = e.b_batchid\n"+
            "where  e.f_event_type = 'PMX'\n"+
            "and e.workflow_id = 'talend'\n"+
            "AND e.f_event_type = 'PMX'\n"+
            "and e.f_workflow_source = 'PMX' )";

    public static String gettingNumberOfIds="SELECT st.\"RELATIONSHIP_PMX_SOURCEREF\"  as random_value\n" +
            " FROM ephsit_talend_owner.stg_10_pmx_work_rel st\n" +
            " left join semarchy_eph_mdm.sa_work_relationship_mirror sa on st.\"RELATIONSHIP_PMX_SOURCEREF\" =cast(sa.work_rel_mirror_id as numeric)\n" +
            "WHERE \"F_RELATIONSHIP_TYPE\"='MIR' and sa.b_error_status is null ORDER BY RANDOM()\n" +
            " LIMIT PARAM1;";

    public static String gettingWorkID="SELECT work_id as work_id from semarchy_eph_mdm.sa_wwork where pmx_source_reference \n" +
            "in ('%s') ORDER BY pmx_source_reference";


    public static String GET_PMX_Mirror ="SELECT\n" +
            "\t WL.PRODUCT_WORK_LINK_ID AS RELATIONSHIP_PMX_SOURCEREF\n" +
            "\t,W1.PRODUCT_WORK_ID AS CHILD_PMX_SOURCE\n" +
            "\t,W2.PRODUCT_WORK_ID AS PARENT_PMX_SOURCE\n" +
            "\t,CASE WHEN WL.F_PRODUCT_WORK_LINK_TYPE = 51 THEN 'TRS'\n" +
            "          WHEN WL.F_PRODUCT_WORK_LINK_TYPE = 21 THEN 'MIR' END AS F_RELATIONSHIP_TYPE\n" +
            "\t,CASE WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NULL THEN TO_DATE('2019-01-01', 'YYYY-MM-DD')\n" +
            "\t      WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NOT NULL THEN EFFFROM_DATE\n" +
            "\t      ELSE NULL END AS EFFECTIVE_START_DATE  -- available work (81)\n" +
            "\t,NVL(W1.EFFECTIVE_TO_DATE, NVL(W2.EFFECTIVE_TO_DATE, WL.EFFTO_DATE)) AS ENDON\n" +
            "FROM\n" +
            "\tGD_PRODUCT_WORK_LINK WL,\n" +
            "\tGD_PRODUCT_WORK W1,\n" +
            "\tGD_PRODUCT_WORK W2\n" +
            "WHERE\n" +
            "\tWL.F_PRODUCT_WORK = W1.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tWL.F_RELATED_PRODUCT_WORK = W2.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tNVL(W1.EFFECTIVE_TO_DATE, NVL(W2.EFFECTIVE_TO_DATE, WL.EFFTO_DATE)) IS NULL\n" +
            "AND\n" +
            "\tW1.F_WORK_STATUS = 81\n" +
            "AND\n" +
            "\tWL.F_PRODUCT_WORK_LINK_TYPE IN (51,21)\t-- 51 = mirror, 21 = mirror\n" +
            "\tAND WL.PRODUCT_WORK_LINK_ID in ('%s') order by RELATIONSHIP_PMX_SOURCEREF";

    public static String GET_STG_Mirror_DATA ="SELECT \n" +
            " \"RELATIONSHIP_PMX_SOURCEREF\" as RELATIONSHIP_PMX_SOURCEREF " +
            "  ,\"CHILD_PMX_SOURCE\" as CHILD_PMX_SOURCE\n" +
            "  ,\"PARENT_PMX_SOURCE\" as PARENT_PMX_SOURCE\n" +
            "  ,\"F_RELATIONSHIP_TYPE\" as F_RELATIONSHIP_TYPE\n" +
            "  ,\"EFFECTIVE_START_DATE\" as EFFECTIVE_START_DATE\n"+
            "  ,\"ENDON\" as ENDON\n"+
            "  FROM ephsit_talend_owner.stg_10_pmx_work_rel \n"+
            "  WHERE \"RELATIONSHIP_PMX_SOURCEREF\" in ('%s')"+
            "  AND \"F_RELATIONSHIP_TYPE\"='MIR' order by RELATIONSHIP_PMX_SOURCEREF";

    public static String GET_SA_mirror_DATA ="SELECT \n" +
            " WORK_REL_mirror_ID as WORK_REL_mirror_ID " +
            "  ,f_original as RELATIONSHIP_PMX_SOURCEREF\n" +
            "  ,f_mirrored as CHILD_PMX_SOURCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,effective_start_date as EFFECTIVE_START_DATE"+
            "  ,effective_to_date as ENDON"+
            "  FROM ephsit.semarchy_eph_mdm.sa_work_relationship_mirror sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_relationship_mirror join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_original ='PARAM1' and f_mirrored = 'PARAM2'";

    public static String GET_SA_Mirror_DATA_All ="SELECT \n" +
            " WORK_REL_mirror_ID as WORK_REL_mirror_ID " +
            "  ,f_original as RELATIONSHIP_PMX_SOURCEREF\n" +
            "  ,f_mirrored as CHILD_PMX_SOURCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,effective_start_date as EFFECTIVE_START_DATE"+
            "  ,effective_to_date as ENDON"+
            "  FROM ephsit.semarchy_eph_mdm.sa_work_relationship_mirror sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_relationship_mirror join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_original in ('%s') order by WORK_REL_mirror_ID";

    public static String GET_GD_Mirror_DATA ="SELECT \n" +
            " WORK_REL_mirror_ID as WORK_REL_mirror_ID " +
            "  ,f_original as RELATIONSHIP_PMX_SOURCEREF\n" +
            "  ,f_mirrored as CHILD_PMX_SOURCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,effective_start_date as EFFECTIVE_START_DATE"+
            "  ,effective_to_date as ENDON"+
            "  FROM ephsit.semarchy_eph_mdm.gd_work_relationship_mirror sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_relationship_mirror join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_original in ('%s') order by WORK_REL_mirror_ID";

    public static String Get_work_id = "select eph_id as workID from ephsit_talend_owner.map_sourceref_2_ephid " +
            "where ref_type = 'WORK' and source_ref='PARAM1'";

    public static String Get_mirror_id = "select numeric_id as mirrorId from ephsit_talend_owner.map_sourceref_2_numericid " +
            "where source_ref = 'PARAM1'";

    public static String Get_child_id = "select eph_id as workID from ephsit_talend_owner.map_sourceref_2_ephid " +
            "where ref_type = 'WORK' and source_ref='PARAM1'";

}