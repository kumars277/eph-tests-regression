package com.eph.automation.testing.services.db.sql;

public class TranslationsSQL {

    public static String GET_PMX_TRANSLATIONS_COUNT ="SELECT count(*) as pmxCount FROM (SELECT\n" +
            "\t WL.PRODUCT_WORK_LINK_ID AS RELATIONSHIP_PMX_SOURCEREF\n" +
            "\t,W1.PRODUCT_WORK_ID AS CHILD_PMX_SOURCE\n" +
            "\t,W2.PRODUCT_WORK_ID AS PARENT_PMX_SOURCE\n" +
            "\t,CASE WHEN WL.F_PRODUCT_WORK_LINK_TYPE = 51 THEN 'TRS'\n" +
            "          WHEN WL.F_PRODUCT_WORK_LINK_TYPE = 21 THEN 'MIR' END AS F_RELATIONSHIP_TYPE\n" +
            "\t,CASE WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NULL THEN TO_DATE('2019-01-01', 'YYYY-MM-DD')\n" +
            "\t      WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NOT NULL THEN EFFFROM_DATE\n" +
            "\t      ELSE NULL END AS EFFECTIVE_START_DATE  -- available work (81)\n" +
            "\t,NVL(WL.EFFTO_DATE,\n" +
            "\t\tCASE WHEN W1.EFFECTIVE_TO_DATE IS NULL AND W2.EFFECTIVE_TO_DATE IS NULL THEN NULL \n" +
            "\t\tELSE GREATEST(NVL(W1.EFFECTIVE_TO_DATE,TO_DATE('1900-01-01', 'YYYY-MM-DD')),NVL(W2.EFFECTIVE_TO_DATE,TO_DATE('1900-01-01', 'YYYY-MM-DD'))) END) AS ENDON\n" +
            "\t,TO_CHAR(NVL(WL.B_UPDDATE,WL.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PRODUCT_WORK_LINK WL,\n" +
            "\tGD_PRODUCT_WORK W1,\n" +
            "\tGD_PRODUCT_WORK W2\n" +
            "WHERE\n" +
            "\tWL.F_PRODUCT_WORK = W1.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tWL.F_RELATED_PRODUCT_WORK = W2.PRODUCT_WORK_ID\n" +
            "--AND\n" +
            "--\tNVL(W1.EFFECTIVE_TO_DATE, W2.EFFECTIVE_TO_DATE) IS NULL -- removing this filter as we need to include end dated records\n" +
            "AND\n" +
            "\tW1.F_WORK_STATUS = 81\n" +
            "AND\n" +
            "\tWL.F_PRODUCT_WORK_LINK_TYPE IN (51,21)\t-- 51 = translation, 21 = mirror\n" +
            "\t)\n";

    public static String GET_STG_ALL_COUNT ="select count(*) as stgCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_rel";

    public static String GET_STG_TRANSLATIONS_COUNT ="select count(*) as stgCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_rel " +
            "  join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar)  d1\n" +
            "on STG_10_PMX_WORK_REL.\"PARENT_PMX_SOURCE\"::varchar = d1.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) d2\n" +
            "on STG_10_PMX_WORK_REL.\"CHILD_PMX_SOURCE\"::varchar = d2.consol\n"+
            "left join   (select distinct external_reference, work_relationship_id from semarchy_eph_mdm.sa_work_relationship) a on\n" +
            GetEPHDBUser.getDBUser()+".STG_10_PMX_WORK_REL.\"RELATIONSHIP_PMX_SOURCEREF\"::varchar = a.external_reference\n"+
            "where d1.dq_err != 'Y' and d2.dq_err != 'Y'";

    public static String GET_STG_TRANSLATIONS_COUNT_Updated ="select count(*) as stgCount from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_rel " +
            "  join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar)  d1\n" +
            "on STG_10_PMX_WORK_REL.\"PARENT_PMX_SOURCE\"::varchar = d1.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) d2\n" +
            "on STG_10_PMX_WORK_REL.\"CHILD_PMX_SOURCE\"::varchar = d2.consol\n" +
            "left join   (select distinct external_reference, work_relationship_id from semarchy_eph_mdm.sa_work_relationship) a on\n" +
            GetEPHDBUser.getDBUser()+".STG_10_PMX_WORK_REL.\"RELATIONSHIP_PMX_SOURCEREF\"::varchar = a.external_reference "+
            "where d1.dq_err != 'Y' and d2.dq_err != 'Y' and TO_DATE(\"UPDATED\",'YYYYMMDDHH24MI') > TO_DATE('PARAM1','YYYYMMDDHH24MI')";

    public static String GET_SA_TRANSLATIONS_COUNT ="select count(*) as saCount from semarchy_eph_mdm.sa_work_relationship sa\n"+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.sa_event\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n";

    public static String GET_GD_TRANSLATIONS_COUNT =
            "select count(*) as gdCount from semarchy_eph_mdm.gd_work_relationship \n" +
            " where b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n" +
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )" ;

    public static String GET_AE_TRANSLATIONS_COUNT ="select count(distinct ae.work_relationship_id) as aeCount from semarchy_eph_mdm.ae_work_relationship ae\n" +
            "join semarchy_eph_mdm.sa_work_relationship sa on sa.work_relationship_id=ae.work_relationship_id\n" +
            " where ae.b_batchid =  (select max (ae.b_batchid) from\n" +
            "semarchy_eph_mdm.ae_work_relationship ae join \n" +
            "semarchy_eph_mdm.gd_event e on ae.b_batchid = e.b_batchid\n" +
            "where  e.f_event_type = 'PMX'\n" +
            "and e.workflow_id = 'talend'\n" +
            "AND e.f_event_type = 'PMX'\n" +
            "and e.f_workflow_source = 'PMX' )\n" +
            "and sa.effective_end_date is null";

    public static String gettingNumberOfIds="SELECT \"RELATIONSHIP_PMX_SOURCEREF\"  as random_value\n" +
            " FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_rel \n" +
            "  join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar)  d1\n" +
            "on STG_10_PMX_WORK_REL.\"PARENT_PMX_SOURCE\"::varchar = d1.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) d2\n" +
            "on STG_10_PMX_WORK_REL.\"CHILD_PMX_SOURCE\"::varchar = d2.consol\n"+
            "left join   (select distinct external_reference, work_relationship_id from semarchy_eph_mdm.sa_work_relationship) a on\n" +
            GetEPHDBUser.getDBUser()+".STG_10_PMX_WORK_REL.\"RELATIONSHIP_PMX_SOURCEREF\"::varchar = a.external_reference"+
            " where \"F_RELATIONSHIP_TYPE\"='TRS' and d1.dq_err != 'Y' and d2.dq_err != 'Y' ORDER BY RANDOM()\n" +
            " LIMIT PARAM1;";

    public static String gettingWorkID="SELECT work_id as work_id from semarchy_eph_mdm.sa_wwork where external_reference \n" +
            "in ('%s') ORDER BY external_reference";


    public static String GET_PMX_TRANSLATIONS ="SELECT\n" +
            "\t WL.PRODUCT_WORK_LINK_ID AS RELATIONSHIP_PMX_SOURCEREF\n" +
            "\t,W1.PRODUCT_WORK_ID AS CHILD_PMX_SOURCE\n" +
            "\t,W2.PRODUCT_WORK_ID AS PARENT_PMX_SOURCE\n" +
            "\t,CASE WHEN WL.F_PRODUCT_WORK_LINK_TYPE = 51 THEN 'TRS'\n" +
            "          WHEN WL.F_PRODUCT_WORK_LINK_TYPE = 21 THEN 'MIR' END AS F_RELATIONSHIP_TYPE\n" +
            "\t,CASE WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NULL THEN TO_DATE('2019-01-01', 'YYYY-MM-DD')\n" +
            "\t      WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NOT NULL THEN EFFFROM_DATE\n" +
            "\t      ELSE NULL END AS EFFECTIVE_START_DATE  -- available work (81)\n" +
            "\t,NVL(WL.EFFTO_DATE,\n" +
            "\t\tCASE WHEN W1.EFFECTIVE_TO_DATE IS NULL AND W2.EFFECTIVE_TO_DATE IS NULL THEN NULL \n" +
            "\t\tELSE GREATEST(NVL(W1.EFFECTIVE_TO_DATE,TO_DATE('1900-01-01', 'YYYY-MM-DD')),NVL(W2.EFFECTIVE_TO_DATE,TO_DATE('1900-01-01', 'YYYY-MM-DD'))) END) AS ENDON\n" +
            "\t,TO_CHAR(NVL(WL.B_UPDDATE,WL.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PRODUCT_WORK_LINK WL,\n" +
            "\tGD_PRODUCT_WORK W1,\n" +
            "\tGD_PRODUCT_WORK W2\n" +
            "WHERE\n" +
            "\tWL.F_PRODUCT_WORK = W1.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tWL.F_RELATED_PRODUCT_WORK = W2.PRODUCT_WORK_ID\n" +
            "--AND\n" +
            "\t--NVL(W1.EFFECTIVE_TO_DATE, NVL(W2.EFFECTIVE_TO_DATE, WL.EFFTO_DATE)) IS NULL\n" +
            "AND\n" +
            "\tW1.F_WORK_STATUS = 81\n" +
            "AND\n" +
            "\tWL.F_PRODUCT_WORK_LINK_TYPE IN (51,21)"+
            "\tAND WL.PRODUCT_WORK_LINK_ID in ('%s') order by RELATIONSHIP_PMX_SOURCEREF";

    public static String GET_STG_Translation_DATA ="SELECT \n" +
            " \"RELATIONSHIP_PMX_SOURCEREF\" as RELATIONSHIP_PMX_SOURCEREF " +
            "  ,\"CHILD_PMX_SOURCE\" as CHILD_PMX_SOURCE\n" +
            "  ,\"PARENT_PMX_SOURCE\" as PARENT_PMX_SOURCE\n" +
            "  ,\"F_RELATIONSHIP_TYPE\" as F_RELATIONSHIP_TYPE\n" +
            "  ,\"EFFECTIVE_START_DATE\" as EFFECTIVE_START_DATE\n"+
            "  ,\"ENDON\" as ENDON\n"+
            "  ,\"UPDATED\" as UPDATED\n"+
            "  FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_rel \n" +
            "  WHERE \"RELATIONSHIP_PMX_SOURCEREF\" in ('%s')"+
            "  AND \"F_RELATIONSHIP_TYPE\"='TRS' order by RELATIONSHIP_PMX_SOURCEREF";

    public static String GET_SA_Translation_DATA ="SELECT \n" +
            " WORK_RELATIONSHIP_ID as WORK_REL_TRANSLATION_ID " +
            "  ,f_parent as RELATIONSHIP_PMX_SOURCEREF\n" +
            "  ,f_child as CHILD_PMX_SOURCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,effective_start_date as EFFECTIVE_START_DATE"+
            "  ,effective_end_date as ENDON"+
            "  ,f_relationship_type as f_relationship_type"+
            "  ,external_reference as RELATIONSHIP_PMX_SOURCEREF\n"+
            "  FROM semarchy_eph_mdm.sa_work_relationship sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_relationship join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_parent ='PARAM1' and f_child = 'PARAM2' order by WORK_REL_TRANSLATION_ID";

    public static String GET_SA_Translation_DATA_All ="SELECT \n" +
            " WORK_RELATIONSHIP_ID as WORK_REL_TRANSLATION_ID " +
            "  ,f_parent as RELATIONSHIP_PMX_SOURCEREF\n" +
            "  ,f_child as CHILD_PMX_SOURCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,effective_start_date as EFFECTIVE_START_DATE"+
            "  ,effective_end_date as ENDON"+
            "  ,f_relationship_type as f_relationship_type\n"+
            "  ,external_reference as RELATIONSHIP_PMX_SOURCEREF\n"+
            "  FROM semarchy_eph_mdm.sa_work_relationship sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_relationship join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n" +
            "and f_relationship_type='TRS'\n"+
            "  AND f_parent in ('%s') and b_error_status is null order by WORK_REL_TRANSLATION_ID";

    public static String GET_GD_Translation_DATA ="SELECT \n" +
            " WORK_RELATIONSHIP_ID as WORK_REL_TRANSLATION_ID " +
            "  ,f_parent as RELATIONSHIP_PMX_SOURCEREF\n" +
            "  ,f_child as CHILD_PMX_SOURCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,effective_start_date as EFFECTIVE_START_DATE"+
            "  ,effective_end_date as ENDON"+
            "  ,f_relationship_type as f_relationship_type"+
            "  ,external_reference as RELATIONSHIP_PMX_SOURCEREF\n"+
            "  FROM semarchy_eph_mdm.gd_work_relationship sa\n"+
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_relationship join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )\n"+
            "  AND f_parent in ('%s') order by WORK_REL_TRANSLATION_ID";

    public static String Get_work_id = "select eph_id as workID from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid " +
            "where ref_type = 'WORK' and source_ref='PARAM1'";

    public static String Get_translation_id = "select numeric_id as translationId from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid " +
            "where source_ref = 'PARAM1'";

    public static String Get_child_id = "select eph_id as workID from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid " +
            "where ref_type = 'WORK' and source_ref='PARAM1'";



}
