package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonWorkRoleDataSQL {

    public static String GET_COUNT_PERSONS_WORK_ROLE_PMX = "SELECT count(*) as count FROM (\n" +
            "SELECT\n" +
            "     W.PRODUCT_WORK_ID||PMG.F_PARTY||'-PD' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "    ,PMG.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "    ,W.PRODUCT_WORK_ID AS PMX_WORK_SOURCE_REF\n" +
            "    ,'PD' AS F_ROLE\n" +
            "    ,CURRENT_DATE AS START_DATE\n" +
            "    ,NULL AS END_DATE\n" +
            "    ,TO_CHAR(PMG.B_UPDDATE,'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "    GD_PRODUCT_WORK W\n" +
            "JOIN\n" +
            "    GD_PMC PMC ON W.F_PMC = PMC.PMCCODE\n" +
            "JOIN\n" +
            "    GD_PMG PMG ON PMC.F_PMG = PMG.PMGCODE\n" +
            "WHERE\n" +
            "    PMG.F_PARTY IS NOT NULL\n" +
            "UNION\n" +
            "SELECT\n" +
            "\t P.PARTY_IN_PRODUCT_ID||'-AU' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "\t,P.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "\t,P.F_PRODUCT_WORK  AS PMX_WORK_SOURCE_REF\n" +
            "\t,'AU' AS F_ROLE\n" +
            "\t,P.EFFFROM_DATE AS START_DATE\n" +
            "\t,P.EFFTO_DATE AS END_DATE\n" +
            "\t,TO_CHAR(NVL(P.B_UPDDATE,P.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PARTY_IN_PRODUCT P\n" +
            "WHERE\n" +
            "\tP.F_ROLE_TYPE = 1\n" +
            "--AND P.EFFTO_DATE IS NULL\n" +
            "UNION\n" +
            "SELECT\n" +
            "\t P.PARTY_IN_PRODUCT_ID||'-PU' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "\t,P.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "\t,P.F_PRODUCT_WORK  AS PMX_WORK_SOURCE_REF\n" +
            "\t,'PU' AS F_ROLE\n" +
            "\t,P.EFFFROM_DATE AS START_DATE\n" +
            "\t,P.EFFTO_DATE AS END_DATE\n" +
            "\t,TO_CHAR(NVL(P.B_UPDDATE,P.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PARTY_IN_PRODUCT P\n" +
            "WHERE\n" +
            "\tP.F_ROLE_TYPE IN (1120,1126)\n" +
            "--AND P.EFFTO_DATE IS NULL\n" +
            "UNION\n" +
            "SELECT\n" +
            "\t P.PARTY_IN_PRODUCT_ID||'-AE' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "\t,P.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "\t,P.F_PRODUCT_WORK  AS PMX_WORK_SOURCE_REF\n" +
            "\t,'AE' AS F_ROLE\n" +
            "\t,P.EFFFROM_DATE AS START_DATE\n" +
            "\t,P.EFFTO_DATE AS END_DATE\n" +
            "\t,TO_CHAR(NVL(P.B_UPDDATE,P.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PARTY_IN_PRODUCT P\n" +
            "WHERE\n" +
            "\tP.F_ROLE_TYPE IN (1103,1104)\n" +
            ")\n";


    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHSTG = "select count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_work_person_role" ;

    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHSTGGoingToSA = "select count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_work_person_role\n" +
            "join  ((select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar))  perd\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = perd.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) word\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_WORK_SOURCE_REF\"::varchar = word.consol\n" +
            "join (select distinct person_id, external_reference from semarchy_eph_mdm.sa_person) p \n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, work_person_role_id from semarchy_eph_mdm.sa_work_person_role) a\n" +
            "    on STG_10_PMX_WORK_PERSON_ROLE.\"WORK_PERSON_ROLE_SOURCE_REF\" = a.external_reference\n" +
            "where perd.dq_err != 'Y' and word.dq_err != 'Y'";

    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHSTG_DELTA ="select count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_work_person_role\n" +
            "join  ((select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar))  perd\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = perd.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) word\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_WORK_SOURCE_REF\"::varchar = word.consol\n" +
            "join (select distinct person_id, external_reference from semarchy_eph_mdm.sa_person) p \n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, work_person_role_id from semarchy_eph_mdm.sa_work_person_role) a\n" +
            "    on STG_10_PMX_WORK_PERSON_ROLE.\"WORK_PERSON_ROLE_SOURCE_REF\" = a.external_reference\n" +
            "where perd.dq_err != 'Y' and word.dq_err != 'Y'\n" +
            " and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')\n" ;
//            " and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')\n" ;


    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHAE = "select count(distinct ae.work_person_role_id) as count from semarchy_eph_mdm.ae_work_person_role ae\n" +
            "            join semarchy_eph_mdm.sa_work_person_role sa on sa.work_person_role_id = ae.work_person_role_id\n" +
            "            where sa.effective_end_date is null and b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";


//    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHSA = "select count(*) as count from semarchy_eph_mdm.sa_work_person_role sa\n" +
//            "where f_event =  (\n" +
//            "select max (f_event) from \n" +
//            "semarchy_eph_mdm.sa_work_person_role   \n" +
//            "join \n" +
//            "semarchy_eph_mdm.sa_event on f_event = event_id \n" +
//            "where semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
//            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
//            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";

    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHSA = "select count(*) as count from semarchy_eph_mdm.sa_work_person_role sa\n" +
            "where f_event =  (select  max(event_id) from semarchy_eph_mdm.sa_event sa2  \n" +
            "where sa2.f_event_type = 'PMX'\n" +
            "and sa2.workflow_id = 'talend'\n" +
            "and sa2.f_workflow_source = 'PMX')";


    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHSATOGD = "select count(*) as count from semarchy_eph_mdm.sa_work_person_role sa\n" +
            "where effective_end_date is null and f_event =  (\n" +
            "select max (f_event) from \n" +
            "semarchy_eph_mdm.sa_work_person_role   \n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "where semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";

    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHGD = "select count(*) as count from semarchy_eph_mdm.gd_work_person_role \n" +
            "where effective_end_date is null and b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";


    public static String GET_DATA_PERSONS_WORK_ROLE_PMX_PD = "SELECT\n" +
            "     W.PRODUCT_WORK_ID||PMG.F_PARTY||'-PD' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "    ,PMG.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "    ,W.PRODUCT_WORK_ID AS PMX_WORK_SOURCE_REF\n" +
            "    ,'PD' AS F_ROLE\n" +
            "    ,CURRENT_DATE AS START_DATE\n" +
            "    ,NULL AS END_DATE\n" +
            "    ,TO_CHAR(PMG.B_UPDDATE,'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "    GD_PRODUCT_WORK W\n" +
            "JOIN\n" +
            "    GD_PMC PMC ON W.F_PMC = PMC.PMCCODE\n" +
            "JOIN\n" +
            "    GD_PMG PMG ON PMC.F_PMG = PMG.PMGCODE\n" +
            "WHERE\n" +
            "    PMG.F_PARTY IS NOT NULL\n" +
            "    AND W.PRODUCT_WORK_ID||PMG.F_PARTY IN ('%s')";

    public static String GET_DATA_PERSONS_WORK_ROLE_PMX_AU = "SELECT\n" +
            "\t P.PARTY_IN_PRODUCT_ID||'-AU' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "\t,P.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "\t,P.F_PRODUCT_WORK  AS PMX_WORK_SOURCE_REF\n" +
            "\t,'AU' AS F_ROLE\n" +
            "\t,P.EFFFROM_DATE AS START_DATE\n" +
            "\t,P.EFFTO_DATE AS END_DATE\n" +
            "\t,TO_CHAR(NVL(P.B_UPDDATE,P.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PARTY_IN_PRODUCT P\n" +
            "WHERE\n" +
            "\tP.F_ROLE_TYPE = 1\n" +
            "\tAND P.PARTY_IN_PRODUCT_ID IN ('%s')";

    public static String GET_DATA_PERSONS_WORK_ROLE_PMX_PU = "SELECT\n" +
            "\t P.PARTY_IN_PRODUCT_ID||'-PU' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "\t,P.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "\t,P.F_PRODUCT_WORK  AS PMX_WORK_SOURCE_REF\n" +
            "\t,'PU' AS F_ROLE\n" +
            "\t,P.EFFFROM_DATE AS START_DATE\n" +
            "\t,P.EFFTO_DATE AS END_DATE\n" +
            "\t,TO_CHAR(NVL(P.B_UPDDATE,P.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PARTY_IN_PRODUCT P\n" +
            "WHERE\n" +
            "\tP.F_ROLE_TYPE IN (1120,1126)\n" +
            "\t\tAND P.PARTY_IN_PRODUCT_ID IN ('%s')\n";

    public static String GET_DATA_PERSONS_WORK_ROLE_PMX_AE = "SELECT\n" +
            "\t P.PARTY_IN_PRODUCT_ID||'-AE' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "\t,P.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "\t,P.F_PRODUCT_WORK  AS PMX_WORK_SOURCE_REF\n" +
            "\t,'AE' AS F_ROLE\n" +
            "\t,P.EFFFROM_DATE AS START_DATE\n" +
            "\t,P.EFFTO_DATE AS END_DATE\n" +
            "\t,TO_CHAR(NVL(P.B_UPDDATE,P.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PARTY_IN_PRODUCT P\n" +
            "WHERE\n" +
            "\tP.F_ROLE_TYPE IN (1103,1104)\n" +
            "\t\tAND P.PARTY_IN_PRODUCT_ID IN ('%s')";


    public static String GET_DATA_PERSONS_WORK_ROLE_EPHSTG = "select\n" +
            "\"WORK_PERSON_ROLE_SOURCE_REF\" as WORK_PERSON_ROLE_SOURCE_REF,\n" +
            "\"PMX_PARTY_SOURCE_REF\" as PMX_PARTY_SOURCE_REF,\n" +
            "\"PMX_WORK_SOURCE_REF\" as PMX_WORK_SOURCE_REF,\n" +
            "\"F_ROLE\" as F_ROLE,\n" +
            "\"START_DATE\" as START_DATE,\n" +
            "\"END_DATE\" as END_DATE,\n" +
            "\"UPDATED\" as UPDATED\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_work_person_role\n" +
            "join  ((select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar))  perd\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = perd.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) word\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_WORK_SOURCE_REF\"::varchar = word.consol\n" +
            "join (select distinct person_id, external_reference from semarchy_eph_mdm.sa_person) p \n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, work_person_role_id from semarchy_eph_mdm.sa_work_person_role) a\n" +
            "    on STG_10_PMX_WORK_PERSON_ROLE.\"WORK_PERSON_ROLE_SOURCE_REF\" = a.external_reference\n" +
            "where perd.dq_err != 'Y' and word.dq_err != 'Y'\t\n" +
            "and \"WORK_PERSON_ROLE_SOURCE_REF\" in ('%s') ";

    public static String GET_DATA_PERSONS_WORK_ROLE_EPHSA = "select \n" +
            "b_loadid as B_LOADID,\n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "work_person_role_id as WORK_PERSON_ROLE_ID,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_wwork as F_WWORK,\n" +
            "f_person as F_PERSON,\n" +
            "external_reference as WORK_PERSON_ROLE_SOURCE_REF\n" +
            "from semarchy_eph_mdm.sa_work_person_role p\n" +
            "where p.b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_work_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX' )\n" +
            "and external_reference in ('%s')";

    public static String GET_DATA_PERSONS_WORK_ROLE_EPHGD = "select \n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "work_person_role_id as WORK_PERSON_ROLE_ID,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_wwork as F_WWORK,\n" +
            "f_person as F_PERSON\n" +
            "from semarchy_eph_mdm.gd_work_person_role \n" +
            "where external_reference in ('%s')";

    public static String GET_RANDOM_PERSON_WORK_ROLE_IDS = "select \n" +
            "\"WORK_PERSON_ROLE_SOURCE_REF\" as WORK_PERSON_ROLE_SOURCE_REF\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_work_person_role\n" +
            "where \"WORK_PERSON_ROLE_SOURCE_REF\" like '%%%s'\n" +
            "order by random() limit '%s'";

    public static String GET_RANDOM_PERSON_WORK_ROLE_IDS_FROM_SA_WITH_NO_ERROR = " select  \n" +
            "work_person_role_id as WORK_PERSON_ROLE_ID\n" +
            "from semarchy_eph_mdm.sa_work_person_role p\n" +
            "where  p.effective_end_date is null \n" +
            "and p.b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_work_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX')\n" +
            "and F_ROLE like '%s' and b_error_status is null\n" +
            "order by random() limit '%s'";

    public static String GET_RANDOM_PERSON_WORK_ROLE_IDS_FROM_STG_WITH_NO_ERROR = "\n" +
            "select \n" +
            "\"WORK_PERSON_ROLE_SOURCE_REF\" as WORK_PERSON_ROLE_SOURCE_REF\n" +
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_person_role\n" +
            "join  ((select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar))  perd\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = perd.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) word\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_WORK_SOURCE_REF\"::varchar = word.consol\n" +
            "join (select distinct person_id, external_reference from semarchy_eph_mdm.sa_person ) p \n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, work_person_role_id from semarchy_eph_mdm.sa_work_person_role sa where effective_end_date is null \n" +
            "and b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_work_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX') \n" +
            "and b_error_status is null) a\n" +
            "    on STG_10_PMX_WORK_PERSON_ROLE.\"WORK_PERSON_ROLE_SOURCE_REF\" = a.external_reference\n" +
            "where perd.dq_err != 'Y' and word.dq_err != 'Y' and \"F_ROLE\" = '%s'\n" +
            "order by random() limit '%s' ";

    public static String GET_RANDOM_DELTA = "\n" +
            "\n" +
            "select \n" +
            "\"WORK_PERSON_ROLE_SOURCE_REF\" as WORK_PERSON_ROLE_SOURCE_REF\n" +
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_work_person_role\n" +
            "join  ((select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar))  perd\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = perd.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) word\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_WORK_SOURCE_REF\"::varchar = word.consol\n" +
            "join (select distinct person_id, external_reference from semarchy_eph_mdm.sa_person ) p \n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, work_person_role_id from semarchy_eph_mdm.sa_work_person_role sa where effective_end_date is null \n" +
            "and b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_work_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX') \n" +
            "and b_error_status is null) a\n" +
            "    on STG_10_PMX_WORK_PERSON_ROLE.\"WORK_PERSON_ROLE_SOURCE_REF\" = a.external_reference\n" +
            "where perd.dq_err != 'Y' and word.dq_err != 'Y' and \"F_ROLE\" = '%s'\n" +
            " and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')\n" +
//            " and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')\n" +
            "order by random() limit '%s' ";


    public static String GET_IDS_FROM_LOOKUP_TABLE = "select source_ref as WORK_PERSON_ROLE_SOURCE_REF from " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid\n" +
            "where numeric_id IN ('%s')";

    public static String GET_IDS_FROM_STG_FOR_GIVEN_IDS_IN_SA = "select\n" +
            "\"WORK_PERSON_ROLE_SOURCE_REF\" as WORK_PERSON_ROLE_SOURCE_REF\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_work_person_role\n" +
            "join  ((select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar))  perd\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = perd.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_wwork g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq s on g.external_reference = s.pmx_source_reference::varchar) word\n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_WORK_SOURCE_REF\"::varchar = word.consol\n" +
            "join semarchy_eph_mdm.sa_person p \n" +
            "     on STG_10_PMX_WORK_PERSON_ROLE.\"PMX_PARTY_SOURCE_REF\"::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, work_person_role_id from semarchy_eph_mdm.sa_work_person_role ) a\n" +
            "    on STG_10_PMX_WORK_PERSON_ROLE.\"WORK_PERSON_ROLE_SOURCE_REF\" = a.external_reference\n" +
            "where perd.dq_err != 'Y' and word.dq_err != 'Y'\t\n" +
            "and work_person_role_id in ('%s') ";
}


