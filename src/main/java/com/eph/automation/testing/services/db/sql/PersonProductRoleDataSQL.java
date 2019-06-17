package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonProductRoleDataSQL {

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPH_STG_DQ = "select count(*) as count from (\n" +
            "select\n" +
            "     concat(pr.pmx_source_reference,'-',wp.\"WORK_PERSON_ROLE_SOURCE_REF\",'-PO') as prod_per_role_source_ref\n" +
            "    ,pr.pmx_source_reference as product_source_ref\n" +
            "    ,wp.\"PMX_PARTY_SOURCE_REF\" as person_source_ref\n" +
            "    ,'PO' as f_role\n" +
            "    ,wp.\"F_ROLE\" as work_role\n" +
            "    ,wp.\"START_DATE\" as start_date\n" +
            "    ,wp.\"END_DATE\" as end_date\n" +
            "    ,wp.\"UPDATED\"::text as updated\n" +
            "from\n" +
            GetEPHDBUser.getDBUser() + "    .stg_10_pmx_product_dq pr\n" +
            "join\n" +
            GetEPHDBUser.getDBUser() + "    .stg_10_pmx_work_person_role wp on pr.ult_work_ref::varchar = wp.\"PMX_WORK_SOURCE_REF\"::varchar\n" +
            "where\n" +
            "    case when pr.work_type = 'JOURNAL' and wp.\"F_ROLE\" = 'PU' then true\n" +
            "         when pr.work_type = 'BOOK' and wp.\"F_ROLE\" = 'AE' then true\n" +
            "         else false end = true\n" +
            ") a";

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG = "select count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_person_role";


    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG_GOING_TO_SA ="select count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_person_role\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar)  prod\n" +
            "      on STG_10_PMX_PRODUCT_PERSON_ROLE.\"product_source_ref\" = prod.consol\n" +
            "join  (select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join "  + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar)  perd\n" +
            "     on STG_10_PMX_PRODUCT_PERSON_ROLE.\"person_source_ref\"::varchar = perd.consol\n" +
            "join semarchy_eph_mdm.sa_person p\n" +
            "     on STG_10_PMX_PRODUCT_PERSON_ROLE.person_source_ref::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, product_person_role_id from semarchy_eph_mdm.sa_product_person_role) a\n" +
            "    on STG_10_PMX_PRODUCT_PERSON_ROLE.prod_per_role_source_ref::varchar = a.external_reference\n" +
            "    where perd.dq_err != 'Y' and  prod.dq_err != 'Y'";


    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG_GOING_TO_SA_DELTA ="select count(*) as count\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_person_role \n" +
            "join  (select s.pmx_source_reference as stage, g.pmx_source_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.pmx_source_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.pmx_source_reference = s.pmx_source_reference::varchar)  perd\n" +
            "on " + GetEPHDBUser.getDBUser() + ".STG_10_PMX_PRODUCT_PERSON_ROLE.\"person_source_ref\" = perd.consol\n" +
            "join  (select s.person_source_ref as stage, replace(m.source_ref,'PERSON-','') as gold,\n" +
            "coalesce(s.person_source_ref::varchar,replace(m.source_ref,'PERSON-','')) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from gd_person g join map_sourceref_2_numericid m on g.person_id = m.numeric_id\n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on replace(m.source_ref,'PERSON-','') = s.person_source_ref::varchar)  prod\n" +
            "on " + GetEPHDBUser.getDBUser() + ".STG_10_PMX_PRODUCT_PERSON_ROLE.\"product_source_ref\" = prod.consol\n" +
            "where perd.dq_err != 'Y' and  prod.dq_err != 'Y' and TO_DATE(UPDATED,'YYYYMMDDHH24MI') >= TO_DATE('%s','YYYYMMDDHH24MI')";


    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSA = "select count(*) as count from semarchy_eph_mdm.sa_product_person_role sa\n" +
            "where f_event =  (\n" +
            "select max (f_event) from \n" +
            "semarchy_eph_mdm.sa_product   \n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n";

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHGD = "select count(*) as count from semarchy_eph_mdm.gd_product_person_role where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";


    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPH_DQ = "select\n" +
            "     concat(pr.pmx_source_reference,'-',wp.\"WORK_PERSON_ROLE_SOURCE_REF\",'-PO') as prod_per_role_source_ref\n" +
            "    ,pr.pmx_source_reference as product_source_ref\n" +
            "    ,wp.\"PMX_PARTY_SOURCE_REF\" as person_source_ref\n" +
            "    ,'PO' as f_role\n" +
            "    ,wp.\"F_ROLE\" as work_role\n" +
            "    ,wp.\"START_DATE\" as start_date\n" +
            "    ,wp.\"END_DATE\" as end_date\n" +
            "    ,wp.\"UPDATED\"::text as updated\n" +
            "from\n" +
                GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq pr\n" +
            "join\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_work_person_role wp on pr.ult_work_ref::varchar = wp.\"PMX_WORK_SOURCE_REF\"::varchar\n" +
            "where\n" +
            "    case when pr.work_type = 'JOURNAL' and wp.\"F_ROLE\" = 'PU' then true\n" +
            "         when pr.work_type = 'BOOK' and wp.\"F_ROLE\" = 'AE' then true\n" +
            "         else false end = true\n" +
            "         and pr.pmx_source_reference in ('%s')";

    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPHSTG = "select \n" +
            "PROD_PER_ROLE_SOURCE_REF as PROD_PER_ROLE_SOURCE_REF,\n" +
            "PRODUCT_SOURCE_REF as PRODUCT_SOURCE_REF,\n" +
            "PERSON_SOURCE_REF as PERSON_SOURCE_REF,\n" +
            "F_ROLE as F_ROLE,\n" +
            "p.PERSON_ID as F_PERSON\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_person_role\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar)  prod\n" +
            "      on STG_10_PMX_PRODUCT_PERSON_ROLE.\"product_source_ref\" = prod.consol\n" +
            "join  (select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar)  perd\n" +
            "     on STG_10_PMX_PRODUCT_PERSON_ROLE.\"person_source_ref\"::varchar = perd.consol\n" +
            "join semarchy_eph_mdm.sa_person p\n" +
            "     on STG_10_PMX_PRODUCT_PERSON_ROLE.person_source_ref::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, product_person_role_id from semarchy_eph_mdm.sa_product_person_role) a\n" +
            "    on STG_10_PMX_PRODUCT_PERSON_ROLE.prod_per_role_source_ref::varchar = a.external_reference\n" +
            "    where perd.dq_err != 'Y' and  prod.dq_err != 'Y'\t\n" +
            "and PROD_PER_ROLE_SOURCE_REF in ('%s') ";

    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPHSTG_BY_PROD_PER_ROLE_SOURCE_REF = "select \n" +
            "ppr.PROD_PER_ROLE_SOURCE_REF as PROD_PER_ROLE_SOURCE_REF,\n" +
            "ppr.PRODUCT_SOURCE_REF as PRODUCT_SOURCE_REF,\n" +
            "ppr.PERSON_SOURCE_REF as PERSON_SOURCE_REF,\n" +
            "ppr.F_ROLE as F_ROLE,\n" +
            "ppr.WORK_ROLE as WORK_ROLE\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_person_role ppr\n" +
            "join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq perd on ppr.person_source_ref = perd.person_source_ref \n"+
            "join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq prod on ppr.product_source_ref = prod.pmx_source_reference \n" +
            "where PROD_PER_ROLE_SOURCE_REF in ('%s') \n" +
            "and perd.dq_err !='Y' and prod.dq_err != 'Y' \n";

    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPHSA = "select \n" +
            "b_loadid as B_LOADID,\n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "product_person_role_id as PRODUCT_PERSON_ROLE_ID,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_product as F_PRODUCT,\n" +
            "f_person as F_PERSON,\n" +
            "external_reference as EXTERNAL_REFERENCE\n" +
            "from semarchy_eph_mdm.sa_product_person_role p\n" +
            "where p.b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_product_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX' )\n" +
            "and external_reference in ('%s')";

    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPHGD = "select \n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "product_person_role_id as PRODUCT_PERSON_ROLE_ID,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_product as F_PRODUCT,\n" +
            "f_person as F_PERSON,\n" +
            "external_reference as EXTERNAL_REFERENCE\n" +
            "from semarchy_eph_mdm.gd_product_person_role \n" +
            "where external_reference in ('%s')";

    public static String GET_RANDOM_PERSON_PRODUCT_ROLE_IDS = "select PROD_PER_ROLE_SOURCE_REF as PROD_PER_ROLE_SOURCE_REF\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_person_role\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar)  prod\n" +
            "      on STG_10_PMX_PRODUCT_PERSON_ROLE.\"product_source_ref\" = prod.consol\n" +
            "join  (select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar)  perd\n" +
            "     on STG_10_PMX_PRODUCT_PERSON_ROLE.\"person_source_ref\"::varchar = perd.consol\n" +
            "join semarchy_eph_mdm.sa_person p\n" +
            "     on STG_10_PMX_PRODUCT_PERSON_ROLE.person_source_ref::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, product_person_role_id from semarchy_eph_mdm.sa_product_person_role) a\n" +
            "    on STG_10_PMX_PRODUCT_PERSON_ROLE.prod_per_role_source_ref::varchar = a.external_reference\n" +
            "    where perd.dq_err != 'Y' and  prod.dq_err != 'Y'\t\n" +
            "order by random() limit '%s'";

    public static String GET_RANDOM_PERSON_PRODUCT_ROLE_IDS_FROM_SA = "select \n" +
            "PROD_PER_ROLE_SOURCE_REF\n as PROD_PER_ROLE_SOURCE_REF\n\n" +
            "from semarchy_eph_mdm.sa_product_person_role p,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_person_role stg,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq perd,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq prod\n" +
            "where p.b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_product_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX' )\n" +
            "and stg.\"person_source_ref\" = perd.person_source_ref\n" +
            "and stg.\"product_source_ref\" = prod.pmx_source_reference\n" +
            "and perd.dq_err != 'Y' and  prod.dq_err != 'Y'\t\n" +
            "order by random() limit '%s'";

//    public static String GET_END_DATED_RECORDS_FROM_GD = "select \n" +
//            "gd.product_person_role_id as PRODUCT_PERSON_ROLE_ID\n" +
//            "from semarchy_eph_mdm.gd_product_person_role gd, \n" +
//            "semarchy_eph_mdm.sa_product_person_role p,\n" +
//            GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_person_role stg,\n" +
//            GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq perd,\n" +
//            GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq prod\n" +
//            "where p.b_loadid =  (\n" +
//            "select max (p1.b_loadid) from \n" +
//            "semarchy_eph_mdm.sa_product_person_role p1\n" +
//            "join \n" +
//            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
//            "where  sa.f_event_type = 'PMX'\n" +
//            "and sa.workflow_id = 'talend'\n" +
//            "and sa.f_workflow_source = 'PMX' )\n" +
//            "and stg.\"person_source_ref\" = perd.person_source_ref\n" +
//            "and stg.\"product_source_ref\" = prod.pmx_source_reference\n" +
//            "and perd.dq_err != 'Y' and  prod.dq_err != 'Y'\t\n" +
//            "and gd.effective_end_date is not null";

    public static final String SELECT_END_DATED_RECORDS_STG_AND_GD = "select ephsit_talend_owner.stg_10_pmx_product_person_role.F_ROLE as \"STG\", gd.f_role as \"GD\"\n" +
            "from ephsit_talend_owner.stg_10_pmx_product_person_role\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar)  prod\n" +
            "      on STG_10_PMX_PRODUCT_PERSON_ROLE.\"product_source_ref\" = prod.consol\n" +
            "join  (select s.person_source_ref as stage, aa.external_reference as gold,\n" +
            "coalesce(s.person_source_ref::varchar,aa.external_reference) as consol,\n" +
            "case when s.person_source_ref is null then 'N' else s.dq_err end as dq_err\n" +
            "from (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) aa\n" +
            "full outer join "+ GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq s on aa.external_reference = s.person_source_ref::varchar)  perd\n" +
            "     on STG_10_PMX_PRODUCT_PERSON_ROLE.\"person_source_ref\"::varchar = perd.consol\n" +
            "join semarchy_eph_mdm.sa_person p\n" +
            "     on STG_10_PMX_PRODUCT_PERSON_ROLE.person_source_ref::varchar = p.external_reference\n" +
            "left join\n" +
            "    (select distinct external_reference, product_person_role_id from semarchy_eph_mdm.sa_product_person_role) a\n" +
            "    on STG_10_PMX_PRODUCT_PERSON_ROLE.prod_per_role_source_ref::varchar = a.external_reference\n" +
            "join semarchy_eph_mdm.gd_product_person_role gd on STG_10_PMX_PRODUCT_PERSON_ROLE.prod_per_role_source_ref::varchar = gd.external_reference\n" +
            "    where perd.dq_err != 'Y' and  prod.dq_err != 'Y'\t\n" +
            "    and gd.effective_end_date is not null";

    public static String GET_SOURCE_REF = "select source_ref as source_ref from "+ GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid where numeric_id = '%s'";
}
