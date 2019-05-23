package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonProductRoleDataSQL {

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPH_STG_DQ = "    select count(*) as count\n" +
            "    from\n" +
            "    ephsit_talend_owner.stg_10_pmx_product_dq pr\n" +
            "join\n" +
            "    ephsit_talend_owner.stg_10_pmx_work_person_role wp on pr.ult_work_ref::varchar = wp.\"PMX_WORK_SOURCE_REF\"::varchar\n" +
            "where\n" +
            "    wp.\"F_ROLE\" = 'PU'";

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG = "select count(*) as count from ephsit_talend_owner.stg_10_pmx_product_person_role";

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG_DELTA = "select count(*) as count from ephsit_talend_owner.stg_10_pmx_product_person_role where TO_DATE(\"UPDATED\",'DD-MON-YY HH.MI.SS') > TO_DATE('%s','YYYYMMDDHH24MI')\n";


    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTGDQ = "select count(*) as count from ephsit_talend_owner.stg_10_pmx_product_person_role ppr \n " +
            "join ephsit_talend_owner.stg_10_pmx_person_dq perd on ppr.person_source_ref = perd.person_source_ref \n" +
            "join ephsit_talend_owner.stg_10_pmx_product_dq prod on ppr.product_source_ref = prod.pmx_source_reference \n" +
            "where perd.dq_err !='Y' and prod.dq_err != 'Y' ";

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSA = "select count(*) as count from semarchy_eph_mdm.sa_product_person_role sa\n" +
            "where f_event =  (\n" +
            "select max (f_event) from \n" +
            "semarchy_eph_mdm.sa_product   \n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n";

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHGD = "select count(*) as count from semarchy_eph_mdm.gd_product_person_role";


    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPH_DQ = "select\n" +
            "     concat(pr.pmx_source_reference,'-',wp.\"WORK_PERSON_ROLE_SOURCE_REF\",'-PO') as prod_per_role_source_ref\n" +
            "    ,pr.pmx_source_reference as product_source_ref\n" +
            "    ,wp.\"PMX_PARTY_SOURCE_REF\" as person_source_ref\n" +
            "    ,'PO' as f_role\n" +
            "    ,wp.\"F_ROLE\" as work_role\n" +
            "\t,wp.\"START_DATE\" as start_date\n" +
            "    ,wp.\"END_DATE\" as end_date\n" +
            "    ,wp.\"UPDATED\"::text as updated\n" +
            "from\n" +
            "    ephsit_talend_owner.stg_10_pmx_product_dq pr\n" +
            "join\n" +
            "    ephsit_talend_owner.stg_10_pmx_work_person_role wp on pr.ult_work_ref::varchar = wp.\"PMX_WORK_SOURCE_REF\"::varchar\n" +
            "join ephsit_talend_owner.stg_10_pmx_person_dq perd on wp.\"PMX_PARTY_SOURCE_REF\" = perd.\"person_source_ref\" \n" +
            " \n" +
            "where\n" +
            "    wp.\"F_ROLE\" = 'PU'\n" +
            "    and pr.pmx_source_reference in ('%s') \n" +
            "and pr.dq_err !='Y' and perd.dq_err !='Y'";

    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPHSTG = "select \n" +
            "ppr.PROD_PER_ROLE_SOURCE_REF as PROD_PER_ROLE_SOURCE_REF,\n" +
            "ppr.PRODUCT_SOURCE_REF as PRODUCT_SOURCE_REF,\n" +
            "ppr.PERSON_SOURCE_REF as PERSON_SOURCE_REF,\n" +
            "ppr.F_ROLE as F_ROLE,\n" +
            "ppr.WORK_ROLE as WORK_ROLE\n" +
            "from ephsit_talend_owner.stg_10_pmx_product_person_role ppr\n" +
            "join ephsit_talend_owner.stg_10_pmx_person_dq perd on ppr.person_source_ref = perd.person_source_ref \n"+
            "join ephsit_talend_owner.stg_10_pmx_product_dq prod on ppr.product_source_ref = prod.pmx_source_reference \n" +
            "where PRODUCT_SOURCE_REF in ('%s') \n" +
            "and perd.dq_err !='Y' and prod.dq_err != 'Y' \n" ;

    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPHSA = "select \n" +
            "b_loadid as B_LOADID,\n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "product_person_role_id as PRODUCT_PERSON_ROLE_ID,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_product as F_PRODUCT,\n" +
            "f_person as F_PERSON\n" +
            "from semarchy_eph_mdm.sa_product_person_role p\n" +
            "where p.b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_product_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX' )\n" +
            "and product_person_role_id in ('%s')";

    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPHGD = "select \n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "product_person_role_id as PRODUCT_PERSON_ROLE_ID,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_product as F_PRODUCT,\n" +
            "f_person as F_PERSON\n" +
            "from semarchy_eph_mdm.gd_product_person_role \n" +
            "where product_person_role_id in ('%s')";

    public static String GET_RANDOM_PERSON_PRODUCT_ROLE_IDS = "select  PRODUCT_SOURCE_REF as PRODUCT_SOURCE_REF\n" +
            "from ephsit_talend_owner.stg_10_pmx_product_person_role ppr\n" +
            "join ephsit_talend_owner.stg_10_pmx_person_dq perd on ppr.person_source_ref = perd.person_source_ref \n"+
            "join ephsit_talend_owner.stg_10_pmx_product_dq prod on ppr.product_source_ref = prod.pmx_source_reference \n" +
            "left join ephsit_talend_owner.map_sourceref_2_ephid mp on  mp.source_ref = ppr.product_source_ref \n" +
            "left join semarchy_eph_mdm.sa_product_person_role sa on sa.f_product = mp.eph_id\n" +
            "where sa.b_error_status is null \n" +
            "and perd.dq_err !='Y' and prod.dq_err != 'Y' \n" +
            "and SA.b_loadid in (select max(b_loadid) from semarchy_eph_mdm.sa_event where f_event_type ='PMX' and f_workflow_source = 'PMX' and workflow_id = 'talend')\n" +
            "order by random() limit '%s'";

    public static String GET_RANDOM_PERSON_PRODUCT_ROLE_IDS_FROM_SA = "select \n" +
            "product_person_role_id as PRODUCT_PERSON_ROLE_ID\n" +
            "from semarchy_eph_mdm.sa_product_person_role p\n" +
            "where p.b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_product_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX' )\n" +
            "order by random() limit '%s'";


}
