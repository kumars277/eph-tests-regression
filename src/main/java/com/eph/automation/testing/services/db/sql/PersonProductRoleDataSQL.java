package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonProductRoleDataSQL {

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPH_STG_CAN = "select count (*) as count\n" +
            "from\n" +
            "    ephsit_talend_owner.stg_10_pmx_product_can p\n" +
            "left join\n" +
            "    ephsit_talend_owner.stg_10_pmx_manifestation m on p.f_manifestation_source_ref = m.\"MANIFESTATION_ID\"::text\n" +
            "left join\n" +
            "    ephsit_talend_owner.stg_10_pmx_work_person_role wp on coalesce(p.f_work_source_ref, m.\"F_PRODUCT_WORK\"::text) = wp.\"PMX_WORK_SOURCE_REF\"::text \n";

    public static String GET_COUNT_PERSONS_PRODUCT_ROLE_EPHSTG = "select count(*) as count from ephsit_talend_owner.stg_10_pmx_product_person_role";

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


    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPH_STG_CAN = "select  \n" +
            "pmx_source_reference as PRODUCT_SOURCE_REF,\n" +
            "wp.\"PMX_PARTY_SOURCE_REF\" as PERSON_SOURCE_REF\n" +
            "from\n" +
            "    ephsit_talend_owner.stg_10_pmx_product_can p\n" +
            "left join\n" +
            "    ephsit_talend_owner.stg_10_pmx_manifestation m on p.f_manifestation_source_ref = m.\"MANIFESTATION_ID\"::text\n" +
            "left join\n" +
            "    ephsit_talend_owner.stg_10_pmx_work_person_role wp on coalesce(p.f_work_source_ref, m.\"F_PRODUCT_WORK\"::text) = wp.\"PMX_WORK_SOURCE_REF\"::text \n" +
            "    where pmx_source_reference in ('%s');";

    public static String GET_DATA_PERSONS_PRODUCT_ROLE_EPHSTG = "select \n" +
            "PROD_PER_ROLE_SOURCE_REF as PROD_PER_ROLE_SOURCE_REF,\n" +
            "PRODUCT_SOURCE_REF as PRODUCT_SOURCE_REF,\n" +
            "PERSON_SOURCE_REF as PERSON_SOURCE_REF,\n" +
            "F_ROLE as F_ROLE\n" +
            "from ephsit_talend_owner.stg_10_pmx_product_person_role\n" +
            "where PRODUCT_SOURCE_REF in ('%s')";

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
            "product_person_role_id as WORK_PERSON_ROLE_ID,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_product as F_PRODUCT,\n" +
            "f_person as F_PERSON\n" +
            "from semarchy_eph_mdm.gd_product_person_role \n" +
            "where product_person_role_id in ('%s')";

    public static String GET_RANDOM_PERSON_PRODUCT_ROLE_IDS = "select  \n" +
            "pmx_source_reference as PRODUCT_SOURCE_REF\n" +
            "from\n" +
            "    ephsit_talend_owner.stg_10_pmx_product_can p\n" +
            "left join\n" +
            "    ephsit_talend_owner.stg_10_pmx_manifestation m on p.f_manifestation_source_ref = m.\"MANIFESTATION_ID\"::text\n" +
            "left join\n" +
            "    ephsit_talend_owner.stg_10_pmx_work_person_role wp on coalesce(p.f_work_source_ref, m.\"F_PRODUCT_WORK\"::text) = wp.\"PMX_WORK_SOURCE_REF\"::text \n" +
            "    order by random() limit '%s'";


}
