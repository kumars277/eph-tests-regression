package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonWorkRoleDataSQL {

    public static String GET_COUNT_PERSONS_WORK_ROLE_PMX = "SELECT count(*) AS count\n" +
            "FROM\n" +
            "    GD_PRODUCT_WORK W\n" +
            "JOIN\n" +
            "    GD_PMC PMC ON W.F_PMC = PMC.PMCCODE\n" +
            "JOIN\n" +
            "    GD_PMG PMG ON PMC.F_PMG = PMG.PMGCODE\n" +
            "WHERE\n" +
            "    PMG.F_PARTY IS NOT NULL";

    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHSTG = "select count(*) as count from ephsit_talend_owner.stg_10_pmx_work_person_role";

    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHSA = "select count(*) as count from semarchy_eph_mdm.sa_work_person_role sa\n" +
            "where f_event =  (\n" +
            "select max (f_event) from \n" +
            "semarchy_eph_mdm.sa_product   \n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";

    public static String GET_COUNT_PERSONS_WORK_ROLE_EPHGD = "select count(*) as count from semarchy_eph_mdm.gd_work_person_role";


    public static String GET_DATA_PERSONS_WORK_ROLE_PMX = "SELECT\n" +
            "     W.PRODUCT_WORK_ID||'-PD' AS WORK_PERSON_ROLE_SOURCE_REF\n" +
            "    ,PMG.F_PARTY AS PMX_PARTY_SOURCE_REF\n" +
            "    ,W.PRODUCT_WORK_ID AS PMX_WORK_SOURCE_REF\n" +
            "    ,'PD' AS F_ROLE\n" +
            "FROM\n" +
            "    GD_PRODUCT_WORK W\n" +
            "JOIN\n" +
            "    GD_PMC PMC ON W.F_PMC = PMC.PMCCODE\n" +
            "JOIN\n" +
            "    GD_PMG PMG ON PMC.F_PMG = PMG.PMGCODE\n" +
            "WHERE\n" +
            "    PMG.F_PARTY IS NOT NULL \n" +
            "      AND W.PRODUCT_WORK_ID IN ('%s')";

    public static String GET_DATA_PERSONS_WORK_ROLE_EPHSTG = "select \n" +
            "\"WORK_PERSON_ROLE_SOURCE_REF\" as WORK_PERSON_ROLE_SOURCE_REF,\n" +
            "\"PMX_PARTY_SOURCE_REF\" as PMX_PARTY_SOURCE_REF,\n" +
            "\"PMX_WORK_SOURCE_REF\" as PMX_WORK_SOURCE_REF,\n" +
            "\"F_ROLE\" as F_ROLE\n" +
            "from ephsit_talend_owner.stg_10_pmx_work_person_role\n" +
            "where \"WORK_PERSON_ROLE_SOURCE_REF\" in ('%s')";

    public static String GET_DATA_PERSONS_WORK_ROLE_EPHSA = "select \n" +
            "b_loadid as B_LOADID,\n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "work_person_role_id as WORK_PERSON_ROLE_ID,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE,\n" +
            "f_role as F_ROLE,\n" +
            "f_wwork as F_WWORK,\n" +
            "f_person as F_PERSON\n" +
            "from semarchy_eph_mdm.sa_work_person_role p\n" +
            "where p.b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_work_person_role p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX' )\n" +
            "and work_person_role_id in ('%s')";

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
            "where work_person_role_id in ('%s')";

    public static String GET_RANDOM_PERSON_WORK_ROLE_IDS = "select \n" +
            "\"WORK_PERSON_ROLE_SOURCE_REF\" as WORK_PERSON_ROLE_SOURCE_REF\n" +
            "from ephsit_talend_owner.stg_10_pmx_work_person_role\n" +
            "order by random() limit '%s'";

}