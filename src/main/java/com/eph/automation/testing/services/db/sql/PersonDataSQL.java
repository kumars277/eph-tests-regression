package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 13/03/2019
 */
public class PersonDataSQL {

    public static String GET_COUNT_PERSONS_PMX = "select count(*) FROM GD_PARTY P\n" +
            "LEFT JOIN GD_PARTY_IN_PRODUCT PI ON P.PARTY_ID = PI.F_PARTY\n" +
            "LEFT JOIN GD_PMG PM ON P.PARTY_ID = PM.F_PARTY\n" +
            "WHERE PARTY_ID IN (\n" +
            "SELECT F_PARTY FROM GD_PMG\n" +
            "UNION\n" +
            "SELECT F_PARTY FROM GD_PARTY_IN_PRODUCT WHERE F_ROLE_TYPE IN (\n" +
            "    SELECT ROLE_TYPE_ID FROM GD_ROLE_TYPE WHERE ROLE_TYPE_CODE IN \n" +
            "        ('PPC','PUB','A01','A02','B01','B13','B09','B11','PUBDIR')))";

    public static String GET_COUNT_PERSONS_EPHSTG = "select distinct count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person";

    public static String GET_COUNT_PERSONS_EPHSTG_TO_DQ = "select count(distinct \"PERSON_SOURCE_REF\") from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person;";

    public static String GET_COUNT_PERSONS_EPHSTG_TO_DQ_DELTA =  "select count(distinct \"PERSON_SOURCE_REF\") from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person\n"+
//            "where TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')";
            "where TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')";


    public static String GET_COUNT_PERSONS_EPHDQ = "select count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq";

    public static String GET_COUNT_PERSONS_EPHDQ_TO_SA = "select count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq where dq_err != 'Y'";


// old   public static String GET_COUNT_PERSONS_EPHSA = "select count(*) as count from semarchy_eph_mdm.sa_person p\n" +
//            "where p.b_loadid =  (\n" +
//            "select max (p1.b_loadid) from \n" +
//            "semarchy_eph_mdm.sa_person p1\n" +
//            "join \n" +
//            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
//            "where  sa.f_event_type = 'PMX'\n" +
//            "and sa.workflow_id = 'talend'\n" +
//            "and sa.f_workflow_source = 'PMX' )";

    public static String GET_COUNT_PERSONS_EPHSA = "select count(*) as count from semarchy_eph_mdm.sa_person p\n" +
            "where p.b_loadid =  (\n" +
            "select  max(b_loadid) from semarchy_eph_mdm.sa_event sa2  where sa2.f_event_type = 'PMX'\n" +
            "and sa2.workflow_id = 'talend'\n" +
            "and sa2.f_workflow_source = 'PMX' )";

    public static String GET_COUNT_PERSONS_EPHGD = "select count(*) from semarchy_eph_mdm.gd_person where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";


    public static String GET_DATA_PERSONS_PMX = "SELECT DISTINCT\n" +
            "     P.PARTY_ID AS PERSON_SOURCE_REF\n" +
            "    ,P.PERSON_FIRST_NAME\n" +
            "    ,P.PERSON_FAMILY_NAME\n" +
            "    ,P.PEOPLEHUB_ID\n" +
            "    ,TO_CHAR(GREATEST(NVL(P.B_UPDDATE,P.B_CREDATE),NVL(NVL(PI.B_UPDDATE,PI.B_CREDATE),TO_DATE('01/01/1900','DD/MM/YYYY')),NVL(NVL(PM.B_UPDDATE,PM.B_CREDATE),TO_DATE('01/01/1900','DD/MM/YYYY'))),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM GD_PARTY P\n" +
            "LEFT JOIN GD_PARTY_IN_PRODUCT PI ON P.PARTY_ID = PI.F_PARTY\n" +
            "LEFT JOIN GD_PMG PM ON P.PARTY_ID = PM.F_PARTY\n" +
            "WHERE PARTY_ID IN (\n" +
            "SELECT F_PARTY FROM GD_PMG\n" +
            "UNION\n" +
            "SELECT F_PARTY FROM GD_PARTY_IN_PRODUCT WHERE F_ROLE_TYPE IN (\n" +
            "    SELECT ROLE_TYPE_ID FROM GD_ROLE_TYPE WHERE ROLE_TYPE_CODE IN \n" +
            "        ('PPC','PUB','A01','A02','B01','B13','B09','B11','PUBDIR')))\n" +
            "        AND PARTY_ID IN ('%s')";

    public static String GET_DATA_PERSONS_EPHSTG = "select \n" +
            "\"PERSON_SOURCE_REF\" as PERSON_SOURCE_REF,\n" +
            "\"PERSON_FIRST_NAME\" as PERSON_FIRST_NAME,\n" +
            "\"PERSON_FAMILY_NAME\" as PERSON_FAMILY_NAME,\n" +
            "\"PEOPLEHUB_ID\" as PEOPLEHUB_ID,\n" +
            "\"UPDATED\" as UPDATED\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person\n" +
            "where \"PERSON_SOURCE_REF\" in ('%s')\n";

    public static String GET_DATA_PERSONS_EPHDQ = "select distinct\n" +
            " PERSON_SOURCE_REF as PERSON_SOURCE_REF,\n" +
            " PERSON_FIRST_NAME as PERSON_FIRST_NAME,\n" +
            " PERSON_FAMILY_NAME as PERSON_FAMILY_NAME,\n" +
            " PEOPLEHUB_ID as PEOPLEHUB_ID,\n" +
            " DQ_ERR as DQ_ERR\n" +
            " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq\n" +
            " where PERSON_SOURCE_REF in ('%s')";

    public static String GET_DATA_PERSONS_EPHSA = "select\n" +
            "b_loadid as B_LOADID, \n" +
            "b_classname as B_CLASSNAME,\n" +
            "person_id as PERSON_ID,\n" +
            "given_name as PERSON_FIRST_NAME,\n" +
            "family_name as PERSON_FAMILY_NAME\n" +
            "from semarchy_eph_mdm.sa_person p\n" +
            "where p.b_loadid =  (\n" +
            "select max (p1.b_loadid) from \n" +
            "semarchy_eph_mdm.sa_person p1\n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event sa on sa.b_loadid = p1.b_loadid \n" +
            "where  sa.f_event_type = 'PMX'\n" +
            "and sa.workflow_id = 'talend'\n" +
            "and sa.f_workflow_source = 'PMX' )\n" +
            "and person_id in ('%s')";

    public static String GET_DATA_PERSONS_EPHGD = "select\n" +
            "b_classname as B_CLASSNAME,\n" +
            "person_id as PERSON_ID,\n" +
            "given_name as PERSON_FIRST_NAME,\n" +
            "family_name as PERSON_FAMILY_NAME\n" +
            "from semarchy_eph_mdm.gd_person \n" +
            "where person_id in ('%s')";

    public static String GET_RANDOM_PERSON_IDS = "select   \n" +
            "\"PERSON_SOURCE_REF\" AS PERSON_SOURCE_REF\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person p\n" +
            "join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq dq on p.\"PERSON_SOURCE_REF\" = dq.person_source_ref\n" +
            "left join (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person) a on dq.person_source_ref::varchar = a.external_reference\n" +
            "where dq.dq_err != 'Y'\n" +
            "order by random() limit '%s'";

    public static String GET_IDS_FROM_LOOKUP_TABLE = "select numeric_id as PERSON_ID from " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid\n" +
            "where source_ref IN ('%s')";

    public static String GET_IDS_FROM_LOOKUP_TABLE_EPH = "select eph_id as PERSON_ID from " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid\n" +
            "where ref_type = '%s' and source_ref IN ('%s')";

    public static String GET_PERSON_IDS_FROM_SA  = "select  a.person_id as PERSON_ID\n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person p\n" +
            "join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_person_dq dq on p.\"PERSON_SOURCE_REF\" = dq.person_source_ref\n" +
            "left join (select distinct external_reference, person_id from semarchy_eph_mdm.sa_person sa) a on dq.person_source_ref::varchar = a.external_reference\n" +
            "where dq.dq_err != 'Y'\n" +
            "and p.\"PERSON_SOURCE_REF\" in ('%s')\n";
}
