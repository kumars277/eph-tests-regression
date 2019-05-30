package com.eph.automation.testing.services.db.sql;

public class WorksIdentifierSQL {
    public static String getEphWorkID="SELECT \n" +
            "WORK_ID as WORK_ID FROM semarchy_eph_mdm.sa_wwork\n"  +
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_wwork join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )"+
            " AND PMX_SOURCE_REFERENCE='PARAM1'";

    public static String getIdentifierDataFromSA="SELECT \n" +
            " wi.B_LOADID as B_LOADID\n" +
            " ,wi.F_EVENT as F_EVENT\n" +
            " ,wi.B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS F_WWORK -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.sa_work_identifier wi\n" +
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_identifier join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            " AND  f_wwork='PARAM1'\n" +
            "AND effective_end_date is null";

    /*
     // old logic
    public static String getRandomProductNum="SELECT \n" +
            "    \"PRODUCT_WORK_ID\" as random_value\n" +
            "FROM\n" +
            "  "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg \n" +
            "where \"WORK_TYPE\" = 'PARAM1' \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1";
    */

     // EPH-366 changes  - Introduced DQ layer checks before identifying data to compare
    public static String getRandomProductNum="SELECT   \n" +
            "stg.\"PRODUCT_WORK_ID\" as random_value\n" +
            "from \n" +
            ""+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork stg,\n" +
            ""+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq wdq\n" +
            "where \n" +
            "stg.\"WORK_TYPE\" = 'PARAM1' and \n" +
            "stg.\"PRODUCT_WORK_ID\" = wdq.pmx_source_reference and\n" +
            "wdq.dq_err != 'Y'\n" +
            "ORDER BY RANDOM()  LIMIT 1";

    //No need to check for DQ error as it is checked while determining the random ID
    public static String getIdentifiers = "SELECT "+
            "  \"JOURNAL_NUMBER\" AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"ISSN_L\" as ISSN_L-- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"JOURNAL_ACRONYM\" AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"DAC_KEY\" as DAC_KEY-- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PROJECT_NUM\" AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PRODUCT_WORK_ID\" AS PRODUCT_WORK_ID-- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork\n" +
            "  WHERE \"PRODUCT_WORK_ID\"='PARAM1'";

    public static String getIdentifierDataFromGD="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS F_WWORK -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'\n" +
            "  AND effective_end_date is null";

    public static String getTypeId="SELECT \n" +
            "   F_TYPE AS F_TYPE -- Identifier type\n" +
            "  ,identifier AS IDENTIFER -- identifier value \n" +
            "  FROM semarchy_eph_mdm.sa_work_identifier\n" +
            "  join semarchy_eph_mdm.sa_event on f_event = event_id and f_event = (select max(f_event) from \n" +
            "            semarchy_eph_mdm.sa_wwork join \n" +
            "            semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "            where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "            and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "            and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n" +
            "  WHERE f_wwork='PARAM1'"+
            "  AND F_TYPE='PARAM2'\n" +
            "AND effective_end_date is null";

    public static String getTypeIdGD="SELECT \n" +
            "  F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,identifier AS IDENTIFER -- identifier value \n" +
            "  FROM  semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'"+
            "  AND F_TYPE='PARAM2'\n" +
            "AND effective_end_date is null";


    public static String GET_F_WWORK = "select eph_id as F_WWORK from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid  where ref_type= 'WORK' and source_ref = '%s' ";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_WORK_TABLE = "select count(distinct \"PRODUCT_WORK_ID\") AS count \n" +
            " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork stg ,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq  mdq\n" +
            "where \n" +
            "stg.\"PARAM1\" is not null  and \n" +
            "   stg.\"PRODUCT_WORK_ID\" = mdq.PMX_SOURCE_REFERENCE and mdq.dq_err != 'Y' \n" +
            "   \n";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_WORK_DELTA = "select count(distinct \"PRODUCT_WORK_ID\") AS count \n" +
            " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation stg ,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq  mdq\n" +
            "where \n" +
            "stg.\"PARAM1\" is not null  and \n" +
            "   stg.\"PRODUCT_WORK_ID\" = mdq.PMX_SOURCE_REFERENCE and mdq.dq_err != 'Y' \n" +
            "   and TO_DATE(\"UPDATED\",'DD-MON-YY HH.MI.SS') > TO_DATE('PARAM2','YYYYMMDDHH24MI')";

    public static String COUNT_SA_WORK_IDENTIFIER = "select count(*) AS count from semarchy_eph_mdm.sa_work_identifier where f_type = 'PARAM1'" +
            " and effective_end_date is null\n" +
            " and f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_identifier join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";

    public static String COUNT_GD_WORK_IDENTIFIER = "select count(*) AS count from semarchy_eph_mdm.gd_work_identifier where f_type = 'PARAM1'"+
            " and effective_end_date is null\n" +
            " and f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_identifier join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";

    public static String getEndDatedIdentifierDataFromGD="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS F_WWORK -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.gd_work_identifier\n" +
            "  where effective_end_date is not null\n"+
            " AND f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_work_identifier join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";


    public static String getPmxSourceRef="SELECT \n" +
            "PMX_SOURCE_REFERENCE as WORK_ID FROM semarchy_eph_mdm.gd_wwork\n"  +
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_wwork join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )"+
            " AND work_id='PARAM1'";

}
