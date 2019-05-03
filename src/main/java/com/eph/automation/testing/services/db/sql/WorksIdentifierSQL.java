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
            "  FROM ephsit.semarchy_eph_mdm.sa_work_identifier wi\n" +
            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.sa_work_identifier join \n"+
            "semarchy_eph_mdm.sa_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n"+
            " AND  f_wwork='PARAM1'";

    /*
     // old logic
    public static String getRandomProductNum="SELECT \n" +
            "    \"PRODUCT_WORK_ID\" as random_value\n" +
            "FROM\n" +
            "  ephsit_talend_owner.stg_10_pmx_wwork stg \n" +
            "where \"WORK_TYPE\" = 'PARAM1' \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1";
    */

     // EPH-366 changes  - Introduced DQ layer checks before identifying data to compare
    public static String getRandomProductNum="SELECT   \n" +
            "stg.\"PRODUCT_WORK_ID\" as random_value\n" +
            "from \n" +
            "ephsit_talend_owner.stg_10_pmx_wwork stg,\n" +
            "ephsit_talend_owner.stg_10_pmx_wwork_dq wdq\n" +
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
            "  FROM ephsit_talend_owner.stg_10_pmx_wwork\n" +
            "  WHERE \"PRODUCT_WORK_ID\"='PARAM1'";

    public static String getIdentifierDataFromGD="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS F_WWORK -- WORK IDENTIFIER\n" +
            "  FROM semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'";

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
            "  AND F_TYPE='PARAM2'";

    public static String getTypeIdGD="SELECT \n" +
            "  F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,identifier AS IDENTIFER -- identifier value \n" +
            "  FROM  semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'"+
            "  AND F_TYPE='PARAM2'";


    public static String GET_F_WWORK = "select eph_id as F_WWORK from ephsit_talend_owner.map_sourceref_2_ephid  where ref_type= 'WORK' and source_ref = '%s' ";

}
