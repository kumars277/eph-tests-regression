package com.eph.automation.testing.services.db.sql;

public class WorksIdentifierSQL {
    public static String getRandomIdentifier="SELECT \n" +
            "    \"PARAM1\" as random_value\n" +
            "FROM\n" +
            "    ephsit.ephsit_talend_owner.stg_pmx_wwork\n" +
            "where \"PARAM1\" is not null \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1;";

    public static String getCountOfIds="SELECT count (*) as workCountEPH from ephsit.semarchy_eph_mdm.sa_work_identifier where f_wwork='PARAM1'";

    public static String getRandomWithSeveralIdentifiers="SELECT \n" +
            "    \"PARAM1\" as random_value\n" +
            "FROM\n" +
            "    ephsit.ephsit_talend_owner.stg_pmx_wwork\n" +
            "where \"PARAM1\" is not null \n" +
            "where \"PARAM2\" is not null \n" +
            "where \"PARAM3\" is not null \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1;";

    public static String getIdentifierDataFromSTG="SELECT DISTINCT \n"+
            "  \"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "  ,\"PARAM1\" AS IDENTIFIER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM ephsit.ephsit_talend_owner.stg_pmx_wwork\n" +
            "  WHERE \"PARAM1\"='PARAM2'";

    public static String getEphWorkID="SELECT \n" +
            "WORK_ID as WORK_ID FROM ephsit.semarchy_eph_mdm.sa_wwork WHERE PMX_SOURCE_REFERENCE='PARAM1'";

    public static String getIdentifierDataFromSA="SELECT \n" +
            " B_LOADID as B_LOADID\n" +
            " ,F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS PRODUCT_WORK_ID -- WORK IDENTIFIER\n" +
            "  FROM ephsit.semarchy_eph_mdm.sa_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'";

    public static String getRandomProductNum="SELECT \n" +
            "    \"PARAM1\" as random_value\n" +
            "FROM\n" +
            "    ephsit.ephsit_talend_owner.stg_pmx_wwork\n" +
            "where \"WORK_TYPE\" = 'PARAM2' \n" +
            "ORDER BY RANDOM()\n" +
            "LIMIT 1;";

    public static String getIdentifiers = "SELECT "+
            "  \"JOURNAL_NUMBER\" AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"ISSN_L\" as ISSN_L-- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"JOURNAL_ACRONYM\" AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"DAC_KEY\" as DAC_KEY-- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PROJECT_NUM\" AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM ephsit.ephsit_talend_owner.stg_pmx_wwork\n" +
            "  WHERE \"PARAM1\"='PARAM2'";

    public static String getIdentifierDataFromGD="SELECT \n" +
            " F_EVENT as F_EVENT\n" +
            " ,B_CLASSNAME as B_CLASSNAME\n" +
            " ,WORK_IDENTIFIER_ID AS WORK_IDENTIFIER_ID -- WORK IDENTIFIER\n" +
            " ,IDENTIFIER AS IDENTIFIER --  IDENTIFIER\n" +
            " ,F_TYPE AS F_TYPE -- WORK IDENTIFIER\n" +
            " ,F_WWORK AS PRODUCT_WORK_ID -- WORK IDENTIFIER\n" +
            "  FROM ephsit.semarchy_eph_mdm.gd_work_identifier\n" +
            "  WHERE f_wwork='PARAM1'";

}
