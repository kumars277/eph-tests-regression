package com.eph.automation.testing.services.db.sql;

public class ProductDataSQL {
    public static String PMX_PRODUCT_EXTRACT="SELECT * FROM (\n" +
            "SELECT\n" +
            "\t M.ELSEVIER_PRODUCT_ID AS PRODUCT_ID -- Product Manifestation Reference,  not needed in EPH but extracted for record linking purposes\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 1 THEN CONCAT(M.PRODUCT_MANIFESTATION_TIT, ' (Print)')\n" +
            "\t\t  WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 2 THEN CONCAT(M.PRODUCT_MANIFESTATION_TIT, ' (Online)')\n" +
            "\t\t  ELSE M.PRODUCT_MANIFESTATION_TIT END AS PRODUCT_NAME -- Title\n" +
            "\t,M.PRODUCT_MANIF_SHORT_TITLE AS PRODUCT_SHORT_NAME -- Short Title\n" +
            "\t,M.SEPARATELY_SALEABLE_IND as SEPARATELY_SALEABLE_IND-- All products were considered separately Saleable in Prototype, this will not hold for implementation\n" +
            "\t,M.TRIAL_ALLOWED_IND as TRIAL_ALLOWED_IND-- Trial Allowed Indicator\n" +
            "\t,TRUNC(M.COPYRIGHT_DATE) AS FIRST_PUB_DATE -- Launch Date (dropping Time component)\n" +
            "\t,M.ELSEVIER_TAX_CODE as ELSEVIER_TAX_CODE-- Tax Code to link to LOV table (this may not be imported in the end, I have a meeting on Thursday to discuss further)\n" +
            "\t,M.PRODUCT_MANIFESTATION_ID as PRODUCT_MANIFESTATION_ID-- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,M.F_PRODUCT_WORK as F_PRODUCT_WORK-- Internal PMX Work ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,M.F_PRODUCT_MANIFESTATION_TYP as F_PRODUCT_MANIFESTATION_TYP--Print (1) or Electronic (2)\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' THEN 'Y' ELSE 'N' END AS SUBSCRIPTION\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 1 THEN 'Y' ELSE 'N' END AS BULK_SALES\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 2 THEN 'Y' ELSE 'N' END AS BACK_FILES\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND WT.OA_TYPE = 'Y' THEN 'Y' ELSE 'N' END AS OPEN_ACCESS\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 1 THEN 'Y' ELSE 'N' END AS REPRINTS\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' THEN 'Y' ELSE 'N' END AS AUTHOR_CHARGES\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'BOOK' THEN 'Y' ELSE 'N' END AS ONE_OFF_ACCESS\n" +
            "\t,CASE \n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC IN ('Planned to be available','Planned to be available (secret)'))  THEN 'PPL'\n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC = 'Available for customer sale') THEN 'PAS' \n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC = 'Available but item not for sale') THEN 'PNS'\n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC IN ('Elsevier owned content, no longer published','No longer published by Elsevier. Old content can still be on SD'))\n" +
            "\t\t\tTHEN 'PSTB'\n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC = 'Currently unavailable') THEN 'PST'\n" +
            "\t\tELSE 'UNK' \n" +
            "\tEND AS AVAILABILITY_STATUS\n" +
            "\n" +
            "FROM GD_PRODUCT_MANIFESTATION M\n" +
            "\n" +
            "LEFT JOIN GD_PRODUCT_SUBSTATUS MSS ON M.F_MANIFESTATION_SUBSTATUS = MSS.PRODUCT_SUBSTATUS_ID\n" +
            "\n" +
            "JOIN (SELECT MAN.PRODUCT_MANIFESTATION_ID\n" +
            "\t  ,CASE WHEN T.F_PRODUCT_GROUP_TYPE = 1 THEN 'BOOK' WHEN T.F_PRODUCT_GROUP_TYPE = 2 THEN 'JOURNAL' ELSE 'OTHER' END AS WORK_TYPE\n" +
            "\t  ,CASE WHEN W.F_OPEN_ACCESS_JOURNAL_TYPE IN (10,11,12) THEN 'Y' ELSE 'N' END AS OA_TYPE\n" +
            "\t  FROM GD_PRODUCT_MANIFESTATION MAN \n" +
            "\t  JOIN GD_PRODUCT_WORK W ON MAN.F_PRODUCT_WORK = W.PRODUCT_WORK_ID\n" +
            "\t  JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID) WT ON WT.PRODUCT_MANIFESTATION_ID = M.PRODUCT_MANIFESTATION_ID)\n" +
            "WHERE PARAM1='PARAM2'";

    public static String EPH_STG_PRODUCT_EXTRACT="SELECT\n"+
            "\"PRODUCT_ID\" as PRODUCT_ID\n"+
            "\"PRODUCT_NAME\" as PRODUCT_NAME\n"+
            "\"PRODUCT_SHORT_NAME\" as PRODUCT_SHORT_NAME\n"+
            "\"SEPARATELY_SALEABLE_IND\" as SEPARATELY_SALEABLE_IND\n"+
            "\"TRIAL_ALLOWED_IND\" as TRIAL_ALLOWED_IND\n"+
            "\"FIRST_PUB_DATE\" as FIRST_PUB_DATE\n"+
            "\"ELSEVIER_TAX_CODE\" as ELSEVIER_TAX_CODE\n"+
            "\"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID\n"+
            "\"F_PRODUCT_WORK\" as F_PRODUCT_WORK\n"+
            "\"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP\n"+
            "\"SUBSCRIPTION\" as SUBSCRIPTION\n"+
            "\"BULK_SALES\" as BULK_SALES\n"+
            "\"BACK_FILES\" as BACK_FILES\n"+
            "\"OPEN_ACCESS\" as OPEN_ACCESS\n"+
            "\"REPRINTS\" as REPRINTS\n"+
            "\"AUTHOR_CHARGES\" as AUTHOR_CHARGES\n"+
            "\"ONE_OFF_ACCESS\" as ONE_OFF_ACCESS\n"+
            "\"AVAILABILITY_STATUS\" as AVAILABILITY_STATUS\n"+
            "FROM ephsit.ephsit_talend_owner.stg_pmx_product\n"+
            "WHERE \"PARAM1\" = 'PARAM2'";

    public static String EPH_SA_PRODUCT_EXTRACT ="SELECT \n" +
            "   B_LOADID AS LOADID\n" +
            "  ,F_EVENT AS F_EVENT\n" +
            "  ,sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,product_id AS product_id -- Title\n" +
            "  ,pmx_source_reference AS pmx_source_reference -- Subtitle\n" +
            "  ,name as name\n" +
            "  ,short_name as short_name\n" +
            "  ,separately_sale_indicator as separately_sale_indicator\n" +
            "  ,trial_allowed_indicator as trial_allowed_indicator\n" +
            "  ,launch_date as launch_date\n" +
            "  ,f_type AS f_type\n" +
            "  ,f_status AS f_status\n" +
            "  ,f_revenue_model AS f_revenue_model\n" +
            "  ,f_wwork AS f_wwork\n" +
            "  ,f_manifestation AS f_manifestation\n" +
            "  FROM ephsit.semarchy_eph_mdm.sa_product sa\n" +
            "  join semarchy_eph_mdm.sa_event on f_event = event_id and f_event = (select max (f_event) from semarchy_eph_mdm.sa_product)\n" +
            "  and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "  WHERE pmx_source_reference='PARAM1'";


}
