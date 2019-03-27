package com.eph.automation.testing.services.db.sql;

public class ProductDataSQL {
    public static String PMX_PRODUCT_EXTRACT = "SELECT\n" +
            "\t M.ELSEVIER_PRODUCT_ID AS PRODUCT_ID -- Product Manifestation Reference,  not needed in EPH but extracted for record linking purposes\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 1 THEN CONCAT(M.PRODUCT_MANIFESTATION_TIT, ' (Print)')\n" +
            "\t\t  WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 2 THEN CONCAT(M.PRODUCT_MANIFESTATION_TIT, ' (Online)')\n" +
            "\t\t  ELSE M.PRODUCT_MANIFESTATION_TIT END AS PRODUCT_NAME -- Title\n" +
            "\t,M.PRODUCT_MANIF_SHORT_TITLE AS PRODUCT_SHORT_NAME -- Short Title\n" +
            "\t,NVL(M.NONSALEABLE_ANCILLARY_IND,'Y') AS SEPARATELY_SALEABLE_IND -- Set flag as 'Y' when manifestation is not flagged as Nonsaleable Ancillary\n" +
            "\t,NVL(M.TRIAL_ALLOWED_IND, 'N') AS TRIAL_ALLOWED_IND -- Trial Allowed Indicator\n" +
            "\t,TRUNC(M.COPYRIGHT_DATE) AS FIRST_PUB_DATE -- Launch Date (dropping Time component)\n" +
            "\t,M.ELSEVIER_TAX_CODE -- Tax Code to link to LOV table (this may not be imported in the end, I have a meeting on Thursday to discuss further)\n" +
            "\t,M.PRODUCT_MANIFESTATION_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,M.F_PRODUCT_WORK -- Internal PMX Work ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,M.F_PRODUCT_MANIFESTATION_TYP --Print (1) or Electronic (2)\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' THEN 'Y' ELSE 'N' END AS SUBSCRIPTION\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 1 THEN 'Y' ELSE 'N' END AS BULK_SALES\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 2 THEN 'Y' ELSE 'N' END AS BACK_FILES\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND WT.OA_TYPE = 'Y' THEN 'Y' ELSE 'N' END AS OPEN_ACCESS\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.F_PRODUCT_MANIFESTATION_TYP = 1 THEN 'Y' ELSE 'N' END AS REPRINTS\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'JOURNAL' AND M.PRODUCT_MANIFESTATION_ID = WT.FIRST_MANIFESTATION THEN 'Y' ELSE 'N' END AS AUTHOR_CHARGES\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'BOOK' THEN 'Y' ELSE 'N' END AS ONE_OFF_ACCESS\n" +
            "\t,CASE WHEN WT.WORK_TYPE = 'PACKAGE' THEN 'Y' ELSE 'N' END AS PACKAGES\n" +
            "\t,CASE\n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC IN ('Planned to be available','Planned to be available (secret)'))  THEN 'PPL'\n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC = 'Available for customer sale') THEN 'PAS'\n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC = 'Available but item not for sale') THEN 'PNS'\n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC IN ('Elsevier owned content, no longer published','No longer published by Elsevier. Old content can still be on SD'))\n" +
            "\t\t\tTHEN 'PSTB'\n" +
            "\t\tWHEN (MSS.SUBSTATUS_DESC = 'Currently unavailable') THEN 'PST'\n" +
            "\t\tELSE 'UNK'\n" +
            "\tEND AS AVAILABILITY_STATUS\n" +
            "\t,WT.WORK_TITLE\n" +
            "\t,WT.WORK_TYPE\n" +
            "FROM GD_PRODUCT_MANIFESTATION M\n" +
            "LEFT JOIN GD_PRODUCT_SUBSTATUS MSS ON M.F_MANIFESTATION_SUBSTATUS = MSS.PRODUCT_SUBSTATUS_ID\n" +
            "JOIN (SELECT\n" +
            "\t     MAN.PRODUCT_MANIFESTATION_ID\n" +
            "\t    ,CASE WHEN T.PRODUCT_TYPE_ID IN (143,21) THEN 'PACKAGE' WHEN T.F_PRODUCT_GROUP_TYPE = 1 THEN 'BOOK' WHEN T.F_PRODUCT_GROUP_TYPE = 2 THEN 'JOURNAL' ELSE 'OTHER' END AS WORK_TYPE\n" +
            "\t    ,CASE WHEN W.F_OPEN_ACCESS_JOURNAL_TYPE IN (10,11,12) AND MAN.PRODUCT_MANIFESTATION_ID = FIR.FIRST_MANIFESTATION THEN 'Y' ELSE 'N' END AS OA_TYPE\n" +
            "\t    ,W.PRODUCT_WORK_TITLE AS WORK_TITLE\n" +
            "\t    ,FIR.FIRST_MANIFESTATION\n" +
            "      FROM GD_PRODUCT_MANIFESTATION MAN\n" +
            "      JOIN GD_PRODUCT_WORK W ON MAN.F_PRODUCT_WORK = W.PRODUCT_WORK_ID\n" +
            "      JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "      JOIN (SELECT FMAN.F_PRODUCT_WORK, MIN(FMAN.PRODUCT_MANIFESTATION_ID) AS FIRST_MANIFESTATION\n" +
            "\t        FROM GD_PRODUCT_MANIFESTATION FMAN GROUP BY FMAN.F_PRODUCT_WORK) FIR ON MAN.F_PRODUCT_WORK = FIR.F_PRODUCT_WORK\n" +
            "\t   ) WT ON WT.PRODUCT_MANIFESTATION_ID = M.PRODUCT_MANIFESTATION_ID\n" +
            "\t WHERE  M.PRODUCT_MANIFESTATION_ID IN ('%s')\n" +
            "\t order by  M.PRODUCT_MANIFESTATION_ID";

    public static String EPH_STG_PRODUCT_EXTRACT = "SELECT\n" +
            "           \"PRODUCT_ID\" as PRODUCT_ID,\n" +
            "            \"PRODUCT_NAME\" as PRODUCT_NAME,\n" +
            "            \"PRODUCT_SHORT_NAME\" as PRODUCT_SHORT_NAME,\n" +
            "            \"TRIAL_ALLOWED_IND\" as TRIAL_ALLOWED_IND,\n" +
            "            \"FIRST_PUB_DATE\" as FIRST_PUB_DATE,\n" +
            "            \"ELSEVIER_TAX_CODE\" as ELSEVIER_TAX_CODE,\n" +
            "            \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID,\n" +
            "            \"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "            \"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "            \"SUBSCRIPTION\" as SUBSCRIPTION,\n" +
            "            \"BULK_SALES\" as BULK_SALES,\n" +
            "            \"BACK_FILES\" as BACK_FILES,\n" +
            "            \"OPEN_ACCESS\" as OPEN_ACCESS,\n" +
            "            \"REPRINTS\" as REPRINTS,\n" +
            "            \"AUTHOR_CHARGES\" as AUTHOR_CHARGES,\n" +
            "            \"ONE_OFF_ACCESS\" as ONE_OFF_ACCESS,\n" +
            "            \"PACKAGES\" as PACKAGES,\n" +
            "            \"AVAILABILITY_STATUS\" as AVAILABILITY_STATUS,\n" +
            "            \"WORK_TITLE\" as WORK_TITLE,\n" +
            "            \"WORK_TYPE\" as WORK_TYPE,\n" +
            "            \"SEPARATELY_SALEABLE_IND\" as SEPARATELY_SALEABLE_IND\n" +
            "            FROM ephsit.ephsit_talend_owner.stg_10_pmx_product\n" +
            "            WHERE \"PRODUCT_MANIFESTATION_ID\" IN ('%s')\n" +
             "           order by \"PRODUCT_MANIFESTATION_ID\"";

    public static String EPH_STG_CAN_PRODUCT_EXTRACT_BOOKS = "SELECT \n" +
            "       pmx_source_reference as PMX_SOURCE_REFERENCE,\n" +
            "       name as PRODUCT_NAME,\n" +
            "       product_short_name as PRODUCT_SHORT_NAME,\n" +
            "       separately_sale_indicator as SEPARATELY_SALEABLE_IND,\n" +
            "       pmx_source_reference as PMX_SOURCE_REFERENCE,\n" +
            "       trial_allowed_indicator as TRIAL_ALLOWED_IND,\n" +
            "       launch_date as FIRST_PUB_DATE,\n" +
            "       f_type AS F_TYPE,\n" +
            "       f_status AS F_STATUS,\n" +
            "       f_revenue_model AS F_REVENUE_MODEL, \n" +
            "       f_work_source_ref AS F_PRODUCT_WORK, \n" +
            "       f_manifestation_source_ref AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "       work_type as WORK_TYPE,\n" +
            "       utl_work_ref as UTL_WORK_REF\n" +
            "FROM ephsit_talend_owner.stg_10_pmx_product_can\n" +
            "where pmx_source_reference in ('%s')";

    public static String EPH_STG_CAN_PRODUCT_EXTRACT_JOURNALS_OR_PACKAGES = "SELECT \n" +
            "       pmx_source_reference as PMX_SOURCE_REFERENCE,\n" +
            "       name as PRODUCT_NAME,\n" +
            "       product_short_name as PRODUCT_SHORT_NAME,\n" +
            "       separately_sale_indicator as SEPARATELY_SALEABLE_IND,\n" +
            "       pmx_source_reference as PMX_SOURCE_REFERENCE,\n" +
            "       trial_allowed_indicator as TRIAL_ALLOWED_IND,\n" +
            "       launch_date as FIRST_PUB_DATE,\n" +
            "       f_type AS F_TYPE,\n" +
            "       f_status AS F_STATUS,\n" +
            "       f_revenue_model AS F_REVENUE_MODEL, \n" +
            "       f_work_source_ref AS F_PRODUCT_WORK, \n" +
            "       f_manifestation_source_ref AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "       work_type as WORK_TYPE,\n" +
            "       utl_work_ref as UTL_WORK_REF\n" +
            "FROM ephsit_talend_owner.stg_10_pmx_product_can\n" +
            "where pmx_source_reference similar to '%s' and pmx_source_reference not like '%%OOA'";

    public static String EPH_STG_PRODUCT_EXTRACT_JOURNAL = "SELECT\n" +
            "           \"PRODUCT_ID\" as PRODUCT_ID,\n" +
            "            \"PRODUCT_NAME\" as PRODUCT_NAME,\n" +
            "            \"PRODUCT_SHORT_NAME\" as PRODUCT_SHORT_NAME,\n" +
            "            \"TRIAL_ALLOWED_IND\" as TRIAL_ALLOWED_IND,\n" +
            "            \"FIRST_PUB_DATE\" as FIRST_PUB_DATE,\n" +
            "            \"ELSEVIER_TAX_CODE\" as ELSEVIER_TAX_CODE,\n" +
            "            \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID,\n" +
            "            \"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "            \"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "            \"SUBSCRIPTION\" as SUBSCRIPTION,\n" +
            "            \"BULK_SALES\" as BULK_SALES,\n" +
            "            \"BACK_FILES\" as BACK_FILES,\n" +
            "            \"OPEN_ACCESS\" as OPEN_ACCESS,\n" +
            "            \"REPRINTS\" as REPRINTS,\n" +
            "            \"AUTHOR_CHARGES\" as AUTHOR_CHARGES,\n" +
            "            \"ONE_OFF_ACCESS\" as ONE_OFF_ACCESS,\n" +
            "            \"AVAILABILITY_STATUS\" as AVAILABILITY_STATUS,\n" +
            "            \"WORK_TITLE\" as WORK_TITLE,\n" +
            "            \"SEPARATELY_SALEABLE_IND\" as SEPARATELY_SALEABLE_IND\n" +
            "            FROM ephsit.ephsit_talend_owner.stg_10_pmx_product\n" +
            "            WHERE \"PRODUCT_MANIFESTATION_ID\" IN ('%s') AND \"SUBSCRIPTION\" = 'Y' AND \"F_PRODUCT_MANIFESTATION_TYP\" = '%s'";

    public static String EPH_STG_PRODUCT_EXTRACT_JOURNAL_NOT_PRINT_OR_ELECTRONIC = "SELECT\n" +
            "           \"PRODUCT_ID\" as PRODUCT_ID,\n" +
            "            \"PRODUCT_NAME\" as PRODUCT_NAME,\n" +
            "            \"PRODUCT_SHORT_NAME\" as PRODUCT_SHORT_NAME,\n" +
            "            \"TRIAL_ALLOWED_IND\" as TRIAL_ALLOWED_IND,\n" +
            "            \"FIRST_PUB_DATE\" as FIRST_PUB_DATE,\n" +
            "            \"ELSEVIER_TAX_CODE\" as ELSEVIER_TAX_CODE,\n" +
            "            \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID,\n" +
            "            \"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "            \"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "            \"SUBSCRIPTION\" as SUBSCRIPTION,\n" +
            "            \"BULK_SALES\" as BULK_SALES,\n" +
            "            \"BACK_FILES\" as BACK_FILES,\n" +
            "            \"OPEN_ACCESS\" as OPEN_ACCESS,\n" +
            "            \"REPRINTS\" as REPRINTS,\n" +
            "            \"AUTHOR_CHARGES\" as AUTHOR_CHARGES,\n" +
            "            \"ONE_OFF_ACCESS\" as ONE_OFF_ACCESS,\n" +
            "            \"AVAILABILITY_STATUS\" as AVAILABILITY_STATUS,\n" +
            "            \"WORK_TITLE\" as WORK_TITLE,\n" +
            "            \"SEPARATELY_SALEABLE_IND\" as SEPARATELY_SALEABLE_IND\n" +
            "            FROM ephsit.ephsit_talend_owner.stg_10_pmx_product\n" +
            "            WHERE \"PRODUCT_MANIFESTATION_ID\" IN ('%s') AND \"SUBSCRIPTION\" = 'Y' AND \"F_PRODUCT_MANIFESTATION_TYP\" not in (1,2)";

    public static String EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_JAS = "SELECT\n" +
            "           \"PRODUCT_ID\" as PRODUCT_ID,\n" +
            "            \"PRODUCT_NAME\" as PRODUCT_NAME,\n" +
            "            \"PRODUCT_SHORT_NAME\" as PRODUCT_SHORT_NAME,\n" +
            "            \"TRIAL_ALLOWED_IND\" as TRIAL_ALLOWED_IND,\n" +
            "            \"FIRST_PUB_DATE\" as FIRST_PUB_DATE,\n" +
            "            \"ELSEVIER_TAX_CODE\" as ELSEVIER_TAX_CODE,\n" +
            "            \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID,\n" +
            "            \"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "            \"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "            \"SUBSCRIPTION\" as SUBSCRIPTION,\n" +
            "            \"BULK_SALES\" as BULK_SALES,\n" +
            "            \"BACK_FILES\" as BACK_FILES,\n" +
            "            \"OPEN_ACCESS\" as OPEN_ACCESS,\n" +
            "            \"REPRINTS\" as REPRINTS,\n" +
            "            \"AUTHOR_CHARGES\" as AUTHOR_CHARGES,\n" +
            "            \"ONE_OFF_ACCESS\" as ONE_OFF_ACCESS,\n" +
            "            \"AVAILABILITY_STATUS\" as AVAILABILITY_STATUS,\n" +
            "            \"WORK_TITLE\" as WORK_TITLE,\n" +
            "            \"SEPARATELY_SALEABLE_IND\" as SEPARATELY_SALEABLE_IND\n" +
            "            FROM ephsit.ephsit_talend_owner.stg_10_pmx_product\n" +
            "            WHERE \"F_PRODUCT_WORK\" IN ('%s') AND \"SUBSCRIPTION\" = 'Y' AND \"F_PRODUCT_MANIFESTATION_TYP\" = '%s' ";

    public static String EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_OAA = "SELECT\n" +
            "           \"PRODUCT_ID\" as PRODUCT_ID,\n" +
            "            \"PRODUCT_NAME\" as PRODUCT_NAME,\n" +
            "            \"PRODUCT_SHORT_NAME\" as PRODUCT_SHORT_NAME,\n" +
            "            \"TRIAL_ALLOWED_IND\" as TRIAL_ALLOWED_IND,\n" +
            "            \"FIRST_PUB_DATE\" as FIRST_PUB_DATE,\n" +
            "            \"ELSEVIER_TAX_CODE\" as ELSEVIER_TAX_CODE,\n" +
            "            \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID,\n" +
            "            \"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "            \"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "            \"SUBSCRIPTION\" as SUBSCRIPTION,\n" +
            "            \"BULK_SALES\" as BULK_SALES,\n" +
            "            \"BACK_FILES\" as BACK_FILES,\n" +
            "            \"OPEN_ACCESS\" as OPEN_ACCESS,\n" +
            "            \"REPRINTS\" as REPRINTS,\n" +
            "            \"AUTHOR_CHARGES\" as AUTHOR_CHARGES,\n" +
            "            \"ONE_OFF_ACCESS\" as ONE_OFF_ACCESS,\n" +
            "            \"AVAILABILITY_STATUS\" as AVAILABILITY_STATUS,\n" +
            "            \"WORK_TITLE\" as WORK_TITLE,\n" +
            "            \"SEPARATELY_SALEABLE_IND\" as SEPARATELY_SALEABLE_IND\n" +
            "            FROM ephsit.ephsit_talend_owner.stg_10_pmx_product\n" +
            "            WHERE \"F_PRODUCT_WORK\" IN ('%s') AND \"SUBSCRIPTION\" = 'Y' AND \"F_PRODUCT_MANIFESTATION_TYP\" = '%s' ";

    public static String EPH_STG_GET_COUNT_OF_RECORDS_WITH_GIVEN_F_PRODUCT_WORK = "select count(*) as count from ephsit_talend_owner.stg_10_pmx_product where \"SUBSCRIPTION\" ='Y' and \"F_PRODUCT_WORK\"  = '%s' group by \"F_PRODUCT_WORK\"";

    public static String EPH_STG_GET_COUNT_OF_RECORDS_WITH_OAA_GIVEN_F_PRODUCT_WORK = "select count(*) as count from ephsit_talend_owner.stg_10_pmx_product where \"SUBSCRIPTION\" ='Y' and \"F_PRODUCT_WORK\"  = '%s' and \"OPEN_ACCESS\" ='Y'  group by \"F_PRODUCT_WORK\"";

    public static String EPH_STG_GET_COUNT_OF_RECORDS_WITH_JAS_GIVEN_F_PRODUCT_WORK = "select count(*) as count from ephsit_talend_owner.stg_10_pmx_product where \"SUBSCRIPTION\" ='Y' and \"F_PRODUCT_WORK\"  = '%s' and \"AUTHOR_CHARGES\" ='Y'  group by \"F_PRODUCT_WORK\"";

    public static String EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_NOT_PRINT_OR_ELECTRONIC_JAS = "SELECT\n" +
            "           \"PRODUCT_ID\" as PRODUCT_ID,\n" +
            "            \"PRODUCT_NAME\" as PRODUCT_NAME,\n" +
            "            \"PRODUCT_SHORT_NAME\" as PRODUCT_SHORT_NAME,\n" +
            "            \"TRIAL_ALLOWED_IND\" as TRIAL_ALLOWED_IND,\n" +
            "            \"FIRST_PUB_DATE\" as FIRST_PUB_DATE,\n" +
            "            \"ELSEVIER_TAX_CODE\" as ELSEVIER_TAX_CODE,\n" +
            "            \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID,\n" +
            "            \"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "            \"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "            \"SUBSCRIPTION\" as SUBSCRIPTION,\n" +
            "            \"BULK_SALES\" as BULK_SALES,\n" +
            "            \"BACK_FILES\" as BACK_FILES,\n" +
            "            \"OPEN_ACCESS\" as OPEN_ACCESS,\n" +
            "            \"REPRINTS\" as REPRINTS,\n" +
            "            \"AUTHOR_CHARGES\" as AUTHOR_CHARGES,\n" +
            "            \"ONE_OFF_ACCESS\" as ONE_OFF_ACCESS,\n" +
            "            \"AVAILABILITY_STATUS\" as AVAILABILITY_STATUS,\n" +
            "            \"WORK_TITLE\" as WORK_TITLE,\n" +
            "            \"SEPARATELY_SALEABLE_IND\" as SEPARATELY_SALEABLE_IND\n" +
            "            FROM ephsit.ephsit_talend_owner.stg_10_pmx_product\n" +
            "            WHERE \"F_PRODUCT_WORK\" IN ('%s') AND \"SUBSCRIPTION\" = 'Y' AND \"F_PRODUCT_MANIFESTATION_TYP\" NOT IN (1, 2) AND \"AUTHOR_CHARGES\" = 'Y'";

    public static String EPH_STG_PRODUCT_EXTRACT_BY_GIVEN_F_PRODUCT_WORK_NOT_PRINT_OR_ELECTRONIC_OAA = "SELECT\n" +
            "           \"PRODUCT_ID\" as PRODUCT_ID,\n" +
            "            \"PRODUCT_NAME\" as PRODUCT_NAME,\n" +
            "            \"PRODUCT_SHORT_NAME\" as PRODUCT_SHORT_NAME,\n" +
            "            \"TRIAL_ALLOWED_IND\" as TRIAL_ALLOWED_IND,\n" +
            "            \"FIRST_PUB_DATE\" as FIRST_PUB_DATE,\n" +
            "            \"ELSEVIER_TAX_CODE\" as ELSEVIER_TAX_CODE,\n" +
            "            \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID,\n" +
            "            \"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "            \"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "            \"SUBSCRIPTION\" as SUBSCRIPTION,\n" +
            "            \"BULK_SALES\" as BULK_SALES,\n" +
            "            \"BACK_FILES\" as BACK_FILES,\n" +
            "            \"OPEN_ACCESS\" as OPEN_ACCESS,\n" +
            "            \"REPRINTS\" as REPRINTS,\n" +
            "            \"AUTHOR_CHARGES\" as AUTHOR_CHARGES,\n" +
            "            \"ONE_OFF_ACCESS\" as ONE_OFF_ACCESS,\n" +
            "            \"AVAILABILITY_STATUS\" as AVAILABILITY_STATUS,\n" +
            "            \"WORK_TITLE\" as WORK_TITLE,\n" +
            "            \"SEPARATELY_SALEABLE_IND\" as SEPARATELY_SALEABLE_IND\n" +
            "            FROM ephsit.ephsit_talend_owner.stg_10_pmx_product\n" +
            "            WHERE \"F_PRODUCT_WORK\" IN ('%s') AND \"SUBSCRIPTION\" = 'Y' AND \"F_PRODUCT_MANIFESTATION_TYP\" NOT IN (1, 2) AND \"OPEN_ACCESS\" = 'Y'";


    public static String EPH_SA_PRODUCT_EXTRACT = "SELECT \n" +
            "   sa.B_LOADID AS LOADID\n" +
            "  ,F_EVENT AS F_EVENT\n" +
            "  ,sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,product_id AS PRODUCT_ID -- Title\n" +
            "  ,pmx_source_reference AS PMX_SOURCE_REFERENCE -- Subtitle\n" +
            "  ,name as PRODUCT_NAME\n" +
            "  ,short_name as PRODUCT_SHORT_NAME\n" +
            "  ,separately_sale_indicator as SEPARATELY_SALEABLE_IND\n" +
            "  ,trial_allowed_indicator as TRIAL_ALLOWED_IND\n" +
            "  ,launch_date as FIRST_PUB_DATE\n" +
            "  ,f_type AS F_TYPE\n" +
            "  ,f_status AS F_STATUS\n" +
            "  ,f_revenue_model AS F_REVENUE_MODEL\n" +
            "  ,f_wwork AS F_PRODUCT_WORK\n" +
            "  ,f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "  FROM ephsit.semarchy_eph_mdm.sa_product sa where f_event =  (\n" +
            "select max (f_event) from \n" +
            "semarchy_eph_mdm.sa_product   \n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n" +
            "  AND pmx_source_reference IN ('%s')";

    public static String EPH_GD_PRODUCT_EXTRACT = "SELECT \n" +
            "  F_EVENT AS F_EVENT\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,product_id AS PRODUCT_ID -- Title\n" +
            "  ,pmx_source_reference AS PMX_SOURCE_REFERENCE -- Subtitle\n" +
            "  ,name as PRODUCT_NAME\n" +
            "  ,short_name as PRODUCT_SHORT_NAME\n" +
            "  ,separately_sale_indicator as SEPARATELY_SALEABLE_IND\n" +
            "  ,trial_allowed_indicator as TRIAL_ALLOWED_IND\n" +
            "  ,launch_date as FIRST_PUB_DATE\n" +
            "  ,f_type AS F_TYPE\n" +
            "  ,f_status AS F_STATUS\n" +
            "  ,f_revenue_model AS F_REVENUE_MODEL\n" +
            "  ,f_wwork AS F_PRODUCT_WORK\n" +
            "  ,f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "  FROM ephsit.semarchy_eph_mdm.gd_product" +
            "  WHERE pmx_source_reference IN ('%s')\n" ;

    public static String EPH_SA_PRODUCT_EXTRACT_JOURNALS_OR_PACKAGES = "SELECT \n" +
            "   sa.B_LOADID AS LOADID\n" +
            "  ,F_EVENT AS F_EVENT\n" +
            "  ,sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,product_id AS PRODUCT_ID -- Title\n" +
            "  ,pmx_source_reference AS PMX_SOURCE_REFERENCE -- Subtitle\n" +
            "  ,name as PRODUCT_NAME\n" +
            "  ,short_name as PRODUCT_SHORT_NAME\n" +
            "  ,separately_sale_indicator as SEPARATELY_SALEABLE_IND\n" +
            "  ,trial_allowed_indicator as TRIAL_ALLOWED_IND\n" +
            "  ,launch_date as FIRST_PUB_DATE\n" +
            "  ,f_type AS F_TYPE\n" +
            "  ,f_status AS F_STATUS\n" +
            "  ,f_revenue_model AS F_REVENUE_MODEL\n" +
            "  ,f_wwork AS F_PRODUCT_WORK\n" +
            "  ,f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "  FROM ephsit.semarchy_eph_mdm.sa_product sa where f_event =  (\n" +
            "select max (f_event) from \n" +
            "semarchy_eph_mdm.sa_product   \n" +
            "join \n" +
            "semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n" +
            "  and pmx_source_reference in ('%s') and pmx_source_reference not like '%%OOA'";


    public static String EPH_GD_PRODUCT_EXTRACT_JOURNALS_OR_PACKAGES = "SELECT \n" +
            "  F_EVENT AS F_EVENT\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,product_id AS PRODUCT_ID -- Title\n" +
            "  ,pmx_source_reference AS PMX_SOURCE_REFERENCE -- Subtitle\n" +
            "  ,name as PRODUCT_NAME\n" +
            "  ,short_name as PRODUCT_SHORT_NAME\n" +
            "  ,separately_sale_indicator as SEPARATELY_SALEABLE_IND\n" +
            "  ,trial_allowed_indicator as TRIAL_ALLOWED_IND\n" +
            "  ,launch_date as FIRST_PUB_DATE\n" +
            "  ,f_type AS F_TYPE\n" +
            "  ,f_status AS F_STATUS\n" +
            "  ,f_revenue_model AS F_REVENUE_MODEL\n" +
            "  ,f_wwork AS F_PRODUCT_WORK\n" +
            "  ,f_manifestation AS F_PRODUCT_MANIFESTATION_TYP\n" +
            "  FROM ephsit.semarchy_eph_mdm.gd_product\n" +
            "  where pmx_source_reference similar to '%s' and pmx_source_reference not like '%%OOA'";


    public static String SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_JOURNALS = "select \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID from ephsit_talend_owner.stg_10_pmx_product where \"SUBSCRIPTION\" = 'Y' order by random() limit '%s'";

    public static String SELECT_F_PRODUCT_WORK_IDS_FOR_GIVEN_MANIFESTATION_IDS = "select \"F_PRODUCT_WORK\" as F_PRODUCT_WORK from ephsit_talend_owner.stg_10_pmx_product where \"PRODUCT_MANIFESTATION_ID\" IN ('%s')";

    public static String SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_PRINT_JOURNALS= "select \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID  from ephsit_talend_owner.stg_10_pmx_product where \"SUBSCRIPTION\" = 'Y' and \"F_PRODUCT_MANIFESTATION_TYP\" = '1' and \"OPEN_ACCESS\" = '%s' and \"AUTHOR_CHARGES\" = '%s' order by random() limit '%s'";

    public static String SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_ELECTRONIC_JOURNALS = "select \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID from ephsit_talend_owner.stg_10_pmx_product where \"SUBSCRIPTION\" = 'Y' and \"F_PRODUCT_MANIFESTATION_TYP\" = '2' and \"OPEN_ACCESS\" = '%s' and \"AUTHOR_CHARGES\" = '%s' order by random() limit '%s'";

    public static String SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_NON_PRINT_OR_ELECTRONIC_JOURNALS = "select \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID from ephsit_talend_owner.stg_10_pmx_product where \"SUBSCRIPTION\" = 'Y' and \"F_PRODUCT_MANIFESTATION_TYP\" NOT IN ('1', '2') and \"AUTHOR_CHARGES\" = '%s' order by random() limit '%s'";

    public static String SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_BOOKS = "select \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID from ephsit_talend_owner.stg_10_pmx_product where \"ONE_OFF_ACCESS\" = 'Y' order by random() limit '%s'";

    public static String SELECT_RANDOM_PRODUCT_MANIFESTATION_IDS_FOR_PACKAGES = "select \"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID from ephsit_talend_owner.stg_10_pmx_product where \"PACKAGES\" = 'Y' order by random() limit '%s'";

    public static String SELECT_DUPLICATE_PRODUCT_IDS = "select distinct \"PRODUCT_ID\" as PRODUCT_ID from ephsit_talend_owner.stg_10_pmx_product  where \"PRODUCT_ID\" in (select b.\"PRODUCT_ID\" from ephsit_talend_owner.stg_10_pmx_product B ,ephsit_talend_owner.stg_10_pmx_product C where B.\"PRODUCT_ID\" = C.\"PRODUCT_ID\" \n" +
            "and b.\"F_PRODUCT_WORK\" != c.\"F_PRODUCT_WORK\" and B.\"SUBSCRIPTION\" = 'Y' and C.\"SUBSCRIPTION\" ='Y');\n";

}
