package com.eph.automation.testing.services.db.sql;

public class WorkDataCheckSQL {

    public static String GET_PMX_WORKS_STG_DATA_ISBN ="SELECT \n" +
            "   WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,PRODUCT_SUB_TITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,DAC_KEY as DAC_KEY-- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,PRIMARY_ISBN AS PRIMARY_ISBN -- DAC ISBN (may go in IDs table, depending on implementation of data model)\n" +
            "  ,PROJECT_NUM AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,ISSN_L as ISSN_L-- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,JOURNAL_NUMBER AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,ELECTRONIC_RIGHTS_IND as ELECTRONIC_RIGHTS_IND -- Electronic Rights Indicator\n" +
            "  ,BOOK_EDITION_NAME as BOOK_EDITION_NAME-- Edition Number\n" +
            "  ,BOOK_VOLUME_NAME as BOOK_VOLUME_NAME-- Volume\n" +
            "  ,PMC AS PMC -- PMC to link to LOV table\n" +
            "  ,PMG AS PMG -- PMG to link to LOV table\n" +
            "  ,WORK_STATUS AS WORK_STATUS\t-- Work Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,WORK_SUBSTATUS AS WORK_SUBSTATUS -- Work Level substatus which is part of Status mapping\n" +
            "  ,WORK_TYPE AS WORK_TYPE -- Work Type to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,IMPRINT AS IMPRINT -- Imprint Code to link to LOV table\n" +
            "  ,OPEN_ACCESS_JNL_TYPE_CODE as OPEN_ACCESS_JNL_TYPE_CODE-- Open Access Journal Type to link to LOV table\n" +
            "  ,PRODUCT_WORK_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "  ,F_ACC_PROD_HIERARCHY -- Product Parent Code in Accountable Product Entity\n" +
            "  ,F_RESPONSIBILITY_CENTRE -- Responsibility Centre in Accountable Product Entity to link to LOV table\n" +
            "  ,F_OPCO_R12 -- Company in Accountable Product Entity to link to LOV table\n" +
            "  ,PRODUCT_WORK_PUB_DATE -- Work Publication Date\n" +
            "  ,JOURNAL_ACRONYM AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM ephdev_talend_owner_ci.stg_pmx_wwork\n" +
            "  WHERE PRIMARY_ISBN='PARAM1'";

    public static String GET_EPH_WORKS_DATA ="SELECT \n" +
            "   WORK_ID AS WORK_ID\n" +
            "  -- ,PMX_SOURCE_REFERENCE AS PMX_SOURCE_REFERENCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,WORK_SUB_TITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,ELECTRO_RIGHTS_INDICATOR as ELECTRONIC_RIGHTS_IND\n" +
            "  ,VOLUME as VOLUME\n" +
            "  ,COPYRIGHT_YEAR as COPYRIGHT_YEAR\n" +
            " -- ,EDITION_NUMBER as EDITION_NUMBER\n" +
            "  ,F_PMC as F_PMC\n" +
            "  ,F_OA_TYPE AS OPEN_ACCESS_JNL_TYPE_CODE\n" +
            "  ,F_TYPE AS WORK_TYPE\n" +
            "  ,F_STATUS AS WORK_STATUS\n" +
            "  ,F_IMPRINT AS IMPRINT\n" +
            "  FROM ephsit.semarchy_eph_mdm.sa_wwork\n" +
            "  WHERE WORK_ID='PARAM1'";

    public static String PMX_WORK_EXTRACT_BY_ISSN = "  SELECT * FROM (\n" +
            "  SELECT DISTINCT 1 \n" +
//            "  , M.ELSEVIER_PRODUCT_ID AS PRODUCT_ID -- Product Manifestation Reference,  not needed in EPH but extracted for record linking purposes\n" +
            "  ,W.PRODUCT_WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,W.PRODUCT_SUBTITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,W.DAC_KEY -- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,W.MASTER_ISBN_STRIPPED AS PRIMARY_ISBN -- DAC ISBN (may go in IDs table, depending on implementation of data model)\n" +
            "  ,A.WORK_ALTERNATIVE_ID_REF AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,W.ISSN_L as ISSN_L-- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,W.ELSEVIER_JOURNAL_NUMBER AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,W.ELECTRONIC_RIGHTS_IND as ELECTRONIC_RIGHTS_IND-- Electronic Rights Indicator\n" +
            "  ,W.BOOK_EDITION_NAME as BOOK_EDITION_NAME-- Edition Number\n" +
            "  ,W.BOOK_VOLUME_NAME as BOOK_VOLUME_NAME-- Volume\n" +
            "  ,W.F_PMC AS PMC -- PMC to link to LOV table\n" +
            "  ,W.F_PMG AS PMG -- PMG to link to LOV table\n" +
            "  ,S.STATUS_CODE AS WORK_STATUS\t-- Work Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,SS.SUBSTATUS_NAME AS WORK_SUBSTATUS -- Work Level substatus which is part of Status mapping\n" +
            "  ,T.PRODUCT_TYPE_CODE AS WORK_TYPE -- Work Type to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,I.IMPRINT_CODE AS IMPRINT -- Imprint Code to link to LOV table\n" +
            "  ,O.OPEN_ACCESS_JNL_TYPE_CODE as OPEN_ACCESS_JNL_TYPE_CODE-- Open Access Journal Type to link to LOV table\n" +
            "  ,W.PRODUCT_WORK_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "  ,W.F_ACC_PROD_HIERARCHY -- Product Parent Code in Accountable Product Entity\n" +
            "  ,W.F_RESPONSIBILITY_CENTRE -- Responsibility Centre in Accountable Product Entity to link to LOV table\n" +
            "  ,W.F_OPCO_R12 -- Company in Accountable Product Entity to link to LOV table\n" +
            "  ,W.PRODUCT_WORK_PUB_DATE -- Work Publication Date\n" +
            "  ,W.JOURNAL_ACRONYM_PTS AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM GD_PRODUCT_WORK W\n" +
            "  JOIN GD_PRODUCT_MANIFESTATION M ON W.PRODUCT_WORK_ID = M.F_PRODUCT_WORK\n" +
            "  LEFT JOIN GD_WORK_ALT_IDENTIFIER A ON W.PRODUCT_WORK_ID = A.F_PRODUCT_WORK AND A.F_ALTERNATIVE_IDENTIFIER_TY = 24\n" +
            "  LEFT JOIN GD_OPEN_ACCESS_JOURNAL_TYPE O ON W.F_OPEN_ACCESS_JOURNAL_TYPE = O.OPEN_ACCESS_JOURNAL_TYPE_\n" +
            "  JOIN GD_PRODUCT_SUBSTATUS SS ON W.F_WORK_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
            "  JOIN GD_PRODUCT_STATUS S ON W.F_WORK_STATUS = S.PRODUCT_STATUS_ID\n" +
            "  JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "  LEFT JOIN GD_IMPRINT I ON W.F_IMPRINT = I.IMPRINT_ID)\n" +
            "   WHERE ISSN_L = 'PARAM1'";

    public static String GET_PMX_WORKS_STG_DATA_ISSN ="SELECT \n" +
            "   WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,PRODUCT_SUB_TITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,DAC_KEY as DAC_KEY-- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,PRIMARY_ISBN AS PRIMARY_ISBN -- DAC ISBN (may go in IDs table, depending on implementation of data model)\n" +
            "  ,PROJECT_NUM AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,ISSN_L as ISSN_L-- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,JOURNAL_NUMBER AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,ELECTRONIC_RIGHTS_IND as ELECTRONIC_RIGHTS_IND -- Electronic Rights Indicator\n" +
            "  ,BOOK_EDITION_NAME as BOOK_EDITION_NAME-- Edition Number\n" +
            "  ,BOOK_VOLUME_NAME as BOOK_VOLUME_NAME-- Volume\n" +
            "  ,PMC AS PMC -- PMC to link to LOV table\n" +
            "  ,PMG AS PMG -- PMG to link to LOV table\n" +
            "  ,WORK_STATUS AS WORK_STATUS\t-- Work Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,WORK_SUBSTATUS AS WORK_SUBSTATUS -- Work Level substatus which is part of Status mapping\n" +
            "  ,WORK_TYPE AS WORK_TYPE -- Work Type to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,IMPRINT AS IMPRINT -- Imprint Code to link to LOV table\n" +
            "  ,OPEN_ACCESS_JNL_TYPE_CODE as OPEN_ACCESS_JNL_TYPE_CODE-- Open Access Journal Type to link to LOV table\n" +
            "  ,PRODUCT_WORK_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "  ,F_ACC_PROD_HIERARCHY -- Product Parent Code in Accountable Product Entity\n" +
            "  ,F_RESPONSIBILITY_CENTRE -- Responsibility Centre in Accountable Product Entity to link to LOV table\n" +
            "  ,F_OPCO_R12 -- Company in Accountable Product Entity to link to LOV table\n" +
            "  ,PRODUCT_WORK_PUB_DATE -- Work Publication Date\n" +
            "  ,JOURNAL_ACRONYM AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM ephdev_talend_owner_ci.stg_pmx_wwork\n" +
            "  WHERE PRIMARY_ISSN='PARAM1'";
}
