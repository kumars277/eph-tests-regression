package com.eph.automation.testing.services.db.sql;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class ProductExtractSQL {

    public static String PMX_WORK_EXTRACT_BY_ISBN = "  SELECT * FROM (\n" +
            "  SELECT DISTINCT 1 \n" +
//            "  , M.ELSEVIER_PRODUCT_ID AS PRODUCT_ID -- Product Manifestation Reference,  not needed in EPH but extracted for record linking purposes\n" +
            "  ,W.PRODUCT_WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,W.PRODUCT_SUBTITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,W.DAC_KEY -- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,W.MASTER_ISBN_STRIPPED AS PRIMARY_ISBN -- DAC ISBN (may go in IDs table, depending on implementation of data model)\n" +
            "  ,A.WORK_ALTERNATIVE_ID_REF AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,W.ISSN_L -- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,W.ELSEVIER_JOURNAL_NUMBER AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,W.ELECTRONIC_RIGHTS_IND -- Electronic Rights Indicator\n" +
            "  ,W.BOOK_EDITION_NAME -- Edition Number\n" +
            "  ,W.BOOK_VOLUME_NAME -- Volume\n" +
            "  ,W.F_PMC AS PMC -- PMC to link to LOV table\n" +
            "  ,W.F_PMG AS PMG -- PMG to link to LOV table\n" +
            "  ,S.STATUS_CODE AS WORK_STATUS\t-- Work Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,SS.SUBSTATUS_NAME AS WORK_SUBSTATUS -- Work Level substatus which is part of Status mapping\n" +
            "  ,T.PRODUCT_TYPE_CODE AS WORK_TYPE -- Work Type to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,I.IMPRINT_CODE AS IMPRINT -- Imprint Code to link to LOV table\n" +
            "  ,O.OPEN_ACCESS_JNL_TYPE_CODE -- Open Access Journal Type to link to LOV table\n" +
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
            "   WHERE PRIMARY_ISBN = 'PARAM1'";

        public static String GET_PRODUCT_EXPORT_FROM_PMX_BY_PMC = "  select distinct * from\n" +
                "  (SELECT \n" +
                "     M.ELSEVIER_PRODUCT_ID AS PRODUCT_ID -- Product Manifestation Reference,  not needed in EPH but extracted for record linking purposes\n" +
                "    ,W.PRODUCT_WORK_TITLE AS WORK_TITLE -- Title\n" +
                "    ,W.PRODUCT_SUBTITLE AS WORK_SUBTITLE -- Subtitle\n" +
                "    ,W.DAC_KEY -- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
                "    ,W.MASTER_ISBN_STRIPPED AS PRIMARY_ISBN -- DAC ISBN (may go in IDs table, depending on implementation of data model)\n" +
                "    ,A.WORK_ALTERNATIVE_ID_REF AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
                "    ,W.ISSN_L -- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
                "    ,W.ELSEVIER_JOURNAL_NUMBER AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
                "    ,W.ELECTRONIC_RIGHTS_IND -- Electronic Rights Indicator\n" +
                "    ,W.BOOK_EDITION_NAME -- Edition Number\n" +
                "    ,W.BOOK_VOLUME_NAME -- Volume\n" +
                "    ,W.F_PMC AS PMC -- PMC to link to LOV table\n" +
                "    ,W.F_PMG AS PMG -- PMG to link to LOV table\n" +
                "    ,S.STATUS_CODE AS WORK_STATUS\t-- Work Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
                "    ,SS.SUBSTATUS_NAME AS WORK_SUBSTATUS -- Work Level substatus which is part of Status mapping\n" +
                "    ,T.PRODUCT_TYPE_CODE AS WORK_TYPE -- Work Type to link to LOV table (this will need mapping to new values, logic TBC)\n" +
                "    ,I.IMPRINT_CODE AS IMPRINT -- Imprint Code to link to LOV table\n" +
                "    ,O.OPEN_ACCESS_JNL_TYPE_CODE -- Open Access Journal Type to link to LOV table\n" +
                "    ,W.PRODUCT_WORK_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
                "    ,W.F_ACC_PROD_HIERARCHY -- Product Parent Code in Accountable Product Entity\n" +
                "    ,W.F_RESPONSIBILITY_CENTRE -- Responsibility Centre in Accountable Product Entity to link to LOV table\n" +
                "    ,W.F_OPCO_R12 -- Company in Accountable Product Entity to link to LOV table\n" +
                "    ,W.PRODUCT_WORK_PUB_DATE -- Work Publication Date\n" +
                "    ,W.JOURNAL_ACRONYM_PTS AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
                "  FROM GD_PRODUCT_WORK W\n" +
                "  JOIN GD_PRODUCT_MANIFESTATION M ON W.PRODUCT_WORK_ID = M.F_PRODUCT_WORK\n" +
                "  LEFT JOIN GD_WORK_ALT_IDENTIFIER A ON W.PRODUCT_WORK_ID = A.F_PRODUCT_WORK AND A.F_ALTERNATIVE_IDENTIFIER_TY = 24\n" +
                "  LEFT JOIN GD_OPEN_ACCESS_JOURNAL_TYPE O ON W.F_OPEN_ACCESS_JOURNAL_TYPE = O.OPEN_ACCESS_JOURNAL_TYPE_\n" +
                "  JOIN GD_PRODUCT_SUBSTATUS SS ON W.F_WORK_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
                "  JOIN GD_PRODUCT_STATUS S ON W.F_WORK_STATUS = S.PRODUCT_STATUS_ID\n" +
                "  JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
                "  LEFT JOIN GD_IMPRINT I ON W.F_IMPRINT = I.IMPRINT_ID)\n" +
                "  where pmc = 'PARAM1' ";

        public static String PRODUCT_WORK_FROM_EPH_SA = "\tselect work_id, work_title as WORK_TITLE\n" +
                "\t\t,work_sub_title as WORK_SUBTITLE\n" +
                "\t\t,electro_rights_indicator as ELECTRONIC_RIGHTS_IND\n" +
                "\t\t,volume as BOOK_EDITION_VOLUME\n" +
                "\t\t,copyright_year as PRODUCT_WORK_PUB_DATE\n" +
                "\t\t,f_pmc as PMC\n" +
                "\t\t--,edition_number as BOOK_EDITION_NAME\n" +
                "\t\t--,f_pmg as PMG\n" +
                "\t\t,f_oa_type as OPEN_ACCESS_JNL_TYPE_CODE\n" +
                "\t\t,f_type as WORK_TYPE\n" +
                "\t\t,f_status as WORK_STATUS\n" +
                "\t\t,f_imprint as IMPRINT \n" +
                "\tfrom SEMARCHY_EPH_MDM.SA_WWORK"
                ;
        public static String PRODUCT_MANIFESTATION_FROM_EPH_SA = "select SAMI.s_identifier as PRIMARY_ISBN,SAM.*\n" +
                "from semarchy_eph_mdm.sa_manifestation_identifier SAMI,\n" +
                "\t semarchy_eph_mdm.sa_manifestation SAM\n" +
                "where SAMI.s_identifier = 'PARAM1'\n" +
                "and SAM.manifestation_id = SAMI.f_manifestation"
                ;
}
