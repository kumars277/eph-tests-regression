package com.eph.automation.testing.services.db.sql;

public class WorkDataCheckSQL {

    public static String GET_PMX_WORKS_STG_DATA ="SELECT \n" +
            "   \"WORK_TITLE\" AS WORK_TITLE -- Title\n" +
            "  ,\"WORK_SUBTITLE\" AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,\"DAC_KEY\" as DAC_KEY-- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PRIMARY_ISBN\" AS PRIMARY_ISBN -- DAC ISBN (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"PROJECT_NUM\" AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"ISSN_L\" as ISSN_L-- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"JOURNAL_NUMBER\" AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  ,\"ELECTRONIC_RIGHTS_IND\" as ELECTRONIC_RIGHTS_IND -- Electronic Rights Indicator\n" +
            "  ,\"BOOK_EDITION_NAME\" as BOOK_EDITION_NAME-- Edition Number\n" +
            "  ,\"BOOK_VOLUME_NAME\" as BOOK_VOLUME_NAME-- Volume\n" +
            "  ,\"PMC\" AS PMC -- PMC to link to LOV table\n" +
            "  ,\"PMG\" AS PMG -- PMG to link to LOV table\n" +
            "  ,\"WORK_STATUS\" AS WORK_STATUS\t-- Work Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,\"WORK_SUBSTATUS\" AS WORK_SUBSTATUS -- Work Level substatus which is part of Status mapping\n" +
            "  ,\"WORK_TYPE\" AS WORK_TYPE -- Work Type to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  ,\"IMPRINT\" AS IMPRINT -- Imprint Code to link to LOV table\n" +
            "  ,\"OPEN_ACCESS_JNL_TYPE_CODE\" as OPEN_ACCESS_JNL_TYPE_CODE-- Open Access Journal Type to link to LOV table\n" +
            "  ,\"PRODUCT_WORK_ID\" as PRODUCT_WORK_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "  ,\"F_ACC_PROD_HIERARCHY\" as F_ACC_PROD_HIERARCHY-- Product Parent Code in Accountable Product Entity\n" +
            "  ,\"F_RESPONSIBILITY_CENTRE\" as F_RESPONSIBILITY_CENTRE-- Responsibility Centre in Accountable Product Entity to link to LOV table\n" +
            "  ,\"F_OPCO_R12\" as F_OPCO_R12-- Company in Accountable Product Entity to link to LOV table\n" +
            "  ,\"PRODUCT_WORK_PUB_DATE\" as PRODUCT_WORK_PUB_DATE-- Work Publication Date\n" +
            "  ,\"JOURNAL_ACRONYM\" AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "  FROM ephsit.ephsit_talend_owner.stg_pmx_wwork\n" +
            "  WHERE \"PARAM1\"='PARAM2'";

    public static String GET_EPH_WORKS_DATA ="SELECT \n" +
            "   WORK_ID AS WORK_ID\n" +
            "  ,PMX_SOURCE_REFERENCE AS PMX_SOURCE_REFERENCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,WORK_SUB_TITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,ELECTRO_RIGHTS_INDICATOR as ELECTRONIC_RIGHTS_IND\n" +
            "  ,VOLUME as BOOK_VOLUME_NAME\n" +
            "  ,COPYRIGHT_YEAR as PRODUCT_WORK_PUB_DATE\n" +
            " -- ,EDITION_NUMBER as EDITION_NUMBER\n" +
            "  ,F_PMC as PMC\n" +
            "  ,F_OA_TYPE AS OPEN_ACCESS_JNL_TYPE_CODE\n" +
            "  ,F_TYPE AS WORK_TYPE\n" +
            "  ,F_STATUS AS WORK_STATUS\n" +
            "  ,F_IMPRINT AS IMPRINT\n" +
            "  FROM ephsit.semarchy_eph_mdm.sa_wwork\n" +
            "  WHERE pmx_source_reference='PARAM1'";


    public static String GET_EPH_GD_WORKS_DATA ="SELECT \n" +
            "   WORK_ID AS WORK_ID\n" +
            "  ,PMX_SOURCE_REFERENCE AS PMX_SOURCE_REFERENCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,WORK_SUB_TITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,ELECTRO_RIGHTS_INDICATOR as ELECTRONIC_RIGHTS_IND\n" +
            "  ,VOLUME as BOOK_VOLUME_NAME\n" +
            "  ,COPYRIGHT_YEAR as PRODUCT_WORK_PUB_DATE\n" +
            " -- ,EDITION_NUMBER as EDITION_NUMBER\n" +
            "  ,F_PMC as PMC\n" +
            "  ,F_OA_TYPE AS OPEN_ACCESS_JNL_TYPE_CODE\n" +
            "  ,F_TYPE AS WORK_TYPE\n" +
            "  ,F_STATUS AS WORK_STATUS\n" +
            "  ,F_IMPRINT AS IMPRINT\n" +
            "  FROM ephsit.semarchy_eph_mdm.gd_wwork\n" +
            "  WHERE pmx_source_reference='PARAM1'";
}
