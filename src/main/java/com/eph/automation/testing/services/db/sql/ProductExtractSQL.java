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
            "\tfrom SEMARCHY_EPH_MDM.SA_WWORK";

    public static String PRODUCT_MANIFESTATION_FROM_EPH_SA = "select SAMI.s_identifier as PRIMARY_ISBN,SAM.*\n" +
            "from semarchy_eph_mdm.sa_manifestation_identifier SAMI,\n" +
            "\t semarchy_eph_mdm.sa_manifestation SAM\n" +
            "where SAMI.s_identifier = 'PARAM1'\n" +
            "and SAM.manifestation_id = SAMI.f_manifestation";

    public static String COUNT_MANIFESTATIONS_IN_PMX_GD_PRODUCT_MANIFESTATION_TABLE = "SELECT count(*) \n" +
            "FROM PMX.GD_PRODUCT_MANIFESTATION M\n" +
            "JOIN PMX.GD_PRODUCT_WORK W ON M.F_PRODUCT_WORK = W.PRODUCT_WORK_ID\n" +
            "JOIN PMX.GD_PRODUCT_SUBSTATUS SS ON M.F_MANIFESTATION_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID";

    public static final String COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM ephsit_talend_owner.stg_pmx_manifestation";

    public static final String COUNT_MANIFESTATIONS_IN_SA_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM semarchy_eph_mdm.sa_manifestation";

    public static final String COUNT_MANIFESTATIONS_IN_GD_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM semarchy_eph_mdm.gd_manifestation";


    public static final String SELECT_MANIFESTATIONS_DATA_IN_PMX = "SELECT\n" +
            "\t M.ELSEVIER_PRODUCT_ID AS MANIFESTATION_ELS_PROD_ID -- Product Manifestation Reference,  not needed in EPH but extracted for record linking purposes\n" +
            "\t,M.PRODUCT_MANIFESTATION_ID AS MANIFESTATION_ID -- PMX Primary Key for Manifestation \n" +
            "\t,M.PRODUCT_MANIFESTATION_TIT AS MANIFESTATION_KEY_TITLE -- Manifestation Title\n" +
            "\t,M.ISBN_STRIPPED AS ISBN -- ISBN (may go in IDs table, depending on implementation of data model)\n" +
            "\t,M.ISSN AS ISSN -- ISSN (may go in IDs table, depending on implementation of data model)\n" +
            "\t,M.COVER_HEIGHT_AMOUNT AS COVER_HEIGHT -- Cover Height for Format sub entity\n" +
            "\t,M.COVER_WIDTH_AMOUNT AS COVER_WIDTH -- Cover Width for Format sub entity\n" +
            "\t,M.PAGE_HEIGHT_AMOUNT AS PAGE_HEIGHT -- Page Height for Format sub entity\n" +
            "\t,M.PAGE_WIDTH_AMOUNT AS PAGE_WIDTH -- Page Width for Format sub entity\n" +
            "\t,M.WEIGHT_AMOUNT AS WEIGHT -- Weight for Format sub entity\n" +
            "\t,M.CARTON_QTY AS CARTON_QTY -- Carton Quantity for Format sub entity\n" +
            "\t,M.INTERNATIONAL_EDITION_IND AS INTERNATIONAL_EDITION_IND -- International Edition Indicator\n" +
            "\t,M.COPYRIGHT_DATE AS COPYRIGHT_DATE  -- Manifestation First Publication Date\n" +
            "\t,M.F_PRODUCT_MANIFESTATION_TYP AS F_PRODUCT_MANIFESTATION_TYP -- 1 = Print, 2 = Electronic. Will have to map to Manifestation Type (logic TBC)\n" +
            "\t,M.FORMAT_TXT AS FORMAT_TXT-- Additional Format info (may feed Format Entity, may feed logic for Manifestation Type\n" +
            "\t,M.F_MANIFESTATION_STATUS AS MANIFESTATION_STATUS -- Manifestation Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
//            "--\t,M.PRODUCT_MANIFESTATION_ID AS PRODUCT_MANIFESTATION_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,M.F_PRODUCT_WORK AS F_PRODUCT_WORK -- Internal PMX Work ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,W.F_PRODUCT_TYPE AS WORK_TYPE_ID -- Work Type so for mapping Key title and type fields\n" +
            "\t,M.F_PRODUCT_DISTRIBUTION_TYPE AS MANIFESTATION_SUBTYPE -- Manifestation Distribution Type for mapping manifestation type\n" +
            "\t,M.F_COMMODITY_CODE AS COMMODITY --  Commodity Code for mapping manifestation type\n" +
            "\t,SS.SUBSTATUS_NAME AS MANIFESTATION_SUBSTATUS -- Manifestation Sub status for mapping Status\n" +
            "FROM GD_PRODUCT_MANIFESTATION M\n" +
            "JOIN GD_PRODUCT_WORK W ON M.F_PRODUCT_WORK = W.PRODUCT_WORK_ID\n" +
            "JOIN GD_PRODUCT_SUBSTATUS SS ON M.F_MANIFESTATION_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
//            " WHERE M.ISBN_STRIPPED IN ('%s')";
            "WHERE M.ELSEVIER_PRODUCT_ID IN ('%s')";


    public static final String SELECT_MANIFESTATIONS_DATA_IN_PMX_STG = "SELECT " +
//             "\"MANIFESTATION_ELS_PROD_ID\" as MANIFESTATION_ELS_PROD_ID,\n" +
            "\"MANIFESTATION_ID\" as MANIFESTATION_ID,\n" +
            "\"MANIFESTATION_KEY_TITLE\",\n" +
            "\"ISBN\" as ISBN,\n" +
            "\"ISSN\" as ISSN,\n" +
            "\"COVER_HEIGHT\" as COVER_HEIGHT,\n" +
            "\"COVER_WIDTH\" as COVER_WIDTH, \n" +
            "\"PAGE_HEIGHT\" as PAGE_HEIGHT, \n" +
            "\"PAGE_WIDTH\" as PAGE_WIDTH, \n" +
            "\"WEIGHT\" as WEIGHT, \n" +
            "\"CARTON_QTY\" as CARTON_QTY, \n" +
            "\"INTERNATIONAL_EDITION_IND\" as INTERNATIONAL_EDITION_IND,\n" +
            "\"COPYRIGHT_DATE\" as COPYRIGHT_DATE,\n" +
            "\"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "\"FORMAT_TXT\" as FORMAT_TXT,\n" +
            "\"MANIFESTATION_STATUS\" as MANIFESTATION_STATUS,\n" +
            "\"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID, \n" +
            "\"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "\"WORK_TYPE_ID\" as WORK_TYPE_ID,\n" +
            "\"MANIFESTATION_SUBTYPE\" as MANIFESTATION_SUBTYPE,\n" +
            "\"COMMODITY\" as COMMODITY,\n" +
            "\"MANIFESTATION_SUBSTATUS\" as MANIFESTATION_SUBSTATUS\n" +
            "from ephsit_talend_owner.stg_pmx_manifestation\n" +
//            "WHERE \"ISBN\" IN ('%s')";
            "WHERE \"MANIFESTATION_ID\" IN ('%s')";
    public static final String SELECT_RANDOM_ISBN_IDS_PHB = "select \"ISBN\" AS ISBN from ephsit_talend_owner.stg_pmx_manifestation where \"MANIFESTATION_SUBTYPE\" = 424 order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISBN_IDS_PSB = "select \"ISBN\" AS ISBN from ephsit_talend_owner.stg_pmx_manifestation where \"MANIFESTATION_SUBTYPE\" = 425 order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISBN_IDS_EBK = "select \"ISBN\" AS ISBN from ephsit_talend_owner.stg_pmx_manifestation where \"COMMODITY\" = 'EB' order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_JPR = "select \"MANIFESTATION_ID\" from ephsit_talend_owner.stg_pmx_manifestation where \"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" = 1 order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_JEL = "select \"MANIFESTATION_ID\" from ephsit_talend_owner.stg_pmx_manifestation where \"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" != 1 order by random() limit '%s'";

    public static final String SELECT_MANIFESTATIONS_IDS_FOR_SPECIFIC_ISBN = "select \"MANIFESTATION_ID\" AS manifestation_id from ephsit_talend_owner.stg_pmx_manifestation where \"ISBN\" in ('%s')";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_PMX_SA = "select b_loadid as B_LOADID,\n" +
            "F_EVENT  as F_EVENT,\n" +
            "B_CLASSNAME as B_CLASSNAME,\n" +
            "MANIFESTATION_ID as MANIFESTATION_ID,\n" +
            "PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE,\n" +
            "MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE,\n" +
            "INTER_EDITION_FLAG as INTER_EDITION_FLAG,\n" +
            "FIRST_PUB_DATE as FIRST_PUB_DATE,\n" +
            "LAST_PUB_DATE as LAST_PUB_DATE, \n" +
            "F_TYPE as F_TYPE,\n" +
            "F_STATUS as F_STATUS, \n" +
            "F_FORMAT_TYPE as F_FORMAT_TYPE, \n" +
            "F_WWORK as F_WWORK\n" +
            "FROM semarchy_eph_mdm.sa_manifestation WHERE MANIFESTATION_ID IN ('%s')";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_PMX_GD = "select b_loadid as B_LOADID,\n" +
            "F_EVENT  as F_EVENT,\n" +
            "B_CLASSNAME as B_CLASSNAME,\n" +
            "MANIFESTATION_ID as MANIFESTATION_ID,\n" +
            "PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE,\n" +
            "MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE,\n" +
            "INTER_EDITION_FLAG as INTER_EDITION_FLAG,\n" +
            "FIRST_PUB_DATE as FIRST_PUB_DATE,\n" +
            "LAST_PUB_DATE as LAST_PUB_DATE, \n" +
            "F_TYPE as F_TYPE,\n" +
            "F_STATUS as F_STATUS, \n" +
            "F_FORMAT_TYPE as F_FORMAT_TYPE, \n" +
            "F_WWORK as F_WWORK\n" +
            "FROM semarchy_eph_mdm.sa_manifestation WHERE MANIFESTATION_ID IN ('%s')";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_PMX_MANIFESTATION_TABLE = "select count(*) AS count from ephsit_talend_owner.stg_pmx_manifestation where \"%s\" is not null";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_SA_MANIFESTATION_TABLE = "select count(*) AS count from semarchy_eph_mdm.sa_manifestation_identifier where f_type = '%s'";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_GD_MANIFESTATION_TABLE = "select count(*) as count from semarchy_eph_mdm.gd_manifestation_identifier where f_type = '%s'";

    public static final String SELECT_RECORDS_SA = "select\n" +
            "b_loadid as b_loadid,\n" +
            "f_event as f_event,\n" +
            "b_classname as b_classname,\n" +
            "manif_identifier_id as manif_identifier_id,\n" +
            "identifier as identifier,\n" +
            "f_type as f_type,\n" +
            "f_manifestation as f_manifestation\n" +
            "from semarchy_eph_mdm.sa_manifestation_identifier\n" +
            "where f_manifestation = '%s'";

    public static final String SELECT_RECORDS_GD = "select\n" +
            "b_loadid as b_loadid,\n" +
            "f_event as f_event,\n" +
            "b_classname as b_classname,\n" +
            "manif_identifier_id as manif_identifier_id,\n" +
            "identifier as identifier,\n" +
            "f_type as f_type,\n" +
            "f_manifestation as f_manifestation\n" +
            "from semarchy_eph_mdm.gd_manifestation_identifier\n" +
            "where f_manifestation = '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_PHB = "select \"MANIFESTATION_ID\" as manifestation_id from ephsit_talend_owner.stg_pmx_manifestation where \"%s\" is not null and \"MANIFESTATION_SUBTYPE\" = 424 order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_PSB = "select \"MANIFESTATION_ID\" as manifestation_id from ephsit_talend_owner.stg_pmx_manifestation where \"%s\" is not null and \"MANIFESTATION_SUBTYPE\" = 425 order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_EBK = "select \"MANIFESTATION_ID\" as manifestation_id from ephsit_talend_owner.stg_pmx_manifestation where \"%s\" is not null and \"COMMODITY\" = 'EB' order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_JPR_IDS = "select \"MANIFESTATION_ID\" as manifestation_id from ephsit_talend_owner.stg_pmx_manifestation\"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" = 1 order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_JEL_IDS = "select \"MANIFESTATION_ID\" as manifestation_id from ephsit_talend_owner.stg_pmx_manifestation where\"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" != 1 order by random() limit '%s'";

}
