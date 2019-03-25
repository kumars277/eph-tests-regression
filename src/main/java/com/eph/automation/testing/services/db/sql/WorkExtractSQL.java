package com.eph.automation.testing.services.db.sql;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class WorkExtractSQL {

    public static String PMX_WORK_EXTRACT= "  SELECT * FROM (\n" +
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
            "  ,O.OWNERSHIP_SUB_TYPE_ID as OWNERSHIP\n"+
            "  FROM GD_PRODUCT_WORK W\n" +
            "  -- JOIN GD_PRODUCT_MANIFESTATION M ON W.PRODUCT_WORK_ID = M.F_PRODUCT_WORK\n" +
            "  LEFT JOIN GD_WORK_ALT_IDENTIFIER A ON W.PRODUCT_WORK_ID = A.F_PRODUCT_WORK AND A.F_ALTERNATIVE_IDENTIFIER_TY = 24\n" +
            "  LEFT JOIN GD_OPEN_ACCESS_JOURNAL_TYPE O ON W.F_OPEN_ACCESS_JOURNAL_TYPE = O.OPEN_ACCESS_JOURNAL_TYPE_\n" +
            "  LEFT JOIN GD_PRODUCT_SUBSTATUS SS ON W.F_WORK_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
            "  LEFT JOIN GD_PRODUCT_STATUS S ON W.F_WORK_STATUS = S.PRODUCT_STATUS_ID\n" +
            "  LEFT JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "  LEFT JOIN GD_IMPRINT I ON W.F_IMPRINT = I.IMPRINT_ID" +
            "  LEFT JOIN GD_OWNERSHIP_SUB_TYPE O ON PO.F_OWNERSHIP_SUB_TYPE = O.OWNERSHIP_SUB_TYPE_ID)\n" +
            "  WHERE PARAM1 = 'PARAM2'";

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

    public static String COUNT_MANIFESTATIONS_IN_PMX_GD_PRODUCT_MANIFESTATION_TABLE = "SELECT DISTINCT count(*) AS count FROM GD_PRODUCT_MANIFESTATION M\n" +
            "JOIN GD_PRODUCT_WORK W ON M.F_PRODUCT_WORK = W.PRODUCT_WORK_ID\n" +
            "LEFT JOIN GD_PRODUCT_SUBSTATUS SS ON M.F_MANIFESTATION_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
            "JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "WHERE T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES') ";

    public static final String COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM ephsit_talend_owner.stg_10_pmx_manifestation";

    public static final String COUNT_MANIFESTATIONS_IN_SA_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM semarchy_eph_mdm.sa_manifestation join semarchy_eph_mdm.sa_event on f_event = event_id and semarchy_eph_mdm.sa_event.workflow_id = 'talend' and semarchy_eph_mdm.sa_event.f_event_type = 'PMX' and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' and f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation)";

    public static final String COUNT_MANIFESTATIONS_IN_GD_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM semarchy_eph_mdm.gd_manifestation";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_PMX = "SELECT\n" +
            "\t M.PRODUCT_MANIFESTATION_ID AS MANIFESTATION_ID -- PMX Primary Key for Manifestation \n" +
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
            "\t,M.F_PRODUCT_WORK AS F_PRODUCT_WORK -- Internal PMX Work ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,W.F_PRODUCT_TYPE AS WORK_TYPE_ID -- Work Type so for mapping Key title and type fields\n" +
            "\t,M.F_PRODUCT_DISTRIBUTION_TYPE AS MANIFESTATION_SUBTYPE -- Manifestation Distribution Type for mapping manifestation type\n" +
            "\t,M.F_COMMODITY_CODE AS COMMODITY --  Commodity Code for mapping manifestation type\n" +
            "\t,SS.SUBSTATUS_NAME AS MANIFESTATION_SUBSTATUS -- Manifestation Sub status for mapping Status\n" +
            "FROM GD_PRODUCT_MANIFESTATION M\n" +
            "JOIN GD_PRODUCT_WORK W ON M.F_PRODUCT_WORK = W.PRODUCT_WORK_ID\n" +
            "LEFT JOIN GD_PRODUCT_SUBSTATUS SS ON M.F_MANIFESTATION_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
            "JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "WHERE T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES') \n" +
            "AND M.PRODUCT_MANIFESTATION_ID IN ('%s')\n" +
            "ORDER BY M.PRODUCT_MANIFESTATION_ID";


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
            "from ephsit_talend_owner.stg_10_pmx_manifestation\n" +
//            "WHERE \"ISBN\" IN ('%s')";
            "WHERE \"MANIFESTATION_ID\" IN ('%s') order by \"MANIFESTATION_ID\"";

    public static final String SELECT_RANDOM_ISBN_IDS_PHB = "select \"ISBN\" AS ISBN from ephsit_talend_owner.stg_10_pmx_manifestation where \"MANIFESTATION_SUBTYPE\" = 424 and \"ISBN\" is not null order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISBN_IDS_PSB = "select \"ISBN\" AS ISBN from ephsit_talend_owner.stg_10_pmx_manifestation where \"MANIFESTATION_SUBTYPE\" = 425  and \"ISBN\" is not null order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISBN_IDS_EBK = "select \"ISBN\" AS ISBN from ephsit_talend_owner.stg_10_pmx_manifestation where \"COMMODITY\" = 'EB'  and \"ISBN\" is not null order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_JPR = "select \"MANIFESTATION_ID\" from ephsit_talend_owner.stg_10_pmx_manifestation where \"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" = 1 order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_JEL = "select \"MANIFESTATION_ID\" from ephsit_talend_owner.stg_10_pmx_manifestation where \"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" != 1 order by random() limit '%s'";

    public static final String SELECT_MANIFESTATIONS_IDS_FOR_SPECIFIC_ISBN = "select \"MANIFESTATION_ID\" AS manifestation_id from ephsit_talend_owner.stg_10_pmx_manifestation where \"ISBN\" in ('%s')";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_EPH_SA = "select distinct sa.b_loadid as B_LOADID,\n" +
            "sa.F_EVENT  as F_EVENT,\n" +
            "sa.B_CLASSNAME as B_CLASSNAME,\n" +
            "sa.MANIFESTATION_ID as MANIFESTATION_ID,\n" +
            "sa.PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE,\n" +
            "sa.MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE,\n" +
            "sa.INTER_EDITION_FLAG as INTER_EDITION_FLAG,\n" +
            "sa.FIRST_PUB_DATE as FIRST_PUB_DATE,\n" +
            "sa.LAST_PUB_DATE as LAST_PUB_DATE, \n" +
            "sa.F_TYPE as F_TYPE,\n" +
            "sa.F_STATUS as F_STATUS, \n" +
            "sa.F_FORMAT_TYPE as F_FORMAT_TYPE, \n" +
            "sa.F_WWORK as F_WWORK\n" +
            "FROM semarchy_eph_mdm.sa_manifestation sa\n" +
            "where f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation\n" +
            "join semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n" +
            "and pmx_source_reference IN ('%s')";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_EPH_GD = "select F_EVENT  as F_EVENT,\n" +
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
            "FROM semarchy_eph_mdm.gd_manifestation WHERE pmx_source_reference IN ('%s')";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_PMX_MANIFESTATION_TABLE = "select count(*) AS count from ephsit_talend_owner.stg_10_pmx_manifestation where \"%s\" is not null";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_SA_MANIFESTATION_TABLE ="SELECT count(*) AS count FROM semarchy_eph_mdm.sa_manifestation_identifier\n" +
            "where f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation_identifier\n" +
            "join semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_GD_MANIFESTATION_TABLE = "select count(*) as count from semarchy_eph_mdm.gd_manifestation_identifier where f_type = '%s'";

    public static final String SELECT_RECORDS_SA_MANIFESTATION_IDENTIFIER = "SELECT sa.b_loadid as b_loadid,\n" +
            "f_event as f_event,\n" +
            "sa.b_classname as b_classname,\n" +
            "manif_identifier_id as manif_identifier_id,\n" +
            "f_type as f_type,\n" +
            "f_manifestation as f_manifestation\n" +
            "FROM semarchy_eph_mdm.sa_manifestation_identifier sa\n" +
            "where f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation_identifier \n" +
            "join semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n" +
            "and identifier IN ('%s')";

    public static final String SELECT_RECORDS_GD_MANIFESTATION_IDENTIFIER = "select f_event as f_event,\n" +
            "b_classname as b_classname, \n" +
            "manif_identifier_id as manif_identifier_id, \n" +
            "f_type as f_type, \n" +
            "f_manifestation as f_manifestation \n" +
            "from semarchy_eph_mdm.gd_manifestation_identifier \n" +
            "where identifier in ('%s')";

    public static final String SELECT_RANDOM_ISBNS_PHB = "select \"ISBN\" as ISBN from ephsit_talend_owner.stg_10_pmx_manifestation where \"%s\" is not null and \"MANIFESTATION_SUBTYPE\" = 424 order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISBNS_PSB = "select \"ISBN\" as ISBN from ephsit_talend_owner.stg_10_pmx_manifestation where \"%s\" is not null and \"MANIFESTATION_SUBTYPE\" = 425 order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISBNS_EBK = "select \"ISBN\" as ISBN from ephsit_talend_owner.stg_10_pmx_manifestation where \"%s\" is not null and \"COMMODITY\" = 'EB' order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISSNS_JPR_IDS = "select \"ISSN\" as ISSN from ephsit_talend_owner.stg_10_pmx_manifestation where \"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" = 1 order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISSNS_JEL_IDS = "select \"ISSN\" as ISSN from ephsit_talend_owner.stg_10_pmx_manifestation where\"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" != 1 order by random() limit '%s'";

}
