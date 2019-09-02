package com.eph.automation.testing.services.db.sql;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class WorkExtractSQL {

    public static String PMX_WORK_EXTRACT= "  \n" +
            " SELECT\n" +
            "--\t M.ELSEVIER_PRODUCT_ID AS PRODUCT_ID -- Product Manifestation Reference,  not needed in EPH but extracted for record linking purposes\n" +
            "  \t W.PRODUCT_WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  \t,W.PRODUCT_SUBTITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  \t,W.DAC_KEY -- DAC Key (may go in IDs table, depending on implementation of data model)\n" +
            "  \t,W.MASTER_ISBN_STRIPPED AS PRIMARY_ISBN -- DAC ISBN (may go in IDs table, depending on implementation of data model)\n" +
            "  \t,A.WORK_ALTERNATIVE_ID_REF AS PROJECT_NUM -- Project Number (may go in IDs table, depending on implementation of data model)\n" +
            "  \t,W.ISSN_L -- ISSN-L (may go in IDs table, depending on implementation of data model)\n" +
            "  \t,W.ELSEVIER_JOURNAL_NUMBER AS JOURNAL_NUMBER -- Journal Number (may go in IDs table, depending on implementation of data model)\n" +
            "  \t,W.ELECTRONIC_RIGHTS_IND -- Electronic Rights Indicator\n" +
            "  \t,TO_NUMBER(W.BOOK_EDITION_NAME) BOOK_EDITION_NAME -- Edition Number\n" +
            "  \t,W.BOOK_VOLUME_NAME -- Volume\n" +
            "  \t,W.F_PMC AS PMC -- PMC to link to LOV table\n" +
            "  \t,W.F_PMG AS PMG -- PMG to link to LOV table\n" +
            "  \t,S.STATUS_CODE AS WORK_STATUS\t-- Work Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  \t,SS.SUBSTATUS_NAME AS WORK_SUBSTATUS -- Work Level substatus which is part of Status mapping\n" +
            "  \t,T.PRODUCT_TYPE_CODE AS WORK_TYPE -- Work Type to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "  \t,I.IMPRINT_CODE AS IMPRINT -- Imprint Code to link to LOV table\n" +
            "  \t,O.OPEN_ACCESS_JNL_TYPE_CODE -- Open Access Journal Type to link to LOV table\n" +
            "  \t,W.PRODUCT_WORK_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "  \t,W.F_ACC_PROD_HIERARCHY -- Product Parent Code in Accountable Product Entity\n" +
            "  \t,W.F_RESPONSIBILITY_CENTRE -- Responsibility Centre in Accountable Product Entity to link to LOV table\n" +
            "  \t,W.F_OPCO_R12 -- Company in Accountable Product Entity to link to LOV table\n" +
            "  \t,W.PRODUCT_WORK_PUB_DATE -- Work Publication Date\n" +
            "  \t,W.JOURNAL_ACRONYM_PTS AS JOURNAL_ACRONYM -- PTS Journal Acronym (may go in IDs table, depending on implementation of data model)\n" +
            "    ,SO.OWNERSHIP_SUB_TYPE_CODE AS SOC_OWNERSHIP -- This is the Ownership Sub Type Code in PMX for mapping to EPH Ownership Type Code\n" +
            "    ,TO_CHAR(GREATEST(NVL(W.B_UPDDATE,W.B_CREDATE),CASE WHEN PO.B_UPDDATE IS NULL AND PO.B_CREDATE IS NULL THEN TO_DATE('01-01-1900','DD-MM-YYYY') ELSE NVL(PO.B_UPDDATE,PO.B_CREDATE) END),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "    ,WL.LANGUAGE_CODE -- Includes dummy code for multi-langauge titles\n" +
            "    ,W.EFFECTIVE_TO_DATE AS RECORD_END_DATE\n" +
            "    ,MU.MAN_UPDATED AS MANIFESTATION_UPDATE\n" +
            "  FROM GD_PRODUCT_WORK W\n" +
            "--  JOIN GD_PRODUCT_MANIFESTATION M ON W.PRODUCT_WORK_ID = M.F_PRODUCT_WORK\n" +
            "  LEFT JOIN GD_WORK_ALT_IDENTIFIER A ON W.PRODUCT_WORK_ID = A.F_PRODUCT_WORK AND A.F_ALTERNATIVE_IDENTIFIER_TY = 24\n" +
            "  LEFT JOIN GD_OPEN_ACCESS_JOURNAL_TYPE O ON W.F_OPEN_ACCESS_JOURNAL_TYPE = O.OPEN_ACCESS_JOURNAL_TYPE_\n" +
            "  LEFT JOIN GD_PRODUCT_SUBSTATUS SS ON W.F_WORK_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
            "  LEFT JOIN GD_PRODUCT_STATUS S ON W.F_WORK_STATUS = S.PRODUCT_STATUS_ID\n" +
            "  LEFT JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "  LEFT JOIN GD_IMPRINT I ON W.F_IMPRINT = I.IMPRINT_ID\n" +
            "  LEFT JOIN GD_PRODUCT_OWNER PO ON W.PRODUCT_WORK_ID = PO.F_PRODUCT_WORK\n" +
            "  LEFT JOIN GD_OWNERSHIP_SUB_TYPE SO ON PO.F_OWNERSHIP_SUB_TYPE = SO.OWNERSHIP_SUB_TYPE_ID\n" +
            "  LEFT JOIN (SELECT P.F_PRODUCT_WORK, CASE WHEN C.LANGUAGE_COUNT > 1 THEN 'ZZ' ELSE UPPER(L.LANGUAGE_ISO_CODE) END AS LANGUAGE_CODE\n" +
            "             FROM\n" +
            "\t            (SELECT F_PRODUCT_WORK, COUNT(PRODUCT_LANGUAGE_ID) AS LANGUAGE_COUNT FROM GD_PRODUCT_LANGUAGE GROUP BY F_PRODUCT_WORK) C\n" +
            "             JOIN\n" +
            "\t            GD_PRODUCT_LANGUAGE P  ON C.F_PRODUCT_WORK = P.F_PRODUCT_WORK\n" +
            "             JOIN\n" +
            "\t            GD_LANGUAGES L ON P.F_LANGUAGES = L.LANGUAGES_ID) WL ON W.PRODUCT_WORK_ID = WL.F_PRODUCT_WORK\n" +
            "  LEFT JOIN (SELECT M.F_PRODUCT_WORK\n" +
            "  \t\t\t,MAX(TO_CHAR(NVL(NVL(M.B_UPDDATE,M.B_CREDATE),TO_DATE('01-01-1900','DD-MM-YYYY')),'YYYYMMDDHH24MI')) AS MAN_UPDATED \n" +
            "  \t\t\tFROM GD_PRODUCT_MANIFESTATION M GROUP BY M.F_PRODUCT_WORK) MU ON W.PRODUCT_WORK_ID = MU.F_PRODUCT_WORK\n" +
            "  WHERE T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES')\n" +
            "  and    PRODUCT_WORK_ID IN ('%s') ORDER BY PRODUCT_WORK_ID\n" +
            "  \n";

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

    public static String GET_COUNT_MANIFESTATIONS_EPHAE = "select count(*) as count from semarchy_eph_mdm.ae_manifestation where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";

    public static String COUNT_MANIFESTATIONS_IN_PMX_GD_PRODUCT_MANIFESTATION_TABLE = "SELECT DISTINCT count(*) AS count \n" +
            "FROM GD_PRODUCT_MANIFESTATION M\n" +
            "JOIN GD_PRODUCT_WORK W ON M.F_PRODUCT_WORK = W.PRODUCT_WORK_ID\n" +
            "LEFT JOIN GD_PRODUCT_SUBSTATUS SS ON M.F_MANIFESTATION_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
            "JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "LEFT JOIN\n" +
            "    (SELECT\n" +
            "         F_PRODUCT_MANIFESTATION\n" +
            "        ,MIN(AVAILABILITY_START_DATE) FIRST_PUB_DATE\n" +
            "     FROM\n" +
            "        GD_PRODUCT_AVAILABILITY\n" +
            "     GROUP BY F_PRODUCT_MANIFESTATION) PD ON M.PRODUCT_MANIFESTATION_ID = PD.F_PRODUCT_MANIFESTATION\n" +
            "WHERE T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES')";

    public static final String COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation";

    public static final String COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_GOING_TO_DQ = "SELECT count(distinct \"MANIFESTATION_ID\") AS count FROM  "+
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation where \"MANIFESTATION_ID\" is not null\n";

    public static String COUNT_MANIFESTATIONS_IN_EPH_STG_PMX_MANIFESTATION_TABLE_DELTA = "select count(distinct \"MANIFESTATION_ID\") as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation\n" +
//            "where TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')";
            "where TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')";


    public static final String COUNT_MANIFESTATIONS_IN_EPH_STG_DQ_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq";

    public static final String COUNT_MANIFESTATIONS_IN_EPH_DQ_TO_SA =
    "select count(*)  \n" +
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq s\n" +
            "join (select pmx_source_reference as external_reference, concat(pmx_source_reference\n" +
            "||coalesce(manifestation_key_title,'')\n" +
            "||coalesce(inter_edition_flag,false)\n" +
            "||coalesce(first_pub_date,current_date)\n" +
            "||coalesce(f_type,'')\n" +
            "||coalesce(f_status,'')\n" +
            "||coalesce(f_format_type,'')  \n" +
            "--)as string\n" +
            "||coalesce(map_sourceref_2_ephid('WORK'::varchar,f_wwork::varchar),'')) as string\n" +
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq) h \n" +
            "on s.pmx_source_reference::varchar = h.external_reference::varchar \n" +
            "left join (select external_reference, concat(external_reference||coalesce(manifestation_key_title,'')||coalesce(inter_edition_flag,false)||coalesce(first_pub_date,current_date)\n" +
            "||coalesce(f_type,'')||coalesce(f_status,'')||coalesce(f_format_type,'')||coalesce(f_wwork,'')) as string\n" +
            "from semarchy_eph_mdm.gd_manifestation) e on h.external_reference::varchar = e.external_reference::varchar\n" +
            "where md5(e.string) != md5(h.string)\n" +
            "and dq_err != 'Y'  ";

    public static final String COUNT_MANIFESTATIONS_IN_SA_MANIFESTATION_TABLE =
           "SELECT count(*) AS count FROM semarchy_eph_mdm.sa_manifestation sa \n" +
                   "where sa.f_event = (select  max(event_id) from semarchy_eph_mdm.sa_event sa2  \n" +
                   "where sa2.f_event_type = 'PMX'\n" +
                   "and sa2.workflow_id = 'talend'\n" +
                   "and sa2.f_workflow_source = 'PMX')";

    public static final String COUNT_MANIFESTATIONS_IN_GD_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM semarchy_eph_mdm.gd_manifestation where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_PMX = "SELECT\n" +
            "\t M.PRODUCT_MANIFESTATION_ID AS MANIFESTATION_ID -- Product Manifestation Reference,  not needed in EPH but extracted for record linking purposes\n" +
            "\t,M.PRODUCT_MANIFESTATION_TIT AS MANIFESTATION_KEY_TITLE -- Manifestation Title\n" +
            "\t,M.ISBN_STRIPPED AS ISBN -- ISBN (may go in IDs table, depending on implementation of data model)\n" +
            "\t,M.ISSN AS ISSN -- ISSN (may go in IDs table, depending on implementation of data model)\n" +
            "\t,M.COVER_HEIGHT_AMOUNT AS COVER_HEIGHT -- Cover Height for Format sub entity\n" +
            "\t,M.COVER_WIDTH_AMOUNT AS COVER_WIDTH -- Cover Width for Format sub entity\n" +
            "\t,M.PAGE_HEIGHT_AMOUNT AS PAGE_HEIGHT -- Page Height for Format sub entity\n" +
            "\t,M.PAGE_WIDTH_AMOUNT AS PAGE_WIDTH -- Page Width for Format sub entity\n" +
            "\t,M.WEIGHT_AMOUNT AS WEIGHT -- Weight for Format sub entity\n" +
            "\t,M.CARTON_QTY AS CARTON_QTY -- Carton Quantity for Format sub entity\n" +
            "\t,M.INTERNATIONAL_EDITION_IND -- International Edition Indicator\n" +
            "\t,PD.FIRST_PUB_DATE -- Manifestation First Publication Date\n" +
            "\t,M.F_PRODUCT_MANIFESTATION_TYP -- 1 = Print, 2 = Electronic. Will have to map to Manifestation Type (logic TBC)\n" +
            "\t,M.FORMAT_TXT -- Additional Format info (may feed Format Entity, may feed logic for Manifestation Type\n" +
            "\t,M.F_MANIFESTATION_STATUS AS MANIFESTATION_STATUS -- Manifestation Level Status to link to LOV table (this will need mapping to new values, logic TBC)\n" +
            "\t,M.PRODUCT_MANIFESTATION_ID -- Internal PMX ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,M.F_PRODUCT_WORK -- Internal PMX Work ID, not needed in EPH but extracted for record linking purposes\n" +
            "\t,W.F_PRODUCT_TYPE AS WORK_TYPE_ID -- Work Type so for mapping Key title and type fields\n" +
            "\t,M.F_PRODUCT_DISTRIBUTION_TYPE AS MANIFESTATION_SUBTYPE -- Manifestation Distribution Type for mapping manifestation type\n" +
            "\t,M.F_COMMODITY_CODE AS COMMODITY --  Commodity Code for mapping manifestation type\n" +
            "\t,SS.SUBSTATUS_NAME AS MANIFESTATION_SUBSTATUS -- Manifestation Substatus for mapping Status\n" +
            "\t,TO_CHAR(NVL(M.B_UPDDATE,M.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED -- Last updated date on Manifestation record, all other entities are reference or linking\n" +
            "\t,M.EFFECTIVE_TO_DATE AS RECORD_END_DATE\n" +
            "FROM GD_PRODUCT_MANIFESTATION M\n" +
            "JOIN GD_PRODUCT_WORK W ON M.F_PRODUCT_WORK = W.PRODUCT_WORK_ID\n" +
            "LEFT JOIN GD_PRODUCT_SUBSTATUS SS ON M.F_MANIFESTATION_SUBSTATUS = SS.PRODUCT_SUBSTATUS_ID\n" +
            "JOIN GD_PRODUCT_TYPE T ON W.F_PRODUCT_TYPE = T.PRODUCT_TYPE_ID\n" +
            "LEFT JOIN\n" +
            "    (SELECT\n" +
            "         F_PRODUCT_MANIFESTATION\n" +
            "        ,MIN(AVAILABILITY_START_DATE) FIRST_PUB_DATE\n" +
            "     FROM\n" +
            "        GD_PRODUCT_AVAILABILITY\n" +
            "     GROUP BY F_PRODUCT_MANIFESTATION) PD ON M.PRODUCT_MANIFESTATION_ID = PD.F_PRODUCT_MANIFESTATION\n" +
            "WHERE T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES')\n" +
            "AND M.PRODUCT_MANIFESTATION_ID IN ('%s')\n" +
            "ORDER BY M.PRODUCT_MANIFESTATION_ID";


    public static final String SELECT_MANIFESTATIONS_DATA_IN_PMX_STG = "SELECT \n" +
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
            "\"FIRST_PUB_DATE\" as FIRST_PUB_DATE,\n" +
            "\"F_PRODUCT_MANIFESTATION_TYP\" as F_PRODUCT_MANIFESTATION_TYP,\n" +
            "\"FORMAT_TXT\" as FORMAT_TXT,\n" +
            "\"MANIFESTATION_STATUS\" as MANIFESTATION_STATUS,\n" +
            "\"PRODUCT_MANIFESTATION_ID\" as PRODUCT_MANIFESTATION_ID, \n" +
            "\"F_PRODUCT_WORK\" as F_PRODUCT_WORK,\n" +
            "\"WORK_TYPE_ID\" as WORK_TYPE_ID,\n" +
            "\"MANIFESTATION_SUBTYPE\" as MANIFESTATION_SUBTYPE,\n" +
            "\"COMMODITY\" as COMMODITY,\n" +
            "\"MANIFESTATION_SUBSTATUS\" as MANIFESTATION_SUBSTATUS,\n" +
            "\"UPDATED\" as \"UPDATED\",\n" +
            "\"RECORD_END_DATE\" as RECORD_END_DATE\n" +
            "from  "+ GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation\n" +
            "WHERE \"MANIFESTATION_ID\" IN ('%s') order by \"MANIFESTATION_ID\"";

    public static final String SELECT_RANDOM_ISBN_IDS_PHB =
            "select \"ISBN\" AS ISBN \n" +
                    " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man\n" +
                    " join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq dq on man.\"PRODUCT_MANIFESTATION_ID\" = dq.pmx_source_reference\n" +
                    "left join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq w on dq.f_wwork = w.pmx_source_reference\n" +
                    " where dq.dq_err != 'Y' and w.dq_err != 'Y' \n" +
                    " and man.\"MANIFESTATION_SUBTYPE\" = 424 \n" +
                    " and \"ISBN\" is not null order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISBN_IDS_PSB = "select \"ISBN\" AS ISBN \n" +
            " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man\n" +
            " join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq dq on man.\"PRODUCT_MANIFESTATION_ID\" = dq.pmx_source_reference\n" +
            " left join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq w on dq.f_wwork = w.pmx_source_reference\n" +
            " where dq.dq_err != 'Y' and w.dq_err != 'Y' \n" +
            " and man.\"MANIFESTATION_SUBTYPE\" = 425 \n" +
            " and \"ISBN\" is not null order by random() limit '%s'\n";

    public static final String SELECT_RANDOM_ISBN_IDS_EBK = "\n" +
            " select \"ISBN\" AS ISBN \n" +
            " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man\n" +
            " join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq dq on man.\"PRODUCT_MANIFESTATION_ID\" = dq.pmx_source_reference\n" +
            " left join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq w on dq.f_wwork = w.pmx_source_reference\n" +
            " where dq.dq_err != 'Y' and w.dq_err != 'Y' \n" +
            " and man.\"COMMODITY\" = 'EB'\n" +
            " and \"ISBN\" is not null order by random() limit '%s'\n" +
            " ";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_JPR = "select \"MANIFESTATION_ID\" AS manifestation_id \n" +
            " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man\n" +
            " join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq dq on man.\"PRODUCT_MANIFESTATION_ID\" = dq.pmx_source_reference\n" +
            " left join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq w on dq.f_wwork = w.pmx_source_reference\n" +
            " where dq.dq_err != 'Y' and w.dq_err != 'Y' \n" +
            " and  man.\"WORK_TYPE_ID\" IN (4,3,102) and man.\"F_PRODUCT_MANIFESTATION_TYP\" = 1 \n" +
            " order by random() limit '%s'";

    public static final String SELECT_RANDOM_MANIFESTATION_IDS_JEL = "select \"MANIFESTATION_ID\" AS manifestation_id \n" +
            " from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man\n" +
            " join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq dq on man.\"PRODUCT_MANIFESTATION_ID\" = dq.pmx_source_reference\n" +
            " left join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq w on dq.f_wwork = w.pmx_source_reference\n" +
            " where dq.dq_err != 'Y' and w.dq_err != 'Y' \n" +
            " and  man.\"WORK_TYPE_ID\" IN (4,3,102) and man.\"F_PRODUCT_MANIFESTATION_TYP\" != 1 \n" +
            " order by random() limit '%s'";

    public static final String SELECT_MANIFESTATIONS_IDS_FOR_SPECIFIC_ISBN = "select \"MANIFESTATION_ID\" AS manifestation_id from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation where \"ISBN\" in ('%s')";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_EPH_DQ = "select distinct \n" +
            "dq.PMX_SOURCE_REFERENCE as PMX_SOURCE_REFERENCE,\n" +
            "MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE,\n" +
            "INTER_EDITION_FLAG as INTER_EDITION_FLAG,\n" +
            "FIRST_PUB_DATE as FIRST_PUB_DATE,\n" +
            "LAST_PUB_DATE as LAST_PUB_DATE, \n" +
            "dq.F_TYPE as F_TYPE,\n" +
            "dq.F_STATUS as F_STATUS, \n" +
            "F_FORMAT_TYPE as F_FORMAT_TYPE, \n" +
            "F_WWORK as F_WWORK,\n" +
            "dq.DQ_ERR as DQ_ERR\n" +
            "FROM " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq dq\n" +
            "join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_wwork_dq w on dq.f_wwork = w.pmx_source_reference\n" +
            "where \n" +
            "dq.dq_err != 'Y' and w.dq_err != 'Y' and\n" +
            " dq.pmx_source_reference IN ('%s')";


    public static final String SELECT_MANIFESTATIONS_DATA_IN_EPH_SA = "select distinct \n" +
            "sa.b_loadid as B_LOADID,\n" +
            "sa.F_EVENT  as F_EVENT,\n" +
            "sa.B_CLASSNAME as B_CLASSNAME,\n" +
            "sa.MANIFESTATION_ID as MANIFESTATION_ID,\n" +
            "sa.external_reference as PMX_SOURCE_REFERENCE,\n" +
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
            "and external_reference IN ('%s')";

    public static final String SELECT_MANIFESTATIONS_DATA_IN_EPH_GD = "select F_EVENT  as F_EVENT,\n" +
            "B_CLASSNAME as B_CLASSNAME,\n" +
            "MANIFESTATION_ID as MANIFESTATION_ID,\n" +
            "EXTERNAL_REFERENCE as PMX_SOURCE_REFERENCE,\n" +
            "MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE,\n" +
            "INTER_EDITION_FLAG as INTER_EDITION_FLAG,\n" +
            "FIRST_PUB_DATE as FIRST_PUB_DATE,\n" +
            "LAST_PUB_DATE as LAST_PUB_DATE, \n" +
            "F_TYPE as F_TYPE,\n" +
            "F_STATUS as F_STATUS, \n" +
            "F_FORMAT_TYPE as F_FORMAT_TYPE, \n" +
            "F_WWORK as F_WWORK\n" +
            "FROM semarchy_eph_mdm.gd_manifestation WHERE EXTERNAL_REFERENCE IN ('%s')";

    /* Old logic
    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_PMX_MANIFESTATION_TABLE = "select count(*) AS count from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation  where \"%s\" is not null";
    */

    //EPH - 366 - Change to introduce DQ layer
    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_PMX_MANIFESTATION_TABLE =
     "select count(*)\n"+
             "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man, \n"+
             GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq , \n"+
             "semarchy_eph_mdm.sa_manifestation_identifier sman \n"+
             ", " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid map1 \n"+
             "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n"+
             "and map1.source_ref = mdq.pmx_source_reference::text\n"+
             "and concat(map1.eph_id,'ISBN',man.\"ISBN\") = sman.external_reference\n"+
             "and  b_loadid = (select max(b_loadid) from \n"+
             "          semarchy_eph_mdm.sa_event\n"+
             "            where  f_event_type = 'PMX'\n"+
             "            and workflow_id = 'talend'\n"+
             "            AND f_event_type = 'PMX'\n"+
             "            and f_workflow_source = 'PMX' )\n"+
             "and  \"%s\" is not null\n";

    public static final String COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_PMX_MANIFESTATION_DELTA =

    "select count(*)\n"+
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man, \n"+
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq , \n"+
            "semarchy_eph_mdm.sa_manifestation_identifier sman \n"+
            ", " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid map1 \n"+
            "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n"+
            "and map1.source_ref = mdq.pmx_source_reference::text\n"+
            "and concat(map1.eph_id,'ISBN',man.\"ISBN\") = sman.external_reference\n"+
            "and  b_loadid = (select max(b_loadid) from \n"+
            "          semarchy_eph_mdm.sa_event\n"+
            "            where  f_event_type = 'PMX'\n"+
            "            and workflow_id = 'talend'\n"+
            "            AND f_event_type = 'PMX'\n"+
            "            and f_workflow_source = 'PMX' )\n"+
            "and  \"ISBN\" is not null\n"+
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')";
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')";



    public static final String COUNT_OF_RECORDS_WITH_ISSN_IN_EPH_STG_PMX_MANIFESTATION_DELTA =

            "select count(*)\n"+
                    "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man, \n"+
                    GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq , \n"+
                    "semarchy_eph_mdm.sa_manifestation_identifier sman \n"+
                    ", " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid map1 \n"+
                    "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n"+
                    "and map1.source_ref = mdq.pmx_source_reference::text\n"+
                    "and concat(map1.eph_id,'ISSN',man.\"ISSN\") = sman.external_reference\n"+
                    "and  b_loadid = (select max(b_loadid) from \n"+
                    "          semarchy_eph_mdm.sa_event\n"+
                    "            where  f_event_type = 'PMX'\n"+
                    "            and workflow_id = 'talend'\n"+
                    "            AND f_event_type = 'PMX'\n"+
                    "            and f_workflow_source = 'PMX' )\n"+
                    "and  \"ISSN\" is not null\n"+
//                    "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')";
                    "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')";



    public static final String COUNT_OF_RECORDS_IN_EPH_SA_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM semarchy_eph_mdm.sa_manifestation_identifier\n" +
//            "where f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation_identifier\n" +
//            "join semarchy_eph_mdm.sa_event on f_event = event_id \n" +
//            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
//            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
//            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n" +
            "where f_type = '%s' \n" +
            "and identifier is not null\n" +
            "and effective_end_date is null\n" +
            "and f_event = (select max (event_id) from \n" +
            "semarchy_eph_mdm.sa_event\n" +
            "where semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')";

    public static final String COUNT_OF_ALL_RECORDS_IN_EPH_SA_MANIFESTATION_TABLE = "SELECT count(*) AS count FROM semarchy_eph_mdm.sa_manifestation_identifier\n" +
            "where identifier is not null\n" +
            "and effective_end_date is null\n" +
            "and f_event = (select max (event_id) from \n" +
            "semarchy_eph_mdm.sa_event\n" +
            "where semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')";

    public static final String COUNT_OF_RECORDS_IN_EPH_GD_MANIFESTATION_TABLE = "select count(*) as count from semarchy_eph_mdm.gd_manifestation_identifier " +
            "where f_type = '%s'" +
            " and effective_end_date is null and b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";

    public static final String COUNT_OF_ALL_RECORDS_IN_EPH_GD_MANIFESTATION_TABLE = "select count(*) as count from semarchy_eph_mdm.gd_manifestation_identifier " +
            " where effective_end_date is null and b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";

    public static String GET_COUNT_MANIFESTATIONS_IDENTIFIERS_EPHAE = "select count(*) as count from semarchy_eph_mdm.ae_manifestation_identifier where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";

    public static final String SELECT_RECORDS_SA_MANIFESTATION_IDENTIFIER = "SELECT sa.b_loadid as b_loadid,\n" +
            "f_event as f_event,\n" +
            "sa.b_classname as b_classname,\n" +
            "manif_identifier_id as manif_identifier_id,\n" +
            "identifier as identifier, \n " +
            "f_type as f_type,\n" +
            "f_manifestation as f_manifestation,\n" +
            "external_reference as external_reference\n" +
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


    public static final String SELECT_RANDOM_ISBNS_PHB = "\n" +
            "select \"ISBN\" as ISBN, sman.external_reference \n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man, \n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq , \n" +
            "semarchy_eph_mdm.sa_manifestation_identifier sman \n" +
            ", " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid map1 \n" +
            "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n" +
            "and map1.source_ref = mdq.pmx_source_reference::text\n" +
            "and concat(map1.eph_id,'ISBN',man.\"ISBN\") = sman.external_reference\n" +
            "and f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation_identifier \n" +
            "join semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n" +
            "and  \"%s\" is not null and man.\"MANIFESTATION_SUBTYPE\" = 424 order by random() limit '%s' ";


    public static final String SELECT_ISBN_FOR_RECORDS_WITH_SET_END_DATA = "select \"ISBN\" as ISBN\n" +
            "from \n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation stg,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq,\n" +
            GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid sid,   \n" +
            "semarchy_eph_mdm.sa_manifestation_identifier  sa\n" +
            "where stg.\"PRODUCT_MANIFESTATION_ID\" = mdq.PMX_SOURCE_REFERENCE\n" +
            "and stg.\"MANIFESTATION_ID\"::varchar = sid.source_ref\n" +
            "and mdq.dq_err != 'Y' \n" +
            "and sa.effective_end_date is not null";

    public static final String SELECT_ISSN_FOR_RECORDS_WITH_SET_END_DATA = "select \"ISSN\" as ISSN\n" +
            "from \n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation stg,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq,\n" +
            GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid sid,   \n" +
            "semarchy_eph_mdm.sa_manifestation_identifier  sa\n" +
            "where stg.\"PRODUCT_MANIFESTATION_ID\" = mdq.PMX_SOURCE_REFERENCE\n" +
            "and stg.\"MANIFESTATION_ID\"::varchar = sid.source_ref\n" +
            "and mdq.dq_err != 'Y' \n" +
            "and sa.effective_end_date is not null";

    public static final String SELECT_RANDOM_ISBNS_PSB =  "\n" +
            "select \"ISBN\" as ISBN, sman.external_reference \n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man, \n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq , \n" +
            "semarchy_eph_mdm.sa_manifestation_identifier sman \n" +
            ", " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid map1 \n" +
            "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n" +
            "and map1.source_ref = mdq.pmx_source_reference::text\n" +
            "and concat(map1.eph_id,'ISBN',man.\"ISBN\") = sman.external_reference\n" +
            "and f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation_identifier \n" +
            "join semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n" +
            "and  \"%s\" is not null and man.\"MANIFESTATION_SUBTYPE\" = 425 order by random() limit '%s' ";

    public static final String SELECT_RANDOM_ISBNS_EBK =
            "select \"ISBN\" as ISBN, sman.external_reference \n"+
                    "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation man, \n"+
                    ""+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq mdq , \n"+
                    "semarchy_eph_mdm.sa_manifestation_identifier sman \n"+
                    ", "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid map1 \n"+
                    "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n"+
                    "and map1.source_ref = mdq.pmx_source_reference::text\n"+
                    "and concat(map1.eph_id,'ISBN',man.\"ISBN\") = sman.external_reference\n"+
                    "and f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation_identifier \n"+
                    "join semarchy_eph_mdm.sa_event on f_event = event_id \n"+
                    "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
                    "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
                    "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
                    "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n"+
                    "and  \"%s\" is not null and man.\"COMMODITY\" = 'EB' order by random() limit '%s'";

    public static final String SELECT_RANDOM_ISSNS_JPR_IDS =
      "select \"ISBN\" as ISBN, sman.external_reference \n"+
              "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation man, \n"+
              ""+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq mdq , \n"+
              "semarchy_eph_mdm.sa_manifestation_identifier sman \n"+
              ", "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid map1 \n"+
              "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n"+
              "and map1.source_ref = mdq.pmx_source_reference::text\n"+
              "and concat(map1.eph_id,'ISBN',man.\"ISBN\") = sman.external_reference\n"+
              "and f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation_identifier \n"+
              "join semarchy_eph_mdm.sa_event on f_event = event_id \n"+
              "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
              "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
              "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
              "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n"+
              "and  \"%s\" is not null and  \"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" = 1 order by random() limit '%s'";


    public static final String SELECT_RANDOM_ISSNS_JEL_IDS =  "select \"ISBN\" as ISBN, sman.external_reference \n"+
            "from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation man, \n"+
            ""+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq mdq , \n"+
            "semarchy_eph_mdm.sa_manifestation_identifier sman \n"+
            ", "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid map1 \n"+
            "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n"+
            "and map1.source_ref = mdq.pmx_source_reference::text\n"+
            "and concat(map1.eph_id,'ISBN',man.\"ISBN\") = sman.external_reference\n"+
            "and f_event = (select max (f_event) from semarchy_eph_mdm.sa_manifestation_identifier \n"+
            "join semarchy_eph_mdm.sa_event on f_event = event_id \n"+
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n"+
            "and  \"%s\" is not null and  \"WORK_TYPE_ID\" IN (4,3,102) and \"F_PRODUCT_MANIFESTATION_TYP\" != 1 order by random() limit '%s'";;

    public static final String SELECT_RECORDS_STG_MANIF_IDENTIFIER_ISBN = "select \"ISBN\" as identifier,\n" +
            "map1.eph_id as f_manifestation,\n" +
            "concat(map1.eph_id,'ISBN',man.\"ISBN\") as external_reference\n" +
            "from\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man, \n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq ,\n" +
            "semarchy_eph_mdm.sa_manifestation_identifier sman , \n" +
            GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid map1 \n" +
            "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n" +
            "and map1.source_ref = mdq.pmx_source_reference::text\n" +
            "and concat(map1.eph_id,'ISBN',man.\"ISBN\") = sman.external_reference\n" +
            "and \"ISBN\" IN ('%s')";




    public static final String SELECT_RECORDS_STG_MANIF_IDENTIFIER_ISSN ="select \"ISSN\" as identifier,\n" +
            "map1.eph_id as f_manifestation,\n" +
            "concat(map1.eph_id,'ISSN',man.\"ISSN\") as external_reference\n" +
            "from\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man, \n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq ,\n" +
            "semarchy_eph_mdm.sa_manifestation_identifier sman , \n" +
            GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid map1 \n" +
            "where man.\"MANIFESTATION_ID\" = mdq.pmx_source_reference\n" +
            "and map1.source_ref = mdq.pmx_source_reference::text\n" +
            "and concat(map1.eph_id,'ISSN',man.\"ISSN\") = sman.external_reference\n" +
            "and \"ISSN\" IN ('%s')";





    public static final String SELECT_MANIFESTATION_IDS_ISBN = "select \"MANIFESTATION_ID\" as MANIFESTATION_ID\n" +
            "   from \n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation stg,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq dq,\n" +
            GetEPHDBUser.getDBUser() + ".map_identref_2_identid mid,\n" +
            GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid sid,\n" +
            "semarchy_eph_mdm.gd_manifestation_identifier  gd\n" +
            " where stg.\"MANIFESTATION_ID\"::varchar = sid.source_ref\n" +
            "and concat(sid.eph_id,'+','ISBN', '+', stg.\"ISBN\") = mid.ident_ref\n" +
            "and stg.\"MANIFESTATION_ID\" = dq.pmx_source_reference\n" +
            "and gd.manif_identifier_id = mid.ident_id\n" +
            "and gd.effective_end_date is not null\n" +
            "and dq.dq_err != 'Y'\n" +
            "and f_event = (select max (event_id) from \n" +
            "semarchy_eph_mdm.sa_event\n" +
            "where semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')";

    public static final String SELECT_MANIFESTATION_IDS_ISSN ="select \"MANIFESTATION_ID\" as MANIFESTATION_ID\n" +
            "   from \n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation stg,\n" +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq dq,\n" +
            GetEPHDBUser.getDBUser() + ".map_identref_2_identid mid,\n" +
            GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid sid,\n" +
            "semarchy_eph_mdm.gd_manifestation_identifier  gd\n" +
            " where stg.\"MANIFESTATION_ID\"::varchar = sid.source_ref\n" +
            "and concat(sid.eph_id,'+','ISSN', '+', stg.\"ISSN\") = mid.ident_ref\n" +
            "and stg.\"MANIFESTATION_ID\" = dq.pmx_source_reference\n" +
            "and gd.manif_identifier_id = mid.ident_id\n" +
            "and gd.effective_end_date is not null\n" +
            "and dq.dq_err != 'Y'\n" +
            "and f_event = (select max (event_id) from \n" +
            "semarchy_eph_mdm.sa_event\n" +
            "where semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')";

    public static final String SELECT_ISBNS_FROM_STG_AND_SA_FOR_END_DATED_RECORDS  = "select gdm.manifestation_id as GD_MANIG_ID ,gdm.external_reference as GD_PMX_SOURCE_REFERENCE ,gid.f_type as typ ,gid.identifier as gd_old_identifier ,gid.effective_start_date,gid.effective_end_date,mdq.f_type as typ_1,man.\"ISBN\" as new_identifier from \n" +
            "semarchy_eph_mdm.gd_manifestation_identifier gid ,semarchy_eph_mdm.gd_manifestation gdm ," +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq," +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man \n" +
            "where  gid.f_manifestation = gdm.manifestation_id\n" +
            "and    gdm.external_reference = mdq.pmx_source_reference::text\n" +
            "and    mdq.pmx_source_reference = man.\"PRODUCT_MANIFESTATION_ID\"\n" +
            "and    mdq.dq_err = 'N'\n" +
            "and gid.b_batchid in (select max(b_batchid) from semarchy_eph_mdm.gd_event where description = 'PMX Talend Load' and workflow_id = 'talend' and f_workflow_source = 'PMX') \n" +
            "and gid.f_type ='ISBN' \n" +
            "and gid.effective_end_date is not null;\n";


    public static final String SELECT_ISSNS_FROM_STG_AND_SA_FOR_END_DATED_RECORDS  =  "select gdm.manifestation_id as GD_MANIG_ID ,gdm.external_reference as GD_PMX_SOURCE_REFERENCE ,gid.f_type as typ ,gid.identifier as gd_old_identifier ,gid.effective_start_date,gid.effective_end_date,mdq.f_type as typ_1,man.\"ISSN\" as new_identifier from \n" +
            "semarchy_eph_mdm.gd_manifestation_identifier gid ,semarchy_eph_mdm.gd_manifestation gdm ," +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation_dq mdq," +
            GetEPHDBUser.getDBUser() + ".stg_10_pmx_manifestation man \n" +
            "where  gid.f_manifestation = gdm.manifestation_id\n" +
            "and    gdm.external_reference = mdq.pmx_source_reference::text\n" +
            "and    mdq.pmx_source_reference = man.\"PRODUCT_MANIFESTATION_ID\"\n" +
            "and    mdq.dq_err = 'N'\n" +
            "and gid.b_batchid in (select max(b_batchid) from semarchy_eph_mdm.gd_event where description = 'PMX Talend Load' and workflow_id = 'talend' and f_workflow_source = 'PMX') \n" +
            "and gid.f_type ='ISSN' \n" +
            "and gid.effective_end_date is not null;\n";




}
