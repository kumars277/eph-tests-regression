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
            "  ,\"SOC_OWNERSHIP\" AS OWNERSHIP \n" +
            "  ,\"UPDATED\" as UPDATED\n" +
            "  ,\"LANGUAGE_CODE\" as LANGUAGE_CODE\n" +
            "  ,\"RECORD_END_DATE\" AS RECORD_END_DATE\n"+
            "  FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork\n" +
            "  WHERE \"PRODUCT_WORK_ID\" IN ('%s') ORDER BY \"PRODUCT_WORK_ID\"";

    public static String GET_STG_DQ_WORKS_DATA =
            //old
//            "SELECT \n" +
//            "  ww.PMX_SOURCE_REFERENCE AS EXTERNAL_REFERENCE\n" +
//            "  ,ww.WORK_TITLE AS WORK_TITLE -- Title\n" +
//            "  ,ww.WORK_SUBTITLE AS WORK_SUBTITLE -- Subtitle\n" +
//            "  ,ww.ELECTRO_RIGHTS_INDICATOR as ELECTRONIC_RIGHTS_IND\n" +
//            "  ,ww.VOLUME as BOOK_VOLUME_NAME\n" +
//            "  ,ww.COPYRIGHT_YEAR as PRODUCT_WORK_PUB_DATE\n" +
//            "  ,ww.EDITION_NUMBER as BOOK_EDITION_NAME\n" +
//            "  ,ww.F_PMC as PMC\n" +
//            "  ,ww.F_OA_JOURNAL_TYPE AS OPEN_ACCESS_JNL_TYPE_CODE\n" +
//            "  ,ww.F_TYPE AS WORK_TYPE\n" +
//            "  ,ww.F_STATUS AS WORK_STATUS\n" +
//            "  ,ww.F_IMPRINT AS IMPRINT\n" +
//            "  ,ww.F_SOCIETY_OWNERSHIP AS OWNERSHIP\n" +
//            "  ,ww.opco AS F_OPCO_R12\n" +
//            "  ,ww.resp_centre AS F_RESPONSIBILITY_CENTRE\n" +
//            "  ,ap.accountable_product_id as f_accountable_product" +
//            "  --,ap.\"PARENT_ACC_PROD\" as PARENT_ACC_PROD\n" +
//            "  ,ww.LANGUAGE_CODE as LANGUAGE_CODE\n"+
//            "  FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq ww\n"+
//            "  left join \n" +
//            "(select distinct s.product_work_id, a.external_reference, a.accountable_product_id \n" +
//            "\t from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_accountable_product_dq s join semarchy_eph_mdm.sa_accountable_product a on\n" +
//            "\t s.pmx_source_reference = a.external_reference where s.dq_err != 'Y') ap on ww.pmx_source_reference::varchar = ap.product_work_id::varchar\n"+
//            "  WHERE PMX_SOURCE_REFERENCE IN ('%s') ORDER BY PMX_SOURCE_REFERENCE";
            //NEW
            " with existing_hash as (select external_reference, concat(external_reference||coalesce(work_title,'')||coalesce(work_sub_title,'')||coalesce(work_key_title,'')||\n"+
                    "coalesce(electro_rights_indicator::varchar,'')||coalesce(volume::varchar,'')||coalesce(copyright_year::varchar,'')||coalesce(edition_number::varchar,'')||\n"+
                    "coalesce(f_type::varchar,'')||coalesce(f_status::varchar,'')||coalesce(f_accountable_product::varchar,'')||coalesce(f_pmc,'')||coalesce(f_oa_type,'')||coalesce(f_imprint,'')||\n"+
                    "coalesce(f_society_ownership,'')||coalesce(f_llanguage,'')) as string from semarchy_eph_mdm.gd_wwork),\n"+
                    "base as\n"+
                    "(select\n"+
                    "distinct\n"+
                    "-- {loadid} b_loadid\n"+
                    "--,{eventid} f_event\n"+
                    "map_sourceref_2_ephid('WORK'::varchar, ww.pmx_source_reference::varchar) work_id\n"+
                    ",ww.pmx_source_reference as EXTERNAL_REFERENCE\n"+
                    "--,'Work' b_classname as B_CLASSNAME\n"+
                    ",ww.work_title as WORK_TITLE\n"+
                    ",ww.work_subtitle as WORK_SUBTITLE\n"+
                    ",ww.electro_rights_indicator as ELECTRONIC_RIGHTS_IND\n"+
                    ",ww.volume as BOOK_VOLUME_NAME\n"+
                    ",ww.copyright_year as PRODUCT_WORK_PUB_DATE\n"+
                    ",ww.edition_number as BOOK_EDITION_NAME\n"+
                    ",ww.f_pmc as PMC\n"+
                    ",ww.f_oa_journal_type AS OPEN_ACCESS_JNL_TYPE_CODE\n"+
                    ",ww.f_type  AS WORK_TYPE\n"+
                    ",ww.f_status  AS WORK_STATUS\n"+
                    ",ww.f_imprint  AS IMPRINT\n"+
                    ",ww.f_society_ownership  AS OWNERSHIP\n"+
                    ",case when coalesce(ap.accountable_product_id,gw.f_accountable_product) is null then null else \n"+
                    " coalesce(ap.accountable_product_id,gw.f_accountable_product) end as f_accountable_product\n"+
                    ",ww.language_code as LANGUAGE_CODE\n"+
                    "FROM stg_10_pmx_wwork_dq ww\n"+
                    "left join semarchy_eph_mdm.gd_wwork gw on ww.pmx_source_reference::varchar = gw.external_reference::varchar\n"+
                    "left join \n"+
                    "\t(select distinct s.product_work_id, a.external_reference, a.accountable_product_id \n"+
                    "\t from stg_10_pmx_accountable_product_dq s join (select distinct * from semarchy_eph_mdm.sa_accountable_product) a on\n"+
                    "\t s.pmx_source_reference = a.external_reference where s.dq_err != 'Y') ap on ww.pmx_source_reference::varchar = ap.product_work_id::varchar\n"+
                    "WHERE ww.dq_err != 'Y'\n"+
                    "--and (TO_TIMESTAMP(ww.work_updated,'YYYYMMDDHH24MI') > TO_TIMESTAMP('{LAST_REFRESH_VALUE}','YYYYMMDDHH24MI') or {full_load} = true)\n"+
                    ")\n"+
                    ",inbound_hash as (select external_reference, concat(external_reference||coalesce(work_title,'')||coalesce(work_subtitle,'')||''||\n"+
                    "coalesce(ELECTRONIC_RIGHTS_IND::varchar,'')||coalesce(BOOK_VOLUME_NAME::varchar,'')||coalesce(PRODUCT_WORK_PUB_DATE::varchar,'')||coalesce(BOOK_EDITION_NAME::varchar,'')||\n"+
                    "coalesce(WORK_TYPE::varchar,'')||coalesce(WORK_STATUS::varchar,'')||coalesce(f_accountable_product::varchar,'')||coalesce(PMC,'')||coalesce(OPEN_ACCESS_JNL_TYPE_CODE,'')||coalesce(IMPRINT,'')||\n"+
                    "coalesce(OWNERSHIP,'')||coalesce(LANGUAGE_CODE,'')) as string from base)\n"+
                    "select \n"+
                    "  b.EXTERNAL_REFERENCE\n"+
                    "  ,WORK_TITLE -- Title\n"+
                    "  ,WORK_SUBTITLE -- Subtitle\n"+
                    "  ,ELECTRONIC_RIGHTS_IND\n"+
                    "  ,BOOK_VOLUME_NAME\n"+
                    "  ,PRODUCT_WORK_PUB_DATE\n"+
                    "  ,BOOK_EDITION_NAME\n"+
                    "  ,PMC\n"+
                    "  ,OPEN_ACCESS_JNL_TYPE_CODE\n"+
                    "  ,WORK_TYPE\n"+
                    "  ,WORK_STATUS\n"+
                    "  ,IMPRINT\n"+
                    "  ,OWNERSHIP\n"+
                    "--  ,F_OPCO_R12\n"+
                    "--  ,F_RESPONSIBILITY_CENTRE\n"+
                    "  ,f_accountable_product\n"+
                    "  ,LANGUAGE_CODE \n"+
                    "from base b join inbound_hash h on b.external_reference = h.external_reference left join existing_hash e on h.external_reference::varchar = e.external_reference::varchar\n"+
                    "where md5(e.string) != md5(h.string)\n"+
                    "and    b.EXTERNAL_REFERENCE IN ('%s') ORDER BY  b.EXTERNAL_REFERENCE";

    public static String GET_EPH_WORKS_DATA ="SELECT \n" +
            "   WORK_ID AS WORK_ID\n" +
            "  ,external_reference AS PMX_SOURCE_REFERENCE\n" +
            "  ,sa.B_CLASSNAME as B_CLASSNAME\n" +
            "  ,WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,WORK_SUB_TITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,ELECTRO_RIGHTS_INDICATOR as ELECTRONIC_RIGHTS_IND\n" +
            "  ,VOLUME as BOOK_VOLUME_NAME\n" +
            "  ,COPYRIGHT_YEAR as PRODUCT_WORK_PUB_DATE\n" +
            "  ,EDITION_NUMBER as BOOK_EDITION_NAME\n" +
            "  ,F_PMC as PMC\n" +
            "  ,F_OA_TYPE AS OPEN_ACCESS_JNL_TYPE_CODE\n" +
            "  ,F_TYPE AS WORK_TYPE\n" +
            "  ,F_STATUS AS WORK_STATUS\n" +
            "  ,F_IMPRINT AS IMPRINT\n" +
            "  ,F_SOCIETY_OWNERSHIP AS OWNERSHIP\n" +
            "  ,f_accountable_product as f_accountable_product\n" +
            "  ,f_llanguage as LANGUAGE_CODE\n"+
            "  FROM semarchy_eph_mdm.sa_wwork sa\n"+
            "where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.sa_event\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )\n" +
            "AND external_reference IN ('%s')";


    public static String GET_EPH_GD_WORKS_DATA ="SELECT \n" +
            "   WORK_ID AS WORK_ID\n" +
            "  ,external_reference AS PMX_SOURCE_REFERENCE\n" +
            "  ,B_CLASSNAME as B_CLASSNAME\n" +
            "  ,WORK_TITLE AS WORK_TITLE -- Title\n" +
            "  ,WORK_SUB_TITLE AS WORK_SUBTITLE -- Subtitle\n" +
            "  ,ELECTRO_RIGHTS_INDICATOR as ELECTRONIC_RIGHTS_IND\n" +
            "  ,VOLUME as BOOK_VOLUME_NAME\n" +
            "  ,COPYRIGHT_YEAR as PRODUCT_WORK_PUB_DATE\n" +
            "  ,EDITION_NUMBER as BOOK_EDITION_NAME\n" +
            "  ,F_PMC as PMC\n" +
            "  ,F_OA_TYPE AS OPEN_ACCESS_JNL_TYPE_CODE\n" +
            "  ,F_TYPE AS WORK_TYPE\n" +
            "  ,F_STATUS AS WORK_STATUS\n" +
            "  ,F_IMPRINT AS IMPRINT\n" +
            "  ,F_SOCIETY_OWNERSHIP AS OWNERSHIP\n" +
            "  ,f_accountable_product as f_accountable_product\n" +
            "  ,f_llanguage as LANGUAGE_CODE\n"+
            "  FROM semarchy_eph_mdm.gd_wwork\n" +
            "  WHERE external_reference IN ('%s')";

    public static String GET_Acc_Prod ="SELECT \n" +
            "numeric_id as f_accountable_product from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid " +
            "where source_ref = 'PARAM1'";


}
