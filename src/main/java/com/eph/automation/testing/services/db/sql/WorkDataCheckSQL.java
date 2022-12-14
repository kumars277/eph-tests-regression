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
            "  ,\"SUBSCRIPTION_TYPE\" as SUBSCRIPTION_TYPE\n" +
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
            "SELECT \n" +
                    "  ww.PMX_SOURCE_REFERENCE AS EXTERNAL_REFERENCE\n" +
                    "  ,ww.WORK_TITLE AS WORK_TITLE -- Title\n" +
                    "  ,ww.WORK_SUBTITLE AS WORK_SUBTITLE -- Subtitle\n" +
                    "  ,ww.ELECTRO_RIGHTS_INDICATOR as ELECTRONIC_RIGHTS_IND\n" +
                    "  ,ww.VOLUME as BOOK_VOLUME_NAME\n" +
                    "  ,ww.COPYRIGHT_YEAR as PRODUCT_WORK_PUB_DATE\n" +
                    "  ,ww.EDITION_NUMBER as BOOK_EDITION_NAME\n" +
                    "  ,ww.F_PMC as PMC\n" +
                    "  ,ww.F_OA_JOURNAL_TYPE AS OPEN_ACCESS_JNL_TYPE_CODE\n" +
                    "  ,ww.F_TYPE AS WORK_TYPE\n" +
                    "  ,ww.F_STATUS AS WORK_STATUS\n" +
                    "  ,ww.F_IMPRINT AS IMPRINT\n" +
                    "  ,ww.F_SOCIETY_OWNERSHIP AS OWNERSHIP\n" +
                    "  ,ww.opco AS F_OPCO_R12\n" +
                    "  ,ww.resp_centre AS F_RESPONSIBILITY_CENTRE\n" +
                    "  ,ap.accountable_product_id as f_accountable_product\n" +
                    "  --,ap.\"PARENT_ACC_PROD\" as PARENT_ACC_PROD\n" +
                    "  ,ww.LANGUAGE_CODE as LANGUAGE_CODE\n" +
                    "  ,ww.SUBSCRIPTION_TYPE as SUBSCRIPTION_TYPE\n" +
                    "   FROM stg_10_pmx_wwork_dq ww\n" +
                    "left join semarchy_eph_mdm.gd_wwork gw on ww.pmx_source_reference::varchar = gw.external_reference::varchar\n" +
                    "left join \n" +
                    "      (select distinct s.product_work_id, a.external_reference, a.accountable_product_id \n" +
                    "       from stg_10_pmx_accountable_product_dq s join (select distinct * from semarchy_eph_mdm.sa_accountable_product) a on\n" +
                    "      s.pmx_source_reference = a.external_reference where s.dq_err != 'Y') ap on ww.pmx_source_reference::varchar = ap.product_work_id::varchar\n" +
                    "WHERE ww.dq_err != 'Y'\n" +
                    "and PMX_SOURCE_REFERENCE IN ('%s') ORDER BY PMX_SOURCE_REFERENCE"
            ;

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
            "  ,f_subscription_type as SUBSCRIPTION_TYPE\n"+
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
            "  ,f_subscription_type as SUBSCRIPTION_TYPE\n"+
            "  FROM semarchy_eph_mdm.gd_wwork\n" +
            "  WHERE external_reference IN ('%s')";

    public static String GET_Acc_Prod ="SELECT \n" +
            "numeric_id as f_accountable_product from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid " +
            "where source_ref = 'PARAM1'";


}
