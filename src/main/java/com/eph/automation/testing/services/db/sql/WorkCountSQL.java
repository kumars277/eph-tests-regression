package com.eph.automation.testing.services.db.sql;

public class WorkCountSQL {

    public static String PMX_WORKS_COUNT = "SELECT count(*) as workCountPmx\n" +
            " FROM GD_PRODUCT_WORK W\n" +
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
            "  WHERE T.PRODUCT_TYPE_CODE NOT IN ('COMPENDIUM','JCOLSC','ADVERTISING','FS','DUES')";

    public static String PMX_STG_WORKS_COUNT = "select count (*) as workCountPMXSTG from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork";

    public static String GET_REFRESH_DATE = "select refresh_value as refresh_timestamp from "+GetEPHDBUser.getDBUser()+".etl_run \n" +
            " where loadset_code='10_PMX_FULL' and refresh_value is not null\n"+
            "order by TO_DATE(refresh_value,'YYYYMMDDHH24MI') desc";

    public static String PMX_STG_WORKS_COUNT_DELTA = "select count(distinct \"PRODUCT_WORK_ID\") as workCountPMXSTG from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork\n" +
//            "where TO_TIMESTAMP(greatest(\"UPDATED\",coalesce(\"MANIFESTATION_UPDATE\",'190001010000')),'YYYYMMDDHH24MI') > TO_TIMESTAMP('PARAM1','YYYYMMDDHH24MI') ";
            "where TO_TIMESTAMP(greatest(\"UPDATED\",coalesce(\"MANIFESTATION_UPDATE\",'190001010000')),'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200 ','YYYYMMDDHH24MI') ";

    public static String PMX_STG_WORKS_COUNT_Distinct = "  select count(distinct \"PRODUCT_WORK_ID\") as workCountPMXSTG from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork";

    public static String PMX_STG_DQ_WORKS_COUNT =

            "select count(*) as workCountDQSTG from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq ww\n" +
            "left join semarchy_eph_mdm.gd_wwork gw on ww.pmx_source_reference::varchar = gw.external_reference::varchar\n";
//old
//    public static String PMX_STG_DQ_WORKS_COUNT_NoErr =
//         "select count(*)  as workCountDQSTGnoError FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq ww\n"+
//        "left join semarchy_eph_mdm.gd_wwork gw on ww.pmx_source_reference::varchar = gw.external_reference::varchar\n"+
//        "left join \n"+
//        "\t(select distinct s.product_work_id, a.external_reference, a.accountable_product_id \n"+
//        "\t from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_accountable_product_dq s join (select distinct * from semarchy_eph_mdm.sa_accountable_product) a on\n"+
//        "\t s.pmx_source_reference = a.external_reference where s.dq_err != 'Y') ap on ww.pmx_source_reference::varchar = ap.product_work_id::varchar\n"+
//        "WHERE ww.dq_err != 'Y'\n";
////        "and TO_TIMESTAMP(ww.work_updated,'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')";

    public static String PMX_STG_DQ_WORKS_COUNT_NoErr = " with existing_hash as (select external_reference, concat(external_reference||coalesce(work_title,'')||coalesce(work_sub_title,'')||coalesce(work_key_title,'')||\n" +
            "coalesce(electro_rights_indicator::varchar,'')||coalesce(volume::varchar,'')||coalesce(copyright_year::varchar,'')||coalesce(edition_number::varchar,'')||\n" +
            "coalesce(f_type::varchar,'')||coalesce(f_status::varchar,'')||coalesce(f_accountable_product::varchar,'')||coalesce(f_pmc,'')||coalesce(f_oa_type,'')||coalesce(f_imprint,'')||\n" +
            "coalesce(f_society_ownership,'')||coalesce(f_llanguage,'')) as string from semarchy_eph_mdm.gd_wwork),\n" +
            "base as\n" +
            "(select\n" +
            "distinct\n" +
            "-- {loadid} b_loadid\n" +
            "--,{eventid} f_event\n" +
            "map_sourceref_2_ephid('WORK'::varchar, ww.pmx_source_reference::varchar) work_id\n" +
            ",ww.pmx_source_reference as EXTERNAL_REFERENCE\n" +
            "--,'Work' b_classname as B_CLASSNAME\n" +
            ",ww.work_title as WORK_TITLE\n" +
            ",ww.work_subtitle as WORK_SUBTITLE\n" +
            ",ww.electro_rights_indicator as ELECTRONIC_RIGHTS_IND\n" +
            ",ww.volume as BOOK_VOLUME_NAME\n" +
            ",ww.copyright_year as PRODUCT_WORK_PUB_DATE\n" +
            ",ww.edition_number as BOOK_EDITION_NAME\n" +
            ",ww.f_pmc as PMC\n" +
            ",ww.f_oa_journal_type AS OPEN_ACCESS_JNL_TYPE_CODE\n" +
            ",ww.f_type  AS WORK_TYPE\n" +
            ",ww.f_status  AS WORK_STATUS\n" +
            ",ww.f_imprint  AS IMPRINT\n" +
            ",ww.f_society_ownership  AS OWNERSHIP\n" +
            ",case when coalesce(ap.accountable_product_id,gw.f_accountable_product) is null then null else \n" +
            " coalesce(ap.accountable_product_id,gw.f_accountable_product) end as f_accountable_product\n" +
            ",ww.language_code as LANGUAGE_CODE\n" +
            "FROM stg_10_pmx_wwork_dq ww\n" +
            "left join semarchy_eph_mdm.gd_wwork gw on ww.pmx_source_reference::varchar = gw.external_reference::varchar\n" +
            "left join \n" +
            "\t(select distinct s.product_work_id, a.external_reference, a.accountable_product_id \n" +
            "\t from stg_10_pmx_accountable_product_dq s join (select distinct * from semarchy_eph_mdm.sa_accountable_product) a on\n" +
            "\t s.pmx_source_reference = a.external_reference where s.dq_err != 'Y') ap on ww.pmx_source_reference::varchar = ap.product_work_id::varchar\n" +
            "WHERE ww.dq_err != 'Y'\n" +
            "--and (TO_TIMESTAMP(ww.work_updated,'YYYYMMDDHH24MI') > TO_TIMESTAMP('{LAST_REFRESH_VALUE}','YYYYMMDDHH24MI') or {full_load} = true)\n" +
            ")\n" +
            ",inbound_hash as (select external_reference, concat(external_reference||coalesce(work_title,'')||coalesce(work_subtitle,'')||''||\n" +
            "coalesce(ELECTRONIC_RIGHTS_IND::varchar,'')||coalesce(BOOK_VOLUME_NAME::varchar,'')||coalesce(PRODUCT_WORK_PUB_DATE::varchar,'')||coalesce(BOOK_EDITION_NAME::varchar,'')||\n" +
            "coalesce(WORK_TYPE::varchar,'')||coalesce(WORK_STATUS::varchar,'')||coalesce(f_accountable_product::varchar,'')||coalesce(PMC,'')||coalesce(OPEN_ACCESS_JNL_TYPE_CODE,'')||coalesce(IMPRINT,'')||\n" +
            "coalesce(OWNERSHIP,'')||coalesce(LANGUAGE_CODE,'')) as string from base)\n" +
            "select \n" +
            "count(*)\n" +
            "from base b join inbound_hash h on b.external_reference = h.external_reference left join existing_hash e on h.external_reference::varchar = e.external_reference::varchar\n" +
            "where md5(e.string) != md5(h.string)";

    public static String EPH_SA_WORKS_COUNT = "select count (distinct external_reference) as workCountEPH from semarchy_eph_mdm.sa_wwork "+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.sa_event\n"+
            "where  semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX' )";


    public static String EPH_GD_WORKS_COUNT = "select count (*) as workCountEPHGD from semarchy_eph_mdm.GD_wwork"+
            " where f_event =  (select max (event_id) from\n" +
            "semarchy_eph_mdm.gd_event\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";

    public static String EPH_AE_WORKS_COUNT = "select count (*) as errorsCountEPH from semarchy_eph_mdm.ae_wwork ae\n"+
            " where ae.b_batchid =  (select max (b_batchid) from\n" +
            "semarchy_eph_mdm.gd_event e\n"+
            "where  e.f_event_type = 'PMX'\n"+
            "and e.workflow_id = 'talend'\n"+
            "AND e.f_event_type = 'PMX'\n"+
            "and e.f_workflow_source = 'PMX' )";
}
