package com.eph.automation.testing.services.db.sql;

public class GeneratingRandomSQL {
    public static String generatingValue=

            //old
//            "SELECT dq.pmx_source_reference as random_value\n" +
//   " FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq dq\n" +
//            " join semarchy_eph_mdm.sa_wwork sa on dq.pmx_source_reference=cast(sa.external_reference as numeric)\n" +
//            "join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork st on dq.pmx_source_reference=st.\"PRODUCT_WORK_ID\"\n"+
//            "where dq.\"dq_err\" = 'N' and sa.b_error_status is null\n" +
//            "ORDER BY RANDOM()\n" +
//            " LIMIT PARAM1;";

    "with existing_hash as (select external_reference, concat(external_reference||coalesce(work_title,'')||coalesce(work_sub_title,'')||coalesce(work_key_title,'')||\n" +
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
            "b.EXTERNAL_REFERENCE as random_value\n" +
            "from base b join inbound_hash h on b.external_reference = h.external_reference left join existing_hash e on h.external_reference::varchar = e.external_reference::varchar\n" +
            "where md5(e.string) != md5(h.string)\n" +
            "order by RANDOM()\n" +
            "limit PARAM1;";

    public static String generatingValueDelta =


            "with existing_hash as (select external_reference, concat(external_reference||coalesce(work_title,'')||coalesce(work_sub_title,'')||coalesce(work_key_title,'')||\n" +
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
                    "and (TO_TIMESTAMP(ww.work_updated,'YYYYMMDDHH24MI') > TO_TIMESTAMP('201908221200','YYYYMMDDHH24MI'))\n" +
                    ")\n" +
                    ",inbound_hash as (select external_reference, concat(external_reference||coalesce(work_title,'')||coalesce(work_subtitle,'')||''||\n" +
                    "coalesce(ELECTRONIC_RIGHTS_IND::varchar,'')||coalesce(BOOK_VOLUME_NAME::varchar,'')||coalesce(PRODUCT_WORK_PUB_DATE::varchar,'')||coalesce(BOOK_EDITION_NAME::varchar,'')||\n" +
                    "coalesce(WORK_TYPE::varchar,'')||coalesce(WORK_STATUS::varchar,'')||coalesce(f_accountable_product::varchar,'')||coalesce(PMC,'')||coalesce(OPEN_ACCESS_JNL_TYPE_CODE,'')||coalesce(IMPRINT,'')||\n" +
                    "coalesce(OWNERSHIP,'')||coalesce(LANGUAGE_CODE,'')) as string from base)\n" +
                    "select \n" +
                    "b.EXTERNAL_REFERENCE as random_value\n" +
                    "from base b join inbound_hash h on b.external_reference = h.external_reference left join existing_hash e on h.external_reference::varchar = e.external_reference::varchar\n" +
                    "where md5(e.string) != md5(h.string)\n" +
                    "order by RANDOM()\n" +
                    "limit PARAM1;";
}
