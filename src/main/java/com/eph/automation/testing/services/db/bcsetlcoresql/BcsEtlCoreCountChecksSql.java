package com.eph.automation.testing.services.db.bcsetlcoresql;


public class BcsEtlCoreCountChecksSql {
    private BcsEtlCoreCountChecksSql(){
        throw new IllegalStateException("Utility class");
    }

    public static final String GET_BCS_ETL_CORE_ACC_PROD_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_accountable_product_current_v";

    public static final String GET_BCS_ETL_CORE_ACC_PROD_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_part  where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_part)";

    public static final String GET_BCS_ETL_CORE_ACC_PROD_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_accountable_product_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_accountable_product_transform_file_history_part)";


    public static final String GET_BCS_ETL_CORE_MANIF_CURR_COUNT =
               "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_current_v";

    public static final String GET_BCS_ETL_CORE_MANIF_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_part where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_part) ";

    public static final String GET_BCS_ETL_CORE_MANIF_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_transform_file_history_part) ";


     public static final String GET_BCS_ETL_CORE_PERSON_CURR_COUNT =
               "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_person_current_v";

    public static final String GET_BCS_ETL_CORE_PERSON_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_part where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_part) ";

    public static final String GET_BCS_ETL_CORE_PERSON_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_person_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_person_transform_file_history_part) ";

    public static final String GET_BCS_ETL_CORE_PRODUCT_CURR_COUNT =
               "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_product_current_v";

    public static final String GET_BCS_ETL_CORE_PRODUCT_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_part where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_part) ";

    public static final String GET_BCS_ETL_CORE_PRODUCT_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_product_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_product_transform_file_history_part) ";

    public static final String GET_BCS_ETL_CORE_WRK_PERS_CURR_COUNT =
               "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_current_v";

    public static final String GET_BCS_ETL_CORE_WRK_PERS_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_part where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_part) ";

    public static final String GET_BCS_ETL_CORE_WRK_PERS_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_transform_file_history_part) ";



    public static final String GET_BCS_ETL_CORE_WRK_RELT_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_relationship_current_v";

    public static final String GET_BCS_ETL_CORE_WRK_RELT_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_part where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_part) ";
    public static final String GET_BCS_ETL_CORE_WRK_RELT_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_relationship_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_relationship_transform_file_history_part) ";


    public static final String GET_BCS_ETL_CORE_WRK_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_current_v";
    public static final String GET_BCS_ETL_CORE_WRK_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_part where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_part) ";
    public static final String GET_BCS_ETL_CORE_WRK_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_transform_file_history_part) ";


    public static final String GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_identifier_current_v";

    public static final String GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_part where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_part) ";
    public static final String GET_BCS_ETL_CORE_WRK_IDENTIF_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_identifier_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_identifier_transform_file_history_part) ";


    public static final String GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_identifier_current_v";

    public static final String GET_LEAD_INDICATOR_MANIF_IDENTIF_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_identifier_current_v where lead_indicator=true \n";


    public static final String GET_BCS_ETL_CORE_MANIF_STATUSES_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".all_manifestation_statuses_v";


    public static final String GET_BCS_ETL_CORE_MANIF_PUBDATES_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".all_manifestation_pubdates_v";


    public static final String GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part where delete_flag=false and " +
                    "transform_ts = (select max(transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part) ";
    public static final String GET_BCS_ETL_CORE_MANIF_IDENTIF_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part) ";


    public static final String GET_MANIF_PUBDATES_INBOUND_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "select \n" +
                    "a.sourceref, \n" +
                    "versionfamily.workmasterprojectno, \n" +
                    "min(publishedondate) min_actual_pubdate, \n" +
                    "min(pubdateplanneddate) min_planned_pubdate\n" +
                    "from (\n" +
                    "    select content.ownership\n" +
                    "         , content.sourceref\n" +
                    "         , cast(date_parse(nullif(product.publishedon, ''), '%d-%b-%Y') as date) publishedondate\n" +
                    "         , cast(date_parse(nullif(product.pubdateplanned, ''), '%d-%b-%Y') as date) pubdateplanneddate\n" +
                    "    from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product product\n" +
                    "    inner join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content content on product.sourceref = content.sourceref\n" +
                    "    union all\n" +
                    "    select location.warehouse\n" +
                    "         , location.sourceref\n" +
                    "         , cast(date_parse(nullif(location.pubdateactual, ''), '%d-%b-%Y') as date) publishedondate\n" +
                    "         , cast(date_parse(nullif(location.plannedpubdate, ''), '%d-%b-%Y') as date) pubdateplanneddate\n" +
                    "    from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_sublocation location\n" +
                    "    ) a\n" +
                    "inner join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily versionfamily on a.sourceref = versionfamily.sourceref \n" +
                    "and nullif(versionfamily.workmasterprojectno,'')is not null\n" +
                    "group by a.sourceref, versionfamily.workmasterprojectno) \n" ;

    public static final String GET_MANIF_STATUSES_INBOUND_COUNT =
           "select count(*) as Source_Count from(\n" +
                   "select\n" +
                   "   a.sourceref,\n" +
                   "   versionfamily.workmasterprojectno,\n" +
                   "   coalesce(nullif(min(div_class_manifestation_priority), 6),\n" +
                   "            nullif(min(ref_key_manifestation_priority), 6),\n" +
                   "            nullif(min(delivery_status_manifestation_priority), 6),\n" +
                   "            nullif(min(delta_status_manifestation_priority), 6), 6) manifestation_priority,\n" +
                   "   coalesce(nullif(min(div_class_product_priority), 6),\n" +
                   "            nullif(min(ref_key_product_priority), 6),\n" +
                   "            nullif(min(delivery_status_product_priority), 6),\n" +
                   "            nullif(min(delta_status_product_priority), 6), 6) product_priority,\n" +
                   "   min(div_class_product_priority) div_class_product_priority,\n" +
                   "   min(ref_key_product_priority) ref_key_product_priority,\n" +
                   "   min(delivery_status_product_priority) delivery_status_product_priority,\n" +
                   "   min(delta_status_product_priority) delta_status_product_priority,\n" +
                   "   min(div_class_product_priority) div_class_manifestation_priority,\n" +
                   "   min(ref_key_manifestation_priority) ref_key_manifestation_priority,\n" +
                   "   min(delivery_status_manifestation_priority) delivery_status_manifestation_priority,\n" +
                   "   min(delta_status_manifestation_priority) delta_status_manifestation_priority\n" +
                   "from\n" +
                   "   (\n" +
                   "      select\n" +
                   "         sourceref,\n" +
                   "         product_priority div_class_product_priority,\n" +
                   "         manifestation_priority div_class_manifestation_priority,\n" +
                   "         cast(null as integer) ref_key_product_priority,\n" +
                   "         cast(null as integer) ref_key_manifestation_priority,\n" +
                   "         cast(null as integer) delivery_status_product_priority,\n" +
                   "         cast(null as integer) delivery_status_manifestation_priority,\n" +
                   "         cast(null as integer) delta_status_product_priority,\n" +
                   "         cast(null as integer) delta_status_manifestation_priority\n" +
                   "      from\n" +
                   "         "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification\n" +
                   "         inner join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".statuscode\n" +
                   "            on ('NLP' = ppm_code)\n" +
                   "      where\n" +
                   "         classificationcode like 'DIVTO%'\n" +
                   "         and nullif(value, '') is not null\n" +
                   "      union all\n" +
                   "      select\n" +
                   "         sourceref,\n" +
                   "         cast(null as integer) div_class_product_priority,\n" +
                   "         cast(null as integer) div_class_manifestation_priority,\n" +
                   "         min(product_priority) ref_key_product_priority,\n" +
                   "         min(manifestation_priority) ref_key_manifestation_priority,\n" +
                   "         cast(null as integer) delivery_status_product_priority,\n" +
                   "         cast(null as integer) delivery_status_manifestation_priority,\n" +
                   "         cast(null as integer) delta_status_product_priority,\n" +
                   "         cast(null as integer) delta_status_manifestation_priority\n" +
                   "      from\n" +
                   "         (\n" +
                   "            select\n" +
                   "               sourceref,\n" +
                   "               split_part(refkey, ' | ', 1) refkey,\n" +
                   "               product_priority,\n" +
                   "               manifestation_priority\n" +
                   "            from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product\n" +
                   "                 left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".statuscode\n" +
                   "                    on (split_part(refkey, ' | ', 1) = ppm_code)\n" +
                   "            union\n" +
                   "            select\n" +
                   "               sourceref,\n" +
                   "               split_part(refkey, ' | ', 1) refkey,\n" +
                   "               product_priority,\n" +
                   "               manifestation_priority\n" +
                   "            from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_sublocation\n" +
                   "                 left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".statuscode\n" +
                   "                    on (split_part(refkey, ' | ', 1) = ppm_code)\n" +
                   "         )\n" +
                   "      group by sourceref\n" +
                   "      union all\n" +
                   "      select\n" +
                   "         sourceref,\n" +
                   "         cast(null as integer) div_class_product_priority,\n" +
                   "         cast(null as integer) div_class_manifestation_priority,\n" +
                   "         cast(null as integer) ref_key_product_priority,\n" +
                   "         cast(null as integer) ref_key_manifestation_priority,\n" +
                   "         min(product_priority) delivery_status_product_priority,\n" +
                   "         min(manifestation_priority) delivery_status_manifestation_priority,\n" +
                   "         cast(null as integer) delta_status_product_priority,\n" +
                   "         cast(null as integer) delta_status_manifestation_priority\n" +
                   "      from\n" +
                   "         (\n" +
                   "            select\n" +
                   "               sourceref,\n" +
                   "               split_part(deliverystatus, ' | ', 1) delivery_status,\n" +
                   "               product_priority,\n" +
                   "               manifestation_priority\n" +
                   "            from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product\n" +
                   "                 left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".statuscode\n" +
                   "                    on (split_part(deliverystatus, ' | ', 1) = ppm_code)\n" +
                   "            union\n" +
                   "            select\n" +
                   "               sourceref,\n" +
                   "               split_part(status, ' | ', 1) delivery_status,\n" +
                   "               product_priority,\n" +
                   "               manifestation_priority\n" +
                   "            from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_sublocation\n" +
                   "                 left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".statuscode\n" +
                   "                    on (split_part(status, ' | ', 1) = ppm_code)\n" +
                   "            where\n" +
                   "               not (refkey = '' and status = 'IPL | Planned')\n" +
                   "         )\n" +
                   "      group by sourceref\n" +
                   "      union all\n" +
                   "      select\n" +
                   "         sourceref,\n" +
                   "         cast(null as integer) div_class_product_priority,\n" +
                   "         cast(null as integer) div_class_manifestation_priority,\n" +
                   "         cast(null as integer) ref_key_product_priority,\n" +
                   "         cast(null as integer) ref_key_manifestation_priority,\n" +
                   "         cast(null as integer) delivery_status_product_priority,\n" +
                   "         cast(null as integer) delivery_status_manifestation_priority,\n" +
                   "         min(product_priority) delta_status_product_priority,\n" +
                   "         min(manifestation_priority) delta_status_manifestation_priority\n" +
                   "      from\n" +
                   "         (\n" +
                   "            select\n" +
                   "               sourceref,\n" +
                   "               split_part(value, ' | ', 1) delta_status,\n" +
                   "               product_priority,\n" +
                   "               manifestation_priority\n" +
                   "            from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification\n" +
                   "                 left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".statuscode\n" +
                   "                    on (split_part(value, ' | ', 1) = ppm_code)\n" +
                   "            where\n" +
                   "               split_part(classificationcode, ' | ', 1) in ('DCADA', 'DCAADAUS', 'DCAANZ')\n" +
                   "         )\n" +
                   "      group by sourceref\n" +
                   "   ) a\n" +
                   "   inner join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily versionfamily\n" +
                   "      on (a.sourceref = versionfamily.sourceref\n" +
                   "      and nullif(versionfamily.workmasterprojectno, '') is not null)\n" +
                   "group by a.sourceref, versionfamily.workmasterprojectno\n" +
                   ")";

    public static final String GET_MANIF_IDENTIF_INBOUND_CURRENT_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT A.*\n" +
                    ", sourceref||identifier||identifier_type as u_key FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(p.sourceref,'') sourceref\n" +
                    "   , NULLIF(isbn13,'') identifier\n" +
                    "   ,'ISBN' identifier_type\n" +
                    "   , case when v.sourceref is null then null else true end as lead_indicator\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product p\n" +
                    "   LEFT OUTER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily v on (p.sourceref = v.sourceref and p.sourceref = v.workmasterprojectno)\n" +
                    "   WHERE (isbn13 <> '')\n" +
                    ")A WHERE A.sourceref is not null and A.identifier is not null)";

    public static final String GET_LEAD_INDICATOR_INBOUND_CURRENT_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT A.*\n" +
                    ", sourceref||identifier||identifier_type as u_key FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(p.sourceref,'') sourceref\n" +
                    "   , NULLIF(isbn13,'') identifier\n" +
                    "   ,'ISBN' identifier_type\n" +
                    "   , case when v.sourceref is null then null else true end as lead_indicator\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product p\n" +
                    "   LEFT OUTER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily v on (p.sourceref = v.sourceref and p.sourceref = v.workmasterprojectno)\n" +
                    "   WHERE (isbn13 <> '')\n" +
                    ")A WHERE A.sourceref is not null and A.identifier is not null)where lead_indicator=true";

    public static final String GET_WRK_IDENTIF_INBOUND_CURRENT_COUNT =
            "WITH\n" +
                    "  work_id AS (\n" +
                    "   SELECT DISTINCT workmasterprojectno\n" +
                    "   FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily\n" +
                    ") \n" +
                    "select count(*) as Source_Count from( \n" +
                    "SELECT\n" +
                    "  A.*\n" +
                    ", sourceref||identifier||identifier_type as u_key\n" +
                    "FROM\n" +
                    "  (\n" +
                    "   SELECT\n" +
                    "     NULLIF(sourceref, '') sourceref\n" +
                    "   , NULLIF(piidack, '') identifier\n" +
                    "   , 'DAC-K' identifier_type\n" +
                    "   FROM\n" +
                    "     ("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content content\n" +
                    "   INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    "   WHERE (piidack <> '')\n" +
                    "UNION ALL SELECT DISTINCT\n" +
                    "     NULLIF(seriesid, '') sourceref\n" +
                    "   , NULLIF(seriesissn, '') identifier\n" +
                    "   , 'ISSN-L' identifier_type\n" +
                    " from ("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content content\n" +
                    " INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    " WHERE (seriesissn <> '')" +
                    " UNION ALL SELECT" +
                    "     NULLIF(sourceref,'') sourceref\n" +
                    "   , NULLIF(orderno,'') identifier\n" +
                    "   , 'PPM-PART' identifier_type\n" +
                    "   FROM\n" +
                    "     ("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN work_id ON (product.sourceref = work_id.workmasterprojectno))\n" +
                    "   WHERE (orderno <> '')\n" +
                    ")  A\n" +
                    "WHERE ((A.sourceref IS NOT NULL) AND (A.identifier IS NOT NULL)))";

    public static final String GET_WRK_INBOUND_CURRENT_COUNT =
        "WITH\n" +
                "  man_status AS (\n" +
                "   SELECT\n" +
                "     \"m\".\"sourceref\"\n" +
                "   , \"w\".\"seriesid\"\n" +
                "   , \"m\".\"work_priority\"\n" +
                "   FROM\n" +
                "     ("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".all_status_summary_v m\n" +
                "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content w ON (\"m\".\"sourceref\" = \"w\".\"sourceref\"))\n" +
                "   WHERE (\"w\".\"seriesid\" <> '')\n" +
                ") \n" +
                "SELECT count(*) as Source_Count\t\n" +
                "FROM\n" +
                "  (\n" +
                "   SELECT DISTINCT\n" +
                "     \"content\".\"sourceref\" \"sourceref\"\n" +
                "   , \"content\".\"sourceref\" \"u_key\"\n" +
                "   , \"content\".\"title\" \"title\"\n" +
                "   , \"content\".\"subtitle\" \"subtitle\"\n" +
                "   , \"content\".\"volumeno\" \"volumeno\"\n" +
                "   , \"content\".\"copyrightyear\" \"copyrightyear\"\n" +
                "   , \"content\".\"editionno\" \"editionno\"\n" +
                "   , pubdates.work_planned_pubdate \"planned_pubdate\"\n" +
                "   , pubdates.work_actual_pubdate \"actual_pubdate\"\n" +
                "   , \"content\".\"pmc\" \"pmc\"\n" +
                "   , COALESCE(NULLIF(\"worktypecode\".\"ephcode\", ''), 'UNK') \"work_type\"\n" +
                "   , (CASE WHEN ((\"content\".\"metadeleted\" = 'Y') OR (\"lower\"(\"content\".\"title\") LIKE '%cam testing%')) THEN 'NVW' ELSE COALESCE(NULLIF(\"all_status\".\"eph_work_status_code\", ''), 'UNK') END) \"statuscode\"\n" +
                "   , \"content\".\"imprintcode\" \"imprintcode\"\n" +
                "   , NULLIF(CAST(\"opcocode\".\"11icode\" AS varchar), '') \"te_opco\"\n" +
                "   , NULLIF(CAST(\"opcocode\".\"r12code\" AS varchar), '') \"opco\"\n" +
                "   , NULLIF(CAST(COALESCE(\"rc_pmg_co\".\"rc\", \"rc_pmg\".\"rc\") AS varchar), '') \"resp_centre\"\n" +
                "   , \"content\".\"pmg\" \"pmg\"\n" +
                "   , NULLIF(\"languagecode\".\"ephcode\", '') \"languagecode\"\n" +
                "   , (CASE WHEN (erights.value = 'Y | Yes') THEN true WHEN (erights.value = 'N | No') THEN false ELSE null END) \"electro_rights_indicator\"\n" +
                "   , 'N' \"f_oa_journal_type\"\n" +
                "   , CAST(null AS varchar) \"f_society_ownership\"\n" +
                "   , CAST(null AS varchar) \"subscription_type\"\n" +
                "   , \"date_parse\"(NULLIF(content.\"metamodifiedon\", ''), '%d-%b-%Y %H:%i:%s') \"modifiedon\"\n" +
                "   FROM\n" +
                "     ((((((((((((\n" +
                "      SELECT\n" +
                "        NULLIF(\"sourceref\", '') \"sourceref\"\n" +
                "      , NULLIF(\"sourceref\", '') \"u_key\"\n" +
                "      , NULLIF(\"title\", '') \"title\"\n" +
                "      , NULLIF(\"subtitle\", '') \"subtitle\"\n" +
                "      , NULLIF(\"volumeno\", '') \"volumeno\"\n" +
                "      , \"metadeleted\" \"metadeleted\"\n" +
                "      , \"copyrightyear\" \"copyrightyear\"\n" +
                "      , \"editionno\" \"editionno\"\n" +
                "      , \"substring\"(\"split_part\"(NULLIF(\"subgroup\", ''), ' | ', 1), 2, 3) \"pmc\"\n" +
                "      , \"split_part\"(NULLIF(\"imprint\", ''), ' | ', 1) \"imprintcode\"\n" +
                "      , \"substring\"(\"split_part\"(NULLIF(\"division\", ''), ' | ', 1), 2, 3) \"pmg\"\n" +
                "      , \"date_parse\"(NULLIF(\"metamodifiedon\", ''), '%d-%b-%Y %H:%i:%s') \"modifiedon\"\n" +
                "      , \"split_part\"(\"language\", ' | ', 1) \"language\"\n" +
                "      , \"ownership\" \"ownership\"\n" +
                "      , \"metamodifiedon\" \"metamodifiedon\"\n" +
                "      FROM\n" +
                "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content\n" +
                "   )  content\n" +
                "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product product ON (content.sourceref = product.sourceref))\n" +
                "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily versionfamily ON (\"content\".\"sourceref\" = \"versionfamily\".\"workmasterprojectno\"))\n" +
                "   LEFT JOIN (\n" +
                "      SELECT\n" +
                "        workmasterprojectno\n" +
                "      , \"min\"(min_planned_pubdate) work_planned_pubdate\n" +
                "      , \"min\"(min_actual_pubdate) work_actual_pubdate\n" +
                "      FROM\n" +
                "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".all_manifestation_pubdates_v\n" +
                "      GROUP BY workmasterprojectno\n" +
                "   )  pubdates ON (product.sourceref = pubdates.workmasterprojectno))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification classification ON ((\"content\".\"sourceref\" = \"classification\".\"sourceref\") AND (\"classification\".\"classificationcode\" = 'DCDFAC | Accounting class')))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification erights ON ((\"content\".\"sourceref\" = \"erights\".\"sourceref\") AND (\"erights\".\"classificationcode\" = 'PAERIGHTS | Electronic rights')))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".all_status_summary_v \"all_status\" ON (\"all_status\".\"sourceref\" = \"content\".\"sourceref\"))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".opcocode opcocode ON (\"content\".\"ownership\" = \"opcocode\".\"ppmcode\"))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".pmgtorcmapping rc_pmg_co ON (((opcocode.\"11icode\" = rc_pmg_co.\"company\") AND (CAST(\"content\".\"pmg\" AS integer) = CAST(rc_pmg_co.\"pmg\" AS integer))) AND (rc_pmg_co.active_end_date IS NULL)))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".pmgtorcmapping rc_pmg ON (((NULLIF(rc_pmg.\"company\", '') IS NULL) AND (CAST(\"content\".\"pmg\" AS integer) = CAST(rc_pmg.\"pmg\" AS integer))) AND (rc_pmg.active_end_date IS NULL)))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".languagecode languagecode ON (\"content\".\"language\" = \"languagecode\".\"ppmcode\"))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".worktypecode worktypecode ON (COALESCE((CASE WHEN (\"split_part\"(product.versiontype, ' | ', 1) IN ('COMB', 'MVOL', 'NBS')) THEN \"split_part\"(product.versiontype, ' | ', 1) ELSE \"split_part\"(\"classification\".\"value\", ' | ', 1) END), 'DEFAULT') = \"worktypecode\".\"ppmcode\"))\n" +
                "UNION ALL    SELECT DISTINCT\n" +
                "     \"content\".\"seriesid\" \"sourceref\"\n" +
                "   , \"content\".\"seriesid\" \"u_key\"\n" +
                "   , \"content\".\"title\" \"title\"\n" +
                "   , CAST(null AS varchar) \"subtitle\"\n" +
                "   , CAST(null AS varchar) \"volumeno\"\n" +
                "   , CAST(null AS integer) \"copyrightyear\"\n" +
                "   , CAST(null AS integer) \"editionno\"\n" +
                "   , CAST(null AS date) \"planned_pubdate\"\n" +
                "   , CAST(null AS date) \"actual_pubdate\"\n" +
                "   , CAST(null AS varchar) \"pmc\"\n" +
                "   , 'BKS' \"work_type\"\n" +
                "   , (CASE WHEN (\"lower\"(\"content\".\"title\") LIKE '%cam testing%') THEN 'NVW' ELSE COALESCE(NULLIF(\"work_status\".\"eph_work_status_code\", ''), 'UNK') END) \"statuscode\"\n" +
                "   , CAST(null AS varchar) \"imprintcode\"\n" +
                "   , CAST(null AS varchar) \"te_opco\"\n" +
                "   , CAST(null AS varchar) \"opco\"\n" +
                "   , CAST(null AS varchar) \"resp_centre\"\n" +
                "   , CAST(null AS varchar) \"pmg\"\n" +
                "   , CAST(null AS varchar) \"languagecode\"\n" +
                "   , CAST(null AS boolean) \"electro_rights_indicator\"\n" +
                "   , 'N' \"f_oa_journal_type\"\n" +
                "   , CAST(null AS varchar) \"f_society_ownership\"\n" +
                "   , CAST(null AS varchar) \"subscription_type\"\n" +
                "   , \"modifiedon\" \"modifiedon\"\n" +
                "   FROM\n" +
                "     (((\n" +
                "      SELECT\n" +
                "        \"seriesid\" \"seriesid\"\n" +
                "      , \"series\" \"title\"\n" +
                "      , \"max\"(\"date_parse\"(NULLIF(\"metamodifiedon\", ''), '%d-%b-%Y %H:%i:%s')) \"modifiedon\"\n" +
                "      FROM\n" +
                "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content\n" +
                "      GROUP BY \"seriesid\", \"series\"\n" +
                "   )  content\n" +
                "   LEFT JOIN (\n" +
                "      SELECT\n" +
                "        \"man_status\".\"seriesid\" \"seriesid\"\n" +
                "      , \"min\"(\"man_status\".\"work_priority\") \"work_priority\"\n" +
                "      FROM\n" +
                "        \"man_status\"\n" +
                "      GROUP BY \"man_status\".\"seriesid\"\n" +
                "   )  work_priority ON (\"content\".\"seriesid\" = \"work_priority\".\"seriesid\"))\n" +
                "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".workstatusmapping work_status ON (\"work_priority\".\"work_priority\" = \"work_status\".\"work_priority\"))\n" +
                ")  \"A\"\n" +
                "WHERE (\"A\".\"sourceref\" IS NOT NULL)\n";

    public static final String GET_WRK_RELT_INBOUND_CURRENT_COUNT =
            "select count(*) as Source_Count FROM (\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  (\n" +
                    "   WITH\n" +
                    "     works AS (\n" +
                    "      SELECT DISTINCT\n" +
                    "        c.sourceref\n" +
                    "      , c.editionno\n" +
                    "      FROM\n" +
                    "        ("+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content c\n" +
                    "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily f ON ((c.sourceref = f.sourceref) AND (c.sourceref = f.workmasterprojectno)))\n" +
                    "   ) \n" +
                    ",    diffed_edition AS (\n" +
                    "      SELECT\n" +
                    "        w1.sourceref work_1_sourceref\n" +
                    "      , w1.editionno work_1_edition\n" +
                    "      , w2.sourceref work_2_sourceref\n" +
                    "      , w2.editionno work_2_edition\n" +
                    "      , (w1.editionno - w2.editionno) editiondiff\n" +
                    "      , \"abs\"((w1.editionno - w2.editionno)) abs_diff\n" +
                    "      , f.metamodifiedon\n" +
                    "      FROM\n" +
                    "        ((works w1\n" +
                    "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_fullversionfamily f ON (w1.sourceref = f.sourceref))\n" +
                    "      INNER JOIN works w2 ON (f.projectno = w2.sourceref))\n" +
                    "      WHERE (f.sourceref <> f.projectno)\n" +
                    "   ) \n" +
                    ",    min_diff AS (\n" +
                    "      SELECT\n" +
                    "        work_1_sourceref\n" +
                    "      , \"min\"(abs_diff) min_diff\n" +
                    "      FROM\n" +
                    "        diffed_edition\n" +
                    "      GROUP BY work_1_sourceref\n" +
                    "   ) \n" +
                    "   SELECT DISTINCT\n" +
                    "     NULLIF(\"concat\"(\"concat\"(CAST(parent.workmasterprojectno AS varchar), \"split_part\"(relations.\"relationtype\", ' | ', 1)), CAST(child.workmasterprojectno AS varchar)), '') \"u_key\"\n" +
                    "   , NULLIF(parent.workmasterprojectno, '') \"parentref\"\n" +
                    "   , NULLIF(child.workmasterprojectno, '') \"childref\"\n" +
                    "   , NULLIF(code.\"ephcode\", '') \"relationtyperef\"\n" +
                    "   , \"date_parse\"(NULLIF(\"max\"(relations.\"metamodifiedon\"), ''), '%d-%b-%Y %H:%i:%s') \"modifiedon\"\n" +
                    "   , 'N' \"dq_err\"\n" +
                    "   FROM\n" +
                    "     ((("+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_relations relations\n" +
                    "   INNER JOIN "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".relationtypecode code ON (\"split_part\"(relations.\"relationtype\", ' | ', 1) = code.\"ppmcode\"))\n" +
                    "   INNER JOIN "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily parent ON ((relations.sourceref = parent.sourceref) AND (parent.workmasterprojectno IS NOT NULL)))\n" +
                    "   INNER JOIN "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily child ON ((relations.projectno = child.sourceref) AND (child.workmasterprojectno IS NOT NULL)))\n" +
                    "   GROUP BY NULLIF(\"concat\"(\"concat\"(CAST(parent.workmasterprojectno AS varchar), \"split_part\"(relations.\"relationtype\", ' | ', 1)), CAST(child.workmasterprojectno AS varchar)), ''), NULLIF(parent.workmasterprojectno, ''), NULLIF(child.workmasterprojectno, ''), NULLIF(code.\"ephcode\", '')\n" +
                    "UNION ALL    SELECT DISTINCT\n" +
                    "     \"concat\"(CAST(content.seriesid AS varchar), 'CON', CAST(content.sourceref AS varchar)) u_key\n" +
                    "   , content.seriesid parentref\n" +
                    "   , content.sourceref childref\n" +
                    "   , 'CON' relationtyperef\n" +
                    "   , \"date_parse\"(NULLIF(content.\"metamodifiedon\", ''), '%d-%b-%Y %H:%i:%s') \"modifiedon\"\n" +
                    "   , 'N' \"dq_err\"\n" +
                    "   FROM\n" +
                    "     ("+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content content\n" +
                    "   INNER JOIN "+GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily family ON ((content.sourceref = family.sourceref) AND (content.sourceref = family.workmasterprojectno)))\n" +
                    "UNION    SELECT\n" +
                    "     NULLIF(\"concat\"(\"concat\"(CAST(d.work_2_sourceref AS varchar), 'IRB'), CAST(d.work_1_sourceref AS varchar)), '') \"u_key\"\n" +
                    "   , d.work_2_sourceref parentref\n" +
                    "   , d.work_1_sourceref childref\n" +
                    "   , 'EDI' relationtyperef\n" +
                    "   , \"date_parse\"(NULLIF(d.\"metamodifiedon\", ''), '%d-%b-%Y %H:%i:%s') \"modifiedon\"\n" +
                    "   , 'N' \"dq_err\"\n" +
                    "   FROM\n" +
                    "     (diffed_edition d\n" +
                    "   INNER JOIN min_diff m ON (((d.work_1_sourceref = m.work_1_sourceref) AND (d.abs_diff = m.min_diff)) AND (editiondiff > 0)))\n" +
                    ")  A\n" +
                    "WHERE (((((\"A\".\"parentref\" IS NOT NULL) AND (\"A\".\"parentref\" <> '')) AND (\"A\".\"childref\" IS NOT NULL)) AND (\"A\".\"relationtyperef\" IS NOT NULL)) AND (\"A\".\"parentref\" <> \"A\".\"childref\"))\n" +
                    ")\n";

    public static final String GET_WRK_PERSON_INBOUND_CURRENT_COUNT =
            "SELECT count(*) as Source_Count\n" +
                    " FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') worksourceref\n" +
                    "   , NULLIF(CAST(businesspartnerid AS varchar),'') personsourceref\n" +
                    "   , NULLIF(CAST(old_businesspartnerid AS varchar),'') linking_id\n" +
                    "   , NULLIF(rolecode.ephcode,'') roletype\n" +
                    "   , NULLIF(sourceref,'')||NULLIF(rolecode.ephcode,'')||NULLIF(CAST(businesspartnerid AS varchar),'') as u_key\n" +
                    "   , NULLIF(CAST(sequence AS varchar),'') sequence\n" +
                    "   , NULLIF(CAST(locationid AS varchar),'') deduplicator\n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon\n" +
                    "   , 'N' dq_err\n" +
                    "FROM\n" +
                    "((SELECT sourceref,\n" +
                   " lower(to_hex(md5(to_utf8(concat(cast(businesspartnerid as varchar)" +
                    ",TRIM(UPPER((CASE WHEN (isperson = 'N') THEN department ELSE firstname END)))" +
                    ",TRIM(UPPER((CASE WHEN (isperson = 'N') THEN institution ELSE lastname END)))))))) businesspartnerid, \n" +
                    "businesspartnerid old_businesspartnerid,\n" +
                    "copyrightholdertype,\n" +
                    "sequence,\n" +
                    "metamodifiedon,\n" +
                    "locationid,\n" +
                    "row_number()\n" +
                    "OVER (partition by sourceref,businesspartnerid,copyrightholdertype\n" +
                    "ORDER BY metamodifiedon,sequence) min_id\n" +
                    "FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_originators)\n" +
                    "INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode))\n" +
                    "where min_id = 1\n" +
                    "UNION \n" +
                    "SELECT\n" +
                    "     NULLIF(r.sourceref,'') worksourceref\n" +
                    "   , NULLIF(lower(to_hex(md5(to_utf8(w.peoplehub_id)))),'') personsourceref \n" +
                    "   , NULLIF(w.peoplehub_id,'') linking_id \n" +
                    "   , NULLIF(rolecode.ephcode,'') roletype\n" +
                    "   , r.sourceref||rolecode.ephcode||lower(to_hex(md5(to_utf8(w.peoplehub_id)))) as u_key \n" +
                    "   , (CASE WHEN (substr(split_part(NULLIF(responsibility,''), ' | ', 1), -1, 1) = '2') THEN '2' ELSE '1' END) sequence\n" +
                    "   , '0' deduplicator\n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon\n" +
                    "   , 'N' dq_err\n" +
                    "   FROM\n" +
                    "     ("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_responsibilities r\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getDlCoreViewDataBase()+".workday_reference_v w on r.email = w.email \n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".rolecode ON (split_part(responsibility, ' | ', 1) = rolecode.ppmcode)) \n" +
                    ") A\n" +
                    "INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily vf on a.worksourceref = vf.workmasterprojectno and a.worksourceref = vf.sourceref\n" +
                    "WHERE (((A.worksourceref IS NOT NULL) AND (A.personsourceref IS NOT NULL)) AND (A.roletype IS NOT NULL)) \n";


       public static final String GET_PRODUCT_INBOUND_CURRENT_COUNT =
             "SELECT count(*) as Source_Count\n" +
                     "FROM\n" +
                     "  (\n" +
                     "   SELECT DISTINCT\n" +
                     "     NULLIF(product.sourceref, '') sourceref\n" +
                     "   , split_part(NULLIF(product.binding, ''), ' | ', 1) bindingcode\n" +
                     "   , COALESCE(product.sourceref, '') u_key\n" +
                     "   , concat(COALESCE(content.title, ''), ' Purchase') name\n" +
                     "   , NULLIF(content.shorttitle, '') shorttitle\n" +
                     "   , CAST(date_parse((CASE WHEN ((publishedon = '') AND (pubdateplanned = '')) THEN CAST(null AS varchar) ELSE (CASE WHEN (publishedon = '') THEN pubdateplanned ELSE publishedon END) END), '%d-%b-%Y') AS date) launchdate\n" +
                     "   , NULLIF(taxcode.ephcode, '') taxcode\n" +
                     "   , (CASE WHEN ((product.metadeleted = 'Y') OR (lower(content.title) LIKE '%cam testing%')) THEN 'NVP' ELSE COALESCE(NULLIF(all_status.eph_product_status_code, ''), 'UNK') END) status\n" +
                     "   , NULLIF(product.sourceref, '') manifestationref\n" +
                     "   , NULLIF(workcontent.sourceref, '') worksource\n" +
                     "   , NULLIF(workcontent.work_type, '') work_type\n" +
                     "   , (CASE WHEN (separately_sale.sourceref IS NULL) THEN true ELSE false END) separately_sale_indicator\n" +
                     "   , CAST(null AS boolean) trial_allowed_indicator\n" +
                     "   , (CASE WHEN (sales_rest.sourceref IS NULL) THEN false ELSE true END) restricted_sale_indicator\n" +
                     "   , CAST(null AS varchar) f_work_source_ref\n" +
                     "   , 'OOA' product_type\n" +
                     "   , 'ONE' f_revenue_model\n" +
                     "   , 'N' dq_err\n" +
                     "   FROM\n" +
                     "     (((((((("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product product\n" +
                     "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily versionfamily ON ((product.sourceref = versionfamily.sourceref) AND (versionfamily.workmasterprojectno IS NOT NULL)))\n" +
                     "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content content ON (versionfamily.workmasterprojectno = content.sourceref))\n" +
                     "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_current_v workcontent ON (versionfamily.workmasterprojectno = workcontent.sourceref))\n" +
                     "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification classification ON ((product.sourceref = classification.sourceref) AND (classificationcode LIKE 'DCDFC1%%')))\n" +
                     "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".taxcode ON (split_part(classification.value, ' | ', 1) = taxcode.ppmcode))\n" +
                     "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".all_status_summary_v all_status ON (product.sourceref = all_status.sourceref))\n" +
                     "   LEFT JOIN (\n" +
                     "      SELECT DISTINCT sourceref\n" +
                     "      FROM\n" +
                     "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_sublocation\n" +
                     "      WHERE (refkey IN ('IPR | IPR - In Preparation (Secret)', 'NR | NR - No rights', 'NRG | NRG - No sales rights', 'CSR | CSR - CS Research', 'AVD | AVD - POD Only from Amazon'))\n" +
                     "UNION       SELECT DISTINCT sourceref\n" +
                     "      FROM\n" +
                     "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product\n" +
                     "      WHERE (refkey IN ('IPR | IPR - In Preparation (Secret)', 'NR | NR - No rights', 'NRG | NRG - No sales rights', 'CSR | CSR - CS Research', 'AVD | AVD - POD Only from Amazon'))\n" +
                     "UNION       SELECT DISTINCT sourceref\n" +
                     "      FROM\n" +
                     "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification\n" +
                     "      WHERE (classificationcode LIKE 'MANOTAVA%')\n" +
                     "   )  sales_rest ON (product.sourceref = sales_rest.sourceref))\n" +
                     "   LEFT JOIN (\n" +
                     "      SELECT DISTINCT sourceref\n" +
                     "      FROM\n" +
                     "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_sublocation\n" +
                     "      WHERE (refkey IN ('NSS | NSS - Not sold separately', 'NSI | NSI - Non-saleable item'))\n" +
                     "UNION       SELECT DISTINCT sourceref\n" +
                     "      FROM\n" +
                     "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product\n" +
                     "      WHERE (refkey IN ('NSS | NSS - Not sold separately', 'NSI | NSI - Non-saleable item'))\n" +
                     "UNION       SELECT sourceref\n" +
                     "      FROM\n" +
                     "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification\n" +
                     "      WHERE ((split_part(classificationcode, ' | ', 1) IN ('DCADA', 'DCAADAUS', 'DCAANZ')) AND (split_part(value, ' | ', 1) IN ('NSS', 'NSI')))\n" +
                     "   )  separately_sale ON (product.sourceref = separately_sale.sourceref))\n" +
                     ")  A\n" +
                     "WHERE (A.sourceref IS NOT NULL)\n";


       public static final String GET_PERSON_INBOUND_CURRENT_COUNT =
               "SELECT count(*) as Source_Count \n" +
                       "FROM \n" +
                       "  ( \n" +
                       "   SELECT DISTINCT \n" +
                       "     businesspartnerid sourceref \n" +
                       "   , lower (to_hex(md5(to_utf8(concat(cast(businesspartnerid as varchar)" +
                       "   ,TRIM(UPPER((CASE WHEN (isperson = 'N') THEN department ELSE firstname END)))" +
                       "   ,TRIM(UPPER((CASE WHEN (isperson = 'N') THEN institution ELSE lastname END))))))))u_key \n" +
                       "   , (CASE WHEN (isperson = 'N') THEN NULLIF(department, '') ELSE NULLIF(firstname, '') END) firstname \n" +
                       "   , (CASE WHEN (isperson = 'N') THEN NULLIF(institution, '') ELSE NULLIF(lastname, '') END) familyname \n" +
                       "   , CAST(null AS varchar) peoplehub_id \n" +
                       "   , CAST(null AS varchar) email_address \n" +
                       "   , 'N' dq_err \n" +
                       "   FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_originators \n" +
                       ")  A \n" +
                       " WHERE (A.sourceref IS NOT NULL)";


    public static final String GET_MANIF_INBOUND_CURRENT_COUNT =
            "SELECT count(*) as Source_Count\n" +
                    "FROM\n" +
                    "  (\n" +
                    "   SELECT DISTINCT\n" +
                    "     NULLIF(product.sourceref, '') sourceref\n" +
                    "   , NULLIF(product.sourceref, '') u_key\n" +
                    "   , NULLIF(content.title, '') title\n" +
                    "   , (CASE WHEN (NULLIF(intedition.classificationcode, '') IS NULL) THEN false ELSE true END) intereditionflag\n" +
                    "   , COALESCE(pubdates.min_actual_pubdate, pubdates.min_planned_pubdate) firstpublisheddate\n" +
                    "   , NULLIF(product.binding, '') binding\n" +
                    "   , NULLIF(manifestationtypecode.ephcode, '') manifestation_type\n" +
                    "   , (CASE WHEN ((product.metadeleted = 'Y') OR (lower(content.title) LIKE '%cam testing%')) THEN 'NVM' ELSE COALESCE(NULLIF(all_status.eph_manifestation_status_code, ''), 'UNK') END) status\n" +
                    "   , NULLIF(workprod.workmasterprojectno, '') work_id\n" +
                    "   , CAST(null AS timestamp) last_pub_date\n" +
                    "   , 'N' dq_err\n" +
                    "   FROM\n" +
                    "     ((((((("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_versionfamily workprod ON ((product.sourceref = workprod.sourceref) AND (workprod.workmasterprojectno IS NOT NULL)))\n" +
                    "   INNER JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_content content ON (workprod.workmasterprojectno = content.sourceref))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        sourceref\n" +
                    "      , classificationcode\n" +
                    "      FROM\n" +
                    "        "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification classification\n" +
                    "      WHERE (classificationcode LIKE 'PARELIE%')\n" +
                    "   )  intedition ON (product.sourceref = intedition.sourceref))\n" +
                    "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".all_manifestation_pubdates_v pubdates ON (product.sourceref = pubdates.sourceref))\n" +
                    "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".all_status_summary_v all_status ON (product.sourceref = all_status.sourceref))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        sourceref\n" +
                    "      , (CASE WHEN (producttype IN ('199 | E-book-199', 'VAC | VAC - Voucher Access Card', 'INK | Inkling E-Book')) THEN split_part(producttype, ' | ', 1) WHEN (binding IN ('BB | Book/Hardback', 'BC | Book/Paperback')) THEN split_part(binding, ' | ', 1) WHEN (versiontype IN ('BKH | Book - Hardback', 'BKP | Book - Paperback')) THEN split_part(versiontype, ' | ', 1) WHEN (deltabinding IN ('1 | 1 - Hardback', '2 | 2 - Paperback')) THEN split_part(deltabinding, ' | ', 1) WHEN (versiontype IN ('NBP | Non - Book Physical', 'ELP | Electronic - Physical', 'ELO | Electronic - Online')) THEN split_part(versiontype, ' | ', 1) WHEN (versiontype IN ('COMB | Combination Set', 'MVOL | Multivol. Run-on Set', 'NBS | Non-Book Set')) THEN concat(split_part(versiontype, ' | ', 1), COALESCE(split_part(medium, ' | ', 1), '')) ELSE '' END) manifestation_type\n" +
                    "      FROM\n" +
                    "        (\n" +
                    "         SELECT\n" +
                    "           p.sourceref\n" +
                    "         , p.versiontype\n" +
                    "         , p.binding\n" +
                    "         , a.value producttype\n" +
                    "         , b.value deltabinding\n" +
                    "         , p.medium\n" +
                    "         FROM\n" +
                    "           (("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_product p\n" +
                    "         LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification a ON ((p.sourceref = a.sourceref) AND (a.classificationcode LIKE 'PTCO%')))\n" +
                    "         LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification b ON ((p.sourceref = b.sourceref) AND (b.classificationcode LIKE 'PTDE%')))\n" +
                    "      ) \n" +
                    "   )  manif_type ON (product.sourceref = manif_type.sourceref))\n" +
                    "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".manifestationtypecode ON (manifestation_type = manifestationtypecode.ppmcode))\n" +
                    ")  A\n" +
                    "WHERE (A.sourceref IS NOT NULL)\n";

    public static final String GET_ACC_PROD_INBOUND_CURRENT_COUNT =
               "select count(*) as Source_Count\n" +
                       "FROM\n" +
                       "  (\n" +
                       "   SELECT DISTINCT\n" +
                       "     NULLIF(sourceref, '') sourceref\n" +
                       "   , NULLIF(CAST(accountableproduct AS varchar), '') accountableproduct\n" +
                       "   , NULLIF(accountablename, '') accountablename\n" +
                       "   , NULLIF(accountableparent, '') accountableparent\n" +
                       "   , concat(NULLIF(sourceref, ''), NULLIF(accountableparent, '')) u_key \n" +
                       "   , 'N' dq_err\n" +
                       "   FROM\n" +
                       "     ("+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".stg_current_classification classification\n" +
                       "   LEFT JOIN "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".worktypecode ON (COALESCE(split_part(classification.value, ' | ', 1), 'DEFAULT') = ppmcode))\n" +
                       "   WHERE (classification.classificationcode = 'DCDFAC | Accounting class')\n" +
                       ")  A\n" +
                       "WHERE ((A.sourceref IS NOT NULL) AND (A.accountableparent IS NOT NULL))";



    public static final String GET_ACC_PROD_DELTA_CURR_COUNT =
               "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_accountable_product";

    public static final String GET_MANIF_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_manifestation";

    public static final String GET_PERSON_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_person";

    public static final String GET_PRODUCT_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_product";

    public static final String GET_WORK_PERSON_ROLE_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_person_role";

    public static final String GET_WORK_RELATION_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_relationship";

    public static final String GET_WORK_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work";

    public static final String GET_WORK_IDENTIF_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_identifier";

    public static final String GET_MANIF_IDENTIF_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_manifestation_identifier";

    public static final String GET_ACC_PROD_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_accountable_product_part where " +
                    "delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_accountable_product_part)";

    public static final String GET_MANIF_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_manifestation_part " +
                    "where delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_manifestation_part)";

    public static final String GET_PERSON_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_person_part where " +
                    "delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_person_part) ";

    public static final String GET_PRODUCT_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_product_part where " +
                    "delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_product_part) ";

    public static final String GET_WORK_PERSON_ROLE_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_work_person_role_part where " +
                    "delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_work_person_role_part) ";

    public static final String GET_WORK_RELATION_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_work_relationship_part where " +
                    "delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_work_relationship_part) ";

    public static final String GET_WORK_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_work_part where" +
                    " delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_work_part) ";

    public static final String GET_WORK_IDENTIF_DELTA_HIST_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_work_identifier_part where" +
                    " delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_work_identifier_part) ";

    public static final String GET_MANIF_IDENTIF_DELTA_HIST_COUNT =
            "select count(*) as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_manifestation_identifier_part " +
                    "where delta_ts = (select max(delta_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_history_manifestation_identifier_part)";


    public static final String GET_ACC_PROD_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_accountable_product d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_part c ))";


    public static final String GET_MANIF_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_manifestation d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_part c ))";

    public static final String GET_PERSON_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_person d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_part c ))";


    public static final String GET_PRODUCT_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_product d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_part c ))";

    public static final String GET_WRK_PERS_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_person_role d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_part c ))";

    public static final String GET_WRK_RELT_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_relationship d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_part c ))";

    public static final String GET_WRK_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_part c ))";

    public static final String GET_WRK_IDENTIF_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_identifier d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_part c ))";

    public static final String GET_MANIF_IDENTIF_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part c\n" +
                    "left join "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_manifestation_identifier d on c.sourceref  = d.sourceref \n" +
                    "where d.sourceref is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part c ))";

    public static final String GET_ACC_PROD_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_excl_delta";

    public static final String GET_MANIF_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_excl_delta";

    public static final String GET_PERSON_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_excl_delta";

    public static final String GET_PRODUCT_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_excl_delta";

    public static final String GET_WRK_PERS_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_excl_delta";

    public static final String GET_WRK_RELT_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_excl_delta";

    public static final String GET_WRK_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_excl_delta";

    public static final String GET_WRK_IDENTIF_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_excl_delta";

    public static final String GET_MANIF_IDENTIF_EXCL_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_excl_delta";

    public static final String GET_ACC_PROD_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_latest";

    public static final String GET_MANIF_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_latest";

    public static final String GET_PERSON_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_latest";

    public static final String GET_PRODUCT_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_latest";

    public static final String GET_WRK_PERS_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_latest";

    public static final String GET_WRK_RELT_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_latest";

    public static final String GET_WRK_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_latest";

    public static final String GET_WRK_IDENTIF_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_latest";

    public static final String GET_MANIF_IDENTIF_LATEST_COUNT =
            "select count(*)as Target_Count from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest";


    public static final String GET_ACC_PROD_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_excl_delta as c union all \n" +
                    "select d.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_accountable_product as d)";

    public static final String GET_MANIF_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_excl_delta as c union all \n" +
                    "select d.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_manifestation as d)";

    public static final String GET_PERSON_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_excl_delta\n" +
                    "as c union all \n" +
                    "select d.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_person as d)";

    public static final String GET_PRODUCT_SUM_DELTACURR_EXCL_COUNT=
           "select count(*) as source_count from \n" +
                   "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_excl_delta\n" +
                   "as c union all \n" +
                   "select d.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_product as d)";

    public static final String GET_WRK_PERS_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_excl_delta as c \n" +
                    "union all \n" +
                    "select d.u_key from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_person_role as d)";

    public static final String GET_WRK_RELT_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_excl_delta\n" +
                    "as c union all \n" +
                    "select d.u_key from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_relationship as d)";

    public static final String GET_WRK_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_excl_delta\n" +
                    "as c union all \n" +
                    "select d.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work as d)";

    public static final String GET_WRK_IDENTIF_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_excl_delta as c union all \n" +
                    "select d.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_work_identifier as d)";


    public static final String GET_MANIF_IDENTIF_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_excl_delta\n" +
                    "as c union all \n" +
                    "select d.sourceref from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_delta_current_manifestation_identifier as d)";


    public static final String GET_DUPLICATES_LATEST_ACC_PROD_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_accountable_product_latest " +
                    " group by u_key having count(*)>1)";


    public static final String GET_DUPLICATES_LATEST_MANIF_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_latest " +
                    " group by sourceref having count(*)>1)";


    public static final String GET_DUPLICATES_LATEST_PROD_COUNT =
            "select count(*) as Duplicate_Count from " +
                    "(SELECT count(*) FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_product_latest " +
                    " group by u_key having count(*)>1)";

    public static final String GET_DUPLICATES_LATEST_PERSON_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_person_latest" +
                    " group by sourceref having count(*)>1)";

    public static final String  GET_DUPLICATES_LATEST_WORK_RELT_COUNT=
        "select count(*) as Duplicate_Count " +
                "from (SELECT count(*) " +
                "FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_relationship_latest " +
                " group by u_key having count(*)>1)";

    public static final String GET_DUPLICATES_LATEST_WORK_PERS_COUNT =
            "select count(*) as Duplicate_Count" +
                    " from (SELECT count(*)" +
                    " FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_person_role_latest " +
                    " group by u_key having count(*)>1)";

    public static final String GET_DUPLICATES_LATEST_WORK_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_latest" +
                    " group by sourceref having count(*)>1)";

    public static final String GET_DUPLICATES_WORK_IDENTIFIER_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_work_identifier_latest" +
                    " group by sourceref,identifier,identifier_type having count(*)>1)";

    public static final String GET_DUPLICATES_MANIF_IDENTIFIER_COUNT =
            "select count(*) as Duplicate_Count from (SELECT count(*) FROM "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest" +
                    " group by sourceref,identifier,identifier_type,lead_indicator having count(*)>1)";

    public static final String GET_PERSON_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select sourceref, u_key, firstname, familyname, peoplehub_id, email_address, dq_err\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_person_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts) \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_person_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select sourceref, u_key, firstname, familyname, peoplehub_id, email_address, dq_err\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_person_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_person_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 0) <> coalesce(prev.sourceref,0 ) or\n" +
                    "            coalesce(crr.u_key, 'na') <> coalesce(prev.u_key, 'na') or\n" +
                    "            coalesce (crr.firstname, 'na') <> coalesce (prev.firstname, 'na') or\n" +
                    "            coalesce (crr.familyname, 'na') <> coalesce (prev.familyname, 'na') or\n" +
                    "            coalesce (crr.peoplehub_id, 'na') <> coalesce (prev.peoplehub_id, 'na') or\n" +
                    "            coalesce (crr.email_address, 'na') <> coalesce (prev.email_address, 'na') or\n" +
                    "            coalesce (crr.dq_err, 'na') <> coalesce (prev.dq_err, 'na')))";

    public static final String GET_ACC_PROD_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select sourceref,accountableproduct, accountablename, accountableparent, u_key, dq_err\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_accountable_product_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_accountable_product_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select sourceref, accountableproduct, accountablename, accountableparent, u_key, dq_err\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_accountable_product_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_accountable_product_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'na') <> coalesce(prev.sourceref,'na' ) or\n" +
                    "            coalesce(crr.accountableproduct, 'na') <> coalesce(prev.accountableproduct, 'na') or\n" +
                    "            coalesce (crr.accountablename, 'na') <> coalesce (prev.accountablename, 'na') or\n" +
                    "            coalesce (crr.accountableparent, 'na') <> coalesce (prev.accountableparent, 'na') or\n" +
                    "            coalesce (crr.u_key, 'na') <> coalesce (prev.u_key, 'na') or\n" +
                    "            coalesce (crr.dq_err, 'na') <> coalesce (prev.dq_err, 'na')))";

    public static final String GET_MANIF_DIFF_TRANSFORM_FILE_COUNT =
    " with crr_dataset as(\n"+
            "  select sourceref,u_key, title, intereditionflag, firstpublisheddate, binding,manifestation_type,status,work_id,last_pub_date,dq_err\n"+
            "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_transform_file_history_part\n"+
            "  where transform_file_ts = (select max(transform_file_ts ) \n"+
            "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_transform_file_history_part)\n"+
            "  ),\n"+
            "  prev_dataset as (\n"+
            "  select sourceref,u_key, title, intereditionflag, firstpublisheddate, binding,manifestation_type,status,work_id,last_pub_date,dq_err\n"+
            "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_transform_file_history_part\n"+
            "  where transform_file_ts \n"+
            "  = (select distinct transform_file_ts from \n"+
            "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n"+
            "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_transform_file_history_part dhap)where rn = 2))                                  \n"+
            "select count(*) as source_count from( \n"+
            "    select crr.u_key as UKEY\n"+
            "    from crr_dataset crr\n"+
            "        left join prev_dataset prev on crr.u_key = prev.u_key\n"+
            "    where prev.u_key is null\n"+
            "    union\n"+
            "    select  prev.u_key as UKEY \n"+
            "    from prev_dataset prev\n"+
            "        left join crr_dataset crr on crr.u_key = prev.u_key\n"+
            "    where crr.u_key is null\n"+
            "    union\n"+
            "    select crr.u_key as UKEY\n"+
            "    from crr_dataset crr\n"+
            "        join prev_dataset prev on crr.u_key = prev.u_key\n"+
            "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n"+
            "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n"+
            "            coalesce (crr.title, 'null') <> coalesce (prev.title, 'null') or\n"+
            "            coalesce (crr.intereditionflag, false) <> coalesce (prev.intereditionflag, false) or\n"+
            "            coalesce (crr.firstpublisheddate, current_date) <> coalesce (prev.firstpublisheddate, current_date) or\n"+
            " coalesce (crr.binding, 'null') <> coalesce (prev.binding, 'null') or\n"+
            " coalesce (crr.manifestation_type, 'null') <> coalesce (prev.manifestation_type, 'null') or\n"+
            " coalesce (crr.status, 'null') <> coalesce (prev.status, 'null') or\n"+
            " coalesce (crr.work_id, 'null') <> coalesce (prev.work_id, 'null') or\n"+
            " coalesce (crr.last_pub_date, current_date) <> coalesce (prev.last_pub_date, current_date) or\n"+
            " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))";



    public static final String GET_PRODUCT_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "            select sourceref,bindingcode, u_key, name, shorttitle, launchdate,taxcode,status,manifestationref,worksource,work_type,separately_sale_indicator,trial_allowed_indicator,f_work_source_ref,product_type,f_revenue_model,dq_err\n" +
                    "            from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_product_transform_file_history_part\n" +
                    "                    where transform_file_ts = (select max(transform_file_ts )\n" +
                    "    from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_product_transform_file_history_part)\n" +
                    "            ),\n" +
                    "    prev_dataset as (\n" +
                    "            select sourceref,bindingcode, u_key, name, shorttitle, launchdate,taxcode,status,manifestationref,worksource,work_type,separately_sale_indicator,trial_allowed_indicator,f_work_source_ref,product_type,f_revenue_model,dq_err\n" +
                    "            from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_product_transform_file_history_part\n" +
                    "                    where transform_file_ts\n" +
                    "                    = (select distinct transform_file_ts from\n" +
                    "                    (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn\n" +
                    "    from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_product_transform_file_history_part dhap)where rn = 2))\n" +
                    "    select count(*) as source_count from(\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "    left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY\n" +
                    "    from prev_dataset prev\n" +
                    "    left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "    join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "    coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "    coalesce (crr.bindingcode, 'null') <> coalesce (prev.bindingcode, 'null') or\n" +
                    "    coalesce (crr.name, 'null') <> coalesce (prev.name, 'null') or\n" +
                    "    coalesce (crr.shorttitle, 'null') <> coalesce (prev.shorttitle, 'null') or\n" +
                    "    coalesce (crr.taxcode, 'null') <> coalesce (prev.taxcode, 'null') or\n" +
                    "    coalesce (crr.launchdate, current_date) <> coalesce (prev.launchdate, current_date) or\n" +
                    "    coalesce (crr.manifestationref, 'null') <> coalesce (prev.manifestationref, 'null') or\n" +
                    "    coalesce (crr.status, 'null') <> coalesce (prev.status, 'null') or\n" +
                    "    coalesce (crr.worksource, 'null') <> coalesce (prev.worksource, 'null') or\n" +
                    "    coalesce (crr.work_type, 'null') <> coalesce (prev.work_type, 'null') or\n" +
                    "    coalesce (crr.separately_sale_indicator, false) <> coalesce (prev.separately_sale_indicator, false) or\n" +
                    "    coalesce (crr.trial_allowed_indicator, false) <> coalesce (prev.trial_allowed_indicator, false) or\n" +
                    "    coalesce (crr.f_work_source_ref, 'null') <> coalesce (prev.f_work_source_ref, 'null') or\n" +
                    "    coalesce (crr.product_type, 'null') <> coalesce (prev.product_type, 'null') or\n" +
                    "    coalesce (crr.f_revenue_model, 'null') <> coalesce (prev.f_revenue_model, 'null') or\n" +
                    "    coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))";

    public static final String GET_WRK_PERS_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select worksourceref,personsourceref, roletype, u_key, sequence, deduplicator,modifiedon,dq_err\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select worksourceref,personsourceref, roletype, u_key, sequence, deduplicator,modifiedon,dq_err\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_person_role_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.worksourceref, 'null') <> coalesce(prev.worksourceref,'null' ) or\n" +
                    "            coalesce(crr.personsourceref, 'null') <> coalesce(prev.personsourceref, 'null') or\n" +
                    "            coalesce (crr.roletype, 'null') <> coalesce (prev.roletype, 'null') or\n" +
                    "            coalesce (crr.u_key, 'null') <> coalesce (prev.u_key, 'null') or\n" +
                    " coalesce (crr.sequence, 'null') <> coalesce (prev.sequence, 'null') or\n" +
                    " coalesce (crr.deduplicator, 'null') <> coalesce (prev.deduplicator, 'null') or\n" +
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))";

    public static final String GET_WRK_RELT_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select u_key,parentref, childref, relationtyperef, modifiedon, dq_err\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_relationship_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_relationship_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select u_key,parentref, childref, relationtyperef, modifiedon, dq_err\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_relationship_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_relationship_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.u_key, 'null') <> coalesce(prev.u_key,'null' ) or\n" +
                    " coalesce(crr.parentref, 'null') <> coalesce(prev.parentref, 'null') or\n" +
                    " coalesce (crr.childref, 'null') <> coalesce (prev.childref, 'null') or\n" +
                    " coalesce (crr.relationtyperef, 'null') <> coalesce (prev.relationtyperef, 'null') or\n" +
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))";

    public static final String GET_WRK_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select sourceref,u_key, title, subtitle, volumeno, copyrightyear,editionno,pmc,work_type,statuscode,imprintcode,te_opco,opco,resp_centre,pmg,languagecode,electro_rights_indicator,f_oa_journal_type,f_society_ownership,subscription_type,modifiedon\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "   select sourceref,u_key, title, subtitle, volumeno, copyrightyear,editionno,pmc,work_type,statuscode,imprintcode,te_opco,opco,resp_centre,pmg,languagecode,electro_rights_indicator,f_oa_journal_type,f_society_ownership,subscription_type,modifiedon\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "            coalesce (crr.title, 'null') <> coalesce (prev.title, 'null') or\n" +
                    "            coalesce (crr.subtitle, 'null') <> coalesce (prev.subtitle, 'null') or\n" +
                    " coalesce (crr.volumeno, 'null') <> coalesce (prev.volumeno, 'null') or\n" +
                    " coalesce (crr.copyrightyear, 0) <> coalesce (prev.copyrightyear, 0) or\n" +
                    "            coalesce (crr.editionno, 0) <> coalesce (prev.editionno, 0) or\n" +
                    " coalesce (crr.pmc, 'null') <> coalesce (prev.pmc, 'null') or\n" +
                    " coalesce (crr.work_type, 'null') <> coalesce (prev.work_type, 'null') or\n" +
                    " coalesce (crr.statuscode, 'null') <> coalesce (prev.statuscode, 'null') or\n" +
                    " coalesce (crr.imprintcode, 'null') <> coalesce (prev.imprintcode, 'null') or\n" +
                    " coalesce (crr.te_opco, 'null') <> coalesce (prev.te_opco, 'null') or\n" +
                    " coalesce (crr.opco, 'null') <> coalesce (prev.opco, 'null') or\n" +
                    " coalesce (crr.resp_centre, 'null') <> coalesce (prev.resp_centre, 'null') or\n" +
                    " coalesce (crr.pmg, 'null') <> coalesce (prev.pmg, 'null') or\n" +
                    " coalesce (crr.languagecode, 'null') <> coalesce (prev.languagecode, 'null') or\n" +
                    " coalesce (crr.electro_rights_indicator, false) <> coalesce (prev.electro_rights_indicator, false) or\n" +
                    " coalesce (crr.f_oa_journal_type, 'null') <> coalesce (prev.f_oa_journal_type, 'null') or\n" +
                    " coalesce (crr.f_society_ownership, 'null') <> coalesce (prev.f_society_ownership, 'null') or\n" +
                    " coalesce (crr.subscription_type, 'null') <> coalesce (prev.subscription_type, 'null') or\n" +
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date)))";

    public static final String GET_WRK_IDENTIF_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select sourceref,u_key, identifier, identifier_type\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_identifier_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_identifier_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "   select sourceref,u_key, identifier, identifier_type\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_identifier_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_work_identifier_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "            coalesce (crr.identifier, 'null') <> coalesce (prev.identifier, 'null') or\n" +
                    "            coalesce (crr.identifier_type, 'null') <> coalesce (prev.identifier_type, 'null')))";


    public static final String GET_MANIF_IDENTIF_DIFF_TRANSFORM_FILE_COUNT =

            " with crr_dataset as(\n" +
                    "  select sourceref,u_key, identifier, identifier_type\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "   select sourceref,u_key, identifier, identifier_type\n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+ GetBcsEtlCoreDLDBUser.getBcsETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key as UKEY \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key as UKEY\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "            coalesce (crr.identifier, 'null') <> coalesce (prev.identifier, 'null') or\n" +
                    "            coalesce (crr.identifier_type, 'null') <> coalesce (prev.identifier_type, 'null')))";


}




