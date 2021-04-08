package com.eph.automation.testing.services.db.SDRMDataLakeSQL;

import org.apache.commons.collections4.Get;

public class SDRMDataChecksSQL {


    public static String GET_RANDOM_SDRM_ISBN_INBOUND="select sku as ISBN from (SELECT\n" +
            "  \"isbn\" as ISBN\n" +
            ", \"sku\" as SKU\n" +
            ", \"title\" as TITLE\n" +
            ", 'Mobi' \"rendition_format\"\n" +
            ", \"inbound_ts\" as INBOUND_TS\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as PRODUCTION_DATE\n" +
            ", \"crf\".\"epr\" as EPR_ID\n" +
            ", \"crf\".\"product_type\" as PRODUCT_TYPE\n" +
            ", CONCAT(\"sku\",'Mobi') as U_KEY\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN' AND mi.effective_end_date is null) ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.ismobi = '1'\n" +
            "UNION\n" +
            "SELECT\n" +
            "  \"isbn\"\n" +
            ", \"sku\"\n" +
            ", \"title\"\n" +
            ", 'ePub' \"rendition_format\"\n" +
            ", \"inbound_ts\"\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as \"production_date\"\n" +
            ", \"crf\".\"epr\" as \"epr_id\"\n" +
            ", \"crf\".\"product_type\"\n" +
            ", CONCAT(\"sku\",'ePub') as \"u_key\"\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN' AND mi.effective_end_date is null) ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.isepub = '1'\n" +
            "UNION\n" +
            "SELECT\n" +
            "  \"isbn\"\n" +
            ", \"sku\"\n" +
            ", \"title\"\n" +
            ", 'PDF' \"rendition_format\"\n" +
            ", \"inbound_ts\"\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as \"production_date\"\n" +
            ", \"crf\".\"epr\" as \"epr_id\"\n" +
            ", \"crf\".\"product_type\"\n" +
            ", CONCAT(\"sku\",'PDF') as \"u_key\"\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN' AND mi.effective_end_date is null) ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.ispdf = '1') where inbound_ts = (select max(inbound_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part) order by rand() limit %s";

    public static String GET_SDRM_SOURCE_INBOUND_DATA="select * from (SELECT\n" +
            "  \"isbn\" as ISBN\n" +
            ", \"sku\" as SKU\n" +
            ", \"title\" as TITLE\n" +
            ", 'Mobi' \"rendition_format\"\n" +
            ", \"inbound_ts\" as INBOUND_TS\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as PRODUCTION_DATE\n" +
            ", \"crf\".\"epr\" as EPR_ID\n" +
            ", \"crf\".\"product_type\" as PRODUCT_TYPE\n" +
            ", CONCAT(\"sku\",'Mobi') as U_KEY\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN' AND mi.effective_end_date is null) ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.ismobi = '1'\n" +
            "UNION\n" +
            "SELECT\n" +
            "  \"isbn\"\n" +
            ", \"sku\"\n" +
            ", \"title\"\n" +
            ", 'ePub' \"rendition_format\"\n" +
            ", \"inbound_ts\"\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as \"production_date\"\n" +
            ", \"crf\".\"epr\" as \"epr_id\"\n" +
            ", \"crf\".\"product_type\"\n" +
            ", CONCAT(\"sku\",'ePub') as \"u_key\"\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN' AND mi.effective_end_date is null) ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.isepub = '1'\n" +
            "UNION\n" +
            "SELECT\n" +
            "  \"isbn\"\n" +
            ", \"sku\"\n" +
            ", \"title\"\n" +
            ", 'PDF' \"rendition_format\"\n" +
            ", \"inbound_ts\"\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as \"production_date\"\n" +
            ", \"crf\".\"epr\" as \"epr_id\"\n" +
            ", \"crf\".\"product_type\"\n" +
            ", CONCAT(\"sku\",'PDF') as \"u_key\"\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN' AND mi.effective_end_date is null) ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.ispdf = '1') where inbound_ts = (select max(inbound_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part) and sku in ('%s') order by sku, rendition_format desc";

    public static String GET_SDRM_SOURCE_INBOUND_DATA1="select * from (\n" +
            "select \"isbn\" as \"ISBN\"\n" +
            ", \"sku\" as \"SKU\"\n" +
            ", \"title\" as \"TITLE\"\n" +
            ", 'Mobi' \"rendition_format\"\n" +
            ", \"inbound_ts\" as \"INBOUND_TS\"\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as \"PRODUCTION_DATE\"\n" +
            ", \"crf\".\"epr\" as \"EPR_ID\"\n" +
            ", \"crf\".\"PRODUCT_TYPE\"\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN') ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.ismobi = '1'\n" +
            "UNION\n" +
            "SELECT\n" +
            "  \"isbn\"\n" +
            ", \"sku\"\n" +
            ", \"title\"\n" +
            ", 'ePub' \"RENDITION_FORMAT\"\n" +
            ", \"inbound_ts\"\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as \"production_date\"\n" +
            ", \"crf\".\"epr\" as \"epr_id\"\n" +
            ", \"crf\".\"product_type\"\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN') ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.isepub = '1'\n" +
            "UNION\n" +
            "SELECT\n" +
            "  \"isbn\"\n" +
            ", \"sku\"\n" +
            ", \"title\"\n" +
            ", 'PDF' \"rendition_format\"\n" +
            ", \"inbound_ts\"\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as \"production_date\"\n" +
            ", \"crf\".\"epr\" as \"epr_id\"\n" +
            ", \"crf\".\"product_type\"\n" +
            "FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part src\n" +
            "LEFT OUTER JOIN "+ GetSDRMDLDBUser.getProdStagingDataBase()+".eph_identifier_cross_reference_v crf\n" +
            "ON src.isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Product'\n" +
            "LEFT JOIN (select mi.identifier, m.first_pub_date from "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation_identifier mi\n" +
            "inner join "+ GetSDRMDLDBUser.getProdDataBase()+".gd_manifestation m on m.manifestation_id = mi.f_manifestation\n" +
            "where mi.f_type='ISBN') ma on ma.identifier = src.isbn\n" +
            "WHERE crf.epr is not null and src.ispdf = '1') where inbound_ts = (select max(inbound_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part) and sku in ('%s') order by sku,rendition_format desc";

    public static String GET_SDRM_CURRENT_PRODUCT_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY from sdrm_staging_sit.sdrm_transform_current_product_availability where sku in ('%s') order by sku, rendition_format desc\n";

    public static String GET_SDRM_TRANSFORM_PRODUCT_HISTORY_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_product_availability_part where transform_ts = (select max(transform_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_product_availability_part) and (delete_flag = false) and sku in ('%s') order by sku, rendition_format desc";

    public static String GET_SDRM_TRANSFORM_PRODUCT_FILE_HISTORY_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part where transform_file_ts = (select max(transform_file_ts ) from "+GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part) and sku in ('%s') order by sku, rendition_format desc";

    public static String GET_RANDOM_SDRM_CURRENT_PRODUCT_ISBN_INBOUND="select sku as ISBN from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_current_product_availability order by rand() limit %s";

    public static String GET_RANDOM_SDRM_DELTA_CURRENT_ISBN_INBOUND="select sku as ISBN from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_current_product_availability order by rand() limit %s";

    public static String GET_SDRM_DELTA_CURRENT_PRODUCT_DATA=
            "select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY,delta_mode as DELTA_MODE from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_current_product_availability where sku in ('%s') order by sku, rendition_format desc\n";

    public static String GET_SDRM_DELTA_PRODUCT_HISTORY_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_history_product_availability_part where delta_ts = (select max(delta_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_history_product_availability_part) and sku in ('%s') order by sku, rendition_format desc";

    public static String GET_SDRM_HISTORY_EXCL_DELTA_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_excl_delta where sku in ('%s') order by sku, rendition_format desc\n";

   // public static String GET_DIFF_BETWEEN_SDRM_CURRENT_DELTA_AND_DELTA_HISTORY_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE from (select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_product_availability_part where transform_ts = (select max(transform_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_product_availability_part) and concat(sku, rendition_format) not in (select concat(sku, rendition_format) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_current_product_availability) and sku not in (select sku from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_current_product_availability)) where sku in ('%s') order by sku, rendition_format desc\n";

    public static String GET_DIFF_BETWEEN_SDRM_CURRENT_DELTA_AND_DELTA_HISTORY_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY from\n" +
            "(select * from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_product_availability_part\n" +
            "where transform_ts = (select max(transform_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_product_availability_part)\n" +
            "and concat(sku, rendition_format) not in (select concat(sku, rendition_format) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_current_product_availability)) where sku in ('%s') order by sku, rendition_format desc";

    public static String GET_RANDOM_SDRM_CURRENT_DELTA_AND_DELTA_HISTORY_ISBN_INBOUND="select sku as ISBN from (select sku from sdrm_staging_sit.sdrm_transform_history_product_availability_part where transform_ts = (select max(transform_ts) from sdrm_staging_sit.sdrm_transform_history_product_availability_part) and concat(sku, rendition_format) not in (select concat(sku, rendition_format) from sdrm_staging_sit.sdrm_delta_current_product_availability) and sku not in (select sku from sdrm_staging_sit.sdrm_delta_current_product_availability)) order by rand() limit %s";

    public static String GET_SDRM_TRANSFORM_LATEST_PRODUCT_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_latest_product_availability where sku in ('%s') order by sku, rendition_format desc\n";

    public static String GET_COMBINEOF_DELTACURRENT_AND_DELTAEXCL_DATA="select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE, u_key as U_KEY from (select isbn, sku, title, rendition_format,inbound_ts , production_date, epr_id,product_type, u_key from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_current_product_availability \n" +
            "union all\n" +
            "select isbn, sku, title, rendition_format,inbound_ts , production_date, epr_id,product_type, u_key as U_KEY from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_excl_delta) where sku in ('%s') order by sku, rendition_format desc\n";

    public static String GET_RANDOM_SDRM_DELTACURRENT_AND_DELTAEXCL_ISBN_INBOUND="select sku as ISBN from (select sku, title, isbn, rendition_format product_type, production_date, epr_id from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_current_product_availability \n" +
            "union all\n" +
            "select sku, title, isbn, rendition_format product_type, production_date, epr_id from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_excl_delta) order by rand() limit %s\n";

    public static String GET_RANDOM_SDRM_CURRENT_AND_PREV_FILE_HISTORY_ISBN_INBOUND="with crr_dataset as(\n" +
            "  select isbn, sku, title, rendition_format, production_date, epr_id, product_type\n" +
            "  from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part\n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part)),\n" +
            "  prev_dataset as (\n" +
            "  select isbn, sku, title, rendition_format, production_date, epr_id, product_type\n" +
            "  from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn\n" +
            "from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ")\n" +
            "select * from (select crr.sku as ISBN from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where prev.isbn is null\n" +
            "    union\n" +
            "select prev.sku from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where crr.isbn is null\n" +
            "    union\n" +
            "select crr.sku from crr_dataset crr\n" +
            "join prev_dataset prev on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where (coalesce(crr.isbn, 'null') <> coalesce(prev.isbn, 'null') or\n" +
            "-- coalesce(crr.sku, 'null') <> coalesce(prev.sku, 'null') or\n" +
            "coalesce (crr.title, 'null') <> coalesce (prev.title, 'null') or\n" +
            "-- coalesce (crr.rendition_format, 'null') <> coalesce (prev.rendition_format, 'null') or\n" +
            "coalesce (crr.production_date, current_date) <> coalesce (prev.production_date, current_date) or\n" +
            "coalesce (crr.epr_id, 'null') <> coalesce (prev.epr_id, 'null') or\n" +
            "coalesce (crr.product_type, 'null') <> coalesce (prev.product_type, 'null'))) order by rand() limit %s\n" +
            "\n";

    public static String GET_SDRM_CURRENT_AND_PREV_FILE_HISTORY_DATA="with crr_dataset as(\n" +
            "  select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT," +
            "  inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE\n" +
            "  from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part\n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part)),\n" +
            "  prev_dataset as (\n" +
            "  select isbn, sku, title, rendition_format,inbound_ts ,production_date, epr_id, product_type\n" +
            "  from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn\n" +
            "from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_file_history_product_availability_part dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ")\n" +
            "select isbn as ISBN,sku as SKU,title as TITLE,rendition_format as RENDITION_FORMAT,inbound_ts as INBOUND_TS,production_date as PRODUCTION_DATE,epr_id as EPR_ID,product_type as PRODUCT_TYPE,delta_mode as DELTA_MODE" +
            " from (select crr.isbn, crr.sku, crr.title, crr.rendition_format, crr.inbound_ts, crr.production_date , crr.epr_id, crr.product_type,'I' as delta_mode from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where prev.isbn is null\n" +
            "    union\n" +
            "select prev.isbn, prev.sku, prev.title, prev.rendition_format, prev.inbound_ts, prev.production_date , prev.epr_id, prev.product_type,'D' as delta_mode from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where crr.isbn is null\n" +
            "    union\n" +
            "select crr.isbn, crr.sku, crr.title, crr.rendition_format, crr.inbound_ts, crr.production_date , crr.epr_id, crr.product_type,'C' as delta_mode from crr_dataset crr\n" +
            "join prev_dataset prev on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where (coalesce(crr.isbn, 'null') <> coalesce(prev.isbn, 'null') or\n" +
            "-- coalesce(crr.sku, 'null') <> coalesce(prev.sku, 'null') or\n" +
            "coalesce (crr.title, 'null') <> coalesce (prev.title, 'null') or\n" +
            "-- coalesce (crr.rendition_format, 'null') <> coalesce (prev.rendition_format, 'null') or\n" +
            "coalesce (crr.production_date, current_date) <> coalesce (prev.production_date, current_date) or\n" +
            "coalesce (crr.epr_id, 'null') <> coalesce (prev.epr_id, 'null') or\n" +
            "coalesce (crr.product_type, 'null') <> coalesce (prev.product_type, 'null'))) where sku in ('%s') order by sku, rendition_format desc\n";



}
