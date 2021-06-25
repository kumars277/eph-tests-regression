package com.eph.automation.testing.services.db.SDRMDataLakeSQL;

import org.apache.commons.collections4.Get;

public class SDRMDataLakeCountCheckSQL {


    public static String GET_SDRM_INBOUND_SOURCE_COUNT="select count(*) as source_count from (SELECT\n" +
            "  \"isbn\"\n" +
            ", \"sku\"\n" +
            ", \"title\"\n" +
            ", 'Mobi' \"rendition_format\"\n" +
            ", \"inbound_ts\"\n" +
            ", COALESCE(\"ma\".\"first_pub_date\",CURRENT_DATE) as \"production_date\"\n" +
            ", \"crf\".\"epr\" as \"epr_id\"\n" +
            ", \"crf\".\"product_type\"\n" +
            ", CONCAT(\"sku\",'Mobi') as \"u_key\"\n" +
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
            "WHERE crf.epr is not null and src.ispdf = '1') where inbound_ts = (select max(inbound_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_inbound_part)";

    public static String GET_SDRM_CURRENT_COUNT= "select count(*) as source_count from "+GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_current_product_availability";

    public static String GET_SDRM_PRODUCT_HISTORY_COUNT="select count(*) as target_count from "+GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_history_product_availability_part where transform_ts = (select max(transform_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_history_product_availability_part) and delete_flag = false";

    public static String GET_SDRM_PRODUCT_FILE_HISTORY_COUNT="select count(*) as target_count from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_file_history_product_availability_part where transform_file_ts = (select max(transform_file_ts ) from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_file_history_product_availability_part)";
    public static String GET_SDRM_DELTA_CURRENT_PRODUCT_COUNT="select count(*) as source_count from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_delta_current_product_availability";

    public static String GET_SDRM_DELTA_PRODUCT_HISTORY_COUNT="select count(*) as target_count from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_delta_history_product_availability_part where delta_ts = (select max(delta_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_delta_history_product_availability_part)";

    public static String GET_SDRM_HISTORY_EXCL_DELTA_COUNT="select count(*) as target_count from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_history_excl_delta";

    public static String GET_SDRM_CURRENT_PRODUCT_AND_PRODUCT_HISTORY_COUNT=
            "select count(*) as source_count from (select sku from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_product_availability_part" +
                    " where transform_ts = (select max(transform_ts) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_transform_history_product_availability_part)" +
                    " and concat(sku, rendition_format) not in (select concat(sku, rendition_format) from "+ GetSDRMDLDBUser.getSDRMDataBase()+".sdrm_delta_current_product_availability) and sku not in (select sku from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_delta_current_product_availability))";

    public static String GET_SDRM_DELTA_CURRENT_AND_SDRM_HISTORY_EXCL_DELTA_COUNT="select count(*) as source_count from (select sku, title, isbn, rendition_format product_type, production_date, epr_id from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_delta_current_product_availability \n" +
            "union all\n" +
            "select sku, title, isbn, rendition_format product_type, production_date, epr_id from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_history_excl_delta)";

    public static String GET_SDRM_LATEST_PRODUCT_COUNT="select count(*) as target_count from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_latest_product_availability";

    public static String GET_SDRM_DUPLICATES_LATEST_PRODUCUT_COUNT="select count(*) as Duplicate_Count from (SELECT concat(sku, rendition_format) ,count(*) FROM "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_latest_product_availability group by concat(sku, rendition_format) having count(*)>1)";



    public static String GET_SDRM_FILEHISTORY_COUNT="with crr_dataset as(\n" +
            "  select isbn, sku, title, rendition_format, production_date, epr_id, product_type\n" +
            "  from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_file_history_product_availability_part\n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_file_history_product_availability_part)),\n" +
            "  prev_dataset as (\n" +
            "  select isbn, sku, title, rendition_format, production_date, epr_id, product_type\n" +
            "  from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_file_history_product_availability_part\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn\n" +
            "from "+ GetSDRMDLDBUser.getSDRMDataBase()+" .sdrm_transform_file_history_product_availability_part dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ")\n" +
            "select count(*) as source_count from (select * from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where prev.isbn is null\n" +
            "    union\n" +
            "select * from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where crr.isbn is null\n" +
            "    union\n" +
            "select * from crr_dataset crr\n" +
            "join prev_dataset prev on crr.sku = prev.sku and crr.rendition_format = prev.rendition_format\n" +
            "where (coalesce(crr.isbn, 'null') <> coalesce(prev.isbn, 'null') or\n" +
            "-- coalesce(crr.sku, 'null') <> coalesce(prev.sku, 'null') or\n" +
            "coalesce (crr.title, 'null') <> coalesce (prev.title, 'null') or\n" +
            "-- coalesce (crr.rendition_format, 'null') <> coalesce (prev.rendition_format, 'null') or\n" +
            "coalesce (crr.production_date, current_date) <> coalesce (prev.production_date, current_date) or\n" +
            "coalesce (crr.epr_id, 'null') <> coalesce (prev.epr_id, 'null') or\n" +
            "coalesce (crr.product_type, 'null') <> coalesce (prev.product_type, 'null')))";



    public static String GET_PRODUCT_EXTENDED_AVAILABILITY_ALL_SOURCE_COUNT="select count(*) as target_count from "+ GetSDRMDLDBUser.getProdStagingDataBase()+".product_availability_extended_allsource_v where \"source\" = 'sdrm'\n";

    public static String GET_PRODUCT_EXTENDED_AVAILABILITY_COUNT="select count(*) as target_count from "+ GetSDRMDLDBUser.getProdExtDataBase()+".product_extended_availability where application_name = 'sdrm'";


}
