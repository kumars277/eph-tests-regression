package com.eph.automation.testing.services.db.SDBooksDataLakeSQL;


public class SDDataLakeCountChecksSQL {

       public static String GET_SD_URL_SOURCE_COUNT =
               "select count(*) as Source_Count from (SELECT\n" +
                       "  src.unformatted_isbn   as isbn\n" +
                       ", src.book_title         as book_title\n" +
                       ", src.shortcut_url       as url\n" +
                       ", 'SDDC'                     as url_code\n" +
                       ", 'Science Direct'           as url_name\n" +
                       ", 'Science Direct (To=URL)'  as url_title\n" +
                       ", inbound_ts               as inbound_ts\n" +
                       ", crf.epr                as epr_id\n" +
                       ", crf.work_type          as work_type\n" +
                       "FROM "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part src\n" +
                       "LEFT OUTER JOIN "+GetSDBooksDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v crf\n" +
                       "ON src.unformatted_isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Work'\n" +
                       "WHERE crf.epr is not null) where inbound_ts = (select max(inbound_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part)";


       public static String GET_SD_URL_CURRENT_COUNT =
               "SELECT count(*) as Target_count FROM "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls";


        public static String GET_SD_URL_CURRENT_HISTORY_COUNT =
                "select count(*) as History_Count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where delete_flag=false and\n " +
                        " transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part)\n";


    public static String GET_SD_URL_TRANSFORM_FILE =
            "select count(*) as Transform_Count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part where\n " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part)\n";

    public static String GET_SD_URL_DIFF_CURR_PREV_COUNT =
            "with crr_dataset as(\n" +
                    "  select isbn, book_title, url, url_code, url_name, url_title, epr_id, work_type\n" +
                    "  from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select isbn, book_title, url, url_code, url_name, url_title, epr_id, work_type\n" +
                    "  from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part\n" +
                    "  where transform_file_ts = (select distinct transform_file_ts\n" +
                    "                                    from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn   \n" +
                    "                                            from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part dhap\n" +
                    "                                            )\n" +
                    "                                    where rn = 2\n" +
                    "                                    )\n" +
                    "                                    )                                  \n" +
                    "select count(*) as source_count from( \n" +
                    "    select crr.isbn as ISBN\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.isbn = prev.isbn\n" +
                    "    where prev.isbn is null\n" +
                    "    union\n" +
                    "    select  prev.isbn as ISBN \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.isbn = prev.isbn\n" +
                    "    where crr.isbn is null\n" +
                    "    union\n" +
                    "    select crr.isbn as ISBN\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.isbn = prev.isbn\n" +
                    "    where (coalesce(crr.isbn, 'na') <> coalesce(prev.isbn, 'na') or\n" +
                    "            coalesce(crr.book_title, 'na') <> coalesce(prev.book_title, 'na') or\n" +
                    "            coalesce (crr.url, 'na') <> coalesce (prev.url, 'na') or\n" +
                    "            coalesce (crr.url_code, 'na') <> coalesce (prev.url_code, 'na') or\n" +
                    "            coalesce (crr.url_name, 'na') <> coalesce (prev.url_name, 'na') or\n" +
                    "            coalesce (crr.url_title, 'na') <> coalesce (prev.url_title, 'na') or\n" +
                    "            coalesce (crr.epr_id, 'na') <> coalesce (prev.epr_id, 'na') or\n" +
                    "            coalesce (crr.work_type, 'na') <> coalesce (prev.work_type, 'na')))";


    public static String GET_SD_URL_DELTA_CURRENT_COUNT =
            "select count(*) as Delta_Count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls";

    public static String GET_SD_URL_DELTA_CURR_HIST_COUNT =
            "select count(*) as Delta_History_Count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_history_urls_part\n " +
                    "where delta_ts = (select max(delta_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_history_urls_part)";


    public static String GET_SD_URL_LATEST_CURRENT_COUNT =
                    "select count(*) as latest_count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_latest_urls";

    public static String GET_SD_URL_SUM_DELTA_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.isbn from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_excl_delta as c union all \n" +
                    "select d.isbn from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls as d)";

    public static String GET_SD_URL_DIFF_DELTA_CURR_HIST_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.isbn from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c\n" +
                    "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls d on c.isbn  = d.isbn \n" +
                    "where d.isbn is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c ))";


    public static String GET_SD_URL_EXCLUDE_CURRENT_COUNT =
            "select count(*) as excl_count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_excl_delta";

    public static String GET_SD_DUPLICATES_LATEST_URL_COUNT =
            "select count(*)  as Duplicate_Count from (SELECT count(*) FROM "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_latest_urls \n" +
                    " group by isbn having count(*)>1)";
}




