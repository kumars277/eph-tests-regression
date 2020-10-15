package com.eph.automation.testing.services.db.SDBooksDataLakeSQL;


public class SDDataLakeCountChecksSQL {

       public static String GET_SD_URL_SOURCE_COUNT =
            "select count(*) as Source_count from\n " +
                    "(select src.unformatted_isbn as isbn, src.book_title as book_title,\n " +
                    "src.shortcut_url as url, 'SDDC' as url_code, 'Science Direct' as url_name,\n " +
                    "'Science Direct (To=URL)'  as url_title, inbound_ts as inbound_ts, crf.epr as epr_id\n" +
                    "FROM "+ GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part src\n" +
                    "LEFT OUTER JOIN "+GetSDBooksDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v crf\n" +
                    "ON src.unformatted_isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Work'\n" +
                    "WHERE crf.epr is not null and inbound_ts = \n" +
                    "(select max(inbound_ts) from "+ GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part))";


       public static String GET_SD_URL_CURRENT_COUNT =
               "SELECT count(*) as Target_count FROM "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls";


        public static String GET_SD_URL_CURRENT_HISTORY_COUNT =
                "select count(*) as History_Count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where\n " +
                        "transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part) and delete_flag = false\n";

    public static String GET_SD_URL_PREVIOUS_COUNT =
            "SELECT count(*) as Previous_count FROM "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls";


    public static String GET_SD_URL_PREVIOUS_HISTORY_COUNT =
            "select count(*) as Previous_History_Count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where\n " +
                    "transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part\n " +
                    "where transform_ts<(select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part)) and delete_flag = false\n";


    public static String GET_SD_URL_TRANSFORM_FILE =
            "select count(*) as Transform_Count from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part where\n " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part)\n";

   /* with crr_dataset as(
            select isbn, book_title, url, url_code, url_name, url_title, epr_id, work_type
            from sdbooks_staging_sit.sdbooks_transform_file_history_urls_part
                    where transform_file_ts = (select max(transform_file_ts ) from sdbooks_staging_sit.sdbooks_transform_file_history_urls_part)
            ),
    prev_dataset as (
            select isbn, book_title, url, url_code, url_name, url_title, epr_id, work_type
            from sdbooks_staging_sit.sdbooks_transform_file_history_urls_part
                    where transform_file_ts = (select distinct transform_file_ts
                    from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn
    from sdbooks_staging_sit.sdbooks_transform_file_history_urls_part dhap
                                            )
    where rn = 2
                                    )
                                            )
    select 'new', count(*)
    from crr_dataset crr
    left join prev_dataset prev on crr.isbn = prev.isbn
    where prev.isbn is null
    union
    select 'deleted', count(*)
    from prev_dataset prev
    left join crr_dataset crr on crr.isbn = prev.isbn
    where crr.isbn is null
    union
    select 'changed', count(*)
    from crr_dataset crr
    join prev_dataset prev on crr.isbn = prev.isbn
    where (coalesce(crr.isbn, 'na') <> coalesce(prev.isbn, 'na') or
    coalesce(crr.book_title, 'na') <> coalesce(prev.book_title, 'na') or
    coalesce (crr.url, 'na') <> coalesce (prev.url, 'na') or
    coalesce (crr.url_code, 'na') <> coalesce (prev.url_code, 'na') or
    coalesce (crr.url_name, 'na') <> coalesce (prev.url_name, 'na') or
    coalesce (crr.url_title, 'na') <> coalesce (prev.url_title, 'na') or
    coalesce (crr.epr_id, 'na') <> coalesce (prev.epr_id, 'na') or
    coalesce (crr.work_type, 'na') <> coalesce (prev.work_type, 'na'));
*/

    public static String GET_SD_URL_DIFF_CURR_PREV_COUNT =
    /*"select count(*) as source_count from (\n" +
             "select c.isbn \n" +
            "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls c\n" +
            "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls p  on \n" +
            "(c.isbn = p.isbn) \n" +
            "where p.isbn is null\n" +
            "union all\n" +
            "select c.isbn \n" +
            "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls p\n" +
            "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls c  on \n" +
            "(p.isbn = c.isbn) \n" +
            "where c.isbn is null\n" +
            "union all\n" +
            "select c.isbn \n" +
            "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls p \n" +
            "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls c  on \n" +
            "(p.isbn = c.isbn) \n" +
            "where (p.book_title != (c.book_title ) or  \n" +
            "p.epr_id != (c.epr_id ) or \n" +
            "p.isbn != (c.isbn ) or \n" +
            "p.url != (c.url ) or \n" +
            "p.url_code != (c.url_code ) or \n" +
            "p.url_name != (c.url_name ) or \n" +
            "p.url_title  != (c.url_title )))";*/
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




