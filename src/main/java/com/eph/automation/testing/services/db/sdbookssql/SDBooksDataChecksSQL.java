package com.eph.automation.testing.services.db.sdbookssql;
public class SDBooksDataChecksSQL {

    private SDBooksDataChecksSQL() {
        throw new IllegalStateException("Utility class");
    }

    public static final String GET_RANDOM_ISBN_INBOUND =
            "select isbn as isbn from" +
                    " (SELECT\n" +
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
                    " ON src.unformatted_isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Work'\n" +
                    " WHERE crf.epr is not null) where inbound_ts = (select max(inbound_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part) order by rand() limit %s";


    public static final String GET_REC_URL_INBOUND =
            "select isbn as isbn" +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from " +
                    " (SELECT\n" +
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
                    " LEFT OUTER JOIN "+GetSDBooksDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v crf\n" +
                    " ON src.unformatted_isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Work'\n" +
                    " WHERE crf.epr is not null) where inbound_ts = (select max(inbound_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part)\n" +
                    "and isbn in ('%s') order by isbn desc";


    public static final String GET_REC_CURRENT_URL =
            "select isbn as isbn" +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls \n"+
                    "where isbn in ('%s') order by isbn desc";

    public static final String GET_RANDOM_ISBN_CURRENT_URL =
            "select isbn as isbn" +
                    " from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls \n"+
                    "order by rand() limit %s";

    public static final String GET_REC_CURRENT_HIST_URL =
            "select isbn as isbn" +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where\n " +
                    "transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part) " +
                    "and delete_flag = false and isbn in ('%s') order by isbn desc \n";


    public static final String GET_REC_CURRENT_TRANS_FILE_URL =
            "select isbn as isbn" +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part where\n " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_file_history_urls_part) " +
                    "and isbn in ('%s') order by isbn desc \n";

    public static final String GET_REC_PREVIOUS_URL =
            "select isbn as isbn" +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls \n"+
                    "where isbn in ('%s') order by isbn desc";

    public static final String GET_REC_PREVIOUS_HIST_URL =
            "select isbn as isbn" +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where\n " +
                    "transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part\n " +
                    "where transform_ts<(select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part))\n " +
                    "and delete_flag = false and isbn in ('%s') order by isbn desc\n";

    public static final String GET_RANDOM_ISBN_PREVIOUS_HIST_URL =
            "select isbn as isbn" +
                     " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where\n " +
                    "transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part\n " +
                    "where transform_ts<(select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part))\n " +
                    "and delete_flag = false order by rand() limit %s\n";

    public static final String GET_REC_DELTA_CURRENT_URL =
            "select isbn as isbn " +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    ",delta_mode as deltaMode"+
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls where isbn in ('%s') order by isbn desc";

    public static final String GET_RANDOM_ISBN_DIFF_DELTA_CURR_HIST_URL =
            "select isbn as isbn " +
                    "from \n" +
                    "(select c.isbn from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c\n" +
                    "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls d on c.isbn  = d.isbn \n" +
                    "where d.isbn is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c ))order by rand () limit %s";

    public static final String GET_REC_DIFF_DELTA_CURR_HIST_URL =
            "select isbn as isbn " +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from \n" +
                    "(select c.isbn,c.book_title ,c.url,c.url_code,c.url_name,c.url_title,c.epr_id,c.work_type " +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c\n" +
                    "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls d on c.isbn  = d.isbn \n" +
                    "where d.isbn is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c ))where isbn in ('%s') order by isbn desc";

    public static final String GET_REC_EXCL_URL =
            "select isbn as isbn " +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_excl_delta where isbn in ('%s') order by isbn desc";


    public static final String GET_RANDOM_ISBN_SUM_DELTA_EXCL_URL =
            "select isbn as isbn " +
                    "from \n" +
                    "(select c.isbn from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_excl_delta as c union all \n" +
                    "select d.isbn from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls as d) order by rand () limit %s";


    public static final String GET_REC_SUM_DELTA_EXCL_URL =
            "select isbn as isbn " +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from \n" +
                    "(select c.isbn,c.book_title ,c.url,c.url_code,c.url_name,c.url_title,c.epr_id,c.work_type from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_excl_delta as c union all \n" +
                    "select d.isbn,d.book_title,d.url,d.url_code,d.url_name,d.url_title,d.epr_id,d.work_type from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls as d) where isbn in ('%s') order by isbn desc";

    public static final String GET_REC_LATEST_URL =
            "select isbn as isbn " +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_latest_urls where isbn in ('%s') order by isbn desc";



    public static final String GET_RANDOM_ISBN_DELTA_CURR =
            "select isbn as isbn " +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls order by rand() limit %s";


    public static final String GET_REC_DELTA_CURR_HIST =
            "select isbn as isbn " +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    ",delta_mode as deltaMode"+
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_history_urls_part \n" +
                    "where delta_ts = (select max(delta_ts) \n" +
                    "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_history_urls_part) and \n" +
                    "isbn in ('%s') order by isbn desc";



    public static final String GET_REC_DIFF_OF_CURR_PREV_TRANS_FILE =
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
                    "                                    )    \n" +
                    "select isbn as isbn " +
                    ",book_title as bookTitle" +
                    ",url as url" +
                    ",url_code as urlCode" +
                    ",url_name as urlName" +
                    ",url_title as urlTitle" +
                    ",epr_id as eprid" +
                    ",work_type as workType" +
                    ",delta_mode as deltaMode from ("+
                    "    select crr.isbn,crr.book_title,crr.url,crr.url_code,crr.url_name,crr.url_title,crr.epr_id,crr.work_type, 'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.isbn = prev.isbn\n" +
                    "    where prev.isbn is null\n" +
                    "    union\n" +
                    "    select  prev.isbn,prev.book_title,prev.url,prev.url_code,prev.url_name,prev.url_title,prev.epr_id,prev.work_type, 'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.isbn = prev.isbn\n" +
                    "    where crr.isbn is null\n" +
                    "    union\n" +
                    "    select crr.isbn,crr.book_title,crr.url,crr.url_code,crr.url_name,crr.url_title,crr.epr_id,crr.work_type, 'C'as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.isbn = prev.isbn\n" +
                    "    where (coalesce(crr.isbn, 'na') <> coalesce(prev.isbn, 'na') or\n" +
                    "            coalesce(crr.book_title, 'na') <> coalesce(prev.book_title, 'na') or\n" +
                    "            coalesce (crr.url, 'na') <> coalesce (prev.url, 'na') or\n" +
                    "            coalesce (crr.url_code, 'na') <> coalesce (prev.url_code, 'na') or\n" +
                    "            coalesce (crr.url_name, 'na') <> coalesce (prev.url_name, 'na') or\n" +
                    "            coalesce (crr.url_title, 'na') <> coalesce (prev.url_title, 'na') or\n" +
                    "            coalesce (crr.epr_id, 'na') <> coalesce (prev.epr_id, 'na') or\n" +
                    "            coalesce (crr.work_type, 'na') <> coalesce (prev.work_type, 'na'))) where isbn in ('%s') order by isbn desc ";


    public static final String GET_RANDOM_ISBN_URL_DIFF_CURR_PREV_TRANS_FILE =
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
                    "    select crr.isbn as isbn\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.isbn = prev.isbn\n" +
                    "    where prev.isbn is null\n" +
                    "    union\n" +
                    "    select  prev.isbn as isbn \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.isbn = prev.isbn\n" +
                    "    where crr.isbn is null\n" +
                    "    union\n" +
                    "    select crr.isbn as isbn\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.isbn = prev.isbn\n" +
                    "    where (coalesce(crr.isbn, 'na') <> coalesce(prev.isbn, 'na') or\n" +
                    "            coalesce(crr.book_title, 'na') <> coalesce(prev.book_title, 'na') or\n" +
                    "            coalesce (crr.url, 'na') <> coalesce (prev.url, 'na') or\n" +
                    "            coalesce (crr.url_code, 'na') <> coalesce (prev.url_code, 'na') or\n" +
                    "            coalesce (crr.url_name, 'na') <> coalesce (prev.url_name, 'na') or\n" +
                    "            coalesce (crr.url_title, 'na') <> coalesce (prev.url_title, 'na') or\n" +
                    "            coalesce (crr.epr_id, 'na') <> coalesce (prev.epr_id, 'na') or\n" +
                    "            coalesce (crr.work_type, 'na') <> coalesce (prev.work_type, 'na'))order by rand() limit %s";
}






