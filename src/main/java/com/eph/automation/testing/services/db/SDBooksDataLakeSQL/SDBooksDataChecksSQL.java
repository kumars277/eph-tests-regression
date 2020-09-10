package com.eph.automation.testing.services.db.SDBooksDataLakeSQL;


import com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL.GetJRBIDLDBUser;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class SDBooksDataChecksSQL {


    public static String GET_RANDOM_ISBN_INBOUND =
           "select isbn as ISBN from " +
                   "(select src.unformatted_isbn as isbn, src.book_title as book_title, src.shortcut_url as url, " +
                   "'SDDC' as url_code, 'Science Direct' as url_name, 'Science Direct (To=URL)'  as url_title, " +
                   "inbound_ts as inbound_ts, crf.epr as epr_id\n" +
                   " FROM "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part src\n" +
                   " LEFT OUTER JOIN "+GetSDBooksDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v crf\n" +
                   " ON src.unformatted_isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Work'\n" +
                   " WHERE crf.epr is not null and inbound_ts = (select max(inbound_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part)) order by rand() limit %s";

    public static String GET_REC_URL_INBOUND =
            "select isbn as ISBN" +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    " from " +
                    "(select src.unformatted_isbn as isbn, src.book_title as book_title, src.shortcut_url as url, " +
                    "'SDDC' as url_code, 'Science Direct' as url_name, 'Science Direct (To=URL)'  as url_title, " +
                    "inbound_ts as inbound_ts, crf.epr as epr_id\n" +
                    " FROM "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part src\n" +
                    " LEFT OUTER JOIN "+GetSDBooksDLDBUser.getProductDatabase()+".eph_identifier_cross_reference_v crf\n" +
                    " ON src.unformatted_isbn = crf.identifier AND crf.identifier_type = 'ISBN' AND crf.record_level = 'Work'\n" +
                    " WHERE crf.epr is not null and inbound_ts = (select max(inbound_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_inbound_part))\n " +
                    "where isbn in ('%s') order by isbn desc";


    public static String GET_REC_CURRENT_URL =
            "select isbn as ISBN" +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    " from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls \n"+
                    "where isbn in ('%s') order by isbn desc";

    public static String GET_RANDOM_ISBN_CURRENT_URL =
            "select isbn as ISBN" +
                    " from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls \n"+
                    "order by rand() limit %s";

    public static String GET_REC_CURRENT_HIST_URL =
            "select isbn as ISBN" +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where\n " +
                    "transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part) " +
                    "and delete_flag = false and isbn in ('%s') order by isbn desc \n";

    public static String GET_REC_PREVIOUS_URL =
            "select isbn as ISBN" +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    " from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls \n"+
                    "where isbn in ('%s') order by isbn desc";

    public static String GET_REC_PREVIOUS_HIST_URL =
            "select isbn as ISBN" +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where\n " +
                    "transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part\n " +
                    "where transform_ts<(select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part))\n " +
                    "and delete_flag = false and isbn in ('%s') order by isbn desc\n";

    public static String GET_RANDOM_ISBN_PREVIOUS_HIST_URL =
            "select isbn as ISBN" +
                     " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part where\n " +
                    "transform_ts = (select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part\n " +
                    "where transform_ts<(select max(transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part))\n " +
                    "and delete_flag = false order by rand() limit %s\n";

    public static String GET_RANDOM_ISBN_URL_DIFF_CURR_PREV =
            "select isbn as ISBN " +
                    "from (\n" +
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
                    "p.url_title  != (c.url_title ))) order by rand () limit %s";


    public static String GET_REC_ISBN_URL_DIFF_CURR_PREV =
            "select isbn as ISBN " +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    ",delta_mode as DELTA_MODE"+
                    " from (\n" +
                    "select c.isbn,c.book_title,c.url,c.url_code,c.url_name,c.url_title,c.epr_id,c.work_type,'I' as delta_mode \n" +
                    "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls c\n" +
                    "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls p  on \n" +
                    "(c.isbn = p.isbn) \n" +
                    "where p.isbn is null\n" +
                    "union all\n" +
                    "select c.isbn,c.book_title,c.url,c.url_code,c.url_name,c.url_title,c.epr_id,c.work_type,'D' as delta_mode \n" +
                    "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls p\n" +
                    "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls c  on \n" +
                    "(p.isbn = c.isbn) \n" +
                    "where c.isbn is null\n" +
                    "union all\n" +
                    "select c.isbn,c.book_title,c.url,c.url_code,c.url_name,c.url_title,c.epr_id,c.work_type,'C' as delta_mode \n" +
                    "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_previous_urls p \n" +
                    "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_current_urls c  on \n" +
                    "(p.isbn = c.isbn) \n" +
                    "where (p.book_title != (c.book_title ) or  \n" +
                    "p.epr_id != (c.epr_id ) or \n" +
                    "p.isbn != (c.isbn ) or \n" +
                    "p.url != (c.url ) or \n" +
                    "p.url_code != (c.url_code ) or \n" +
                    "p.url_name != (c.url_name ) or \n" +
                    "p.url_title  != (c.url_title ))) where isbn in ('%s') order by isbn desc\n";


    public static String GET_REC_DELTA_CURRENT_URL =
            "select isbn as ISBN " +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    ",delta_mode as DELTA_MODE"+
                    " from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls where isbn in ('%s') order by isbn desc";


    public static String GET_RANDOM_ISBN_DELTA_CURR_HIST_URL =
            "select isbn as ISBN " +
                    "from \n" +
                    "(select c.isbn from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c\n" +
                    "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls d on c.isbn  = d.isbn \n" +
                    "where d.isbn is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c ))order by rand () limit %s";

    public static String GET_REC_ISBN_DELTA_CURR_HIST_URL =
            "select isbn as ISBN " +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    "from \n" +
                    "(select c.isbn from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c\n" +
                    "left join "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls d on c.isbn  = d.isbn \n" +
                    "where d.isbn is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_urls_part c ))where isbn in ('%s') order by isbn desc";

    public static String GET_REC_ISBN_EXCL_URL =
            "select isbn as ISBN " +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_excl_delta where isbn in ('%s') order by isbn desc";


    public static String GET_RANDOM_ISBN_SUM_DELTA_EXCL_URL =
            "select isbn as ISBN " +
                    "from \n" +
                    "(select c.isbn from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_excl_delta as c union all \n" +
                    "select d.isbn from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls as d) order by rand () limit %s";


    public static String GET_REC_SUM_DELTA_EXCL_URL =
            "select isbn as ISBN " +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    "from \n" +
                    "(select c.isbn from " +GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_history_excl_delta as c union all \n" +
                    "select d.isbn from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_delta_current_urls as d) where isbn in ('%s') order by isbn desc";

    public static String GET_REC_LATEST_URL =
            "select isbn as ISBN " +
                    ",book_title as BOOK_TITLE" +
                    ",url as URL" +
                    ",url_code as URL_CODE" +
                    ",url_name as URL_NAME" +
                    ",url_title as URL_TITLE" +
                    ",epr_id as EPRID" +
                    ",work_type as WORK_TYPE" +
                    "from "+GetSDBooksDLDBUser.getSDDataBase()+".sdbooks_transform_latest_urls";

}






