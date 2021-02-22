package com.eph.automation.testing.services.db.StitchingExtendedSQL;


import com.eph.automation.testing.services.db.StitchingExtendedSQL.GetStitchDLDBUser;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class StitchingExtDataChecksSQL {

//    public static String currentDate(){
//        Calendar cal = Calendar.getInstance();
//         DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        String todayDate= dateFormat.format(cal.getTime());
//        return todayDate;
//
//    }
//
//    public static String previousDate(){
//        Calendar cal = Calendar.getInstance();
//        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        cal.add(Calendar.DATE, -1);
//        String yesterdayDate = dateFormat.format(cal.getTime());
//        return yesterdayDate;
//    }


    public static String GET_MANIF_EXT_REC =
            "select epr_id as epr_id\n" +
                    ",manifestation_type as manifestation_type\n" +
                    ",last_updated_date as last_updated_date\n" +
                    ",uk_textbook_ind as uk_textbook_ind\n" +
                    ",us_textbook_ind as us_textbook_ind\n" +
                    ",manifestation_trim_text as manifestation_trim_text\n" +
                    ",commodity_code as commodity_code\n" +
                    ",discount_code_emea as discount_code_emea\n" +
                    ",discount_code_us as discount_code_us\n" +
                    ",manifestation_weight as manifestation_weight\n" +
                    ",journal_issue_trim_size as journal_issue_trim_size\n" +
                    ",war_reference as war_reference\n" +
                    " from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended where epr_id in ('%s')\n";

    public static String GET_RANDOM_EPR_MANIF_EXTENDED =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended where delete_flag=false order by rand() limit %s\n";

    public static String GET_MANIF_EXT_JSON_REC =
            "select json as json, epr_id as epr_id, type as type from "+GetStitchDLDBUser.getStitchingExtdb()+".stch_manifestation_ext_json WHERE epr_id in ('%s')\n";



}




