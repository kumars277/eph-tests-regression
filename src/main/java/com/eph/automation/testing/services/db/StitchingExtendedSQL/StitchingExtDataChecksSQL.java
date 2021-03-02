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


    public static String GET_RANDOM_EPR_MANIF_EXTENDED =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended where delete_flag=false order by rand() limit %s\n";

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
                    ",delete_flag as delete_flag\n" +
                    " from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended where epr_id in ('%s') and delete_flag=false\n";

    public static String GET_MANIF_EXT_JSON_REC =
            "select json as json, epr_id as epr_id, type as type from "+GetStitchDLDBUser.getStitchingExtdb()+".stch_manifestation_ext_json WHERE epr_id in ('%s')\n";

    public static String GET_RANDOM_EPR_MANIF_EXT_PAGE_COUNT =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended_page_count where delete_flag=false order by rand() limit %s\n";

    public static String GET_MANIF_EXT_PAFE_COUNT_REC =
            "select epr_id as epr_id\n" +
                    ",manifestation_type as manifestation_type\n" +
                    ",last_updated_date as last_updated_date\n" +
                    ",count_type_code  as count_type_code \n" +
                    ",count_type_name as count_type_name\n" +
                    ",count as count\n" +
                    ",delete_flag as delete_flag\n" +
                    " from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended_page_count where epr_id in ('%s') and delete_flag=false\n";

    public static String GET_RANDOM_EPR_WORK_EXTENDED =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".work_extended where delete_flag=false order by rand() limit %s\n";

    public static String GET_WORK_EXT_REC =
            "select epr_id as epr_id\n" +
                    ",work_type as work_type\n" +
                    ",last_updated_date as last_updated_date\n" +
                    ",journal_els_com_ind as journal_els_com_ind\n" +
                    ",journs_aims_scope as journs_aims_scope\n" +
                    ",delta_business_unit as delta_business_unit\n" +
                    ",image_file_ref as image_file_ref\n" +
                    ",master_isbn as master_isbn\n" +
                    ",author_by_line_text as author_by_line_text\n" +
                    ",key_features as key_features\n" +
                    ",product_awards as product_awards\n" +
                    ",product_long_desc as product_long_desc" +
                    ",product_short_desc as product_short_desc" +
                    ",review_quotes as review_quotes" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience_text as audience_text" +
                    ",book_sub_business_unit as book_sub_business_unit" +
                    ",internal_els_div as internal_els_div" +
                    ",profit_centre as profit_centre" +
                    ",text_ref_trade as text_ref_trade" +
                    ",primary_site_system as primary_site_system" +
                    ",primary_site_acronym as primary_site_acronym" +
                    ",primary_site_support_level as primary_site_support_level" +
                    ",issue_prod_type_code as issue_prod_type_code" +
                    ",catalogue_volumes_qty as catalogue_volumes_qty" +
                    ",catalogue_issues_qty as catalogue_issues_qty" +
                    ",catalogue_volume_from as catalogue_volume_from" +
                    ",catalogue_volume_to as catalogue_volume_to" +
                    ",rf_issues_qty as rf_issues_qty" +
                    ",rf_total_pages_qty as rf_total_pages_qty" +
                    ",rf_fvi as rf_fvi" +
                    ",rf_lvi as rf_lvi\n" +
                    ",business_unit_desc as business_unit_desc\n" +
                    ",journal_prod_site_code as journal_prod_site_code\n" +
                    ",delete_flag as delete_flag\n" +
                    " from "+GetStitchDLDBUser.getProdExtDB()+".work_extended where epr_id in ('EPR-W-108SKT')\n";

    public static String GET_WORK_EXT_JSON_REC =
            "select json as json, epr_id as epr_id, type as type from "+GetStitchDLDBUser.getStitchingExtdb()+".stch_work_ext_json WHERE epr_id in ('EPR-W-108SKT')\n";




}




