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
            "select json as json, epr_id as epr_id, type as type from "+GetStitchDLDBUser.getStitchingExtdb()+".stch_manifestation_ext_json WHERE epr_id in ('%s')\n"; //EPR-M-115H09

    public static String GET_RANDOM_EPR_MANIF_EXT_PAGE_COUNT =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended_page_count where delete_flag=false order by rand() limit %s\n";

    public static String GET_RANDOM_EPR_MANIF_EXT_RESTRICT_COUNT =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended_restriction where delete_flag=false order by rand() limit %s\n";


    public static String GET_MANIF_EXT_PAFE_COUNT_REC =
            "select epr_id as epr_id\n" +
                    ",manifestation_type as manifestation_type\n" +
                    ",last_updated_date as last_updated_date\n" +
                    ",count_type_code  as count_type_code \n" +
                    ",count_type_name as count_type_name\n" +
                    ",count as count\n" +
                    ",delete_flag as delete_flag\n" +
                    " from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended_page_count where epr_id in ('%s') and delete_flag=false\n";  //EPR-M-115H09


    public static String GET_MANIF_EXT_RESTRICT_REC =
            "select epr_id as epr_id\n" +
                    ",manifestation_type as manifestation_type\n" +
                    ",last_updated_date as last_updated_date\n" +
                    ",restriction_code  as restriction_code \n" +
                    ",restriction_name as restriction_name\n" +
                    " from "+GetStitchDLDBUser.getProdExtDB()+".manifestation_extended_restriction where epr_id in ('%s') and delete_flag=false\n";  //EPR-M-115H09

    public static String GET_PROD_EXT_AVAIL_REC =
            "select epr_id as epr_id\n" +
                    ",product_type as product_type\n" +
                    ",last_updated_date as last_updated_date\n" +
                    ",application_name  as application_name \n" +
                    ",delta_answer_code_uk as delta_answer_code_uk\n" +
                    ",delta_answer_code_us as delta_answer_code_us\n" +
                    ",publication_status_anz as publication_status_anz\n" +
                    ",availability_format as availability_format\n" +
                    ",availability_start_date as availability_start_date\n" +
                    ",availability_status  as availability_status \n" +
                    " from "+GetStitchDLDBUser.getProdExtDB()+".product_extended_availability where epr_id in ('%s') and delete_flag=false\n";  //EPR-M-115H09



    public static String GET_RANDOM_EPR_WORK_EXTENDED =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".work_extended where delete_flag=false order by rand() limit %s\n";

    public static String GET_COUNT_WORK_EXT_TABLE =
            "select count(*) as Source_Count from "+GetStitchDLDBUser.getProdExtDB()+".work_extended where delete_flag=false";

    public static String GET_COUNT_WORK_EXT_STITCH_TABLE =
            "select count(*) as Target_Count from "+GetStitchDLDBUser.getStitchingExtdb()+".stch_work_ext_json\n";


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
                    " from "+GetStitchDLDBUser.getProdExtDB()+".work_extended where epr_id in ('%s')\n";

    public static String GET_WORK_EXT_JSON_REC =
            "select json as json, epr_id as epr_id, type as type from "+GetStitchDLDBUser.getStitchingExtdb()+".stch_work_ext_json WHERE epr_id in ('%s')\n";


    public static String GET_RANDOM_EPR_PROD_EXTENDED_AVAILABILITY =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".product_extended_availability where delete_flag=false order by rand() limit %s\n";


    public static String GET_PROD_EXT_AVAIL_JSON_REC =
            "select json as json, epr_id as epr_id, type as type from "+GetStitchDLDBUser.getStitchingExtdb()+".stch_product_ext_json WHERE epr_id in ('%s') and  extension_type='Availability'\n"; //EPR-M-115H09

    public static String GET_RANDOM_EPR_PROD_EXTENDED_PRICING =
            "select epr_id as epr_id from "+GetStitchDLDBUser.getProdExtDB()+".product_extended_pricing where delete_flag=false order by rand() limit %s\n";


    public static String GET_PROD_EXT_PRICING_REC =
            "select epr_id as epr_id\n" +
                    ",product_type as product_type\n" +
                    ",last_updated_date as last_updated_date\n" +
                    ",price_currency as price_currency \n" +
                    ",price_amount as price_amount\n" +
                    ",price_start_date as price_start_date\n" +
                    ",price_end_date as price_end_date\n" +
                    ",price_region as price_region\n" +
                    ",price_category as price_category\n" +
                    ",price_customer_category as price_customer_category\n" +
                    ",price_purchase_quantity as price_purchase_quantity\n" +
                    " from "+GetStitchDLDBUser.getProdExtDB()+".product_extended_pricing where epr_id in ('%s') and delete_flag=false\n";  //EPR-M-115H09

    public static String GET_PROD_EXT_PRICING_JSON_REC =
            "select json as json, epr_id as epr_id, type as type from "+GetStitchDLDBUser.getStitchingExtdb()+".stch_product_ext_json WHERE epr_id in ('%s') and extension_type='Prices'\n"; //EPR-M-115H09


}




