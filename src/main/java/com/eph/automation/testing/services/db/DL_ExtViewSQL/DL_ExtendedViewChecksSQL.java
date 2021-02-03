package com.eph.automation.testing.services.db.DL_ExtViewSQL;


import com.eph.automation.testing.services.db.BCS_ETLCoreSQL.GetBCS_ETLCoreDLDBUser;

public class DL_ExtendedViewChecksSQL {




    public static String GET_DL_PROD_EXT_AVAILABILITY_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".product_extended_availability";

    public static String GET_DL_PROD_EXT_PRICING_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".product_extended_pricing";

    public static String GET_DL_MANIF_EXT_RESTRICT_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".manifestation_extended_restriction";

     public static String GET_DL_MANIF_EXT_PAGE_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".manifestation_extended_page_count";

    public static String GET_DL_MANIF_EXT_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".manifestation_extended";

    public static String GET_DL_WORK_EXT_COUNT =
               "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".work_extended";

    public static String GET_DL_WORK_EXT_EDIT_COUNT =
           "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".work_extended_editorial_board";

    public static String GET_DL_WORK_EXT_METRIC_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".work_extended_metric";

    public static String GET_DL_WORK_EXT_PERS_ROLE_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".work_extended_person_role";

    public static String GET_DL_WORK_EXT_RELATION_SIBLING_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".work_extended_relationship_sibling";

    public static String GET_DL_WORK_EXT_SUBJ_AREA_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".work_extended_subject_area";

    public static String GET_DL_WORK_EXT_URL_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getDL_ExtViewDataBase()+".work_extended_url";


    public static String GET_DL_ALL_PROD_EXT_AVAILABILITY_COUNT =
    "select count(*) as Source_Count from(\n"+
            "SELECT DISTINCT\n"+
            "epr_id epr_id\n"+
            ", product_type product_type\n"+
            ", greatest(last_updated_date) last_updated_date\n"+
            ", application_name application_name\n"+
            ", delta_answer_code_uk delta_answer_code_uk\n"+
            ", delta_answer_code_us delta_answer_code_us\n"+
            ", publication_status_anz publication_status_anz\n"+
            ", availability_format availability_format\n"+
            ", availability_start_date availability_start_date\n"+
            ", availability_status availability_status\n"+
            ", delete_flag delete_flag\n"+
            " from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".product_availability_extended_allsource_v\n"+
            "where product_type in ('OOA','SUB'))";

    public static String GET_DL_ALL_PROD_EXT_PRICING_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT DISTINCT epr_id, product_type, last_updated_date, price_currency, \n" +
                    "price_amount, price_start_date, price_end_date, price_region, price_category, \n" +
                    "price_customer_category, price_purchase_quantity, delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".product_extended_pricing_allsource_v)\n";

    public static String GET_DL_ALL_MANIF_EXT_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT DISTINCT epr_id, manifestation_type, last_updated_date\n" +
                    ", uk_textbook_ind, us_textbook_ind, manifestation_trim_text\n" +
                    ", commodity_code, discount_code_emea, discount_code_us, manifestation_weight\n" +
                    ", journal_prod_site_code, journal_issue_trim_size, war_reference, delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".manifestation_extended_allsource_v)\n";

    public static String GET_DL_ALL_MANIF_EXT_PAGE_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT DISTINCT epr_id, manifestation_type, last_updated_date, \n" +
                    "count_type_code, count_type_name, count, delete_flag\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".manifestation_extended_page_count_allsource_v)";

    public static String GET_DL_ALL_MANIF_EXT_RESTRICT_COUNT =
            "select count(*) as Source_Count from(\n" +
            "SELECT DISTINCT epr_id, manifestation_type," +
                    " last_updated_date, restriction_code, " +
                    "restriction_name, delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".manifestation_extended_restriction_allsource_v)";

    public static String GET_DL_ALL_WORK_EXT_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT DISTINCT\n" +
                    "epr_id epr_id\n" +
                    ", max(work_type) work_type\n" +
                    ", max(last_updated_date) last_updated_date\n" +
                    ", max(journal_els_com_ind) journal_els_com_ind\n" +
                    ", max(journs_aims_scope) journs_aims_scope\n" +
                    ", max(delta_business_unit) delta_business_unit\n" +
                    ", max(image_file_ref) image_file_ref\n" +
                    ", max(master_isbn) master_isbn\n" +
                    ", max(author_by_line_text) author_by_line_text\n" +
                    ", max(key_features) key_features\n" +
                    ", max(product_awards) product_awards\n" +
                    ", max(product_long_desc) product_long_desc\n" +
                    ", max(product_short_desc) product_short_desc\n" +
                    ", max(review_quotes) review_quotes\n" +
                    ", max(toc_long) toc_long\n" +
                    ", max(toc_short) toc_short\n" +
                    ", max(audience_text) audience_text\n" +
                    ", max(book_sub_business_unit) book_sub_business_unit\n" +
                    ", max(internal_els_div) internal_els_div\n" +
                    ", max(profit_centre) profit_centre\n" +
                    ", max(text_ref_trade) text_ref_trade\n" +
                    ", max(primary_site_system) primary_site_system\n" +
                    ", max(primary_site_acronym) primary_site_acronym\n" +
                    ", max(primary_site_support_level) primary_site_support_level\n" +
                    ", max(issue_prod_type_code) issue_prod_type_code\n" +
                    ", max(catalogue_volumes_qty) catalogue_volumes_qty\n" +
                    ", max(catalogue_issues_qty) catalogue_issues_qty\n" +
                    ", max(catalogue_volume_from) catalogue_volume_from\n" +
                    ", max(catalogue_volume_to) catalogue_volume_to\n" +
                    ", max(rf_issues_qty) rf_issues_qty\n" +
                    ", max(rf_total_pages_qty) rf_total_pages_qty\n" +
                    ", max(rf_fvi) rf_fvi\n" +
                    ", max(rf_lvi) rf_lvi\n" +
                    ", max(business_unit_desc) business_unit_desc\n" +
                    ", case when sum(cast(delete_flag as integer)) = count(cast(delete_flag as integer)) then true else false end as delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_allsource_v\n" +
                    "group by epr_id)";

    public static String GET_DL_ALL_WORK_EXT_METRIC_COUNT =
            "select count(*) as Source_Count from(\n" +
            "SELECT DISTINCT\n" +
                    "epr_id, work_type, last_updated_date, metric_code, metric_name, metric, metric_year, metric_url, delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_metric_allsource_v)";

    public static String GET_DL_ALL_WORK_EXT_PERS_ROLE_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT DISTINCT\n" +
                    "epr_id, work_type, last_updated_date, role_code, " +
                    "role_name, sequence_number, group_number, first_name, last_name, " +
                    "peoplehub_id, email, title, honours, affiliation, image_url, footnote_txt, notes_txt, delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_person_role_allsource_v)";

    public static String GET_DL_ALL_WORK_EXT_RELATION_SIBLING_COUNT =
            "select count(*) as Source_Count from(\n" +
                    "SELECT DISTINCT\n" +
                    "epr_id, work_type, last_updated_date, related_epr_id, related_title, " +
                    "related_type_code, related_type_name, related_type_roll_up, related_status_code, related_status_name, " +
                    "related_status_roll_up, relationship_code, relationship_name, delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_relationship_sibling_allsource_v)";

    public static String GET_DL_ALL_WORK_EXT_SUBJ_AREA_COUNT =
            "select count(*) as Source_Count from(\n" +
            "SELECT DISTINCT\n" +
                    "epr_id, work_type, last_updated_date, code, name, priority, type_code, type_name, delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_subject_area_allsource_v)";

    public static String GET_DL_ALL_WORK_EXT_URL_COUNT =
            "select count(*) as Source_Count from(\n" +
            "SELECT DISTINCT\n" +
                    "epr_id, work_type, last_updated_date, url_type_code, url_type_name, url, url_title, delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_url_allsource_v)";

    public static String GET_DL_ALL_PROD_EXT_AVAILABILITY_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".product_availability_extended_allsource_v";

    public static String GET_DL_ALL_PROD_EXT_PRICING_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".product_extended_pricing_allsource_v";

    public static String GET_DL_ALL_MANIF_EXT_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".manifestation_extended_allsource_v";

    public static String    GET_DL_ALL_MANIF_EXT_PAGE_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".manifestation_extended_page_count_allsource_v";

    public static String GET_DL_ALL_MANIF_EXT_RESTRICT_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".manifestation_extended_restriction_allsource_v";

    public static String GET_DL_ALL_WORK_EXT_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_allsource_v";

    public static String GET_DL_ALL_WORK_EXT_EDIT_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_editorial_board_allsource_v";

    public static String GET_DL_ALL_WORK_EXT_METRIC_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_metric_allsource_v";

    public static String GET_DL_ALL_WORK_EXT_PERS_ROLE_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_person_role_allsource_v";

    public static String GET_DL_ALL_WORK_EXT_RELATION_SIBLING_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_relationship_sibling_allsource_v";

    public static String GET_DL_ALL_WORK_EXT_SUBJ_AREA_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_subject_area_allsource_v";

    public static String GET_DL_ALL_WORK_EXT_URL_VIEW_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".work_extended_url_allsource_v";


    public static String GET_SOURCE_PROD_EXT_AVAILABILITY_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT \n" +
                    "sdrm.epr_id\n" +
                    ", 'SDRM' as source\n" +
                    ", sdrm.product_type\n" +
                    ", sdrm.last_updated_date\n" +
                    ", CAST('SDRM' AS varchar) as application_name\n" +
                    ", CAST(null AS varchar) as delta_answer_code_uk\n" +
                    ", CAST(null AS varchar) as delta_answer_code_us\n" +
                    ", CAST(null AS varchar) as publication_status_anz\n" +
                    ", sdrm.rendition_format as availability_format\n" +
                    ", sdrm.production_date as availability_start_date\n" +
                    ", CAST('Available' AS varchar) as availability_status\n" +
                    ", sdrm.delete_flag \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getSDRMDataBase()+".sdrm_transform_latest_product_availability sdrm\n" +
                    "UNION ALL\n" +
                    "\n" +
                    "SELECT\n" +
                    "bcs.eprid as epr_id\n" +
                    ", 'BCS' as source\n" +
                    ", bcs.producttype as product_type\n" +
                    ", bcs.last_updated_date\n" +
                    ", bcs.application as application_name\n" +
                    ", bcs.deltaanswercodeuk as delta_answer_code_uk\n" +
                    ", bcs.deltaanswercodeus as delta_answer_code_us\n" +
                    ", bcs.anzpubstatus as publication_status_anz\n" +
                    ", CAST(null as varchar) as availability_format\n" +
                    ", bcs.pubdateactual as availability_start_date\n" +
                    ", bcs.status as availability_status\n" +
                    ", bcs.delete_flag\n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_latest bcs\n" +
                    "UNION ALL\n" +
                    "\n" +
                    "select\n" +
                    "jnf.epr_id\n" +
                    ", 'JM' as source\n" +
                    ", jnf.product_type\n" +
                    ", jnf.last_updated_date\n" +
                    ", jnf.application_name\n" +
                    ", CAST(null AS varchar) as delta_answer_code_uk\n" +
                    ", CAST(null AS varchar) as delta_answer_code_us\n" +
                    ", CAST(null AS varchar) as publication_status_anz\n" +
                    ", CAST(null AS varchar) as availability_format\n" +
                    ", jnf.availability_start_date\n" +
                    ", jnf.availability_status\n" +
                    ", jnf.delete_flag\n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".jnl_new_fulfilment_system jnf\n" +
                    "UNION ALL\n" +
                    "\n" +
                    "SELECT\n" +
                    "jfs.epr_id as epr_id\n" +
                    ", 'JM_FIXED' as source\n" +
                    ", jfs.product_type\n" +
                    ", jfs.last_updated_date\n" +
                    ", jfs.application_code        as application_name\n" +
                    ", CAST(null AS varchar)       as delta_answer_code_uk\n" +
                    ", CAST(null AS varchar)       as delta_answer_code_us\n" +
                    ", CAST(null AS varchar)       as publication_status_anz\n" +
                    ", CAST(null AS varchar)       as availability_format\n" +
                    ", jfs.availability_start_date\n" +
                    ", jfs.availability_status\n" +
                    ", jfs.delete_flag\n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getJM_CoreDataBase()+".jnl_fulfilment_system_v jfs)";

    public static String GET_SOURCE_PROD_PRICING_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT \n" +
                    "  promis.epr_id\n" +
                    ", 'PROMIS'                 as source\n" +
                    ", promis.product_type\n" +
                    ", promis.last_updated_date\n" +
                    ", promis.currency          as price_currency\n" +
                    ", promis.price             as price_amount\n" +
                    ", promis.start_date        as price_start_date\n" +
                    ", promis.end_date          as price_end_date\n" +
                    ", promis.region            as price_region\n" +
                    ", CAST(null AS varchar)       price_category\n" +
                    ", promis.customer_category as price_customer_category\n" +
                    ", promis.quantity          as price_purchase_quantity\n" +
                    ", promis.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getPromisDataBase()+".promis_transform_latest_pricing promis\n" +
                    "  where (promis.end_date is null or promis.end_date >= current_date)\n" +
                    "UNION ALL\n" +
                    "SELECT\n" +
                    "  bcs.eprid                 as epr_id\n" +
                    ", 'BCS'                     as source\n" +
                    ", bcs.product_type\n" +
                    ", bcs.last_updated_date\n" +
                    ", bcs.pricecurrency         as price_currency\n" +
                    ", bcs.priceamount           as price_amount\n" +
                    ", bcs.pricestartdate        as price_start_date\n" +
                    ", bcs.priceenddate          as price_end_date\n" +
                    ", bcs.priceregion           as price_region\n" +
                    ", bcs.pricecategory         as price_category\n" +
                    ", bcs.pricecustomercategory as price_customer_category\n" +
                    ", bcs.pricepurchasequantity as price_purchase_quantity\n" +
                    ", bcs.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_latest bcs\n" +
                    "  where (bcs.priceenddate is null or bcs.priceenddate >= current_date))";

    public static String GET_SOURCE_MANIF_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT \n" +
                    "  jrbi.epr as epr_id\n" +
                    ", 'JRBI' as source\n" +
                    ", jrbi.manifestation_type\n" +
                    ", jrbi.last_updated_date\n" +
                    ", CAST(null AS Boolean) uk_textbook_ind\n" +
                    ", CAST(null AS Boolean) us_textbook_ind\n" +
                    ", CAST(null AS varchar) manifestation_trim_text\n" +
                    ", CAST(null AS varchar) commodity_code\n" +
                    ", CAST(null AS varchar) discount_code_emea\n" +
                    ", CAST(null AS varchar) discount_code_us\n" +
                    ", CAST(null AS Double) manifestation_weight\n" +
                    ", jrbi.journal_issue_trim_size\n" +
                    ", jrbi.war_reference\n" +
                    ", jrbi.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_manifestation jrbi\n" +
                    "UNION ALL\n" +
                    "SELECT\n" +
                    "  bcs.eprid as epr_id\n" +
                    ", 'BCS' as source\n" +
                    ", bcs.manifestation_type\n" +
                    ", bcs.last_updated_date\n" +
                    ", bcs.uktextbookind as uk_textbook_ind\n" +
                    ", bcs.ustextbookind as us_textbook_ind\n" +
                    ", bcs.trimsize as manifestation_trim_text\n" +
                    ", bcs.commcode as commodity_code\n" +
                    ", bcs.emeadiscountcode as discount_code_emea\n" +
                    ", bcs.usdiscountcode as discount_code_us\n" +
                    ", CAST(null AS double) as manifestation_weight\n" +
                    ", bcs.journalissuetrimsize as journal_issue_trim_size\n" +
                    ", bcs.warreference as war_reference\n" +
                    ", bcs.delete_flag\n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_latest bcs)";

    public static String GET_SOURCE_MANIF_PAGE_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT\n" +
                    "  bcs.eprid as epr_id\n" +
                    ", 'BCS' as source\n" +
                    ", bcs.manifestation_type\n" +
                    ", bcs.last_updated_date\n" +
                    ", bcs.pagecounttypecode as count_type_code\n" +
                    ", bcs.pagecounttypename as count_type_name\n" +
                    ", bcs.pagecount as count\n" +
                    ", bcs.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_latest bcs\n" +
                    "  WHERE bcs.pagecount <> 0)";

    public static String GET_SOURCE_MANIF_RESTRICT_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT\n" +
                    "  bcs.eprid as epr_id\n" +
                    ", 'BCS' as source\n" +
                    ", bcs.manifestation_type\n" +
                    ", bcs.last_updated_date\n" +
                    ", bcs.restrictioncode as restriction_code\n" +
                    ", bcs.restrictionname as restriction_name\n" +
                    ", bcs.delete_flag\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_latest bcs)";

    public static String GET_SOURCE_WORK_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT \n" +
                    "  jrbi.epr as epr_id\n" +
                    ", 'JRBI' as source\n" +
                    ", jrbi.work_type as work_type\n" +
                    ", jrbi.last_updated_date\n" +
                    ", CAST(null AS Boolean) journal_els_com_ind\n" +
                    ", CAST(null AS varchar) journs_aims_scope\n" +
                    ", CAST(null AS varchar) delta_business_unit\n" +
                    ", CAST(null AS varchar) image_file_ref\n" +
                    ", CAST(null AS varchar) master_isbn\n" +
                    ", CAST(null AS varchar) author_by_line_text\n" +
                    ", CAST(null AS varchar) key_features\n" +
                    ", CAST(null AS varchar) product_awards\n" +
                    ", CAST(null AS varchar) product_long_desc\n" +
                    ", CAST(null AS varchar) product_short_desc\n" +
                    ", CAST(null AS varchar) review_quotes\n" +
                    ", CAST(null AS varchar) toc_long\n" +
                    ", CAST(null AS varchar) toc_short\n" +
                    ", CAST(null AS varchar) audience_text\n" +
                    ", CAST(null AS varchar) book_sub_business_unit\n" +
                    ", CAST(null AS varchar) internal_els_div\n" +
                    ", CAST(null AS varchar) profit_centre\n" +
                    ", CAST(null AS varchar) text_ref_trade\n" +
                    ", case when delete_flag=false then jrbi.primary_site_system else null end as primary_site_system\n" +
                    ", case when delete_flag=false then jrbi.primary_site_acronym else null end as primary_site_acronym\n" +
                    ", case when delete_flag=false then jrbi.primary_site_support_level else null end as primary_site_support_level\n" +
                    ", case when delete_flag=false then jrbi.issue_prod_type_code else null end as issue_prod_type_code\n" +
                    ", case when delete_flag=false then jrbi.catalogue_volumes_qty else null end as catalogue_volumes_qty\n" +
                    ", case when delete_flag=false then jrbi.catalogue_issues_qty else null end as catalogue_issues_qty\n" +
                    ", case when delete_flag=false then jrbi.catalogue_volume_from else null end as catalogue_volume_from\n" +
                    ", case when delete_flag=false then jrbi.catalogue_volume_to else null end as catalogue_volume_to\n" +
                    ", case when delete_flag=false then jrbi.rf_issues_qty else null end as rf_issues_qty\n" +
                    ", case when delete_flag=false then jrbi.rf_total_pages_qty else null end as rf_total_pages_qty\n" +
                    ", case when delete_flag=false then jrbi.rf_fvi else null end as rf_fvi\n" +
                    ", case when delete_flag=false then jrbi.rf_lvi else null end as rf_lvi\n" +
                    ", case when delete_flag=false then jrbi.business_unit_desc else null end as business_unit_desc\n" +
                    ", case when delete_flag=false then jrbi.journal_prod_site_code else null end as journal_prod_site_code\n" +
                    ", jrbi.delete_flag\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_work jrbi\n" +
                    "UNION ALL\n" +
                    "SELECT \n" +
                    "  promis.epr_id as epr_id\n" +
                    ", 'PROMIS' as source\n" +
                    ", promis.work_type as work_type\n" +
                    ", promis.last_updated_date\n" +
                    ", case when delete_flag=false then (case when promis.elsevier_com_ind = 'N' then false when promis.elsevier_com_ind = 'Y' then true end) else null end as journal_els_com_ind\n" +
                    ", case when delete_flag=false then promis.journal_aims_scope else null end as journs_aims_scope \n" +
                    ", CAST(null AS varchar) delta_business_unit\n" +
                    ", CAST(null AS varchar) image_file_ref\n" +
                    ", CAST(null AS varchar) master_isbn\n" +
                    ", CAST(null AS varchar) author_by_line_text\n" +
                    ", CAST(null AS varchar) key_features\n" +
                    ", CAST(null AS varchar) product_awards\n" +
                    ", CAST(null AS varchar) product_long_desc\n" +
                    ", CAST(null AS varchar) product_short_desc\n" +
                    ", CAST(null AS varchar) review_quotes\n" +
                    ", CAST(null AS varchar) toc_long\n" +
                    ", CAST(null AS varchar) toc_short\n" +
                    ", CAST(null AS varchar) audience_text\n" +
                    ", CAST(null AS varchar) book_sub_business_unit\n" +
                    ", CAST(internal_elsevier_division AS varchar) internal_els_div\n" +
                    ", CAST(null AS varchar) profit_centre\n" +
                    ", CAST(null AS varchar) text_ref_trade\n" +
                    ", CAST(null AS varchar) primary_site_system\n" +
                    ", CAST(null AS varchar) primary_site_acronym\n" +
                    ", CAST(null AS varchar) primary_site_support_level\n" +
                    ", CAST(null AS varchar) issue_prod_type_code\n" +
                    ", CAST(null AS integer) catalogue_volumes_qty\n" +
                    ", CAST(null AS integer) catalogue_issues_qty\n" +
                    ", CAST(null AS varchar) catalogue_volume_from\n" +
                    ", CAST(null AS varchar) catalogue_volume_to\n" +
                    ", CAST(null AS integer) rf_issues_qty\n" +
                    ", CAST(null AS integer) rf_total_pages_qty\n" +
                    ", CAST(null AS varchar) rf_fvi\n" +
                    ", CAST(null AS varchar) rf_lvi\n" +
                    ", CAST(null AS varchar) business_unit_desc\n" +
                    ", CAST(null AS varchar) journal_prod_site_code\n" +
                    ", promis.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getPromisDataBase()+".promis_transform_latest_works promis\n" +
                    "UNION ALL\n" +
                    "SELECT\n" +
                    "  bcs.eprid as epr_id\n" +
                    ", 'BCS' as source\n" +
                    ", bcs.work_type\n" +
                    ", bcs.last_updated_date\n" +
                    ", CAST(null AS Boolean) journal_els_com_ind\n" +
                    ", CAST(null AS varchar) journs_aims_scope\n" +
                    ", CAST(null AS varchar) delta_business_unit\n" +
                    ", bcs.imagefileref as image_file_ref\n" +
                    ", bcs.workmasterisbn as master_isbn\n" +
                    ", bcs.authorbyline as author_by_line_text\n" +
                    ", bcs.features as key_features\n" +
                    ", bcs.awards as product_awards\n" +
                    ", bcs.description as product_long_desc\n" +
                    ", CAST(null AS varchar) product_short_desc\n" +
                    ", bcs.review as review_quotes\n" +
                    ", bcs.toc_long\n" +
                    ", bcs.toc_short\n" +
                    ", bcs.audience as audience_text\n" +
                    ", bcs.sbu as book_sub_business_unit\n" +
                    ", bcs.companygroup as internal_els_div\n" +
                    ", bcs.profitcentre as profit_centre\n" +
                    ", bcs.textreftrade as text_ref_trade\n" +
                    ", CAST(null AS varchar) primary_site_system\n" +
                    ", CAST(null AS varchar) primary_site_acronym\n" +
                    ", CAST(null AS varchar) primary_site_support_level\n" +
                    ", CAST(null AS varchar) issue_prod_type_code\n" +
                    ", CAST(null AS integer) catalogue_volumes_qty\n" +
                    ", CAST(null AS integer) catalogue_issues_qty\n" +
                    ", CAST(null AS varchar) catalogue_volume_from\n" +
                    ", CAST(null AS varchar) catalogue_volume_to\n" +
                    ", CAST(null AS integer) rf_issues_qty\n" +
                    ", CAST(null AS integer) rf_total_pages_qty\n" +
                    ", CAST(null AS varchar) rf_fvi\n" +
                    ", CAST(null AS varchar) rf_lvi\n" +
                    ", CAST(null AS varchar) business_unit_desc\n" +
                    ", CAST(null AS varchar) journal_prod_site_code\n" +
                    ", bcs.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_latest bcs\n" +
                    "  UNION ALL\n" +
                    "SELECT\n" +
                    "f_wwork work_epr_id\n" +
                    ", 'EPH (Derived)' source\n" +
                    ", gww.f_type work_type\n" +
                    ", gwi.b_upddate last_updated_date\n" +
                    ", CAST(null AS boolean) journal_els_com_ind\n" +
                    ", CAST(null AS varchar) journs_aims_scope\n" +
                    ", CAST(null AS varchar) delta_business_unit\n" +
                    ", concat(concat('http://secure-ecsd.elsevier.com/covers/80/Tango2/large/', s_identifier), '.jpg') image_file_ref\n" +
                    ", CAST(null AS varchar) master_isbn\n" +
                    ", CAST(null AS varchar) author_by_line_text\n" +
                    ", CAST(null AS varchar) key_features\n" +
                    ", CAST(null AS varchar) product_awards\n" +
                    ", CAST(null AS varchar) product_long_desc\n" +
                    ", CAST(null AS varchar) product_short_desc\n" +
                    ", CAST(null AS varchar) review_quotes\n" +
                    ", CAST(null AS varchar) toc_long\n" +
                    ", CAST(null AS varchar) toc_short\n" +
                    ", CAST(null AS varchar) audience_text\n" +
                    ", CAST(null AS varchar) book_sub_business_unit\n" +
                    ", CAST(null AS varchar) internal_els_div\n" +
                    ", CAST(null AS varchar) profit_centre\n" +
                    ", CAST(null AS varchar) text_ref_trade\n" +
                    ", CAST(null AS varchar) primary_site_system\n" +
                    ", CAST(null AS varchar) primary_site_acronym\n" +
                    ", CAST(null AS varchar) primary_site_support_level\n" +
                    ", CAST(null AS varchar) issue_prod_type_code\n" +
                    ", CAST(null AS integer) catalogue_volumes_qty\n" +
                    ", CAST(null AS integer) catalogue_issues_qty\n" +
                    ", CAST(null AS varchar) catalogue_volume_from\n" +
                    ", CAST(null AS varchar) catalogue_volume_to\n" +
                    ", CAST(null AS integer) rf_issues_qty\n" +
                    ", CAST(null AS integer) rf_total_pages_qty\n" +
                    ", CAST(null AS varchar) rf_fvi\n" +
                    ", CAST(null AS varchar) rf_lvi\n" +
                    ", CAST(null AS varchar) business_unit_desc\n" +
                    ", CAST(null AS varchar) journal_prod_site_code\n" +
                    ", false delete_flag\n" +
                    " FROM (("+GetBCS_ETLCoreDLDBUser.getProdDataBase()+".gd_work_identifier gwi\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getProdDataBase()+".gd_wwork gww ON (gwi.f_wwork = gww.work_id))\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getProdDataBase()+".gd_x_lov_work_type glw ON (gww.f_type = glw.code))\n" +
                    "WHERE (((gwi.f_type = 'ISSN-L') AND (glw.roll_up_type = 'Journal')) AND (effective_end_date IS NULL)))";

    public static String GET_SOURCE_WORK_EDITORIAL_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT\n" +
                    "  promis.epr_id as epr_id\n" +
                    ", 'PROMIS' as source\n" +
                    ", promis.work_type as work_type\n" +
                    ", promis.last_updated_date\n" +
                    ", 'EBM' as role_code\n" +
                    ", promis.role_description as role_name\n" +
                    ", promis.sequence_number\n" +
                    ", promis.group_number\n" +
                    ", promis.initials as first_name\n" +
                    ", promis.last_name\n" +
                    ", promis.title\n" +
                    ", promis.honours\n" +
                    ", promis.affiliation\n" +
                    ", promis.image_url\n" +
                    ", promis.footnote_txt\n" +
                    ", promis.notes_txt\n" +
                    ", promis.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getPromisDataBase()+".promis_transform_latest_person_roles promis)";

    public static String GET_SOURCE_WORK_METRIC_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT\n" +
                    "  promis.epr_id as epr_id\n" +
                    ", 'PROMIS' as source\n" +
                    ", promis.work_type as work_type\n" +
                    ", promis.last_updated_date\n" +
                    ", promis.metric_code\n" +
                    ", promis.metric_name\n" +
                    ", promis.metric\n" +
                    ", TRY(CAST (promis.metric_year as integer)) as metric_year\n" +
                    ", promis.metric_url\n" +
                    ", promis.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getPromisDataBase()+".promis_transform_latest_metrics promis)";

    public static String GET_SOURCE_WORK_PERSON_ROLE_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT\n" +
                    "  jrbi.epr as epr_id\n" +
                    ", 'JRBI' as source\n" +
                    ", jrbi.work_type as work_type\n" +
                    ", CAST(null AS integer) as core_work_person_role_id\n" +
                    ", jrbi.last_updated_date\n" +
                    ", jrbi.role_code\n" +
                    ", jrbi.role_description as role_name\n" +
                    ", CAST(null AS integer) sequence_number\n" +
                    ", CAST(null AS integer) group_number\n" +
                    ", jrbi.given_name as first_name\n" +
                    ", jrbi.family_name as last_name\n" +
                    ", jrbi.peoplehub_id\n" +
                    ", jrbi.email\n" +
                    ", CAST(null AS varchar) title\n" +
                    ", CAST(null AS varchar) honours\n" +
                    ", CAST(null AS varchar) affiliation\n" +
                    ", CAST(null AS varchar) image_url\n" +
                    ", CAST(null AS varchar) footnote_txt\n" +
                    ", CAST(null AS varchar) notes_txt\n" +
                    ", jrbi.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getJRBIDataBase()+".jrbi_transform_latest_person jrbi\n" +
                    "union all\n" +
                    "SELECT\n" +
                    "  bcs.eprid as epr_id\n" +
                    ", 'BCS' as source\n" +
                    ", bcs.work_type as work_type\n" +
                    ", CAST(bcs.core_reference AS integer) as core_work_person_role_id\n" +
                    ", bcs.last_updated_date\n" +
                    ", bcs.roletype as role_code\n" +
                    ", bcs.rolename as role_name\n" +
                    ", CAST(bcs.sequence AS integer) as sequence_number\n" +
                    ", bcs.groupnumber as group_number\n" +
                    ", bcs.person_first_name as first_name\n" +
                    ", bcs.person_family_name as last_name\n" +
                    ", CAST(null AS varchar) as peoplehub_id\n" +
                    ", bcs.email_address as email\n" +
                    ", bcs.title\n" +
                    ", bcs.honours\n" +
                    ", bcs.affiliation\n" +
                    ", bcs.imageurl as image_url\n" +
                    ", bcs.footnotetxt as footnote_txt\n" +
                    ", bcs.notestxt as notes_txt\n" +
                    ", bcs.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_latest bcs)";

    public static String GET_SOURCE_WORK_RELATIONSHIP_SIBLING_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT\n" +
                    "  promis.parent_epr_id                as epr_id\n" +
                    ", 'PROMIS'                            as source\n" +
                    ", promis.work_type                    as work_type\n" +
                    ", promis.last_updated_date\n" +
                    ", promis.child_epr_id                 as related_epr_id\n" +
                    ", promis.child_title                   related_title\n" +
                    ", promis.child_related_type_code       related_type_code\n" +
                    ", promis.child_related_type_name       related_type_name\n" +
                    ", promis.child_related_type_roll_up    related_type_roll_up\n" +
                    ", promis.child_related_status_code     related_status_code\n" +
                    ", promis.child_related_status_name     related_status_name\n" +
                    ", promis.child_related_status_roll_up  related_status_roll_up\n" +
                    ", promis.relationship_type_code       as relationship_code\n" +
                    ", promis.relationship_type_name       as relationship_name\n" +
                    ", promis.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getPromisDataBase()+".promis_transform_latest_work_rels promis)";

    public static String GET_SOURCE_WORK_SUBJ_AREA_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT \n" +
                    "  promis.epr_id                    as epr_id\n" +
                    ", 'PROMIS'                         as source\n" +
                    ", promis.work_type                 as work_type\n" +
                    ", promis.last_updated_date\n" +
                    ", promis.subject_area_code         as code\n" +
                    ", promis.subject_area_name         as name\n" +
                    ", CAST(promis.priority as varchar) as priority\n" +
                    ", promis.subject_type_code         as type_code\n" +
                    ", promis.subject_type_name         as type_name\n" +
                    ", promis.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getPromisDataBase()+".promis_transform_latest_subject_areas promis\n" +
                    "UNION ALL\n" +
                    "SELECT\n" +
                    "  bcs.eprid                        as epr_id\n" +
                    ", 'BCS'                            as source\n" +
                    ", bcs.work_type                    as work_type\n" +
                    ", bcs.last_updated_date\n" +
                    ", bcs.subjcode                     as code\n" +
                    ", bcs.subjdesc                     as name\n" +
                    ", CAST(bcs.priority as varchar)    as priority\n" +
                    ", bcs.typecode                     as type_code\n" +
                    ", bcs.typedesc                     as type_name\n" +
                    ", bcs.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_latest bcs)";

    public static String GET_SOURCE_WORK_URL_EXT_COUNT =
            "select count(*) as Source_Count from (\n" +
                    "SELECT \n" +
                    "  promis.epr_id            as epr_id\n" +
                    ", 'PROMIS'                 as source\n" +
                    ", promis.work_type         as work_type\n" +
                    ", promis.last_updated_date\n" +
                    ", promis.url_code          as url_type_code\n" +
                    ", promis.url_name          as url_type_name\n" +
                    ", promis.url\n" +
                    ", promis.url_title\n" +
                    ", promis.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getPromisDataBase()+".promis_transform_latest_urls promis\n" +
                    "UNION ALL\n" +
                    "SELECT\n" +
                    "  bcs.eprid                as epr_id\n" +
                    ", 'BCS'                    as source\n" +
                    ", bcs.work_type\n" +
                    ", bcs.last_updated_date\n" +
                    ", bcs.urltypecode          as url_type_code\n" +
                    ", bcs.urltypename          as url_type_name\n" +
                    ", bcs.source               as url\n" +
                    ", bcs.name                 as url_title\n" +
                    ", bcs.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_latest bcs\n" +
                    "UNION ALL\n" +
                    "SELECT\n" +
                    "  sdbooks.epr_id             as epr_id\n" +
                    ", 'SDBOOKS'                  as source\n" +
                    ", sdbooks.work_type          as work_type\n" +
                    ", sdbooks.last_updated_date\n" +
                    ", sdbooks.url_code           as url_type_code\n" +
                    ", sdbooks.url_name           as url_type_name\n" +
                    ", sdbooks.url\n" +
                    ", sdbooks.url_title\n" +
                    ", sdbooks.delete_flag\n" +
                    " FROM "+GetBCS_ETLCoreDLDBUser.getSDBooksDataBase()+".sdbooks_transform_latest_urls sdbooks)";


}




