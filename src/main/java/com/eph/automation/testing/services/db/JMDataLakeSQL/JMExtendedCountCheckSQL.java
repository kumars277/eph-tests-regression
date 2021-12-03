package com.eph.automation.testing.services.db.JMDataLakeSQL;

import static com.eph.automation.testing.services.db.JMDataLakeSQL.GetJMDLDBUser.getJMDataBase;

public class JMExtendedCountCheckSQL {
//    jm Count checks
//    jm Count
    public static String GET_JMExtendedCount = "select count(*) as TOTAL_COUNT from (\n" +
            "select\n" +
            "jnf.epr_id\n" +
            ", 'jm' as source\n" +
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
            "from "+ GetJMDLDBUser.getJMDB()+ ".jnl_new_fulfilment_system jnf\n" +
            "UNION ALL\n" +
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
            "from "+ GetJMDLDBUser.getJMDB()+ ".jnl_fulfilment_system_v jfs)";

//  Product Extended Count
    public static String GET_Product_Extended = "select count(*) as TOTAL_COUNT from "+GetJMDLDBUser.getProdStagingDataBase()+".product_availability_extended_allsource_v where \"source\" in ('JM','JM_FIXED')";

//    jm Extended data checks
    public static String GET_JM_EXTENDED_IDs = "select epr_id as EPR_ID from (\n" +
            "select\n" +
            "jnf.epr_id\n" +
            ", 'jm' as source\n" +
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
            "from "+ GetJMDLDBUser.getJMDB()+ ".jnl_new_fulfilment_system_v jnf\n" +
            "UNION ALL\n" +
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
            "from "+ GetJMDLDBUser.getJMDB()+ ".jnl_fulfilment_system_v jfs) limit 5";

    public static String GET_JM_EXTENDED_RECORDS = "select * from (\n" +
            "select\n" +
            "jnf.epr_id\n" +
            ", 'jm' as source\n" +
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
            "from "+ GetJMDLDBUser.getJMDB()+".jnl_new_fulfilment_system_v jnf\n" +
            "UNION ALL\n" +
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
            "from "+ GetJMDLDBUser.getJMDB()+".jnl_fulfilment_system_v jfs) where epr_id in ('%s')";

    public static String GET_PRODUCT_EXTENDED_RECORDS = "select * from product_staging_database_uat2.product_availability_extended_allsource_v where \"source\" in ('JM','JM_FIXED') and epr_id in ('%s')";
}
