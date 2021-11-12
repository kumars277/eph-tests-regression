package com.eph.automation.testing.services.db.ermsDataLakeSQL;


import com.eph.automation.testing.services.db.ermsDataLakeSQL.GetErmsDbUser;

public class ErmsEtlCountChecksSql {
    private ErmsEtlCountChecksSql(){
        throw new IllegalStateException("Utility class");
    }

    public static final String GET_WORK_IDENTIFIER_INBOUND_COUNT =
            "select count(*) as Source_Count from(\n" +
                "SELECT\n" +
                    "  \"jnl\".\"u_key\" \"erms_id\"\n" +
                    ", \"jnl\".\"epr_id\"\n" +
                    ", \"jnl\".\"inbound_ts\"\n" +
                    ", \"concat\"(\"jnl\".\"u_key\", \"jnl\".\"epr_id\") \"u_key\"\n" +
                    "FROM\n" +
                    "  ("+GetErmsDbUser.getERMSDataBase()+".erms_journal_current jnl\n" +
                    "LEFT JOIN "+GetErmsDbUser.getProdDataBase()+".gd_wwork w ON (jnl.epr_id = w.work_id))WHERE (w.work_id IS NOT NULL))";

    public static final String GET_WORK_PERSON_ROLE_INBOUND_COUNT =
            "select count(*) as Source_Count from(\n" +
            "SELECT\n" +
                    "  \"concat\"(\"jnl\".\"u_key\", 'PAECP', \"wd\".\"sourceref\") u_key\n" +
                    ", \"jnl\".\"u_key\" work_source_ref\n" +
                    ", \"jnl\".\"epr_id\" eph_work_id\n" +
                    ", \"jnl\".\"PAECP\" erms_person_ref\n" +
                    ", \"wd\".\"sourceref\" person_source_ref\n" +
                    ", 'PAECP' f_role\n" +
                    ", \"cnt\".\"email\"\n" +
                    ", \"cnt\".\"name\"\n" +
                    ", \"cnt\".\"staff_user\"\n" +
                    ", \"cjr\".\"effective_start_date\"\n" +
                    ", \"cjr\".\"effective_end_date\"\n" +
                    ", \"cjr\".\"modified_date\"\n" +
                    ", \"cjr\".\"is_deleted\"\n" +
                    ", \"jnl\".\"inbound_ts\"\n" +
                    "FROM\n" +
                    "  (((("+GetErmsDbUser.getERMSDataBase()+".erms_journal_current jnl\n" +
                    "INNER JOIN "+GetErmsDbUser.getERMSDataBase()+".erms_contactjournalrel_current cjr ON (((jnl.u_key = cjr.journal_ref) AND (jnl.PAECP = cjr.contact_ref)) AND (cjr.role_type = 'Publishing Assistant Editorial Contracts & Payment')))\n" +
                    "INNER JOIN "+GetErmsDbUser.getERMSDataBase()+".erms_contact_current cnt ON ((cjr.contact_ref = cnt.contact_id) AND (cnt.contact_id = jnl.PAECP)))\n" +
                    "INNER JOIN "+GetErmsDbUser.getProdStagingDataBase()+".workday_reference_v wd ON (wd.email = cnt.email))\n" +
                    "LEFT JOIN  "+GetErmsDbUser.getProdDataBase()+".gd_wwork w ON (jnl.epr_id = w.work_id))\n" +
                    "WHERE (w.work_id IS NOT NULL)\n" +
                    "UNION SELECT\n" +
                    "  \"concat\"(\"jnl\".\"u_key\", 'PCS', \"wd\".\"sourceref\") u_key\n" +
                    ", \"jnl\".\"u_key\" work_source_ref\n" +
                    ", \"jnl\".\"epr_id\" eph_work_id\n" +
                    ", \"jnl\".\"PCS\" erms_person_ref\n" +
                    ", \"wd\".\"sourceref\" person_source_ref\n" +
                    ", 'PCS' f_role\n" +
                    ", \"cnt\".\"email\"\n" +
                    ", \"cnt\".\"name\"\n" +
                    ", \"cnt\".\"staff_user\"\n" +
                    ", \"cjr\".\"effective_start_date\"\n" +
                    ", \"cjr\".\"effective_end_date\"\n" +
                    ", \"cjr\".\"modified_date\"\n" +
                    ", \"cjr\".\"is_deleted\"\n" +
                    ", \"jnl\".\"inbound_ts\"\n" +
                    "FROM\n" +
                    "  (((("+GetErmsDbUser.getERMSDataBase()+".erms_journal_current jnl\n" +
                    "INNER JOIN "+GetErmsDbUser.getERMSDataBase()+".erms_contactjournalrel_current cjr ON (((jnl.u_key = cjr.journal_ref) AND (jnl.PCS = cjr.contact_ref)) AND (cjr.role_type = 'Publishing Content Specialist')))\n" +
                    "INNER JOIN "+GetErmsDbUser.getERMSDataBase()+".erms_contact_current cnt ON ((cjr.contact_ref = cnt.contact_id) AND (cnt.contact_id = jnl.PCS)))\n" +
                    "INNER JOIN "+GetErmsDbUser.getProdStagingDataBase()+".workday_reference_v wd ON (wd.email = cnt.email))\n" +
                    "LEFT JOIN "+GetErmsDbUser.getProdDataBase()+".gd_wwork w ON (jnl.epr_id = w.work_id))\n" +
                    "WHERE (w.work_id IS NOT NULL)\n" +
                    "UNION SELECT\n" +
                    "  \"concat\"(\"jnl\".\"u_key\", 'PSM', \"wd\".\"sourceref\") u_key\n" +
                    ", \"jnl\".\"u_key\" work_source_ref\n" +
                    ", \"jnl\".\"epr_id\" eph_work_id\n" +
                    ", \"jnl\".\"PSM\" erms_person_ref\n" +
                    ", \"wd\".\"sourceref\" person_source_ref\n" +
                    ", 'PSM' f_role\n" +
                    ", \"cnt\".\"email\"\n" +
                    ", \"cnt\".\"name\"\n" +
                    ", \"cnt\".\"staff_user\"\n" +
                    ", \"cjr\".\"effective_start_date\"\n" +
                    ", \"cjr\".\"effective_end_date\"\n" +
                    ", \"cjr\".\"modified_date\"\n" +
                    ", \"cjr\".\"is_deleted\"\n" +
                    ", \"jnl\".\"inbound_ts\"\n" +
                    "FROM\n" +
                    "  (((("+GetErmsDbUser.getERMSDataBase()+".erms_journal_current jnl\n" +
                    "INNER JOIN  "+GetErmsDbUser.getERMSDataBase()+".erms_contactjournalrel_current cjr ON (((jnl.u_key = cjr.journal_ref) AND (jnl.PSM = cjr.contact_ref)) AND (cjr.role_type = 'Publishing Support Manager')))\n" +
                    "INNER JOIN  "+GetErmsDbUser.getERMSDataBase()+".erms_contact_current cnt ON ((cjr.contact_ref = cnt.contact_id) AND (cnt.contact_id = jnl.PSM)))\n" +
                    "INNER JOIN  "+GetErmsDbUser.getProdStagingDataBase()+".workday_reference_v wd ON (wd.email = cnt.email))\n" +
                    "LEFT JOIN   "+GetErmsDbUser.getProdDataBase()+".gd_wwork w ON (jnl.epr_id = w.work_id))\n" +
                    "WHERE (w.work_id IS NOT NULL)\n" +
                    "UNION SELECT\n" +
                    "  \"concat\"(\"jnl\".\"u_key\", 'MCM', \"wd\".\"sourceref\") u_key\n" +
                    ", \"jnl\".\"u_key\" work_source_ref\n" +
                    ", \"jnl\".\"epr_id\" eph_work_id\n" +
                    ", \"jnl\".\"MCM\" erms_person_ref\n" +
                    ", \"wd\".\"sourceref\" person_source_ref\n" +
                    ", 'MCM' f_role\n" +
                    ", \"cnt\".\"email\"\n" +
                    ", \"cnt\".\"name\"\n" +
                    ", \"cnt\".\"staff_user\"\n" +
                    ", \"cjr\".\"effective_start_date\"\n" +
                    ", \"cjr\".\"effective_end_date\"\n" +
                    ", \"cjr\".\"modified_date\"\n" +
                    ", \"cjr\".\"is_deleted\"\n" +
                    ", \"jnl\".\"inbound_ts\"\n" +
                    "FROM\n" +
                    "  (((("+GetErmsDbUser.getERMSDataBase()+".erms_journal_current jnl\n" +
                    "INNER JOIN "+GetErmsDbUser.getERMSDataBase()+".erms_contactjournalrel_current cjr ON (((jnl.u_key = cjr.journal_ref) AND (jnl.MCM = cjr.contact_ref)) AND (cjr.role_type = 'Marketing Communications Manager')))\n" +
                    "INNER JOIN "+GetErmsDbUser.getERMSDataBase()+".erms_contact_current cnt ON ((cjr.contact_ref = cnt.contact_id) AND (cnt.contact_id = jnl.MCM)))\n" +
                    "INNER JOIN "+GetErmsDbUser.getProdStagingDataBase()+".workday_reference_v wd ON (wd.email = cnt.email))\n" +
                    "LEFT JOIN  "+GetErmsDbUser.getProdDataBase()+".gd_wwork w ON (jnl.epr_id = w.work_id))\n" +
                    "WHERE (w.work_id IS NOT NULL))";

    public static final String GET_WORK_IDENTIFIER_CURRENT_COUNT =
            " select count(*) as Target_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_work_identifier_v";

    public static final String GET_WORK_PERSON_ROLE_CURRENT_COUNT =
            " select count(*) as Target_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_current_work_person_role";

    public static final String GET_WORK_IDENTIFIER_TRANSFORM_FILE_COUNT =
            "select count(*) as Source_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_file_history_work_identifier_part where transform_file_ts=(select max(transform_file_ts) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_file_history_work_identifier_part)";

    public static final String GET_WORK_PERSON_ROLE_TRANSFORM_FILE_COUNT =
            "select count(*) as Source_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_file_history_work_person_role_part where transform_file_ts=(select max(transform_file_ts) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_file_history_work_person_role_part)";

    public static final String GET_WORK_IDENTIFIER_TRANSFORM_PARTITION_COUNT =
            "select count(*) as Source_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_identifier_part where delete_flag=false and transform_ts=(select max(transform_ts) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_identifier_part)";

    public static final String GET_WORK_PERSON_ROLE_TRANSFORM_PARTITION_COUNT =
            "select count(*) as Source_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_person_role_part where delete_flag=false and transform_ts=(select max(transform_ts) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_person_role_part)";

    public static final String GET_WORK_IDENTIFIER_LATEST_COUNT =
            " select count(*) as Target_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_latest_work_identifier";

    public static final String GET_WORK_PERSON_ROLE_LATEST_COUNT =
            " select count(*) as Target_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_latest_work_person_role";


    public static final String GET_WORK_IDENTIFIER_DELTA_AND_EXCL_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.epr_id from "+ GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_identifier_excl_delta as c union all \n" +
                    "select d.epr_id from "+ GetErmsDbUser.getERMSDataBase()+".erms_delta_current_work_identifier as d)";

    public static final String GET_WORK_PERSON_ROLE_DELTA_AND_EXCL_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_person_role_excl_delta as c union all \n" +
                    "select d.u_key from "+GetErmsDbUser.getERMSDataBase()+".erms_delta_current_work_person_role as d)";

    public static final String GET_DUPLICATES_LATEST_WORK_IDENTIFIER_COUNT =
            "select count(*) as Duplicate_Count from (select count(*) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_latest_work_identifier where delete_flag=false group by u_key having count()>1 )";

    public static final String GET_DUPLICATES_LATEST_WORK_PERSON_ROLE_COUNT =
            "select count(*) as Duplicate_Count from (select count(*) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_latest_work_person_role where delete_flag=false group by u_key having count()>1 )";

}




