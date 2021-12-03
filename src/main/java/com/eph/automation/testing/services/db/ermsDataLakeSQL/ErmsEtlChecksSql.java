package com.eph.automation.testing.services.db.ermsDataLakeSQL;


public class ErmsEtlChecksSql {
    private ErmsEtlChecksSql(){
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

    public static final String GET_RANDOM_WORK_IDENTIFIER_ID_INBOUND =
            "select epr_id as epr_id from(\n" +
                    "SELECT\n" +
                    "  \"jnl\".\"u_key\" \"erms_id\"\n" +
                    ", \"jnl\".\"epr_id\"\n" +
                    ", \"jnl\".\"inbound_ts\"\n" +
                    ", \"concat\"(\"jnl\".\"u_key\", \"jnl\".\"epr_id\") \"u_key\"\n" +
                    "FROM\n" +
                    "  ("+GetErmsDbUser.getERMSDataBase()+".erms_journal_current jnl\n" +
                    "LEFT JOIN "+GetErmsDbUser.getProdDataBase()+".gd_wwork w ON (jnl.epr_id = w.work_id))WHERE (w.work_id IS NOT NULL))order by rand() limit %s";

    public static final String GET_WORK_IDENTIFIER_REC_INBOUND_DATA =
            "select epr_id as epr_id" +
                    ",erms_id as erms_id" +
                    ",u_key as u_key" +
                    " from(\n" +
                    "SELECT\n" +
                    "  \"jnl\".\"u_key\" \"erms_id\"\n" +
                    ", \"jnl\".\"epr_id\"\n" +
                    ", \"jnl\".\"inbound_ts\"\n" +
                    ", \"concat\"(\"jnl\".\"u_key\", \"jnl\".\"epr_id\") \"u_key\"\n" +
                    "FROM\n" +
                    "  ("+GetErmsDbUser.getERMSDataBase()+".erms_journal_current jnl\n" +
                    "LEFT JOIN "+GetErmsDbUser.getProdDataBase()+".gd_wwork w ON (jnl.epr_id = w.work_id))WHERE (w.work_id IS NOT NULL))where epr_id in ('%s') order by epr_id desc";

    public static final String GET_WORK_IDENTIFIER_REC_CURRENT_DATA =
            "select epr_id as epr_id" +
                    ",erms_id as erms_id" +
                    ",u_key as u_key" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_work_identifier_v where epr_id in ('%s') order by epr_id desc";

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

    public static final String GET_WORK_PERSON_ROLE_CURRENT_DATA =
            "select eph_work_id as epr_id" +
                    ",u_key as u_key" +
                    ",work_source_ref as work_source_ref" +
                    ",erms_person_ref as erms_person_ref" +
                    ",person_source_ref as person_source_ref" +
                    ",f_role as f_role" +
                    ",email as email" +
                    ",name as name" +
                    ",staff_user as staff_user" +
                    ",effective_start_date as effective_start_date" +
                    ",effective_end_date as effective_end_date" +
                    ",modified_date as modified_date" +
                    ",is_deleted as is_deleted" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_current_work_person_role where eph_work_id in ('%s') order by u_key desc";

    public static final String GET_RANDOM_WORK_PERSON_ID_INBOUND =
            "select eph_work_id as epr_id" +
                    " from(\n" +
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
                    "WHERE (w.work_id IS NOT NULL))order by rand() limit %s";

    public static final String GET_WORK_PERSON_ROLE_INBOUND_DATA =
            "select eph_work_id as epr_id" +
                    ",u_key as u_key" +
                    ",work_source_ref as work_source_ref" +
                    ",erms_person_ref as erms_person_ref" +
                    ",person_source_ref as person_source_ref" +
                    ",f_role as f_role" +
                    ",email as email" +
                    ",name as name" +
                    ",staff_user as staff_user" +
                    ",effective_start_date as effective_start_date" +
                    ",effective_end_date as effective_end_date" +
                    ",modified_date as modified_date" +
                    ",is_deleted as is_deleted" +
                    " from(\n" +
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
                    "WHERE (w.work_id IS NOT NULL))where eph_work_id in ('%s') order by u_key desc";

    public static final String GET_WORK_IDENTIFIER_CURRENT_COUNT =
            "select count(*) as Target_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_work_identifier_v";

    public static final String GET_WORK_PERSON_ROLE_CURRENT_COUNT =
            "select count(*) as Target_Count from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_current_work_person_role";

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

    public static final String GET_RANDOM_WORK_IDENTIFIER_ID_CURRENT =
            "select epr_id as epr_id" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_work_identifier_v order by rand() limit %s";
    public static final String GET_RANDOM_WORK_PERSON_ID_CURRENT =
            "select eph_work_id as epr_id" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_current_work_person_role order by rand() limit %s";

    public static final String GET_WORK_IDENTIFIER_TRANSFORM_FILE_REC =
            "select epr_id as epr_id" +
                    ",erms_id as erms_id" +
                    ",u_key as u_key" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_file_history_work_identifier_part " +
                    "where transform_file_ts=(select max(transform_file_ts) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_file_history_work_identifier_part)" +
                    "and epr_id in ('%s') order by epr_id desc";

    public static final String GET_WORK_PERSON_ROLE_TRANSFORM_FILE_REC =
            "select eph_work_id as epr_id" +
                    ",u_key as u_key" +
                    ",work_source_ref as work_source_ref" +
                    ",erms_person_ref as erms_person_ref" +
                    ",person_source_ref as person_source_ref" +
                    ",f_role as f_role" +
                    ",email as email" +
                    ",name as name" +
                    ",staff_user as staff_user" +
                    ",effective_start_date as effective_start_date" +
                    ",effective_end_date as effective_end_date" +
                    ",modified_date as modified_date" +
                    ",is_deleted as is_deleted" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_file_history_work_person_role_part" +
                    " where transform_file_ts=(select max(transform_file_ts) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_file_history_work_person_role_part)" +
                    " and eph_work_id in ('%s') order by u_key desc";


    public static final String GET_WORK_IDENTIFIER_HIST_PARTITION_REC =
            "select epr_id as epr_id" +
                    ",erms_id as erms_id" +
                    ",u_key as u_key" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_identifier_part " +
                    "where delete_flag=false and transform_ts=(select max(transform_ts) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_identifier_part)" +
                    "and epr_id in ('%s') order by epr_id desc";

    public static final String GET_WORK_PERSON_ROLE_HIST_PARTITION_REC =
            "select eph_work_id as epr_id" +
                    ",u_key as u_key" +
                    ",work_source_ref as work_source_ref" +
                    ",erms_person_ref as erms_person_ref" +
                    ",person_source_ref as person_source_ref" +
                    ",f_role as f_role" +
                    ",email as email" +
                    ",name as name" +
                    ",staff_user as staff_user" +
                    ",effective_start_date as effective_start_date" +
                    ",effective_end_date as effective_end_date" +
                    ",modified_date as modified_date" +
                    ",is_deleted as is_deleted" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_person_role_part" +
                    " where delete_flag=false and transform_ts=(select max(transform_ts) from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_person_role_part)" +
                    " and eph_work_id in ('%s') order by u_key desc";

    public static final String GET_RANDOM_WORK_IDENTIFIER_ID_LATEST =
            "select epr_id as epr_id" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_latest_work_identifier order by rand() limit %s";
    public static final String GET_RANDOM_WORK_PERSON_ID_LATEST =
            "select eph_work_id as epr_id" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_latest_work_person_role order by rand() limit %s";

    public static final String GET_WORK_IDENTIFIER_DELTA_AND_EXCL_REC =
            "select epr_id as epr_id" +
                    ",erms_id as erms_id" +
                    ",u_key as u_key" +
                    " from \n" +
                    "(select c.epr_id,c.erms_id,c.u_key from "+ GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_identifier_excl_delta as c union all \n" +
                    "select d.epr_id,d.erms_id,d.u_key from "+ GetErmsDbUser.getERMSDataBase()+".erms_delta_current_work_identifier as d)" +
                    " where epr_id in ('%s') order by epr_id desc";

    public static final String GET_WORK_PERSON_ROLE_DELTA_AND_EXCL_REC =
            "select eph_work_id as epr_id" +
                    ",u_key as u_key" +
                    ",work_source_ref as work_source_ref" +
                    ",erms_person_ref as erms_person_ref" +
                    ",person_source_ref as person_source_ref" +
                    ",f_role as f_role" +
                    ",email as email" +
                    ",name as name" +
                    ",staff_user as staff_user" +
                    ",effective_start_date as effective_start_date" +
                    ",effective_end_date as effective_end_date" +
                    ",modified_date as modified_date" +
                    ",is_deleted as is_deleted" +
                    " from \n" +
                    "(select c.u_key,c.eph_work_id,c.work_source_ref,c.erms_person_ref,c.person_source_ref,c.f_role,c.email,c.name,c.staff_user,c.effective_start_date,c.effective_end_date,c.modified_date,c.is_deleted from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_history_work_person_role_excl_delta as c union all \n" +
                    "select d.u_key,d.eph_work_id,d.work_source_ref,d.erms_person_ref,d.person_source_ref,d.f_role,d.email,d.name,d.staff_user,d.effective_start_date,d.effective_end_date,d.modified_date,d.is_deleted from "+GetErmsDbUser.getERMSDataBase()+".erms_delta_current_work_person_role as d)" +
                    " where eph_work_id in ('%s') order by u_key desc";


    public static final String GET_WORK_IDENTIFIER_REC_LATEST_DATA =
            "select epr_id as epr_id" +
                    ",erms_id as erms_id" +
                    ",u_key as u_key" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_latest_work_identifier where epr_id in ('%s') order by epr_id desc";

    public static final String GET_WORK_PERSON_ROLE_LATEST_DATA =
            "select eph_work_id as epr_id" +
                    ",u_key as u_key" +
                    ",work_source_ref as work_source_ref" +
                    ",erms_person_ref as erms_person_ref" +
                    ",person_source_ref as person_source_ref" +
                    ",f_role as f_role" +
                    ",email as email" +
                    ",name as name" +
                    ",staff_user as staff_user" +
                    ",effective_start_date as effective_start_date" +
                    ",effective_end_date as effective_end_date" +
                    ",modified_date as modified_date" +
                    ",is_deleted as is_deleted" +
                    " from "+GetErmsDbUser.getERMSDataBase()+".erms_transform_latest_work_person_role where eph_work_id in ('%s') order by u_key desc";




}




