package features.zOnHold.znotused;

import com.eph.automation.testing.services.db.sql.GetEPHDBUser;

public class DQErrorChecksSQL {

    public static String GET_LAST_ETL_ID = "select etl_id as etl_id from "+ GetEPHDBUser.getDBUser()+".etl_run \n" +
            " where loadset_code='10_PMX_FULL' \n" +
            " and end_datetime is not null\n" +
            "order by end_datetime desc\n" +
            "limit 1";

    public static String GET_DQ_FAIL_RECORDS = "select * from "+GetEPHDBUser.getDBUser()+".dq_fail_record where row_id = '%s' and etl_id = '%s'";

    public static String GET_RANDOM_RECORDS_FAILED_WORKS_DQ = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq where dq_err='Y'" +
            " ORDER BY RANDOM() limit PARAM1";

    public static String GET_EPH_DATA_CHILD_ENTITIES="select distinct work_id as eph_id from semarchy_eph_mdm.sa_wwork ww\n" +
            "left join semarchy_eph_mdm.sa_work_financial_attribs fa on fa.f_wwork=ww.work_id\n" +
            "left join semarchy_eph_mdm.sa_work_identifier wi on wi.f_wwork=ww.work_id\n" +
            "left join semarchy_eph_mdm.sa_work_relationship_mirror mir on mir.f_original=ww.work_id\n" +
            "left join semarchy_eph_mdm.sa_work_subject_area_link sub on sub.f_wwork=ww.work_id\n" +
            "left join semarchy_eph_mdm.sa_manifestation ma on ma.f_wwork=ww.work_id\n" +
            "left join semarchy_eph_mdm.sa_product pr on pr.f_wwork=ww.work_id\n" +
            "left join semarchy_eph_mdm.sa_work_rel_translation tr on tr.f_translated=ww.work_id\n" +
            "where ww.work_id in ('%s')";

    public static String GET_MANIFESTATION_DQ = "select dq_err as dq_err, pmx_source_reference as pmx_source" +
            " from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq where " +
            " f_wwork in ('%s')";

    public static String GET_Product_DQ_Linked_work = "select dq_err as dq_err, pmx_source_reference as pmx_source" +
            " from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product_dq where " +
            " ult_work_ref in ('%s')";

    public static String GET_All_FAILED_WORKS_DQ = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq where dq_err='Y'";

    public static String GET_All_FAILED_MANIFESTATIONS_DQ = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq where dq_err='Y'";

    public static String GET_All_FAILED_Products = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product_dq where dq_err='Y'";

    public static String GET_EPH_ID = "select eph_id as id from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid\n" +
            "where ref_type = '%s' and source_ref in ('%s')";

    public static String GET_RANDOM_RECORDS_FAILED_MANIFESTATIONS_DQ = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq where dq_err='Y'" +
            " ORDER BY RANDOM() limit PARAM1";

    public static String GET_EPH_Manifestation_ID = "select eph_id as manifestation_id from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_ephid\n" +
            "where ref_type = 'MANIFESTATION' and source_ref in ('%s')";

    public static String GET_Product_DQ_Linked_manifestation = "select dq_err as dq_err, pmx_source_reference as pmx_source" +
            " from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product_dq where " +
            " f_manifestation_source_ref in ('%s')";

    public static String GET_RANDOM_DATA_FAILED_PERSONS_DQ = "select person_source_ref as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_person_dq where person_family_name is null" +
            " ORDER BY RANDOM() limit PARAM1";

    public static String GET_RANDOM_DATA_FAILED_WORKS_DQ_NO_TITLE = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_%s_dq where %s is null\n" +
            "order by RANDOM() limit '%s'";

    public static String GET_RANDOM_DATA_FAILED_RECORDS = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_%s_dq where %s = 'UNK'\n" +
            "order by RANDOM() limit '%s'";

    public static String GET_RANDOM_PRODUCTS_FAILED_REC_END_DATE = "SELECT pmx_source_reference as pmx_id\n" +
            "FROM "+GetEPHDBUser.getDBUser()+".stg_10_pmx_product_dq p\n" +
            "left join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork w on case when p.f_type in ('PKG','JAS','OAA') then substring(p.pmx_source_reference,0,length(p.pmx_source_reference)-4) else '' end = w.\"PRODUCT_WORK_ID\"::varchar\n" +
            "left join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation m on case when p.f_type not in ('PKG','JAS','OAA') then substring(p.pmx_source_reference,0,length(p.pmx_source_reference)-4) else '' end = m.\"PRODUCT_MANIFESTATION_ID\"::varchar\n" +
            "where coalesce(m.\"RECORD_END_DATE\", w.\"RECORD_END_DATE\") <= '2019-07-08' \n" +
            "order by random() limit '%s'";

    public static String GET_RANDOM_WORKS_FAILED_REC_END_DATE = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork_dq join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_wwork w on \n" +
            "stg_10_pmx_wwork_dq.pmx_source_reference = w.\"PRODUCT_WORK_ID\" where w.\"RECORD_END_DATE\" <= '2019-07-08' \n" +
            "order by random() limit '%s'";

    public static String GET_RANDOM_MANIFESTATIONS_FAILED_REC_END_DATE = "select pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation_dq dq join "+GetEPHDBUser.getDBUser()+".stg_10_pmx_manifestation w on \n" +
            "dq.pmx_source_reference = w.\"MANIFESTATION_ID\" where w.\"RECORD_END_DATE\" <= '2019-07-08'\n" +
            "order by random() limit '%s'";

    public static String GET_DQ_STATUS_PERSONS = "select dq_err as dq_err, person_source_ref as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_person_dq where " +
            " person_source_ref in ('%s')";

    public static String GET_DQ_STATUS = "select dq_err as dq_err, pmx_source_reference as pmx_id from "+GetEPHDBUser.getDBUser()+".stg_10_pmx_%s_dq where " +
            " pmx_source_reference in ('%s')";

    public static String GET_EPH_ID_PERSONS = "select id as eph_id from "+GetEPHDBUser.getDBUser()+".map_sourceref_2_numericid where " +
            " source_ref = 'PARAM1'";

    public static String GET_PERSONS_SA = "select * from semarchy_eph_mdm.sa_person where external_reference in ('%s')";




}
