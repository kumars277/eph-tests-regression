package com.eph.automation.testing.services.db.sql;

public class DQErrorChecksSQL {

    public static String GET_FAILED_DATA = "select pmx_source_reference as pmx_id from ephsit_talend_owner.stg_10_pmx_wwork_dq where dq_err='Y'" +
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
            " from ephsit_talend_owner.stg_10_pmx_manifestation_dq where " +
            " f_wwork in ('%s')";

    public static String GET_Product_DQ_Linked_work = "select dq_err as dq_err, pmx_source_reference as pmx_source" +
            " from ephsit_talend_owner.stg_10_pmx_product_dq where " +
            " f_work_source_ref in ('%s')";

    public static String GET_All_FAILED_DATA = "select pmx_source_reference as pmx_id from ephsit_talend_owner.stg_10_pmx_wwork_dq where dq_err='Y'";

    public static String GET_All_FAILED_Manifestations = "select pmx_source_reference as pmx_id from ephsit_talend_owner.stg_10_pmx_manifestation_dq where dq_err='Y'";

    public static String GET_All_FAILED_Products = "select pmx_source_reference as pmx_id from ephsit_talend_owner.stg_10_pmx_product_dq where dq_err='Y'";

    public static String GET_EPH_ID = "select eph_id as work_id from ephsit_talend_owner.map_sourceref_2_ephid\n" +
            "where ref_type = 'WORK' and source_ref in ('%s')";

    public static String GET_FAILED_Manifestation_DATA = "select pmx_source_reference as pmx_id from ephsit_talend_owner.stg_10_pmx_manifestation_dq where dq_err='Y'" +
            " ORDER BY RANDOM() limit PARAM1";

    public static String GET_EPH_Manifestation_ID = "select eph_id as manifestation_id from ephsit_talend_owner.map_sourceref_2_ephid\n" +
            "where ref_type = 'MANIFESTATION' and source_ref in ('%s')";

    public static String GET_Product_DQ_Linked_manifestation = "select dq_err as dq_err, pmx_source_reference as pmx_source" +
            " from ephsit_talend_owner.stg_10_pmx_product_dq where " +
            " f_manifestation_source_ref in ('%s')";

    public static String GET_FAILED_Person_DATA = "select person_source_ref as pmx_id from ephsit_talend_owner.stg_10_pmx_person_dq where person_family_name is null" +
            " ORDER BY RANDOM() limit PARAM1";

    public static String GET_DQ_Status_Person = "select dq_err as dq_err, person_source_ref as pmx_source from ephsit_talend_owner.stg_10_pmx_person_dq where " +
            " person_source_ref in ('%s')";

    public static String GET_Person_EPH_ID = "select id as eph_id from ephsit_talend_owner.map_sourceref_2_numericid where " +
            " source_ref = 'PARAM1'";
}
