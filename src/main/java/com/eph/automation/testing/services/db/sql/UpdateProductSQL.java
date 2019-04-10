package com.eph.automation.testing.services.db.sql;

public class UpdateProductSQL {

    public static String SUBMIT_LOAD_FUNCTION_CALL = "{? = call semarchy_repository.submit_load(?,?,?,?)}";


    public static String Update_product = "UPDATE ephsit.semarchy_eph_mdm.sa_product SET b_loadid='LOADID', f_type='SUB', f_event='EVENT' WHERE product_id='EPR-10T5VN'";

    public static String Insert_work = "INSERT INTO ephsit.semarchy_eph_mdm.sa_wwork (b_loadid, work_id, b_classname, " +
            "work_title,f_type,f_status,f_pmc,f_oa_type,f_imprint,f_event)\n" +
            " VALUES ('LOADID','EPR-W-TSTW01','Work','Tetrahedron Letters test','JNL','WAS','541','H','ELSEVIER','EVENT')";

    public static String Insert_product = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event)\n" +
            " VALUES ('LOADID','EPR-TSTP03','Product','Test Product Letters Online'," +
            "'BKF','PAS','EPR-M-TSTM02','EVENT')";

    public static String Insert_manifestation = "INSERT INTO ephsit.semarchy_eph_mdm.sa_manifestation (b_loadid, manifestation_id, b_classname," +
            " manifestation_key_title,f_type,f_status,f_wwork,f_event)\n" +
            " VALUES ('LOADID','EPR-M-TSTM02','ManifestationApiObject','Tetrahedron Letters (Print) test'," +
            "'JPR','MPU','EPR-W-TSTW01','EVENT')";

    public static String Insert_manifestation_1 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_manifestation (b_loadid, manifestation_id, b_classname," +
            " manifestation_key_title,f_type,f_status,f_wwork,f_event)\n" +
            " VALUES ('LOADID','EPR-M-TSTM01','ManifestationApiObject','Tetrahedron Letters (Print) test'," +
            "'JPR','MPU','EPR-W-TSTW01','EVENT')";

    public static String Insert_product_1 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_wwork, f_event)\n" +
            " VALUES ('LOADID','EPR-TSTP01','Product','Test Product Letters Online'," +
            "'BKF','PAS','EPR-W-TSTW01','EVENT')";

    public static String Insert_product_2 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_wwork, f_event)\n" +
            " VALUES ('LOADID','EPR-TSTP02','Product','Test Product Letters Online'," +
            "'BKF','PAS','EPR-W-TSTW01','EVENT')";

    public static String Insert_product_4 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event)\n" +
            " VALUES ('LOADID','EPR-TSTP04','Product','Test Product Letters Online'," +
            "'SUB','PAS','EPR-M-TSTM02','EVENT')";

    public static String Insert_product_5 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event)\n" +
            " VALUES ('LOADID','EPR-TSTP05','Product','Test Product Letters Online'," +
            "'RPR','PAS','EPR-M-TSTM01','EVENT')";

    public static String Insert_product_6 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event)\n" +
            " VALUES ('LOADID','EPR-TSTP06','Product','Test Product Letters Online'," +
            "'SUB','PAS','EPR-M-TSTM01','EVENT')";

    public static String Insert_product_7 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event)\n" +
            " VALUES ('LOADID','EPR-TSTP07','Product','Test Product Letters Online'," +
            "'JBS','PAS','EPR-M-TSTM01','EVENT')";
}
