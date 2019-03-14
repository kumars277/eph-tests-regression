package com.eph.automation.testing.services.db.sql;

public class UpdateProductSQL {

    public static String SUBMIT_LOAD_FUNCTION_CALL = "{? = call semarchy_repository.submit_load(?,?,?,?)}";


    public static String Update_product = "UPDATE ephsit.semarchy_eph_mdm.sa_product SET b_loadid='LOADID', f_type='SUB', f_event='EVENT' WHERE product_id='EPR-10T5VN'";

    public static String Insert_work = "INSERT INTO ephsit.semarchy_eph_mdm.sa_wwork (b_loadid, work_id, b_classname, " +
            "work_title,f_type,f_status,f_pmx,f_oa_type,f_imprint,f_event)\n" +
            " VALUES ('LOADID','EPR-W-1025F9','Work','Tetrahedron Letters test','JNL','WAS','541','H','ELSEVIER','EVENT')";

    public static String Insert_product = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event)\n" +
            " VALUES ('LOADID','EPR-10VNBR','Product','Tetrahedron Letters Online (Online)  Back Files test'," +
            "'BKF','PAS','EPR-M-10NYCG','EVENT')";

    public static String Insert_manifestation = "INSERT INTO ephsit.semarchy_eph_mdm.sa_manifestation (b_loadid, manifestation_id, b_classname," +
            " manifestation_key_title,f_type,f_status,f_wowrk,f_event)\n" +
            " VALUES ('LOADID','EPR-M-10R71F','Manifestation','Tetrahedron Letters (Print) test'," +
            "'JPR','MPU','EPR-W-1025F9','EVENT')";
}
