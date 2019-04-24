package com.eph.automation.testing.services.db.sql;

public class UpdateProductSQL {

    public static String SUBMIT_LOAD_FUNCTION_CALL = "{? = call semarchy_repository.submit_load(?,?,?,?)}";


    public static String Update_product = "UPDATE ephsit.semarchy_eph_mdm.sa_product SET b_loadid='LOADID', f_type='SUB', f_event='EVENT' WHERE product_id='EPR-10T5VN'";

    // Test data - Work
    public static String Insert_work = "INSERT INTO ephsit.semarchy_eph_mdm.sa_wwork (b_loadid, work_id, b_classname, " +
            "work_title,f_type,f_status,f_pmc,f_oa_type,f_imprint,f_event,electro_rights_indicator,volume)\n" +
            " VALUES ('LOADID','EPR-W-TSTW01','Work','Test - Tetrahedron Letters','JNL','WAS','541','H','ELSEVIER','EVENT','false','0')";

    // Test data - Manifestation
    public static String Insert_manifestation = "INSERT INTO ephsit.semarchy_eph_mdm.sa_manifestation (b_loadid, manifestation_id, b_classname," +
            " manifestation_key_title,f_type,f_status,f_wwork,f_event)\n" +
            " VALUES ('LOADID','EPR-M-TSTM02','Manifestation','Test - Tetrahedron Letters (Online)'," +
            "'JEL','MST','EPR-W-TSTW01','EVENT')";

    public static String Insert_manifestation_1 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_manifestation (b_loadid, manifestation_id, b_classname," +
            " manifestation_key_title,f_type,f_status,f_wwork,f_event,inter_edition_flag)\n" +
            " VALUES ('LOADID','EPR-M-TSTM01','Manifestation','Test - Tetrahedron Letters (Print)'," +
            "'JPR','MPU','EPR-W-TSTW01','EVENT','false')";

    public static String Insert_product = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP03','Product','Test - Tetrahedron Letters (Online)  Back Files'," +
            "'BKF','PAS','EPR-M-TSTM02','EVENT','S001','SUB','true','false')";

    public static String Insert_product_1 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_wwork, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP01','Product','Test - Tetrahedron Letters  Author Charges'," +
            "'JAS','PAS','EPR-W-TSTW01','EVENT','S001','ONE','true','false')";

    public static String Insert_product_2 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_wwork, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP02','Product','Test - Tetrahedron Letters  Open Access'," +
            "'OAA','PAS','EPR-W-TSTW01','EVENT','S001','EVE','true','false')";

    public static String Insert_product_4 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP04','Product','Test - Tetrahedron Letters (Online) Subscription'," +
            "'SUB','PST','EPR-M-TSTM02','EVENT','S001','SUB','true','false')";

    public static String Insert_product_5 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP05','Product','Test - Tetrahedron Letters (Print)  Reprints'," +
            "'RPR','PAS','EPR-M-TSTM01','EVENT','G003','ONE','true','false')";

    public static String Insert_product_6 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP06','Product','Test - Tetrahedron Letters (Print) Subscription'," +
            "'SUB','PAS','EPR-M-TSTM01','EVENT','G003','EVE','true','false')";

    public static String Insert_product_7 = "INSERT INTO ephsit.semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP07','Product','Test - Tetrahedron Letters (Print) Bulk Sales'," +
            "'JBS','PAS','EPR-M-TSTM01','EVENT','G003','ONE','true','false')";

    public static String Insert_work_identifier= "INSERT INTO ephsit.semarchy_eph_mdm.sa_work_identifier (b_loadid, work_identifier_id, b_classname, identifier," +
            " f_type,f_wwork, f_event)\n" +
            " VALUES ('LOADID','9999999999','WorkIdentifier','99999999999'," +
            "'PROJECT-NUM','EPR-W-TSTW01','EVENT')";

    public static String Insert_manifestation_identifier= "INSERT INTO ephsit.semarchy_eph_mdm.sa_manifestation_identifier (b_loadid, manif_identifier_id, b_classname, identifier," +
            " f_type,f_manifestation, f_event)\n" +
            " VALUES ('LOADID','88888888888','ManifestationIdentifier','8888-8888'," +
            "'ISSN','EPR-M-TSTM02','EVENT')";
}
