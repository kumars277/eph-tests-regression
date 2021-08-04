package features.zOnHold.znotused;

public class UpdateProductSQL {

    public static String SUBMIT_LOAD_FUNCTION_CALL = "{? = call semarchy_repository.submit_load(?,?,?,?)}";


    public static String Update_product = "UPDATE semarchy_eph_mdm.sa_product SET b_loadid='LOADID', f_type='SUB', f_event='EVENT' WHERE product_id='EPR-10T5VN'";

    // Test data - Work
    public static String Insert_work = "INSERT INTO semarchy_eph_mdm.sa_wwork (b_loadid, work_id, b_classname, " +
            "work_title,f_type,f_status,f_pmc,f_oa_type,f_imprint,f_event,electro_rights_indicator,volume)\n" +
            " VALUES ('LOADID','EPR-W-TSTW01','Work','Test - Tetrahedron Letters','JNL','WDI','541','H','ELSEVIER','EVENT','false','0')";

    // Translation for the work
    public static String Insert_work_1 = "INSERT INTO semarchy_eph_mdm.sa_wwork(b_loadid, work_id, b_classname,work_title,f_type,f_status,f_pmc,f_oa_type," +
            "f_imprint,f_event,electro_rights_indicator,volume)\n" +
            " VALUES ('LOADID','EPR-W-TSTW02','Work','Test - de Tétrahédrón Léttérs','JNL','WLA','200','H','MOSBY','EVENT','false','0')";

    // Mirror for the work
    public static String Insert_work_2 = "INSERT INTO semarchy_eph_mdm.sa_wwork(b_loadid, work_id, b_classname,work_title,f_type,f_status,f_pmc,f_oa_type,f_imprint," +
            "f_event,electro_rights_indicator,volume)\n" +
            " VALUES ('LOADID','EPR-W-TSTW03','Work','Test - Tetrahedron Letters: X','JNL','WDI','541','F','MOSBY','EVENT','false','0')";

    // Test data - Manifestation
    public static String Insert_manifestation = "INSERT INTO semarchy_eph_mdm.sa_manifestation (b_loadid, manifestation_id, b_classname," +
            " manifestation_key_title,f_type,f_status,f_wwork,f_event)\n" +
            " VALUES ('LOADID','EPR-M-TSTM02','Manifestation','Test - Tetrahedron Letters (Online)'," +
            "'JEL','MST','EPR-W-TSTW01','EVENT')";

    public static String Insert_manifestation_1 = "INSERT INTO semarchy_eph_mdm.sa_manifestation (b_loadid, manifestation_id, b_classname," +
            " manifestation_key_title,f_type,f_status,f_wwork,f_event,inter_edition_flag)\n" +
            " VALUES ('LOADID','EPR-M-TSTM01','Manifestation','Test - Tetrahedron Letters (Print)'," +
            "'JPR','MPU','EPR-W-TSTW01','EVENT','false')";

    public static String Insert_product = "INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP03','Product','Test - Tetrahedron Letters (Online)  Back Files'," +
            "'BKF','PAS','EPR-M-TSTM02','EVENT','S001','SUB','true','false')";

    public static String Insert_product_1 = "INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_wwork, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP01','Product','Test - Tetrahedron Letters  Author Charges'," +
            "'JAS','PAS','EPR-W-TSTW01','EVENT','S001','ONE','true','false')";

    public static String Insert_product_2 = "INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_wwork, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP02','Product','Test - Tetrahedron Letters  Open Access'," +
            "'OAA','PAS','EPR-W-TSTW01','EVENT','S001','EVE','true','false')";

    public static String Insert_product_4 = "INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP04','Product','Test - Tetrahedron Letters (Online) Subscription'," +
            "'SUB','PST','EPR-M-TSTM02','EVENT','S001','SUB','true','false')";

    public static String Insert_product_5 = "INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP05','Product','Test - Tetrahedron Letters (Print)  Reprints'," +
            "'RPR','PAS','EPR-M-TSTM01','EVENT','G003','ONE','true','false')";

    public static String Insert_product_6 = "INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP06','Product','Test - Tetrahedron Letters (Print) Subscription'," +
            "'SUB','PAS','EPR-M-TSTM01','EVENT','G003','EVE','true','false')";

    public static String Insert_product_7 = "INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name," +
            " f_type, f_status,f_manifestation, f_event,f_tax_code,f_revenue_model,separately_sale_indicator,trial_allowed_indicator)\n" +
            " VALUES ('LOADID','EPR-TSTP07','Product','Test - Tetrahedron Letters (Print) Bulk Sales'," +
            "'JBS','PAS','EPR-M-TSTM01','EVENT','G003','ONE','true','false')";

    public static String Insert_work_identifier= "INSERT INTO semarchy_eph_mdm.sa_work_identifier (b_loadid, work_identifier_id, b_classname, identifier," +
            " f_type,f_wwork, f_event)\n" +
            " VALUES ('LOADID','9999999999','WorkIdentifier','99999999999'," +
            "'PROJECT-NUM','EPR-W-TSTW01','EVENT')";

    public static String Insert_manifestation_identifier= "INSERT INTO semarchy_eph_mdm.sa_manifestation_identifier (b_loadid, manif_identifier_id, b_classname, identifier," +
            " f_type,f_manifestation, f_event)\n" +
            " VALUES ('LOADID','88888888888','ManifestationIdentifier','8888-8888'," +
            "'ISSN','EPR-M-TSTM02','EVENT')";

    //updated the child work/translated work id
    public static String Insert_work_translation= "INSERT INTO semarchy_eph_mdm.sa_work_relationship (b_loadid, work_relationship_id, b_classname, effective_start_date," +
            " f_parent,f_child, f_relationship_type, f_event)\n" +
            " VALUES ('LOADID','77777777777','WorkRelationship','2017-05-04'," +
            "'EPR-W-TSTW01','EPR-W-TSTW02','TRS','EVENT')";

    public static String Insert_person= "INSERT INTO semarchy_eph_mdm.sa_person (b_loadid, person_id, b_classname, given_name," +
            " family_name)\n" +
            " VALUES ('LOADID','666666666','Person'," +
            "'Kobe','Bryant')";

    public static String Insert_person_role= "INSERT INTO semarchy_eph_mdm.sa_work_person_role (b_loadid, work_person_role_id, b_classname, f_role," +
            " f_wwork,f_person, f_event)\n" +
            " VALUES ('LOADID','999999999','WorkPersonRole','PD'," +
            "'EPR-W-TSTW01','666666666','EVENT')";

    public static String Insert_mirror= "INSERT INTO semarchy_eph_mdm.sa_work_relationship (b_loadid, work_relationship_id, b_classname, effective_start_date," +
            " f_parent,f_child, f_relationship_type, f_event)\n" +
            " VALUES ('LOADID','555555555','WorkRelationship','2017-05-04'," +
            "'EPR-W-TSTW01','EPR-W-TSTW03','MIR','EVENT')";

    public static String Insert_financial_attr= "INSERT INTO semarchy_eph_mdm.sa_work_financial_attribs (b_loadid, work_fin_attribs_id, b_classname, f_gl_company," +
            " f_event,f_gl_cost_resp_centre,f_gl_revenue_resp_centre,f_wwork)\n" +
            " VALUES ('LOADID','44444444','WorkFinancialAttributes','401'," +
            "'EVENT','10014','10014','EPR-W-TSTW01')";

    public static String Insert_ProductPersonRole = "insert into semarchy_eph_mdm.sa_product_person_role (b_loadid, product_person_role_id, b_classname, f_product, f_person, f_role, f_event)\n" +
            "values ('LOADID', '44444444', 'ProductPersonRole', 'EPR-TSTP03'  ,'666666666','PO', 'EVENT' )";

    public static String Insert_prod_pack_rel = "insert into semarchy_eph_mdm.sa_product_rel_package (b_loadid, product_rel_pack_id, b_classname, f_package_owner, f_component, f_relationship_type, f_event) \n" +
            "values ('LOADID', '44444444', 'ProductRelationshipPackage', 'EPR-TSTP03', 'EPR-TSTP02', 'CON', 'EVENT')";

    public static String Insert_work_subject_area_link= "INSERT INTO semarchy_eph_mdm.sa_work_subject_area_link \n" +
            "(b_loadid, work_subject_area_link_id, b_classname, effective_start_date, effective_end_date, f_wwork)\n" +
            "values ('LOADID', '55555555', 'WorkSubjectAreaLink', '2017-02-10', null, 'EPR-W-TSTW01')";
}
