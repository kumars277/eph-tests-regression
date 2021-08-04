package features.zOnHold.znotused;

public class NegativeTestNotificationSQL {

    // Test 1 Scenario : Work - Succeeds , Product - Fails
    public static String WORK_Negative_1 = "INSERT INTO semarchy_eph_mdm.sa_wwork (b_loadid, work_id, b_classname, \n"+
            "work_title,f_type,f_status,f_pmc,f_oa_type,f_imprint,f_event)\n"+
            "VALUES ('LOADID','EPR-W-TSTW10','Work','Big Data and Smart Service Systems test','RBK','WAP','355','N','ACADPR','EVENT')";

    public static String Manifestation_1_Negative_1 = "INSERT INTO semarchy_eph_mdm.sa_manifestation (b_loadid, manifestation_id,\n" +
            " b_classname,manifestation_key_title,f_type,f_status,f_wwork,f_event)\n"+
            "VALUES ('LOADID','EPR-M-TSTM10','Manifestation','Big Data and Smart Service Systems test','UNK','MPU','EPR-W-TSTW10','EVENT')";

    public static String Manifestation_2_Negative_1 = "INSERT INTO semarchy_eph_mdm.sa_manifestation \n" +
            "(b_loadid, manifestation_id, b_classname,manifestation_key_title,f_type,f_status,f_wwork,f_event)\n"+
            "VALUES ('LOADID','EPR-M-TSTM20','Manifestation','Big Data and Smart Service Systems test','PHB','MPU','EPR-W-TSTW10','EVENT')";

    public static String Product_1_Negative_1="INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name,short_name,f_type, f_status,f_manifestation, f_event)\n"+
            "VALUES ('LOADID','EPR-TSTP10','Product','Test Big Data and Smart Service Systems Purchase','Liu - Big Data&Smart Service Systmes \t\n"+
            "\t\n"+
            "\t\n"+
            "','OOA','PAS','EPR-M-TSTM10','EVENT')";

    public static String Product_2_Negative_1 = "INSERT INTO semarchy_eph_mdm.sa_product (b_loadid, product_id, b_classname, name,short_name,f_type, f_status,f_manifestation, f_event)"+
            "VALUES ('LOADID','EPR-TSTP20','Product','Test Big Data and Smart Service Systems Purchase','Liu - Big Data&Smart Service Systmes \t\n" +
            "\t\n"+
            "\t\n"+
            "','OOA','PAS','EPR-M-TSTM20','EVENT')";

    // Test 2 Scenario : Work - Fails  , Product - Fails
    public static String WORK_Negative_2 = "INSERT INTO semarchy_eph_mdm.sa_wwork (b_loadid, work_id, b_classname, "+
            "work_title,work_sub_title,f_type,f_status,f_pmc,f_oa_type,f_imprint,f_event)\n"+
            "VALUES ('LOADID','EPR-W-TSTW20','Work','The Human Body test','Linking Structure and Function " +
            "\t\n"+
            "\t\n"+
            "','RBK','WAP','946','N','ACADPR','EVENT')";

    public static String Manifestation_1_Negative_2 = "INSERT INTO semarchy_eph_mdm.sa_manifestation " +
            "(b_loadid, manifestation_id, b_classname,manifestation_key_title,f_type,f_status,f_wwork,f_event)\n" +
            "VALUES ('LOADID','EPR-M-TSTM30','Manifestation','The Human Body test','PSB','MPU','EPR-W-TSTW20','EVENT')";

    public static String Manifestation_2_Negative_2 = "INSERT INTO semarchy_eph_mdm.sa_manifestation " +
            "(b_loadid, manifestation_id, b_classname,manifestation_key_title,f_type,f_status,f_wwork,f_event)\n" +
            "VALUES ('LOADID','EPR-M-TSTM40','Manifestation','The Human Body test','UNK','MPU','EPR-W-TSTW20','EVENT')";

    public static String Product_1_Negative_2 = "INSERT INTO semarchy_eph_mdm.sa_product " +
            "(b_loadid, product_id, b_classname, name,short_name,f_type, f_status,f_manifestation, f_event)\n" +
            "VALUES ('LOADID','EPR-TSTP30','Product','Test The Human Body  Purchase','Carlson-The Human Body "+
            "\t\n"+
            "\t\n"+
            "','OOA','PAS','EPR-M-TSTM30','EVENT')";

    public static String Product_2_Negative_2 ="INSERT INTO semarchy_eph_mdm.sa_product " +
            "(b_loadid, product_id, b_classname, name,short_name,f_type, f_status,f_manifestation, f_event)\n" +
            "VALUES ('LOADID','EPR-TSTP40','Product','Test The Human Body  Purchase','Carlson-The Human Body " +
            "\t\n"+
            "\t\n"+
            "','OOA','PAS','EPR-M-TSTM40','EVENT')";
}
