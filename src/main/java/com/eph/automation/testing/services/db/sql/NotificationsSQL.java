package com.eph.automation.testing.services.db.sql;

public class NotificationsSQL {
    public static String EPH_GD_PRODUCT_Count = "select count(*) as ephGDCount FROM semarchy_eph_mdm.gd_PARAM1 gd\n" +
            "join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM1)";

    public static String EPH_Notifications_Count = "select count(*) as notificationCount FROM semarchy_eph_stg.st_out_notification st\n" +
            "join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id = " +
            "(select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1' )";

    public static String EPH_GD_ID = "select PARAM2_id as gdID FROM semarchy_eph_mdm.gd_PARAM1 gd\n" +
            "join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM1)";

    public static String EPH_Notification_ID =
            "select PARAM1_id as gdID FROM semarchy_eph_mdm.gd_PARAM2 gd\n" +
                    "join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM2)" +
                    "except select notification_id as notificationID FROM semarchy_eph_stg.st_out_notification st\n" +
                    "join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id = \n" +
                    "(select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1' )";
}
