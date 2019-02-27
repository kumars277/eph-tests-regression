package com.eph.automation.testing.services.db.sql;

public class NotificationsSQL {
    public static String EPH_GD_PRODUCT_Count="select count(*) as ephGDCount FROM semarchy_eph_mdm.gd_PARAM1 gd\n" +
            "join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM1)";

    public static String EPH_Notifications_Count="select count(*) as notificationCount FROM semarchy_eph_stg.st_out_notification\n" +
            "where notification_type='PARAM1' and batch_id = " +
            "(select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1' )";


}
