package com.eph.automation.testing.services.db.sql;

public class NotificationsSQL {
    public static String EPH_GD_PRODUCT_Count="select count(*) as ephGDCount FROM semarchy_eph_mdm.gd_PARAM1 gd\n" +
            "join semarchy_eph_mdm.gd_event on gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM1)\n" +
            "and semarchy_eph_mdm.gd_event.f_event_type = 'PMX'";

    public static String EPH_Notifications_Count="select count(*) as notificationCount FROM semarchy_eph_stg.st_out_notification\n" +
            "where batch_id = (select max (batch_id) from semarchy_eph_stg.st_out_notification)\n"+
            "and notification_type='PARAM1'";


}
