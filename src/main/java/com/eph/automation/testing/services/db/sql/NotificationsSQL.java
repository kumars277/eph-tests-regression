package com.eph.automation.testing.services.db.sql;

public class NotificationsSQL {
    public static String EPH_GD_PRODUCT_Count = "select count(*) as ephGDCount FROM semarchy_eph_mdm.gd_PARAM1 gd\n" +
            "join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM1)";

   /* public static String EPH_Notifications_Count = "select count(*) as notificationCount FROM semarchy_eph_stg.st_out_notification st\n" +
            "join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id = " +
            "(select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1' )";*/


    public static String EPH_Notifications_Count = "select count(*) as notificationCount FROM semarchy_eph_stg.st_out_notification st\n" +
            "join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id = " +
            "(select max (b_batchid) from semarchy_eph_mdm.gd_PARAM2)";

    public static String EPH_GD_ID = "select PARAM2_id as gdID FROM semarchy_eph_mdm.gd_PARAM1 gd\n" +
            "join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM1)";

 /*   public static String EPH_Notification_ID =
            "select * from " +
"((select PARAM1_id as gdID FROM semarchy_eph_mdm.gd_PARAM2 gd\n" +
"    join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM2)\n" +
 "   except select notification_id as notificationID FROM semarchy_eph_stg.st_out_notification st\n" +
  "  join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id =\n" +
   "         (select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1'))\n" +
    "union all\n" +
     "       (select notification_id as gdID FROM semarchy_eph_stg.st_out_notification st\n" +
      "              join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id =\n" +
       "             (select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1' )\n" +
    "except\n" +
    "select PARAM1_id as gdID FROM semarchy_eph_mdm.gd_PARAM2 gd\n" +
    "join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM2)))a"; */

    public static String EPH_Notification_ID =
            "select * from " +
                    "((select PARAM1_id as gdID FROM semarchy_eph_mdm.gd_PARAM2 gd\n" +
                    "    join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM2)\n" +
                    "   except select notification_id as notificationID FROM semarchy_eph_stg.st_out_notification st\n" +
                    "  join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id =\n" +
                    "         (select max (b_batchid) from semarchy_eph_mdm.gd_PARAM2))\n" +
                    "union all\n" +
                    "       (select notification_id as gdID FROM semarchy_eph_stg.st_out_notification st\n" +
                    "              join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id =\n" +
                    "             (select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1' )\n" +
                    "except\n" +
                    "select PARAM1_id as gdID FROM semarchy_eph_mdm.gd_PARAM2 gd\n" +
                    "join semarchy_eph_mdm.gd_event e on gd.b_batchid = e.b_batchid and gd.b_batchid = (select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1')))a";


    public static String EPH_Notification_Unprocessed = "select * from semarchy_eph_stg.st_out_notification where status='UNPROCESSED'";

    public static String EPH_Notification_Failed = "select notification_id as notificationID from semarchy_eph_stg.st_out_notification where status='FAILED'";

    public static String EPH_Notification_Created = "select * from semarchy_eph_stg.st_out_notification where batch_id='PARAM1'";

    public static String EPH_GET_Notify_Status = "select status as status from semarchy_eph_stg.st_out_notification where batch_id='PARAM1'";

    public static String EPH_GET_Payload_Notif_Work = "select payload_key as key, payload_value as value, insert_timestamp as timestamp \n" +
            "from semarchy_eph_stg.st_test_notification_payload\n" +
            " where payload_key in ('EPR-W-1025F9:JNL','EPR-10XT0V:OAA','EPR-10WJ7J:JAS'," +
            "'EPR-10VNBR:BKF','EPR-10T6R0:SUB','EPR-114D1J:RPR','EPR-11392Y:SUB','EPR-112JW0:JBS')";


    public static String EPH_GET_Payload_Notif_Product = "select payload_key as key, payload_value as value, insert_timestamp as timestamp \n" +
            "from semarchy_eph_stg.st_test_notification_payload\n" +
            " where payload_key = 'EPR-10VNBR'";


    public static String EPH_GET_Payload_Notif_Manifestation = "select payload_key as key, payload_value as value, insert_timestamp as timestamp \n" +
            "from semarchy_eph_stg.st_test_notification_payload\n" +
            " where payload_key in ('EPR-M-10R71F:JPR','EPR-10WJ7J','EPR-10VNBR','EPR-10T6R0','EPR-114D1J','EPR-11392Y','EPR-112JW0')";

    public static String EPH_GET_Write_Attempts = "select write_attempts as attempts from from semarchy_eph_stg.st_test_notification_payload\n" +
            " where payload_key='PARAM1'";

}
