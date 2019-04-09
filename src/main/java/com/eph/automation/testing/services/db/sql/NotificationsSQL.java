package com.eph.automation.testing.services.db.sql;

public class NotificationsSQL {
    public static String EPH_GD_PRODUCT_Count = "select count (*) as ephGDCount from ephsit.semarchy_eph_mdm.gd_PARAM1";
/*            " where f_event =  (select max (f_event) from\n" +
            "semarchy_eph_mdm.gd_PARAM1 join \n"+
            "semarchy_eph_mdm.gd_event on f_event = event_id\n"+
            "where  semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.workflow_id = 'talend'\n"+
            "AND semarchy_eph_mdm.gd_event.f_event_type = 'PMX'\n"+
            "and semarchy_eph_mdm.gd_event.f_workflow_source = 'PMX' )";*/

   /* public static String EPH_Notifications_Count = "select count(*) as notificationCount FROM semarchy_eph_stg.st_out_notification st\n" +
            "join semarchy_eph_mdm.gd_event e on st.batch_id = e.b_batchid and st.notification_type='PARAM1' and st.batch_id = " +
            "(select max (batch_id) from semarchy_eph_stg.st_out_notification where notification_type='PARAM1' )";*/


    public static String EPH_Notifications_Count = "select count(distinct notification_id) as notificationCount from semarchy_eph_stg.st_out_notification\n"+
/*    "where batch_id =  (select max (batch_id) from\n"+
            "semarchy_eph_stg.st_out_notification join\n"+
            " semarchy_eph_mdm.gd_event e on batch_id = e.b_batchid\n"+
            "where  e.f_event_type = 'PMX'\n"+
            " and e.workflow_id = 'talend'\n"+
            "AND e.f_event_type = 'PMX'\n"+
            "and e.f_workflow_source = 'PMX')\n"+*/
            "where notification_type='PARAM1'";

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


    public static String EPH_Notification_Processed = "select count(distinct notification_id) as processed from semarchy_eph_stg.st_out_notification \n" +
            "where status = 'PROCESSED' and notification_type = 'PARAM1'";

    public static String EPH_Notification_Payload = "select count(distinct substring(payload_key,1,strpos(payload_key,':')-1) ) as payloadcount " +
            "from semarchy_eph_stg.st_test_notification_payload where substring(payload_key,1,strpos(payload_key,'-')+1)  PARAM1 'EPR-W'";

    public static String EPH_Notification_Created = "select * from semarchy_eph_stg.st_out_notification where batch_id='PARAM1'";

    public static String EPH_GET_Notify_Status = "select status as status from semarchy_eph_stg.st_out_notification where batch_id='PARAM1'";

    public static String EPH_GET_Payload_Notif_Work = "select payload_key as key, payload_value as value, update_timestamp as timestamp \n" +
            "from semarchy_eph_stg.st_test_notification_payload\n" +
            " where payload_key in ('EPR-W-TSTW01:JNL','EPR-TSTP01:BKF','EPR-TSTP02:BKF'," +
            "'EPR-TSTP03:BKF','EPR-TSTP04:SUB','EPR-TSTP05:RPR','EPR-TSTP06:SUB','EPR-TSTP07:JBS') order by key desc";


    public static String EPH_GET_Payload_Notif_Product = "select payload_key as key, payload_value as value, update_timestamp as timestamp \n" +
            "from semarchy_eph_stg.st_test_notification_payload\n" +
            " where payload_key = 'EPR-TSTP03:BKF'";


    public static String EPH_GET_Payload_Notif_Manifestation = "select payload_key as key, payload_value as value, update_timestamp as timestamp \n" +
            "from semarchy_eph_stg.st_test_notification_payload\n" +
            " where payload_key in ('EPR-W-TSTW01:JNL','EPR-TSTP03:BKF','EPR-TSTP04:SUB')";

    public static String EPH_GET_Write_Attempts = "select write_attempts as attempts from semarchy_eph_stg.st_test_notification_payload\n" +
            " where payload_key='PARAM1'";

    public static String EPH_GET_TEST_DATA_Product="select * \n" +
            "from semarchy_eph_mdm.gd_product\n" +
            " where product_id in ('EPR-TSTP01','EPR-TSTP02'," +
            "'EPR-TSTP03','EPR-TSTP04','EPR-TSTP05','EPR-TSTP06','EPR-TSTP07')";

    public static String EPH_GET_TEST_DATA_Work="select * \n" +
            "from semarchy_eph_mdm.gd_wwork\n" +
            " where work_id = 'EPR-W-TSTW01'";

    public static String EPH_GET_TEST_DATA_Manifestation="select * \n" +
            "from semarchy_eph_mdm.gd_manifestation\n" +
            " where manifestation_id in ('EPR-M-TSTM01','EPR-M-TSTM02')";

}
