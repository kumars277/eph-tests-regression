package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.JobUtils;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.LoadBatchContext;
import com.eph.automation.testing.models.contexts.NotificationCountContext;
import com.eph.automation.testing.models.dao.NotificationDataObject;
import com.eph.automation.testing.services.db.DataLoadServiceImpl;
import com.eph.automation.testing.services.db.services.ManageDatesService;
import com.eph.automation.testing.services.db.sql.NotificationsSQL;
import com.google.gson.Gson;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import jdk.nashorn.internal.parser.JSONParser;
import net.minidev.json.JSONObject;
import org.junit.Assert;

public class NotificationTestSteps {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    @StaticInjection
    public NotificationCountContext notificationCountContext;
    private String sql;


    @Given("^We know the number of (.*) GD records after a full-load$")
    public void getGDRecordsAfterFullLoad(String table){
        sql= NotificationsSQL.EPH_GD_PRODUCT_Count.replace("PARAM1",table);
        notificationCountContext.gdCountNumber= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
        Log.info("\nThe number of records in GD is: " + notificationCountContext.gdCountNumber.get(0).ephGDCount);
    }

    @When("^We check the created notifications by (.*) and by (.*)$")
    public void getNotifications(String type,String table){
        sql= NotificationsSQL.EPH_Notifications_Count.replace("PARAM1",type).replace("PARAM2", table);
    //    Log.info(sql);
        notificationCountContext.notificationCountNumber= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
        Log.info("\nThe number of notifications is: " + notificationCountContext.notificationCountNumber.get(0).notificationCount);
    }

    @Then("^A notification is created for each GD record$")
    public void verifyNotificationCount(){
        Assert.assertEquals("There are missing notifications!",
                notificationCountContext.gdCountNumber.get(0).ephGDCount,
                notificationCountContext.notificationCountNumber.get(0).notificationCount);
    }

    @And("^The (.*) id is the same as the id from table (.*)$")
    public void checkIds(String type, String table){
        sql= NotificationsSQL.EPH_Notification_ID.replace("PARAM1",type)
        .replace("PARAM2", table);
        Log.info(sql);
        notificationCountContext.notificationID= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
        if (notificationCountContext.notificationID.isEmpty()) {
            Log.info("Notifications for all ids are created!");
        }else {
            for(int i=0;i<notificationCountContext.notificationID.size();i++) {
                Log.info("There are missing ids: " + notificationCountContext.notificationID.size());
                Log.info("The missing id is: " + notificationCountContext.notificationID.get(i).gdID);
            }
            Assert.fail("There are missing ids");
        }
    }

    @And("^The notification is successfully processed$")
    public void checkNotificationStatus() throws InterruptedException {
        sql=NotificationsSQL.EPH_Notification_Unprocessed;
        for (int i=0;i<3;i++) {
            notificationCountContext.status= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            if (notificationCountContext.status.isEmpty()) {
                Log.info("All notifications were processed!");
                break;
            }else{
                Thread.sleep(10*1000);
            }
            Assert.fail("There are unprocessed notifications!");
        }

        sql=NotificationsSQL.EPH_Notification_Failed;
        notificationCountContext.failedNotifications=DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
        if (notificationCountContext.failedNotifications.isEmpty()){
            Log.info("There are no failed notifications");
        }else{
            for(int i=0;i<notificationCountContext.failedNotifications.size();i++) {
                Log.info("There are failed notifications: " + notificationCountContext.failedNotifications.size());
                Log.info("The notifications for id " + notificationCountContext.failedNotifications.get(i).notificationID + " failed!");
            }
            Assert.fail("There are failed notifications");
        }

    }

    @Given("^A (.*) is updated$")
    public void updateProduct(String type) {
        if (type.equalsIgnoreCase("product")){
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-10VNBR");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            Log.info("The attempts before update are: " + notificationCountContext.writeAttemptsBefore.get(0).attempts);

            loadBatchContext.batchId = DataLoadServiceImpl.createProductByStoreProcedure();
        } else if(type.equalsIgnoreCase("work")){
             sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-10VNBR:BKF");
             notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
             Log.info("The attempts before update are: " + notificationCountContext.writeAttemptsBefore.get(0).attempts);

            loadBatchContext.batchId = DataLoadServiceImpl.createWorkByStoreProcedure();
        } else {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-M-10R71F:JPR");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            Log.info("The attempts before update are: " + notificationCountContext.writeAttemptsBefore.get(0).attempts);
            loadBatchContext.batchId = DataLoadServiceImpl.createManifestationByStoreProcedure();
        }
        System.out.println(loadBatchContext.batchId);
        JobUtils.waitTillTheBatchComplete();
    }

    @When("A notification is created$")
    public void checkNotificationCreated(){
        sql= NotificationsSQL.EPH_Notification_Created.replace("PARAM1",loadBatchContext.batchId);
        notificationCountContext.createdNotification=DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
        if (notificationCountContext.createdNotification.size() == 1){
            Log.info("Notifications were created");
        } else {
            Log.info("Notification was not created! Number of notifications is " + notificationCountContext.createdNotification.size());
            Assert.fail("Notifications were not created");
        }
    }

    @Then("^The notification is processed$")
    public void checkNotifStatus() throws InterruptedException {
        sql= NotificationsSQL.EPH_GET_Notify_Status.replace("PARAM1",loadBatchContext.batchId);
        for (int i=0;i<3;i++) {
            notificationCountContext.status= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            if (notificationCountContext.status.get(0).status.equalsIgnoreCase("PROCESSED")) {
                Log.info("All notifications were processed!");
                break;
            }else{
                Thread.sleep(10*1000);
            }
            Assert.fail("The notifications were not processed!");
        }
    }

    @Then("^The (.*) notification is in the payload table$")
    public void checkPayloadTable(String notType){
        if(notType.equalsIgnoreCase("work")){
            sql= NotificationsSQL.EPH_GET_Payload_Notif_Work;
            Log.info(sql);
            notificationCountContext.payloadResult= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            if(notificationCountContext.payloadResult.size() == 7){
                Log.info("The notification was successfully pulled by the service");
            } else {
                Assert.fail("There are missing notifications");
            }

            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-1025F9:JNL");
            notificationCountContext.writeAttemptsAfter= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);
            Assert.assertEquals("The attempts are not as expected",notificationCountContext.writeAttemptsBefore.get(0).attempts,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts + 1);

        }  else if(notType.equalsIgnoreCase("product")){
            sql= NotificationsSQL.EPH_GET_Payload_Notif_Product;
            Log.info(sql);
            notificationCountContext.payloadResult= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            if(notificationCountContext.payloadResult.size() == 1){
                Log.info("The notification was successfully pulled by the service");
            } else {
                Assert.fail("There are missing notifications");
            }

            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-10VNBR:BKF");
            notificationCountContext.writeAttemptsAfter= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);
            Assert.assertEquals("The attempts are not as expected",notificationCountContext.writeAttemptsBefore.get(0).attempts,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts + 1);

        } else{
            sql= NotificationsSQL.EPH_GET_Payload_Notif_Manifestation;
            Log.info(sql);
            notificationCountContext.payloadResult= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            if(notificationCountContext.payloadResult.size() == 4){
                Log.info("The notification was successfully pulled by the service");
            } else {
                Assert.fail("There are missing notifications");
            }

            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-M-10R71F");
            notificationCountContext.writeAttemptsAfter= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);
            Assert.assertEquals("The attempts are not as expected",notificationCountContext.writeAttemptsBefore.get(0).attempts,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts + 1);
        }

        Gson gson = new Gson();

        for (int i=0;i<notificationCountContext.payloadResult.size();i++) {
            String json = gson.toJson(notificationCountContext.payloadResult.get(i).value);
            Log.info(notificationCountContext.payloadResult.get(i).timestamp.substring(0,10));
            Log.info(json);
            if (notificationCountContext.payloadResult.get(i).timestamp.substring(0, 10).equalsIgnoreCase(ManageDatesService.currentDate())) {
                Log.info("The notification timestamp was updated");
            } else {
                Log.info("The notification timestamp for key:" + notificationCountContext.payloadResult.get(i).key + " was not updated");
                Assert.fail("The notification timestamp for key:" + notificationCountContext.payloadResult.get(i).key + " was not updated");
            }
        }
    }
}
