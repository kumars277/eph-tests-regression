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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Assert;

import java.util.List;

public class NotificationTestSteps {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    @StaticInjection
    public NotificationCountContext notificationCountContext;
    private String sql;
    public static String currentTime;

    public List<NotificationDataObject> status;
    public int attemptsBefore;


    @Given("^We know the number of (.*) GD records after a full-load$")
    public void getGDRecordsAfterFullLoad(String table){
        sql= NotificationsSQL.EPH_GD_PRODUCT_Count.replace("PARAM1",table);
        notificationCountContext.gdCountNumber= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Log.info("\nThe number of records in GD is: " + notificationCountContext.gdCountNumber.get(0).ephGDCount);
    }

    @When("^We check the created notifications by (.*) and by (.*)$")
    public void getNotifications(String type,String table){
        sql= NotificationsSQL.EPH_Notifications_Count.replace("PARAM1",type).replace("PARAM2", table);
    //    Log.info(sql);
        notificationCountContext.notificationCountNumber= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
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
        notificationCountContext.notificationID= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
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

    @And("^The notifications for (.*) is successfully processed$")
    public void checkNotificationStatus(String type) {
        if (type.equalsIgnoreCase("WORK")){
        sql= NotificationsSQL.EPH_Notification_Processed.replace("PARAM1",type);
        Log.info(sql);
        notificationCountContext.processedNotification=DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);

        sql= NotificationsSQL.EPH_Notification_Payload.replace("PARAM1","=");
            Log.info(sql);
        notificationCountContext.payloadNotification=DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);

            Log.info("Processed notifications are: " + notificationCountContext.processedNotification.get(0).processed);
            Log.info("Notifications in payload table are: " + notificationCountContext.processedNotification.get(0).payloadcount);
            Assert.assertEquals("There are difference between processed and payload notifications",
                    notificationCountContext.processedNotification.get(0).processed
                    , notificationCountContext.payloadNotification.get(0).payloadcount);

        } else if (type.equalsIgnoreCase("PRODUCT")){
            sql= NotificationsSQL.EPH_Notification_Processed.replace("PARAM1",type);
            Log.info(sql);
            notificationCountContext.processedNotification=DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);

            sql= NotificationsSQL.EPH_Notification_Payload.replace("PARAM1","!=");
            Log.info(sql);
            notificationCountContext.payloadNotification=DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);

            Log.info("Processed notifications are: " + notificationCountContext.processedNotification.get(0).processed);
            Log.info("Notifications in payload table are: " + notificationCountContext.payloadNotification.get(0).payloadcount);
            Assert.assertEquals("There are difference between processed and payload notifications",
                    notificationCountContext.processedNotification.get(0).processed
                    , notificationCountContext.processedNotification.get(0).payloadcount);
        }
    }

    @Given("^A (.*) is updated$")
    public void updateProduct(String type) {
        if (type.equalsIgnoreCase("product")){
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-TSTP03:BKF");
            Log.info(sql);
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);

            loadBatchContext.batchId = DataLoadServiceImpl.createProductByStoreProcedure();
        } else if(type.equalsIgnoreCase("work")){
             sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01");
             notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);

            loadBatchContext.batchId = DataLoadServiceImpl.createWorkByStoreProcedure();
        } else {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-TSTP03:BKF");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);

            loadBatchContext.batchId = DataLoadServiceImpl.createManifestationByStoreProcedure();
        }
        System.out.println(loadBatchContext.batchId);
        JobUtils.waitTillTheBatchComplete();
    }

    @When("A notification is created$")
    public void checkNotificationCreated(){
        sql= NotificationsSQL.EPH_Notification_Created.replace("PARAM1",loadBatchContext.batchId);
        notificationCountContext.createdNotification=DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.createdNotification.size() == 1){
            Log.info("Notifications were created");
        } else {
            Log.info("Notification was not created! Number of notifications is " + notificationCountContext.createdNotification.size());
            Assert.fail("Notifications were not created");
        }
    }

    @Then("^The notification is processed$")
    public void checkNotifStatus() throws InterruptedException {
        int i = 0;
        sql= NotificationsSQL.EPH_GET_Notify_Status.replace("PARAM1",loadBatchContext.batchId);
        do {
            Thread.sleep(1000);
            i++;
            status= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        }while(status.get(0).status.equalsIgnoreCase("UNPROCESSED") && i<30);
        currentTime=ManageDatesService.currentDate();
        if (status.get(0).status.equalsIgnoreCase("UNPROCESSED")){
            Assert.fail("The notification was not processed!");
        }else{
            Log.info("The notification was processed");
        }
    }

    @Then("^The (.*) notification is in the payload table$")
    public void checkPayloadTable(String notType){
        Gson gson = new Gson();

        if(notType.equalsIgnoreCase("work")){
            sql= NotificationsSQL.EPH_GET_Payload_Notif_Work;
            notificationCountContext.payloadResult= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("Notifications number not as expected",notificationCountContext.payloadResult.size(),8);

            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsAfter= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);
            Assert.assertEquals("The attempts are not as expected",notificationCountContext.writeAttemptsBefore.get(0).attempts + 1,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts);

        }  else if(notType.equalsIgnoreCase("product")){
            sql= NotificationsSQL.EPH_GET_Payload_Notif_Product;
            notificationCountContext.payloadResult= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("Notification number not as expected",notificationCountContext.payloadResult.size(), 1);

            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-TSTP03:BKF");
            notificationCountContext.writeAttemptsAfter= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);
/*
            Assert.assertEquals("The attempts are not as expected",notificationCountContext.writeAttemptsBefore.get(0).attempts + 1,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts);


*/

            String json = gson.toJson(notificationCountContext.payloadResult.get(0).value);
            JsonObject jsonObj = new JsonParser().parse(notificationCountContext.payloadResult.get(0).value).getAsJsonObject();
            Log.info(json);
            String schemaVersion = jsonObj.get("schemaVersion").getAsString();
            String id = jsonObj.get("productId").getAsString();
            String type = jsonObj.getAsJsonObject("productType").get("productTypeCode").getAsString();
            Log.info(notificationCountContext.payloadResult.get(0).timestamp.substring(0,16));
            Log.info(json);
            Log.info(schemaVersion);

            JSONObject jsonSchema = new JSONObject(new JSONTokener(getClass().getClassLoader().getResourceAsStream("jsonValidator/product.json")));
            JSONObject jsonSubject = new JSONObject(new JSONTokener(notificationCountContext.payloadResult.get(0).value));
            Schema schema=SchemaLoader.load(jsonSchema);
            schema.validate(jsonSubject);

            if (notificationCountContext.payloadResult.get(0).key.substring(0,10).equalsIgnoreCase(id)){
                Log.info("The id in the payload message is correct");
            }else{
                Assert.fail("The id in the payload message is different!");
            }

            if (notificationCountContext.payloadResult.get(0).key.substring(11,14).equalsIgnoreCase(type)){
                Log.info("The type in the payload message is correct");
            }else{
                Assert.fail("The type in the payload message is different!");
            }

        } else {
            sql = NotificationsSQL.EPH_GET_Payload_Notif_Manifestation;
            notificationCountContext.payloadResult = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The notification number not as expected", notificationCountContext.payloadResult.size(), 2);

            sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP03:BKF");
            notificationCountContext.writeAttemptsAfter = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);
            Assert.assertEquals("The attempts are not as expected", attemptsBefore + 1,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts);
        }

        for (int i=0;i<notificationCountContext.payloadResult.size();i++) {

            if (notificationCountContext.payloadResult.get(i).timestamp.substring(0, 16).equalsIgnoreCase(currentTime)) {
                Log.info("The notification timestamp was updated");
                Log.info("The update time is: " + notificationCountContext.payloadResult.get(i).timestamp.substring(0, 16) +
                        " and the current time is: " + currentTime);
            } else {
                Log.info("The notification timestamp for key:" + notificationCountContext.payloadResult.get(i).key + " was not updated");
                Log.info("The update time was: " + notificationCountContext.payloadResult.get(i).timestamp.substring(0, 16) +
                        " but the current time is: " + currentTime);
                Assert.fail("The notification timestamp for key:" + notificationCountContext.payloadResult.get(i).key + " was not updated");
            }
        }
    }
}
