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
    private int attemptsBefore;
    public static boolean testDataMissing = false;

    @Given("^We know the number of (.*) GD records after a full-load$")
    public void getGDRecordsAfterFullLoad(String table){
        sql= NotificationsSQL.EPH_GD_PRODUCT_Count.replace("PARAM1",table);
        Log.info(sql);
        notificationCountContext.gdCountNumber= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Log.info("\nThe number of records in GD is: " + notificationCountContext.gdCountNumber.get(0).ephGDCount);
    }

    @When("^We check the created notifications by (.*) and by (.*)$")
    public void getNotifications(String type,String table){
        sql= NotificationsSQL.EPH_Notifications_Count.replace("PARAM1",type).replace("PARAM2", table);
        //Log.info(sql);
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
            Assert.assertTrue("There are difference between processed and payload notifications",
                    notificationCountContext.processedNotification.get(0).processed
                    <= notificationCountContext.payloadNotification.get(0).payloadcount);

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
                    , notificationCountContext.payloadNotification.get(0).payloadcount);
        }
    }

    @Given("^A full load was performed$")
    public void checkForFullLoad(){
        sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
        notificationCountContext.checkTestData= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.checkTestData.isEmpty()){
            testDataMissing = true;
        }else{
            Log.info("Test data exists. Skipping this test...");
        }
    }

    @When("^The test data is inserted$")
    public void createTestData(){
        if (testDataMissing){
            loadBatchContext.batchId = DataLoadServiceImpl.createTestDataByStoreProcedure();
            System.out.println(loadBatchContext.batchId);
            JobUtils.waitTillTheBatchComplete();
        }else{
            Log.info("Test data exists. Skipping this test...");
        }
    }

    @Then("^The test data is created successfully$")
    public void verifyTestDataCreated(){
        if (testDataMissing){
            sql= NotificationsSQL.EPH_GET_TEST_DATA_Work
                    .replace("PARAM1", "EPR-W-TSTW01")
                    .replace("PARAM2", "EPR-W-TSTW02")
                    .replace("PARAM3", "EPR-W-TSTW03");
            notificationCountContext.getWorkTD= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The Work test data is missing", 3
                    ,notificationCountContext.getWorkTD.size());

            sql= NotificationsSQL.EPH_GET_TEST_DATA_Product;
            notificationCountContext.getProductTD= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The Product test data is missing", 7
                    ,notificationCountContext.getProductTD.size());

            sql= NotificationsSQL.EPH_GET_TEST_DATA_Manifestation
                    .replace("PARAM1", "EPR-M-TSTM01")
                    .replace("PARAM2", "EPR-M-TSTM02");
            notificationCountContext.getManifestationTD= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The Manifestation test data is missing", 2
                    ,notificationCountContext.getManifestationTD.size());
            Log.info("Test data is created successfully!");
        }else{
            Log.info("Test data exists. Skipping this test...");
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
             sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
             notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);

            loadBatchContext.batchId = DataLoadServiceImpl.createWorkByStoreProcedure();
        } else if (type.equalsIgnoreCase("manifestation")) {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);

            loadBatchContext.batchId = DataLoadServiceImpl.createManifestationByStoreProcedure();
        } else if (type.equalsIgnoreCase("work_identifier")) {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);

            loadBatchContext.batchId = DataLoadServiceImpl.createIdentifier();
        }else if (type.equalsIgnoreCase("manifestation_identifier")) {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);

            NotificationCountContext.manifestationIdentifier=true;
            loadBatchContext.batchId = DataLoadServiceImpl.createIdentifier();
        }else if (type.equalsIgnoreCase("translation")) {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);
            NotificationCountContext.manifestationIdentifier=false;
            NotificationCountContext.translation=true;
            loadBatchContext.batchId = DataLoadServiceImpl.createIdentifier();
        }else if (type.equalsIgnoreCase("person_role")) {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);
            NotificationCountContext.manifestationIdentifier=false;
            NotificationCountContext.translation=false;
            NotificationCountContext.personRole=true;
            loadBatchContext.batchId = DataLoadServiceImpl.createIdentifier();
        }else if (type.equalsIgnoreCase("mirror")) {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);
            NotificationCountContext.manifestationIdentifier=false;
            NotificationCountContext.translation=false;
            NotificationCountContext.personRole=false;
            NotificationCountContext.mirror = true;
            loadBatchContext.batchId = DataLoadServiceImpl.createIdentifier();
        }else if (type.equalsIgnoreCase("financial_attribute")) {
            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsBefore= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            if (notificationCountContext.writeAttemptsBefore.isEmpty()){
                attemptsBefore = 0;
            }else{
                attemptsBefore = notificationCountContext.writeAttemptsBefore.get(0).attempts;
            }
            Log.info("The attempts before update are: " + attemptsBefore);
            NotificationCountContext.manifestationIdentifier=false;
            NotificationCountContext.translation=false;
            NotificationCountContext.personRole=false;
            NotificationCountContext.mirror = false;
            NotificationCountContext.finAttribute = true;
            loadBatchContext.batchId = DataLoadServiceImpl.createIdentifier();
        }
        System.out.println(loadBatchContext.batchId);
        JobUtils.waitTillTheBatchComplete();
    }

    @When("^A (.*) notification is created$")
    public void checkNotificationCreated(String type){
        sql= NotificationsSQL.EPH_Notification_Created.replace("PARAM1",loadBatchContext.batchId);
        notificationCountContext.createdNotification=DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if ((type.equalsIgnoreCase("translation")) || (type.equalsIgnoreCase("mirror")))
        {
            if (notificationCountContext.createdNotification.size() == 2)
            {
                Log.info("Notifications were created");
            }
            else {
                Log.info("Notification was not created! Number of notifications is " + notificationCountContext.createdNotification.size());
                Assert.fail("Notifications were not created");
            }
        }
        else
        {
            if (notificationCountContext.createdNotification.size() == 1)
            {
                Log.info("Notifications were created");
            }
            else {
                Log.info("Notification was not created! Number of notifications is " + notificationCountContext.createdNotification.size());
                Assert.fail("Notifications were not created");
            }
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
        }while(status.get(0).status.equalsIgnoreCase("UNPROCESSED") && i<300);
        currentTime=ManageDatesService.currentDate();
        if (status.get(0).status.equalsIgnoreCase("UNPROCESSED")){
            Assert.fail("The notification was not processed!");
        }else{
            Log.info("The notification was processed");
        }
    }

    @Then("^The (.*) notification is in the payload table$")
    public void checkPayloadTable(String notType) throws InterruptedException {
        Gson gson = new Gson();

        Log.info("Waiting the timestamp to be printed...");
        Thread.sleep(120*1000);

        if(notType.equalsIgnoreCase("product")){
            sql= NotificationsSQL.EPH_GET_Payload_Notif_Product;
            notificationCountContext.payloadResult= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("Notification number not as expected",notificationCountContext.payloadResult.size(), 1);

            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-TSTP03:BKF");
            notificationCountContext.writeAttemptsAfter= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);

            Assert.assertEquals("The attempts are not as expected",notificationCountContext.writeAttemptsBefore.get(0).attempts + 1,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts);

            String json = gson.toJson(notificationCountContext.payloadResult.get(0).value);
            JsonObject jsonObj = new JsonParser().parse(notificationCountContext.payloadResult.get(0).value).getAsJsonObject();
            Log.info(json);
            String schemaVersion = jsonObj.get("schemaVersion").getAsString();
            String id = jsonObj.get("id").getAsString();
            String type = jsonObj.getAsJsonObject("type").get("code").getAsString();
            String status = jsonObj.getAsJsonObject("status").get("code").getAsString();
            String ravCode = jsonObj.getAsJsonObject("revenueModel").get("code").getAsString();
            String manifestation = jsonObj.getAsJsonObject("manifestation").get("id").getAsString();
//            String identifiers = jsonObj.getAsJsonObject("identifiers").get("identifier").getAsString();
            String pmc = jsonObj.getAsJsonObject("manifestation").getAsJsonObject("work").getAsJsonObject("pmc").get("code").getAsString();
            String pmg = jsonObj.getAsJsonObject("manifestation").getAsJsonObject("work").getAsJsonObject("pmc").getAsJsonObject("pmg").get("code").getAsString();
            Log.info(notificationCountContext.payloadResult.get(0).timestamp.substring(0,16));
            Log.info(json);
            Log.info(schemaVersion);

            JSONObject jsonSchema = new JSONObject(new JSONTokener(getClass().getClassLoader().getResourceAsStream("jsonValidator/product.json")));
            JSONObject jsonSubject = new JSONObject(new JSONTokener(notificationCountContext.payloadResult.get(0).value));
            Schema schema=SchemaLoader.load(jsonSchema);
            schema.validate(jsonSubject);
            Log.info("The schema is valid!");

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

            Assert.assertEquals("The status code is different","PAS",status);
            Assert.assertEquals("The ravCode code is different","SUB",ravCode);
            Assert.assertEquals("The manifestation code is different","EPR-M-TSTM02",manifestation);
//            Assert.assertEquals("The identifier code is different","8888-8888",identifiers);
            Assert.assertEquals("The pmc code is different","541",pmc);
            Assert.assertEquals("The pmg code is different","606",pmg);

        } else if (notType.equalsIgnoreCase("manifestation") || notType.equalsIgnoreCase("manifestation_identifier")){
            sql = NotificationsSQL.EPH_GET_Payload_Notif_Manifestation;
            notificationCountContext.payloadResult = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The notification number not as expected",3,notificationCountContext.payloadResult.size());

            sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsAfter = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);
            Assert.assertEquals("The attempts are not as expected", notificationCountContext.writeAttemptsBefore.get(0).attempts  + 1,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts);
        } else
            {

              if(notType.equalsIgnoreCase("translation") || notType.equalsIgnoreCase("mirror")) {

                  if (notType.equalsIgnoreCase("translation")) {
                      sql = NotificationsSQL.EPH_GET_Payload_Notif_Work_TRS;
                  }
                  else
                  {
                      sql = NotificationsSQL.EPH_GET_Payload_Notif_Work_MIR;
                  }

                  System.out.println(sql);
                  notificationCountContext.payloadResult= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
                  Assert.assertEquals("Notifications number not as expected",9,notificationCountContext.payloadResult.size());
              }

              else {
                  sql = NotificationsSQL.EPH_GET_Payload_Notif_Work;
                  notificationCountContext.payloadResult = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
                  Assert.assertEquals("Notifications number not as expected", 8, notificationCountContext.payloadResult.size());
              }


            sql= NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1","EPR-W-TSTW01:JNL");
            notificationCountContext.writeAttemptsAfter= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
            Log.info("The attempts after update are: " + notificationCountContext.writeAttemptsAfter.get(0).attempts);
            Assert.assertEquals("The attempts are not as expected",notificationCountContext.writeAttemptsBefore.get(0).attempts + 1,
                    notificationCountContext.writeAttemptsAfter.get(0).attempts);

            String json = gson.toJson(notificationCountContext.payloadResult.get(0).value);
            JsonObject jsonObj = new JsonParser().parse(notificationCountContext.payloadResult.get(0).value).getAsJsonObject();
            Log.info(json);
            String schemaVersion = jsonObj.get("schemaVersion").getAsString();
            String id = jsonObj.get("id").getAsString();
            String type = jsonObj.getAsJsonObject("type").get("code").getAsString();

            String status = jsonObj.getAsJsonObject("status").get("code").getAsString();
            String imprint = jsonObj.getAsJsonObject("imprint").get("code").getAsString();
            String pmc = jsonObj.getAsJsonObject("pmc").get("code").getAsString();
            String pmg = jsonObj.getAsJsonObject("pmg").get("code").getAsString();
            String finAttr = jsonObj.getAsJsonObject("glCompany").get("code").getAsString();
            String costRespCentre = jsonObj.getAsJsonObject("costResponsibilityCentre").get("code").getAsString();
            String person = jsonObj.getAsJsonObject("persons").get("id").getAsString();/*
            String personCode = jsonObj.getAsJsonObject("persons").get("code").getAsString();*/
            Log.info(notificationCountContext.payloadResult.get(0).timestamp.substring(0,16));
            Log.info(json);
            Log.info(schemaVersion);

            JSONObject jsonSchema = new JSONObject(new JSONTokener(getClass().getClassLoader().getResourceAsStream("jsonValidator/work.json")));
            JSONObject jsonSubject = new JSONObject(new JSONTokener(notificationCountContext.payloadResult.get(0).value));
            Schema schema=SchemaLoader.load(jsonSchema);
            schema.validate(jsonSubject);

            if (notificationCountContext.payloadResult.get(0).key.substring(0,12).equalsIgnoreCase(id)){
                Log.info("The id in the payload message is correct");
            }else{
                Assert.fail("The id in the payload message is different!");
            }

            if (notificationCountContext.payloadResult.get(0).key.substring(13,16).equalsIgnoreCase(type)){
                Log.info("The type in the payload message is correct");
            }else{
                Assert.fail("The type in the payload message is different!");
            }

            Assert.assertEquals("The status code is different","WDI",status);

            Assert.assertEquals("The imprint code is different","ELSEVIER",imprint);
            Assert.assertEquals("The pmc code is different","541",pmc);
            Assert.assertEquals("The pmg code is different","606",pmg);
            Assert.assertEquals("The glCompany code is different","401",finAttr);
            Assert.assertEquals("The glCompany code is different","10014",costRespCentre);
            Assert.assertEquals("The glCompany code is different","999999999",person);
            //Assert.assertEquals("The glCompany code is different","PD",personCode);
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
