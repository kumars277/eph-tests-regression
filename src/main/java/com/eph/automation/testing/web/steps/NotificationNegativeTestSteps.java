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
import com.eph.automation.testing.services.db.sql.NotificationsSQL;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;

public class NotificationNegativeTestSteps {
    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    @StaticInjection
    public NotificationCountContext notificationCountContext;
    private String sql;
    public static String currentTime;

    public List<NotificationDataObject> status;
    private int attemptsBefore;
    private int attemptsAfter;

    @Given("^A incorrect product data is inserted$")
    public void incorrectProductInsert() {
        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-W-TSTW10:RBK");
        notificationCountContext.getWriteAttemptsBeforeNegWork =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegWork.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegWork.get(0).attempts;
        }
        Log.info("The work attempts before update are: " + attemptsBefore);

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP10:OOA");
        notificationCountContext.getWriteAttemptsBeforeNegProduct1 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegProduct1.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegProduct1.get(0).attempts;
        }
        Log.info("The product 1 attempts before update are: " + attemptsBefore);

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP20:OOA");
        notificationCountContext.getWriteAttemptsBeforeNegProduct2 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegProduct2.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegProduct2.get(0).attempts;
        }
        Log.info("The product 2 attempts before update are: " + attemptsBefore);

        loadBatchContext.batchId = DataLoadServiceImpl.createFalseData1();
        System.out.println(loadBatchContext.batchId);
        JobUtils.waitTillTheBatchComplete();
    }

    @When("^The data and the notifications are created$")
    public void checkDataExists() {
        sql = NotificationsSQL.EPH_GET_TEST_DATA_Work.replace("PARAM1", "EPR-W-TSTW10");
        notificationCountContext.getWorkTDNegative = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Assert.assertEquals("The Work test data is missing", 1
                , notificationCountContext.getWorkTDNegative.size());

        sql = NotificationsSQL.EPH_GET_TEST_DATA_Manifestation
                .replace("PARAM1", "EPR-M-TSTM10")
                .replace("PARAM2", "EPR-M-TSTM20");
        notificationCountContext.getManifestationTDNegative = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Assert.assertEquals("The Manifetation test data is missing", 2
                , notificationCountContext.getManifestationTDNegative.size());

        sql = NotificationsSQL.EPH_GET_TEST_DATA_Product_Neg
                .replace("PARAM1", "EPR-TSTP10")
                .replace("PARAM2", "EPR-TSTP20");
        notificationCountContext.getProductTDNegative = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Assert.assertEquals("The Product test data is missing", 2
                , notificationCountContext.getProductTDNegative.size());
    }

    @Then("^The product notification is not processed$")
    public void checkNotificationStatus() throws InterruptedException {
        int j = 0;
        sql= NotificationsSQL.EPH_GET_Notify_Status.replace("PARAM1",loadBatchContext.batchId);
        do {
            Thread.sleep(1000);
            j++;
            status= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        }while(status.get(0).status.equalsIgnoreCase("UNPROCESSED") && j<30);
        if (status.get(0).status.equalsIgnoreCase("UNPROCESSED")){
            Assert.fail("The notification was not processed!");
        }else{
            Log.info("The notification was processed");
        }

        sql = NotificationsSQL.EPH_GET_Notification_Neg.replace("PARAM1", loadBatchContext.batchId);
        notificationCountContext.getStatusSTNotification = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);

        for (int i = 0; i < notificationCountContext.getStatusSTNotification.size(); i++) {
            Assert.assertEquals("The notifications are not in Failed status",
                    "ERROR",
                    notificationCountContext.getStatusSTNotification.get(i).status);
        }

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-W-TSTW10:RBK");
        notificationCountContext.getWriteAttemptsAfterNegWork =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Log.info("The work attempts after update are: " + notificationCountContext.getWriteAttemptsAfterNegWork.get(0).attempts);

        Assert.assertTrue("The work was not updated!", notificationCountContext.getWriteAttemptsAfterNegWork.get(0).attempts
                > notificationCountContext.getWriteAttemptsBeforeNegWork.get(0).attempts);

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP10:OOA");
        notificationCountContext.getWriteAttemptsAfterNegProduct1 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsAfterNegProduct1.isEmpty()) {
            attemptsAfter = 0;
        } else {
            attemptsAfter = notificationCountContext.getWriteAttemptsAfterNegProduct1.get(0).attempts;
        }
        Log.info("The product 1 attempts after update are: " + attemptsAfter);

        Assert.assertEquals("The product was updated", notificationCountContext.getWriteAttemptsBeforeNegProduct1.get(0).attempts,
                notificationCountContext.getWriteAttemptsBeforeNegProduct1.get(0).attempts);

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP20:OOA");
        notificationCountContext.getWriteAttemptsAfterNegProduct2 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsAfterNegProduct2.isEmpty()) {
            attemptsAfter = 0;
        } else {
            attemptsAfter = notificationCountContext.getWriteAttemptsAfterNegProduct2.get(0).attempts;
        }
        Log.info("The product 1 attempts after update are: " + attemptsAfter);

        Assert.assertEquals("The product was updated", notificationCountContext.getWriteAttemptsBeforeNegProduct2.get(0).attempts,
                notificationCountContext.getWriteAttemptsBeforeNegProduct2.get(0).attempts);
    }


    @Given("^A incorrect work and product data is inserted$")
    public void incorrectProductWorkInsert() {
        sql = NotificationsSQL.GET_FAILED_NOT_PAYLOAD;
        notificationCountContext.getFailedPayloadNotBefore =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getFailedPayloadNotBefore.isEmpty()){
            Log.info("There are no processed notifications");
        }else{
            Log.info("The created notifications before are: " + notificationCountContext.getFailedPayloadNotBefore.size());
        }

        loadBatchContext.batchId = DataLoadServiceImpl.createFalseData2();
        System.out.println(loadBatchContext.batchId);
        JobUtils.waitTillTheBatchComplete();
    }

    @When("^The incorrect data and the notifications are created$")
    public void checkIncorrectDataExists() {
        sql = NotificationsSQL.EPH_GET_TEST_DATA_Work.replace("PARAM1", "EPR-W-TSTW20");
        notificationCountContext.getWorkTDNegative = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Assert.assertEquals("The Work test data is missing", 1
                , notificationCountContext.getWorkTDNegative.size());

        sql = NotificationsSQL.EPH_GET_TEST_DATA_Manifestation
                .replace("PARAM1", "EPR-M-TSTM30")
                .replace("PARAM2", "EPR-M-TSTM40");
        notificationCountContext.getManifestationTDNegative = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Assert.assertEquals("The Manifetation test data is missing", 2
                , notificationCountContext.getManifestationTDNegative.size());

        sql = NotificationsSQL.EPH_GET_TEST_DATA_Product_Neg
                .replace("PARAM1", "EPR-TSTP30")
                .replace("PARAM2", "EPR-TSTP40");
        notificationCountContext.getProductTDNegative = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        Assert.assertEquals("The Product test data is missing", 2
                , notificationCountContext.getProductTDNegative.size());
    }

    @Then("^The work and product notifications are not processed$")
    public void checkNotifications() throws InterruptedException {
        int j = 0;
        sql= NotificationsSQL.EPH_GET_Notify_Status.replace("PARAM1",loadBatchContext.batchId);
        do {
            Thread.sleep(1000);
            j++;
            status= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        }while(status.get(0).status.equalsIgnoreCase("UNPROCESSED") && j<30);
        if (status.get(0).status.equalsIgnoreCase("UNPROCESSED")){
            Assert.fail("The notification was not processed!");
        }else{
            Log.info("The notification was processed");
        }

        sql = NotificationsSQL.EPH_GET_Notification_Neg.replace("PARAM1", loadBatchContext.batchId);
        notificationCountContext.getStatusSTNotification = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);

        for (int i = 0; i < notificationCountContext.getStatusSTNotification.size(); i++) {
            Assert.assertEquals("The notifications are not in Failed status!",
                    "ERROR",
                    notificationCountContext.getStatusSTNotification.get(i).status);
        }

        sql = NotificationsSQL.GET_FAILED_NOT_PAYLOAD;
        notificationCountContext.getFailedPayloadNot =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);

        if(notificationCountContext.getFailedPayloadNot.isEmpty()){
            Log.info("Notifications were not processed!");
        }else{
            for (int i=0;i<notificationCountContext.getFailedPayloadNot.size();i++){
                Log.info("The attempts before were: "+notificationCountContext.getFailedPayloadNotBefore.get(i).attempts);
                Log.info("The attempts after were: "+notificationCountContext.getFailedPayloadNot.get(i).attempts);
                Assert.assertEquals("Notifications were updated!",notificationCountContext.getFailedPayloadNotBefore.get(i).attempts,
                        notificationCountContext.getFailedPayloadNot.get(i).attempts);
                Log.info("The notifications were not processed!");
            }
        }
    }

    @Given("^A correct manifestation is updated connected to incorrect product$")
    public void manifestationUpdate() {
        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-W-TSTW10:RBK");
        notificationCountContext.getWriteAttemptsBeforeNegWork =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegWork.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegWork.get(0).attempts;
        }
        Log.info("The work attempts before update are: " + attemptsBefore);

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP10:OOA");
        notificationCountContext.getWriteAttemptsBeforeNegProduct1 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegProduct1.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegProduct1.get(0).attempts;
        }
        Log.info("The product 1 attempts before update are: " + attemptsBefore);

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP20:OOA");
        notificationCountContext.getWriteAttemptsBeforeNegProduct2 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegProduct2.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegProduct2.get(0).attempts;
        }
        Log.info("The product 2 attempts before update are: " + attemptsBefore);

        loadBatchContext.batchId = DataLoadServiceImpl.updateManifestation1();
        System.out.println(loadBatchContext.batchId);
        JobUtils.waitTillTheBatchComplete();
    }

    @Given("^A correct manifestation is updated connected to incorrect work and product$")
    public void manifestationIsUpdated() {
        sql = NotificationsSQL.GET_FAILED_NOT_PAYLOAD;
        notificationCountContext.getFailedPayloadNotBefore =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getFailedPayloadNotBefore.isEmpty()){
            Log.info("There are no processed notifications");
        }else{
            Log.info("The created notifications before are: " + notificationCountContext.getFailedPayloadNotBefore.size());
        }

        loadBatchContext.batchId = DataLoadServiceImpl.updateManifestation2();
        System.out.println(loadBatchContext.batchId);
        JobUtils.waitTillTheBatchComplete();
    }

    @Given("^An incorrect product is updated connected to correct work$")
    public void productUpdate() {
        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-W-TSTW10:RBK");
        notificationCountContext.getWriteAttemptsBeforeNegWork =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegWork.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegWork.get(0).attempts;
        }
        Log.info("The work attempts before update are: " + attemptsBefore);

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP10:OOA");
        notificationCountContext.getWriteAttemptsBeforeNegProduct1 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegProduct1.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegProduct1.get(0).attempts;
        }
        Log.info("The product 1 attempts before update are: " + attemptsBefore);

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP20:OOA");
        notificationCountContext.getWriteAttemptsBeforeNegProduct2 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsBeforeNegProduct2.isEmpty()) {
            attemptsBefore = 0;
        } else {
            attemptsBefore = notificationCountContext.getWriteAttemptsBeforeNegProduct2.get(0).attempts;
        }
        Log.info("The product 2 attempts before update are: " + attemptsBefore);

        loadBatchContext.batchId = DataLoadServiceImpl.updateProduct1();
        System.out.println(loadBatchContext.batchId);
        JobUtils.waitTillTheBatchComplete();
    }

    @Then("^The failed product notification is not processed$")
    public void checkProductNotificationStatus() throws InterruptedException {
        int j = 0;
        sql= NotificationsSQL.EPH_GET_Notify_Status.replace("PARAM1",loadBatchContext.batchId);
        do {
            Thread.sleep(1000);
            j++;
            status= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        }while(status.get(0).status.equalsIgnoreCase("UNPROCESSED") && j<30);
        if (status.get(0).status.equalsIgnoreCase("UNPROCESSED")){
            Assert.fail("The notification was not processed!");
        }else{
            Log.info("The notification was processed");
        }

        sql = NotificationsSQL.EPH_GET_Notification_Neg.replace("PARAM1", loadBatchContext.batchId);
        notificationCountContext.getStatusSTNotification = DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);

        for (int i = 0; i < notificationCountContext.getStatusSTNotification.size(); i++) {
            Assert.assertEquals("The notifications are not in Failed status",
                    "ERROR",
                    notificationCountContext.getStatusSTNotification.get(i).status);
        }

        sql = NotificationsSQL.EPH_GET_Write_Attempts.replace("PARAM1", "EPR-TSTP10:OOA");
        notificationCountContext.getWriteAttemptsAfterNegProduct1 =
                DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_URL);
        if (notificationCountContext.getWriteAttemptsAfterNegProduct1.isEmpty()) {
            attemptsAfter = 0;
        } else {
            attemptsAfter = notificationCountContext.getWriteAttemptsAfterNegProduct1.get(0).attempts;
        }
        Log.info("The product 1 attempts after update are: " + attemptsAfter);

        Assert.assertEquals("The product was updated", notificationCountContext.getWriteAttemptsBeforeNegProduct1.get(0).attempts,
                notificationCountContext.getWriteAttemptsBeforeNegProduct1.get(0).attempts);

    }
}
