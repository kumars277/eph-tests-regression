package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.NotificationCountContext;
import com.eph.automation.testing.models.dao.NotificationDataObject;
import com.eph.automation.testing.services.db.sql.NotificationsSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

public class NotificationTestSteps {
    @StaticInjection
    public NotificationCountContext notificationCountContext;
    private String sql;


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
}
