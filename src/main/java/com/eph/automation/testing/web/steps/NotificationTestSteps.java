package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.NotificationCountContext;
import com.eph.automation.testing.models.dao.NotificationDataObject;
import com.eph.automation.testing.models.dao.ProductCountObject;
import com.eph.automation.testing.services.db.sql.NotificationsSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import com.eph.automation.testing.helper.Log;

public class NotificationTestSteps {
    @StaticInjection
    public NotificationCountContext notificationCountContext;
    private String sql;


    @Given("^We know the number of (.*) GD records after a full-load$")
    public void getGDRecordsAfterFullLoad(String table){
        sql= NotificationsSQL.EPH_GD_PRODUCT_Count.replace("PARAM1",table);
        notificationCountContext.gdCountNumber= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
        Log.info("\nThe number of records in GD is: " + notificationCountContext.gdCountNumber.get(0).ephGDCount);
    }

    @When("^We check the created notifications by (.*)$")
    public void getNotifications(String type){
        sql= NotificationsSQL.EPH_Notifications_Count.replace("PARAM1",type);
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
        sql= NotificationsSQL.EPH_GD_ID.replace("PARAM1",table)
        .replace("PARAM2", type);
        notificationCountContext.gdID= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
        Log.info("\nThe GD ID is: " + notificationCountContext.gdID.get(0).gdID);

        sql= NotificationsSQL.EPH_Notification_ID.replace("PARAM1",type);
        notificationCountContext.notificationID= DBManager.getDBResultAsBeanList(sql, NotificationDataObject.class, Constants.EPH_SIT_URL);
        Log.info("\nThe Notification ID is: " + notificationCountContext.notificationID.get(0).notificationID);

        if(notificationCountContext.gdCountNumber.get(0).ephGDCount==1) {
            Assert.assertEquals("The notification id is not the same as the GD ID!",
                    notificationCountContext.gdID.get(0).gdID, notificationCountContext.notificationID.get(0).notificationID);
        }
        else{
            Log.info("The ids are matching");
        }
    }
}
