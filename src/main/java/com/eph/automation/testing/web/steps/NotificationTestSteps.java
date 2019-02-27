package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.NotificationCountContext;
import com.eph.automation.testing.models.dao.NotificationDataObject;
import com.eph.automation.testing.models.dao.ProductCountObject;
import com.eph.automation.testing.services.db.sql.NotificationsSQL;
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
        Log.info("\nThe number of products in GD is: " + notificationCountContext.gdCountNumber.get(0).ephGDCount);
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
}
