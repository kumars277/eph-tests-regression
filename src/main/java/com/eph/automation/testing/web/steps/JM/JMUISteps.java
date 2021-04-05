package com.eph.automation.testing.web.steps.JM;
//created by Nishant @ 18 Mar 2021
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.ui.JMTasks;
import com.eph.automation.testing.models.ui.ProductFinderConstants;
import com.eph.automation.testing.models.ui.ProductFinderTasks;
import com.eph.automation.testing.models.ui.TasksNew;
import com.google.inject.Inject;
import cucumber.api.java.en.Given;

import java.text.ParseException;

public class JMUISteps {
    private JMTasks jmTasks;
    private TasksNew tasks;
    @Inject
    public JMUISteps(JMTasks jmTasks, TasksNew tasks) {
        this.jmTasks = jmTasks;
        this.tasks = tasks;
    }
    @Given("user is on JM home page as (.*)")
    public void userOpensHomePage(String role) throws InterruptedException, ParseException {
        //created by Nishant @ 18 Mar 2021
        jmTasks.accessRoleURL(role);
        jmTasks.openJMHomePage();
     //   jmTasks.loginByScienceAccount(ProductFinderConstants.SCIENCE_ID);
        tasks.waitUntilPageLoad();

    }
}
