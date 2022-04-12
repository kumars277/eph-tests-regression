package com.eph.automation.testing.models.ui;
//created by Nishant @ 18 Mar 2021
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.google.inject.Inject;

public class JMTasks {
    private TasksNew tasks;

    @Inject
    public JMTasks(final TasksNew tasks) {
        this.tasks = tasks;
    }

    public void accessRoleURL(String role) throws InterruptedException {
        //created by Nishant @ 18 Mar 2021
        String jmRole_URI="";
        switch (TestContext.getValues().environment)
        {
            case "SIT":jmRole_URI= Constants.JM_ROLE_URL_SIT;break;
            case "UAT":jmRole_URI=Constants.JM_ROLE_URL_UAT;break;
            default:jmRole_URI=Constants.JM_ROLE_URL_SIT;break;
        }
        jmRole_URI+=role;
      //  tasks.openPage(tasks.authenticateUri(jmRole_URI.replace("https://","")));
        Thread.sleep(1000);
        tasks.waitUntilPageLoad();
        Log.info("\nrole URI accessed...");
    }

    public void openJMHomePage() throws InterruptedException {
        String HomePageAddress="";
        switch (TestContext.getValues().environment)
        {
            case "SIT":HomePageAddress= Constants.JM_UI_SIT;break;
            case "UAT":HomePageAddress=Constants.JM_UI_UAT;break;
            case "UAT2":HomePageAddress=Constants.JM_UI_UAT;break;
            default:HomePageAddress=Constants.JM_UI_SIT;break;
        }

        tasks.openPage(HomePageAddress);
        Thread.sleep(1000);
        tasks.waitUntilPageLoad();
        tasks.verifyElementisDisplayed("XPATH","//a[contains (@title,'Start a form for a journal lifecycle event.')]");

        Log.info("\nhome page loaded...");
        tasks.click("XPATH","//a[contains (@title,'Start a form for a journal lifecycle event.')]");
    }
}
