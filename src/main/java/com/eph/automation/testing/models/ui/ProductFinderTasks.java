package com.eph.automation.testing.models.ui;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.Product;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.google.inject.Inject;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ProductFinderTasks {

    private TasksNew tasks;

    @Inject
    public ProductFinderTasks(final TasksNew tasks){this.tasks = tasks;}

    public static String searchResultWorkId;
    public Properties prop_info =new Properties();
    public Properties prop_subArea=new Properties();
    public Properties prop_identifier=new Properties();
    public Properties prop_comCode =new Properties();
    public Properties prop_AccProducts =new Properties();
    public void openHomePage() throws InterruptedException {//updated by Nishant @ 18 May 2020

        if(TestContext.getValues().environment.equalsIgnoreCase("SIT"))
        tasks.openPage(Constants.PRODUCT_FINDER_EPH_SIT_UI);
        else if(TestContext.getValues().environment.equalsIgnoreCase("UAT"))
            tasks.openPage(Constants.PRODUCT_FINDER_EPH_UAT_UI);
        Thread.sleep(1000);
        tasks.waitUntilPageLoad();
        Log.info("home page loaded...");
    }

    public void loginByScienceAccount(String scienceEmailId) throws InterruptedException{
        //updated by Nishant @ 13 Feb 2020

      //  if(!tasks.isObjectpresent("XPATH",ProductFinderConstants.userDetail))

        try{
            tasks.sendKeys("NAME", ProductFinderConstants.loginByEmail, System.getenv("username") + scienceEmailId);
            tasks.click("ID", ProductFinderConstants.nextButton);
        }
        catch(Exception e)
        {
            Log.info("Sign in with Science id absent");
            Log.info(e.getMessage());}
    }

    public void searchFor(final String searchID) throws InterruptedException {
        //updated by Nishant @ 14 May 2020
       while(!tasks.isObjectpresent("XPATH",ProductFinderConstants.searchBar))
       {
           tasks.driver.navigate().refresh();
           Thread.sleep(3000);
       }
        tasks.clearText("XPATH",ProductFinderConstants.searchBar);
        tasks.sendKeys("XPATH",ProductFinderConstants.searchBar,searchID);
        tasks.click("XPATH",ProductFinderConstants.searchButton);
        Thread.sleep(3000);
    }

    public boolean isPageContainingString(String text){return tasks.doesPageContainsText(text);}

    public void clickProductAndPackages_tab() throws InterruptedException {
        //created by Nishant @ 19 May 2020
        tasks.verifyElementisClickable("XPATH", ProductFinderConstants.tab_product_andPackages);
        tasks.click("XPATH",ProductFinderConstants.tab_product_andPackages);
        Thread.sleep(3000);
    }

    public void clickWork(String workId) throws InterruptedException {
        //created by Nishant @ 22 May 2020
        String buildWorkIdLocator = String.format(ProductFinderConstants.buildWorkIdLocator,workId);
        tasks.click("XPATH", buildWorkIdLocator);
        Thread.sleep(5000);
    }

    public void clickManifestation_tab() throws InterruptedException {
        //created by Nishant @ 19 May 2020
        tasks.verifyElementisClickable("XPATH", ProductFinderConstants.tab_manifestation);
        tasks.click("XPATH",ProductFinderConstants.tab_manifestation);
        Thread.sleep(3000);
    }

    public void clickMaximumResultPerPage() throws InterruptedException {
        //created by Nishant @ 18 May 2020
        List<WebElement> elements=null;
        if(tasks.isObjectpresent("XPATH",ProductFinderConstants.resultPerPage_100))
            elements = tasks.findmultipleElements("XPATH", ProductFinderConstants.resultPerPage_100);
        else elements=tasks.findmultipleElements("XPATH",ProductFinderConstants.resultPerPage_50);

        elements.get(0).click();
        Thread.sleep(3000);
    }

    public String[] getPageNofMCount(){
        List<WebElement> elements=tasks.findmultipleElements("XPATH",ProductFinderConstants.SearchResultPageCount);
        return elements.get(0).getText().split(" ");
    }

    public void goToNextPage(){tasks.click("XPATH",ProductFinderConstants.nextPageButton);}

    public boolean searchOnResultPages(String SearchingTerm) throws InterruptedException {
        //created by Nishant @ 21 May 2020
        boolean searchFound=false;
        clickMaximumResultPerPage();
        tasks.waitUntilPageLoad();
        String[] pageNofM=getPageNofMCount();
        int looper=Integer.valueOf(pageNofM[pageNofM.length-1]);
        for(int i=0;i<looper;i++)
        {
            Log.info("searching on page ..."+(i+1)+" - "+SearchingTerm);
            if(!isPageContainingString(SearchingTerm))
            {
                goToNextPage();
                Thread.sleep(5000);
            }
            else {searchFound=true;break;}
        }
        if(!searchFound)Log.info("search complete, could not find "+SearchingTerm+" on all pages");
        return searchFound;
    }

    public WebElement getElementByTitle(String title){return tasks.findElementByText(title);}

    public boolean isUserOnWorkPage(String workID) {
        //updated by Nishant @ 18 May 2020
        String targetURL="";
        if(TestContext.getValues().environment.equalsIgnoreCase("SIT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI+"work/"+workID+"/overview";
        else if(TestContext.getValues().environment.equalsIgnoreCase("UAT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI+"work/"+workID+"/overview";

        Log.info("Target URL "+targetURL);
       if(targetURL.equalsIgnoreCase(tasks.getCurrentPageUrl()))return true; else return false;
    }

    public boolean isUserOnProductPage(String productId) {
        //created by Nishant @ 19 May 2020
        String targetURL="";
        if(TestContext.getValues().environment.equalsIgnoreCase("SIT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI+"product/"+productId+"/overview";
        else if(TestContext.getValues().environment.equalsIgnoreCase("UAT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI+"product/"+productId+"/overview";

        Log.info("Target URL "+targetURL);
        if(targetURL.equalsIgnoreCase(tasks.getCurrentPageUrl()))return true; else return false;
    }

    public boolean isUserOnManifestationPage(String manifestationId) {
        //created by Nishant @ 19 May 2020
        String targetURL="";
        String mani_work=DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getF_WWORK();
        if(TestContext.getValues().environment.equalsIgnoreCase("SIT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_SIT_UI+"manifestation/"+mani_work+"/"+manifestationId+"/overview";
        else if(TestContext.getValues().environment.equalsIgnoreCase("UAT"))
            targetURL = Constants.PRODUCT_FINDER_EPH_UAT_UI+"manifestation/"+mani_work+"/"+manifestationId+"/overview";

        Log.info("Target URL "+targetURL);
        if(targetURL.equalsIgnoreCase(tasks.getCurrentPageUrl()))return true; else return false;
    }

    public void getUI_WorkOverview_Information(){
        //created by Nishant @5 Jun 2020
        String section="//div[@class='section']"; //parent of - subject area and information
        String section_identifier="//div[@class='section identifiers']"; //parent of - identifier

        String sectionDetailInformation =section+"/div[@class='section-detail'][1]";
        String sectionDetailSubArea=section+"/div[@class='section-detail'][2]";
        String sectionDetailIdentifiers=section_identifier+"//div[@class='section-detail']";

        //table[@class='mat-table']/parent::div[@class='section-detail']/following-sibling::h2

        //capture Information values
                List<WebElement> rows_info= tasks.findmultipleElements("XPATH",sectionDetailInformation+"/table/tbody/tr");
               for(int i=0;i< rows_info.size();i++) {
                   String key=tasks.findElement("XPATH",sectionDetailInformation+"/table/tbody/tr["+(i+1)+"]/td[1]").getText();
                   String value=tasks.findElement("XPATH",sectionDetailInformation+"/table/tbody/tr["+(i+1)+"]/td[2]").getText();
                   prop_info.setProperty(key,value);
               }

        //capture subject area values
        List<WebElement> rows_subArea= tasks.findmultipleElements("XPATH",sectionDetailSubArea+"/table/tbody/tr");
        for(int i=0;i< rows_subArea.size();i++) {
            String key=tasks.findElement("XPATH",sectionDetailSubArea+"/table/tbody/tr["+(i+1)+"]/td[1]").getText();
            String value=tasks.findElement("XPATH",sectionDetailSubArea+"/table/tbody/tr["+(i+1)+"]/td[2]").getText();
            prop_subArea.setProperty(key,value);
        }

        //capture identifiers values
        List<WebElement> rows_identifiers= tasks.findmultipleElements("XPATH",sectionDetailIdentifiers+"/table/tbody/tr");
        for(int i=0;i< rows_identifiers.size();i++) {
            String key=tasks.findElement("XPATH",sectionDetailIdentifiers+"/table/tbody/tr["+(i+1)+"]/td[1]").getText();
            String value=tasks.findElement("XPATH",sectionDetailIdentifiers+"/table/tbody/tr["+(i+1)+"]/td[2]").getText();
            prop_identifier.setProperty(key,value);
        }




    }


    public void getUI_WorkOverview_Financial(){ //created by Nishant @15 Jun 2020
        String section="//div[@class='section']"; //parent of - Financial Information
        String costInfo =section+"/div[@class='section-detail'][1]";
        String accountableProduct=section+"/div[@class='section-detail'][2]";

        //click on Financial tab
        String financialTab="//*[@id='mat-tab-label-0-2']/div";
        tasks.click("XPATH",financialTab);

        //capture company code values
        List<WebElement> rows_info= tasks.findmultipleElements("XPATH",costInfo+"/table/tbody/tr");
        for(int i=0;i< rows_info.size();i++) {
            String key=tasks.findElement("XPATH",costInfo+"/table/tbody/tr["+(i+1)+"]/td[1]").getText();
            String value=tasks.findElement("XPATH",costInfo+"/table/tbody/tr["+(i+1)+"]/td[2]").getText();
            prop_comCode.setProperty(key,value);
        }

        //capture accountable product values
        List<WebElement> rows_subArea= tasks.findmultipleElements("XPATH",accountableProduct+"/table/tbody/tr");
        for(int i=0;i< rows_subArea.size();i++) {
            String key=tasks.findElement("XPATH",accountableProduct+"/table/tbody/tr["+(i+1)+"]/td[1]").getText();
            String value=tasks.findElement("XPATH",accountableProduct+"/table/tbody/tr["+(i+1)+"]/td[2]").getText();
            prop_AccProducts.setProperty(key,value);
        }

        Log.info("teting...");



    }


}
