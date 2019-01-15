package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.ProductExtractSQL;
import com.eph.automation.testing.services.db.sql.WorkDataCheckSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WorksDataCheck {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    public String sql;

    @Given("^We have a work with isbn (.*) id to check$")
    public void setWorkID(String work_id){
        dataQualityContext.productIdentifierID = work_id;
    }

    @Given("^We have a work with issn (.*) id to check$")
    public void setWorkISSNID(String work_issn_id){
        dataQualityContext.productIdentifierID = work_issn_id;
    }

    @When("^We get the product data from PMX, EPH Staging and EPH using (.*)$")
    public void getPMXWorkData(String id){
        if(id.equalsIgnoreCase("isbn")) {
            sql = ProductExtractSQL.PMX_WORK_EXTRACT_BY_ISBN
                    .replace("PARAM1", dataQualityContext.productIdentifierID);
            dataQualityContext.productDataObjectsFromSource = DBManager
                    .getDBResultAsBeanList(sql, ProductDataObject.class,Constants.PMX_SIT_URL);

/*        sql = WorkDataCheckSQL.GET_PMX_WORKS_STG_DATA_ISBN.replace("PARAM1",dataQualityContext.productIdentifierID);
        dataQualityContext.productDataObjectsFromPMXSTG = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);*/

        //sql =  WorkDataCheckSQL.GET_EPH_WORKS_DATA.replace("PARAM1",dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID);
        sql =  WorkDataCheckSQL.GET_EPH_WORKS_DATA.replace("PARAM1","EPR-W-000447");
        dataQualityContext.productDataObjectsFromEPH = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        } else {
            sql = WorkDataCheckSQL.PMX_WORK_EXTRACT_BY_ISSN
                    .replace("PARAM1", dataQualityContext.productIdentifierID);
            dataQualityContext.productDataObjectsFromSource = DBManager
                    .getDBResultAsBeanList(sql, ProductDataObject.class,Constants.PMX_SIT_URL);

/*          sql = WorkDataCheckSQL.GET_PMX_WORKS_STG_DATA_ISSN
                    .replace("PARAM1",dataQualityContext.productIdentifierID);
            dataQualityContext.productDataObjectsFromPMXSTG = DBManager
                    .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);*/

            //sql =  WorkDataCheckSQL.GET_EPH_WORKS_DATA.replace("PARAM1",dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID);
            sql =  WorkDataCheckSQL.GET_EPH_WORKS_DATA.replace("PARAM1","EPR-W-000447");
            dataQualityContext.productDataObjectsFromEPH = DBManager
                    .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        }
    }

    @Then("^The work data between PMX and PMX STG is identical$")
    public void checkPMXtoPMGSTGData() {
        System.out.println(dataQualityContext.productDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE);
        System.out.println(dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE);
        //System.out.println(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TITLE);

/*        if (dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE.equalsIgnoreCase("STAB") ||
                dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE.equalsIgnoreCase("SERMEM")) {
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TYPE, "RBK");
        } else if (dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE.equalsIgnoreCase("CONT") ||
                dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE.equalsIgnoreCase("JNL") ||
                dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE.equalsIgnoreCase("CABS")) {
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE, "JNL");
        } else{
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE, "BKS");
        }*/


        if (dataQualityContext.productDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE==null){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect",
                    dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "N");
        }else if (dataQualityContext.productDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("1")){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect",
                    dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "F");
        } else if (dataQualityContext.productDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("2")) {
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect",
                    dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "H");
        } else if (dataQualityContext.productDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("3")){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect",
                    dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "S");
        }else {
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect",
                    dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "N");
        }/*
        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource
                .equals(dataQualityContext.productDataObjectsFromEPHSTG));*/
/*
        assertTrue("Expecting the Work title details from PMX and PMX Staging Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).WORK_TITLE
                        .equals(dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TITLE));*/

   /*     assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).WORK_SUBTITLE
                        .equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).WORK_SUBTITLE));

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).DAC_KEY
                        .equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).DAC_KEY));

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).PRIMARY_ISBN
                        .equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).PRIMARY_ISBN));

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).PROJECT_NUM
                        .equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).PROJECT_NUM));

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).ISSN_L
                        .equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).ISSN_L));

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).JOURNAL_NUMBER
                        .equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).JOURNAL_NUMBER));

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).ELECTRONIC_RIGHTS_IND
                        .equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).ELECTRONIC_RIGHTS_IND));

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).BOOK_EDITION_NAME
                        .equals(dataQualityContext.productDataObjectsFromEPHSTG.get(0).BOOK_EDITION_NAME));*/
    }
    
    @And("^The work data between PMX STG and EPH is identical$")
    public void checkPMXSTGandEPHData() {
        //     System.out.println(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TITLE);
        System.out.println(dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TYPE);

/*        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND.equalsIgnoreCase("Y")){
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "1");
        } else{
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "0");
        }

         if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE==null){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "N");
        }else if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("1")){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "F");
        } else if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("2")) {
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "H");
        } else if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("3")){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "S");
        }else {
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "N");
        }*/

        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("STAB") ||
                dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("SERMEM")) {
            Assert.assertEquals("The Work type is incorrect",
                    dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TYPE, "RBK");
        } else if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("CONT") ||
                dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("JNL") ||
                dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("CABS")) {
            Assert.assertEquals("The Work type is incorrect",
                    dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TYPE, "JNL");
        } else{
            Assert.assertEquals("The Work type is incorrect",
                    dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TYPE, "BKS");
        }
/*
        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_STATUS.equalsIgnoreCase("1")){
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS, "WAS");
        } else if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_STATUS.equalsIgnoreCase("3")) {
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS, "WAP");
        } else if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_SUBSTATUS.equalsIgnoreCase("DIVESTED")){
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS, "WDI");
        }else{
            Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS, "WST");
        }*/
    }
}
