package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.GeneratingRandomSQL;
import com.eph.automation.testing.services.db.sql.WorkExtractSQL;
import com.eph.automation.testing.services.db.sql.WorkDataCheckSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import com.eph.automation.testing.helper.Log;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WorksDataCheck {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    public String sql;
    private static List<WorkDataObject> isbn;

    @Given("^We have a work with (.*) id and type (.*) to check$")
    public void setWorkID(String work_id, String type){
        sql = GeneratingRandomSQL.generatingValue
                .replace("PARAM1", work_id)
                .replace("PARAM2", type);
        //Log.info
        // (sql+"\n");
        isbn = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        dataQualityContext.productIdentifierID = isbn.get(0).random_value;
        Log.info
                ("\n" + work_id + " is " + dataQualityContext.productIdentifierID+" and type is "
                +type+"\n");
    }

    @When("^We get the product data from PMX, EPH Staging and EPH using (.*)$")
    public void getPMXWorkData(String id){
            sql = WorkExtractSQL.PMX_WORK_EXTRACT
                    .replace("PARAM1",id)
                    .replace("PARAM2", dataQualityContext.productIdentifierID);
        //Log.info
        // (sql);
            dataQualityContext.workDataObjectsFromSource = DBManager
                    .getDBResultAsBeanList(sql, WorkDataObject.class,Constants.PMX_URL);

        sql = WorkDataCheckSQL.GET_PMX_WORKS_STG_DATA
                .replace("PARAM1", id)
                .replace("PARAM2",dataQualityContext.productIdentifierID);
        //Log.info
        // (sql);
        dataQualityContext.workDataObjectsFromPMXSTG = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

        sql =  WorkDataCheckSQL.GET_STG_DQ_WORKS_DATA.replace("PARAM1",dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID);
        Log.info
         (sql);
        dataQualityContext.workDataObjectsFromSTGDQ = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

        sql =  WorkDataCheckSQL.GET_EPH_WORKS_DATA.replace("PARAM1",dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID);
        dataQualityContext.workDataObjectsFromEPH = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

        sql =  WorkDataCheckSQL.GET_EPH_GD_WORKS_DATA.replace("PARAM1",dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID);
        dataQualityContext.workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    @Then("^The work data between PMX and PMX STG is identical$")
    public void checkPMXtoPMGSTGData() {
        Log.info
                (dataQualityContext.workDataObjectsFromSource.get(0).WORK_TITLE);
        //Log.info
        // (dataQualityContext.workDataObjectsFromEPH.WORK_TITLE);
        Log.info
                (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TITLE);


        assertTrue("Expecting the Work title details from PMX and PMX Staging Consistent for id=" + dataQualityContext.productIdentifierID,
                dataQualityContext.workDataObjectsFromSource.get(0).WORK_TITLE
                        .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TITLE));
        if (dataQualityContext.workDataObjectsFromSource.get(0).WORK_SUBTITLE!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_SUBTITLE !=null) {
        assertTrue("Expecting the WORK_SUBTITLE details from PMX and EPH Consistent  for id=" + dataQualityContext.productIdentifierID,
                dataQualityContext.workDataObjectsFromSource.get(0).WORK_SUBTITLE
                        .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_SUBTITLE));
    }        if (dataQualityContext.workDataObjectsFromSource.get(0).DAC_KEY!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).DAC_KEY !=null) {
        assertTrue("Expecting the DAC_KEY details from PMX and EPH Consistent  for id=" + dataQualityContext.productIdentifierID,
                dataQualityContext.workDataObjectsFromSource.get(0).DAC_KEY
                        .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).DAC_KEY));
        }
        if (dataQualityContext.workDataObjectsFromSource.get(0).PRIMARY_ISBN!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRIMARY_ISBN !=null) {
            assertTrue("Expecting the PRIMARY_ISBN details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).PRIMARY_ISBN
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRIMARY_ISBN));
        }
        if (dataQualityContext.workDataObjectsFromSource.get(0).PROJECT_NUM!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).PROJECT_NUM !=null) {
            assertTrue("Expecting the PROJECT_NUM from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).PROJECT_NUM
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).PROJECT_NUM));
        }
        if (dataQualityContext.workDataObjectsFromSource.get(0).ISSN_L!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).ISSN_L !=null) {
            assertTrue("Expecting ISSN_L from PMX and EPH Consistent for id=",
                    dataQualityContext.workDataObjectsFromSource.get(0).ISSN_L
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).ISSN_L));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).JOURNAL_NUMBER!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).JOURNAL_NUMBER !=null) {
        assertTrue("Expecting the JOURNAL_NUMBER details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                dataQualityContext.workDataObjectsFromSource.get(0).JOURNAL_NUMBER
                       .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).JOURNAL_NUMBER));
        }
        if (dataQualityContext.workDataObjectsFromSource.get(0).ELECTRONIC_RIGHTS_IND!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND !=null) {
            assertTrue("Expecting the ELECTRONIC_RIGHTS_IND details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).ELECTRONIC_RIGHTS_IND
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).BOOK_EDITION_NAME!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).BOOK_EDITION_NAME !=null) {
            assertTrue("Expecting the BOOK EDITION NAME details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).BOOK_EDITION_NAME
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).BOOK_EDITION_NAME));
        }
        if (dataQualityContext.workDataObjectsFromSource.get(0).BOOK_VOLUME_NAME!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).BOOK_VOLUME_NAME !=null) {
            assertTrue("Expecting the BOOK VOLUME NAME details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).BOOK_VOLUME_NAME
                       .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).BOOK_VOLUME_NAME));
        }
        if (dataQualityContext.workDataObjectsFromSource.get(0).PMC!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).PMC !=null) {
                    assertTrue("Expecting the PMC details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                            dataQualityContext.workDataObjectsFromSource.get(0).PMC
                                    .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).PMC));
            }
       if (dataQualityContext.workDataObjectsFromSource.get(0).PMG!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).PMG !=null) {
           assertTrue("Expecting the PMG details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                   dataQualityContext.workDataObjectsFromSource.get(0).PMG
                           .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).PMG));
       }

        if (dataQualityContext.workDataObjectsFromSource.get(0).WORK_ID!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_ID !=null) {
            assertTrue("Expecting the WORK_ID details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).WORK_ID
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_ID));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).WORK_STATUS!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_STATUS !=null) {
            assertTrue("Expecting the WORK_STATUS details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).WORK_STATUS
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_STATUS));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).WORK_SUBSTATUS!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_SUBSTATUS !=null) {
            assertTrue("Expecting the WORK_SUBSTATUS details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).WORK_SUBSTATUS
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_SUBSTATUS));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).WORK_TYPE!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TYPE !=null) {
            assertTrue("Expecting the WORK_TYPE details from PMX and EPH Consistent for id=",
                    dataQualityContext.workDataObjectsFromSource.get(0).WORK_TYPE
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TYPE));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).IMPRINT!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).IMPRINT !=null) {
            assertTrue("Expecting the IMPRINT details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).IMPRINT
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).IMPRINT));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE !=null) {
            assertTrue("Expecting the OPEN_ACCESS_JNL_TYPE details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).PRODUCT_WORK_ID!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID !=null) {
            assertTrue("Expecting the PRODUCT_WORK_ID details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).PRODUCT_WORK_ID
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).F_ACC_PROD_HIERARCHY!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).F_ACC_PROD_HIERARCHY !=null) {
            assertTrue("Expecting the F_ACC_PROD_HIERARCHY details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).F_ACC_PROD_HIERARCHY
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).F_ACC_PROD_HIERARCHY));
        }
        if (dataQualityContext.workDataObjectsFromSource.get(0).F_RESPONSIBILITY_CENTRE!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).F_RESPONSIBILITY_CENTRE !=null) {
            assertTrue("Expecting the F_RESPONSIBILITY_CENTRE details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).F_RESPONSIBILITY_CENTRE
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).F_RESPONSIBILITY_CENTRE));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).F_OPCO_R12!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).F_OPCO_R12 !=null) {
            assertTrue("Expecting the F_OPCO_R12 details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).F_OPCO_R12
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).F_OPCO_R12));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).PRODUCT_WORK_PUB_DATE!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE !=null) {
            assertTrue("Expecting the Product_WORK_PUB_DATE details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).PRODUCT_WORK_PUB_DATE.substring(0,10)
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE));


        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).JOURNAL_ACRONYM!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).JOURNAL_ACRONYM !=null) {
            assertTrue("Expecting the Journal Acronym details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).JOURNAL_ACRONYM
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).JOURNAL_ACRONYM));
        }

        if (dataQualityContext.workDataObjectsFromSource.get(0).OWNERSHIP!=null
                || dataQualityContext.workDataObjectsFromPMXSTG.get(0).OWNERSHIP !=null) {
            assertTrue("Expecting the Journal Acronym details from PMX and EPH Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSource.get(0).OWNERSHIP
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(0).OWNERSHIP));
        }

    }
    
    @And("^The work data between PMX STG and STG DQ is identical$")
    public void checkPMXSTGandDQData() {

        assertTrue("Expecting the Work title details from PMX Staging and STG DQ Consistent for id="+ dataQualityContext.productIdentifierID,
                dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TITLE
                        .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TITLE));

        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_SUBTITLE!=null
                || dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_SUBTITLE !=null) {
            assertTrue("Expecting the WORK_SUBTITLE details from PMX STG and DQ Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_SUBTITLE
                            .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_SUBTITLE));
        }

        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE!=null
                || dataQualityContext.workDataObjectsFromSTGDQ.get(0).PRODUCT_WORK_PUB_DATE !=null) {

            System.out.print(dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE.substring(0,4) + " \n" +
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).PRODUCT_WORK_PUB_DATE.substring(0,4));
            assertTrue("Expecting the WORK_PUB_DATE details from STG and DQ Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE.substring(0,4)
                            .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(0).PRODUCT_WORK_PUB_DATE.substring(0,4)));

        }

        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND!=null) {
            if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND.equalsIgnoreCase("Y")) {
                Assert.assertEquals("The Work type is incorrect for id="+ dataQualityContext.productIdentifierID
                        , dataQualityContext.workDataObjectsFromSTGDQ.get(0).ELECTRONIC_RIGHTS_IND, "1");
            } else {
                Assert.assertEquals("The Work type is incorrect for id="+ dataQualityContext.productIdentifierID,
                        dataQualityContext.workDataObjectsFromSTGDQ.get(0).ELECTRONIC_RIGHTS_IND, "0");
            }
        }
         if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE==null){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "N");
        }else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("1")){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "F");
        } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("2")) {
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "H");
        } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE.equalsIgnoreCase("3")){
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "S");
        }else {
            Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "N");
        }

        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).BOOK_VOLUME_NAME!=null
                || dataQualityContext.workDataObjectsFromSTGDQ.get(0).BOOK_VOLUME_NAME !=null) {
            Assert.assertEquals("The volume is not 0 for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).BOOK_VOLUME_NAME, "0");
        }



        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).PMC!=null
                || dataQualityContext.workDataObjectsFromSTGDQ.get(0).PMC !=null) {
            assertTrue("Expecting the PMC details from STG and DQ Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromPMXSTG.get(0).PMC
                            .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(0).PMC));
        }

        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TYPE==null
                || dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TYPE ==null){
            Assert.assertEquals("The Work type is incorrect for id="+ dataQualityContext.productIdentifierID,
                    "UNK",dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TYPE);
        } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("STAB") ||
                dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("SERMEM")) {
            Assert.assertEquals("The Work type is incorrect for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TYPE, "RBK");
        } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("CONT") ||
                dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("JNL") ||
                dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_TYPE.equalsIgnoreCase("CABS")) {
            Assert.assertEquals("The Work type is incorrect for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TYPE, "JNL");
        } else{
            Assert.assertEquals("The Work type is incorrect for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TYPE, "BKS");
        }


        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_STATUS==null
                || dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_STATUS ==null){
            Assert.assertEquals("The Work status is incorrect for id="+ dataQualityContext.productIdentifierID,
                    "UNK",dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_STATUS);
        } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_SUBSTATUS.equalsIgnoreCase("DIVESTED")) {
            Assert.assertEquals("The Work status is incorrect for id=" + dataQualityContext.productIdentifierID,
                    "WDI", dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_STATUS);
        } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_STATUS.equalsIgnoreCase("1")){
            Assert.assertEquals("The Work status is incorrect for id="+ dataQualityContext.productIdentifierID,
                    "WAS",dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_STATUS);
        } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).WORK_STATUS.equalsIgnoreCase("3")) {
            Assert.assertEquals("The Work status is incorrect for id="+ dataQualityContext.productIdentifierID
                    , "WAP", dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_STATUS);
        } else{
            Assert.assertEquals("The Work status is incorrect for id="+ dataQualityContext.productIdentifierID,
                    "WST", dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_STATUS);
        }

        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).IMPRINT!=null
                || dataQualityContext.workDataObjectsFromSTGDQ.get(0).IMPRINT !=null) {
            assertTrue("Expecting the Imprint details from STG and DQ Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromPMXSTG.get(0).IMPRINT
                            .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(0).IMPRINT));
        }

        if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).OWNERSHIP!=null) {
            if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).OWNERSHIP.equalsIgnoreCase("EFO")) {
                Assert.assertEquals("The Ownership is incorrect for id="+ dataQualityContext.productIdentifierID
                        , dataQualityContext.workDataObjectsFromSTGDQ.get(0).OWNERSHIP, "ELSOWN");
            } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(0).OWNERSHIP.equalsIgnoreCase("SFO")){
                Assert.assertEquals("The Ownership is incorrect for id="+ dataQualityContext.productIdentifierID,
                        dataQualityContext.workDataObjectsFromSTGDQ.get(0).OWNERSHIP, "SOCOWN");
            }else {
                Assert.assertEquals("The Ownership is incorrect for id="+ dataQualityContext.productIdentifierID,
                        dataQualityContext.workDataObjectsFromSTGDQ.get(0).OWNERSHIP, "COOWN");
            }
        }

    }

    @And("^The work data between STG DQ and SA is identical$")
    public void checkDQandSAData() {
        assertTrue("Expecting the Work title details from DQ and SA Staging Consistent for id="+ dataQualityContext.productIdentifierID,
                dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TITLE
                        .equals(dataQualityContext.workDataObjectsFromEPH.get(0).WORK_TITLE));

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_SUBTITLE!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).WORK_SUBTITLE !=null) {
            assertTrue("Expecting the WORK subtitle from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_SUBTITLE
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).WORK_SUBTITLE));
        }

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).PRODUCT_WORK_PUB_DATE!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE !=null) {
            assertTrue("Expecting the WORK pub date from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).PRODUCT_WORK_PUB_DATE
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE));
        }
        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).ELECTRONIC_RIGHTS_IND!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).ELECTRONIC_RIGHTS_IND !=null) {
            assertTrue("Expecting the Electronic rights ind details from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).ELECTRONIC_RIGHTS_IND
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).ELECTRONIC_RIGHTS_IND));
        }

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).BOOK_VOLUME_NAME!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).BOOK_VOLUME_NAME !=null) {
            assertTrue("Expecting the Volume details from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).BOOK_VOLUME_NAME
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).BOOK_VOLUME_NAME));
        }

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).OPEN_ACCESS_JNL_TYPE_CODE!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE !=null) {
            assertTrue("Expecting the Open access details from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).OPEN_ACCESS_JNL_TYPE_CODE
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE));
        }

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).PMC!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).PMC !=null) {
            assertTrue("Expecting the PMC details from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).PMC
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).PMC));
        }

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TYPE!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).WORK_TYPE !=null) {
            assertTrue("Expecting the Work type details from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_TYPE
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).WORK_TYPE));
        }

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_STATUS!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).WORK_STATUS !=null) {
            assertTrue("Expecting the Work status details from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).WORK_STATUS
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).WORK_STATUS));
        }

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).IMPRINT!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).IMPRINT !=null) {
            assertTrue("Expecting the Imprint details from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).IMPRINT
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).IMPRINT));
        }

        if (dataQualityContext.workDataObjectsFromSTGDQ.get(0).OWNERSHIP!=null
                || dataQualityContext.workDataObjectsFromEPH.get(0).OWNERSHIP !=null) {
            assertTrue("Expecting the Ownership details from DQ and SA Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromSTGDQ.get(0).OWNERSHIP
                            .equals(dataQualityContext.workDataObjectsFromEPH.get(0).OWNERSHIP));
        }
    }


    @And("^The work data between EPH SA and EPH GD is identical$")
    public void checkBetweenSAandGD(){
        assertTrue("Expecting the Work title details from SA and GD Staging Consistent for id="+ dataQualityContext.productIdentifierID,
                dataQualityContext.workDataObjectsFromEPH.get(0).WORK_TITLE
                        .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).WORK_TITLE));

        if (dataQualityContext.workDataObjectsFromEPH.get(0).WORK_SUBTITLE!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).WORK_SUBTITLE !=null) {
            assertTrue("Expecting the Work subtitle details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).WORK_SUBTITLE
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).WORK_SUBTITLE));
        }

        if (dataQualityContext.workDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).PRODUCT_WORK_PUB_DATE !=null) {
            assertTrue("Expecting the Pub date details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).PRODUCT_WORK_PUB_DATE));
        }
        if (dataQualityContext.workDataObjectsFromEPH.get(0).ELECTRONIC_RIGHTS_IND!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).ELECTRONIC_RIGHTS_IND !=null) {
            assertTrue("Expecting the Electronic Rights Ind details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).ELECTRONIC_RIGHTS_IND
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).ELECTRONIC_RIGHTS_IND));
        }

        if (dataQualityContext.workDataObjectsFromEPH.get(0).BOOK_VOLUME_NAME!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).BOOK_VOLUME_NAME !=null) {
            assertTrue("Expecting the BOOK_VOLUME_NAME details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).BOOK_VOLUME_NAME
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).BOOK_VOLUME_NAME));
        }

        if (dataQualityContext.workDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).OPEN_ACCESS_JNL_TYPE_CODE !=null) {
            assertTrue("Expecting the Open access details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).OPEN_ACCESS_JNL_TYPE_CODE));
        }

        if (dataQualityContext.workDataObjectsFromEPH.get(0).PMC!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).PMC !=null) {
            assertTrue("Expecting the PMC details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).PMC
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).PMC));
        }

        if (dataQualityContext.workDataObjectsFromEPH.get(0).WORK_TYPE!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).WORK_TYPE !=null) {
            assertTrue("Expecting the Work type details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).WORK_TYPE
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).WORK_TYPE));
        }

        if (dataQualityContext.workDataObjectsFromEPH.get(0).WORK_STATUS!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).WORK_STATUS !=null) {
            assertTrue("Expecting the Work status details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).WORK_STATUS
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).WORK_STATUS));
        }

        if (dataQualityContext.workDataObjectsFromEPH.get(0).IMPRINT!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).IMPRINT !=null) {
            assertTrue("Expecting the Imprint details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).IMPRINT
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).IMPRINT));
        }

        if (dataQualityContext.workDataObjectsFromEPH.get(0).OWNERSHIP!=null
                || dataQualityContext.workDataObjectsFromEPHGD.get(0).OWNERSHIP !=null) {
            assertTrue("Expecting the Ownership details from SA and GD Consistent for id="+ dataQualityContext.productIdentifierID,
                    dataQualityContext.workDataObjectsFromEPH.get(0).OWNERSHIP
                            .equals(dataQualityContext.workDataObjectsFromEPHGD.get(0).OWNERSHIP));
        }
    }
}
