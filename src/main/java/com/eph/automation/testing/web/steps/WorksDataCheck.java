package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.GeneratingRandomSQL;
import com.eph.automation.testing.services.db.sql.ProductExtractSQL;
import com.eph.automation.testing.services.db.sql.WorkDataCheckSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WorksDataCheck {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    public String sql;
    private static List<ProductDataObject> isbn;

    @Given("^We have a work with (.*) id and type (.*) to check$")
    public void setWorkID(String work_id, String type){
        sql = GeneratingRandomSQL.generatingValue
                .replace("PARAM1", work_id)
                .replace("PARAM2", type);
        //System.out.println(sql+"\n");
        isbn = DBManager.getDBResultAsBeanList(sql,ProductDataObject.class, Constants.EPH_SIT_URL);
        dataQualityContext.productIdentifierID = isbn.get(0).random_value;
        System.out.println("\n" + work_id + " is " + dataQualityContext.productIdentifierID+" and type is "
                +type+"\n");
    }

    @When("^We get the product data from PMX, EPH Staging and EPH using (.*)$")
    public void getPMXWorkData(String id){
            sql = ProductExtractSQL.PMX_WORK_EXTRACT
                    .replace("PARAM1",id)
                    .replace("PARAM2", dataQualityContext.productIdentifierID);
        //System.out.println(sql);
            dataQualityContext.productDataObjectsFromSource = DBManager
                    .getDBResultAsBeanList(sql, ProductDataObject.class,Constants.PMX_SIT_URL);

        sql = WorkDataCheckSQL.GET_PMX_WORKS_STG_DATA
                .replace("PARAM1", id)
                .replace("PARAM2",dataQualityContext.productIdentifierID);
        //System.out.println(sql);
        dataQualityContext.productDataObjectsFromPMXSTG = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);

        sql =  WorkDataCheckSQL.GET_EPH_WORKS_DATA.replace("PARAM1",dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID);
        //System.out.println(sql);
        dataQualityContext.productDataObjectsFromEPH = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);

        sql =  WorkDataCheckSQL.GET_EPH_GD_WORKS_DATA.replace("PARAM1",dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID);
        dataQualityContext.productDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
    }

    @Then("^The work data between PMX and PMX STG is identical$")
    public void checkPMXtoPMGSTGData() {
        System.out.println(dataQualityContext.productDataObjectsFromSource.get(0).WORK_TITLE);
        //System.out.println(dataQualityContext.productDataObjectsFromEPH.WORK_TITLE);
        System.out.println(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TITLE);


        assertTrue("Expecting the Work title details from PMX and PMX Staging Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).WORK_TITLE
                        .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TITLE));
        if (dataQualityContext.productDataObjectsFromSource.get(0).WORK_SUBTITLE!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_SUBTITLE !=null) {
        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).WORK_SUBTITLE
                        .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_SUBTITLE));
    }        if (dataQualityContext.productDataObjectsFromSource.get(0).DAC_KEY!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).DAC_KEY !=null) {
        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).DAC_KEY
                        .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).DAC_KEY));
        }
        if (dataQualityContext.productDataObjectsFromSource.get(0).PRIMARY_ISBN!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRIMARY_ISBN !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).PRIMARY_ISBN
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRIMARY_ISBN));
        }
        if (dataQualityContext.productDataObjectsFromSource.get(0).PROJECT_NUM!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).PROJECT_NUM !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).PROJECT_NUM
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).PROJECT_NUM));
        }
        if (dataQualityContext.productDataObjectsFromSource.get(0).ISSN_L!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).ISSN_L !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).ISSN_L
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).ISSN_L));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).JOURNAL_NUMBER!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).JOURNAL_NUMBER !=null) {
        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataQualityContext.productDataObjectsFromSource.get(0).JOURNAL_NUMBER
                       .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).JOURNAL_NUMBER));
        }
        if (dataQualityContext.productDataObjectsFromSource.get(0).ELECTRONIC_RIGHTS_IND!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).ELECTRONIC_RIGHTS_IND
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).BOOK_EDITION_NAME!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).BOOK_EDITION_NAME !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).BOOK_EDITION_NAME
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).BOOK_EDITION_NAME));
        }
        if (dataQualityContext.productDataObjectsFromSource.get(0).BOOK_VOLUME_NAME!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).BOOK_VOLUME_NAME !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).BOOK_VOLUME_NAME
                       .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).BOOK_VOLUME_NAME));
        }
        if (dataQualityContext.productDataObjectsFromSource.get(0).PMC!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).PMC !=null) {
                    assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                            dataQualityContext.productDataObjectsFromSource.get(0).PMC
                                    .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).PMC));
            }
       if (dataQualityContext.productDataObjectsFromSource.get(0).PMG!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).PMG !=null) {
           assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                   dataQualityContext.productDataObjectsFromSource.get(0).PMG
                           .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).PMG));
       }

        if (dataQualityContext.productDataObjectsFromSource.get(0).WORK_ID!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_ID !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).WORK_ID
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_ID));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).WORK_STATUS!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_STATUS !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).WORK_STATUS
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_STATUS));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).WORK_SUBSTATUS!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_SUBSTATUS !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).WORK_SUBSTATUS
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_SUBSTATUS));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TYPE !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).WORK_TYPE
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TYPE));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).IMPRINT!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).IMPRINT !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).IMPRINT
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).IMPRINT));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).OPEN_ACCESS_JNL_TYPE_CODE
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).OPEN_ACCESS_JNL_TYPE_CODE));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).PRODUCT_WORK_ID!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).PRODUCT_WORK_ID
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_ID));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).F_ACC_PROD_HIERARCHY!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).F_ACC_PROD_HIERARCHY !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).F_ACC_PROD_HIERARCHY
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).F_ACC_PROD_HIERARCHY));
        }
        if (dataQualityContext.productDataObjectsFromSource.get(0).F_RESPONSIBILITY_CENTRE!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).F_RESPONSIBILITY_CENTRE !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).F_RESPONSIBILITY_CENTRE
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).F_RESPONSIBILITY_CENTRE));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).F_OPCO_R12!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).F_OPCO_R12 !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).F_OPCO_R12
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).F_OPCO_R12));
        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).PRODUCT_WORK_PUB_DATE!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).PRODUCT_WORK_PUB_DATE.substring(0,10)
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE));


        }

        if (dataQualityContext.productDataObjectsFromSource.get(0).JOURNAL_ACRONYM!=null
                || dataQualityContext.productDataObjectsFromPMXSTG.get(0).JOURNAL_ACRONYM !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromSource.get(0).JOURNAL_ACRONYM
                            .equals(dataQualityContext.productDataObjectsFromPMXSTG.get(0).JOURNAL_ACRONYM));
        }

    }
    
    @And("^The work data between PMX STG and EPH SA is identical$")
    public void checkPMXSTGandEPHData() {
             System.out.println(dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TITLE);
        System.out.println(dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TITLE);

        Assert.assertEquals("The classname is incorrect!","Work", dataQualityContext.productDataObjectsFromEPH.get(0).B_CLASSNAME);

        assertTrue("Expecting the Work title details from PMX and PMX Staging Consistent ",
                dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_TITLE
                        .equals(dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TITLE));

        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_SUBTITLE!=null
                || dataQualityContext.productDataObjectsFromEPH.get(0).WORK_SUBTITLE !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_SUBTITLE
                            .equals(dataQualityContext.productDataObjectsFromEPH.get(0).WORK_SUBTITLE));
        }

        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE!=null
                || dataQualityContext.productDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE !=null) {

            System.out.print(dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE.substring(0,4) + " \n" +
                    dataQualityContext.productDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE.substring(0,4));
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromPMXSTG.get(0).PRODUCT_WORK_PUB_DATE.substring(0,4)
                            .equals(dataQualityContext.productDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE.substring(0,4)));

        }

        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND!=null) {
            if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).ELECTRONIC_RIGHTS_IND.equalsIgnoreCase("Y")) {
                Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "1");
            } else {
                Assert.assertEquals("The Work type is incorrect", dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE, "0");
            }
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
        }

        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).BOOK_VOLUME_NAME!=null
                || dataQualityContext.productDataObjectsFromEPH.get(0).BOOK_VOLUME_NAME !=null) {
            Assert.assertEquals("The volume is not 0", dataQualityContext.productDataObjectsFromEPH.get(0).BOOK_VOLUME_NAME, "0");
        }



        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).PMC!=null
                || dataQualityContext.productDataObjectsFromEPH.get(0).PMC !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromPMXSTG.get(0).PMC
                            .equals(dataQualityContext.productDataObjectsFromEPH.get(0).PMC));
        }

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

        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_STATUS.equalsIgnoreCase("1")){
            Assert.assertEquals("The Work type is incorrect", "WAS",dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS);
        } else if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_STATUS.equalsIgnoreCase("3")) {
            Assert.assertEquals("The Work type is incorrect", "WAP", dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS);
        } else if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).WORK_SUBSTATUS.equalsIgnoreCase("DIVESTED")){
            Assert.assertEquals("The Work type is incorrect","WDI", dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS );
        }else{
            Assert.assertEquals("The Work type is incorrect","WST", dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS);
        }

        if (dataQualityContext.productDataObjectsFromPMXSTG.get(0).IMPRINT!=null
                || dataQualityContext.productDataObjectsFromEPH.get(0).IMPRINT !=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataQualityContext.productDataObjectsFromPMXSTG.get(0).IMPRINT
                            .equals(dataQualityContext.productDataObjectsFromEPH.get(0).IMPRINT));
        }
    }

    @And("^The work data between EPH SA and EPH GD is identical$")
    public void checkBetweenSAandGD(){
        assertTrue("Expecting the Work title details from SA and GD Staging Consistent ",
                dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TITLE
                        .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).WORK_TITLE));

        if (dataQualityContext.productDataObjectsFromEPH.get(0).WORK_SUBTITLE!=null
                || dataQualityContext.productDataObjectsFromEPHGD.get(0).WORK_SUBTITLE !=null) {
            assertTrue("Expecting the Product details from SA and GD Consistent ",
                    dataQualityContext.productDataObjectsFromEPH.get(0).WORK_SUBTITLE
                            .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).WORK_SUBTITLE));
        }

        if (dataQualityContext.productDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE!=null
                || dataQualityContext.productDataObjectsFromEPHGD.get(0).PRODUCT_WORK_PUB_DATE !=null) {
            assertTrue("Expecting the Product details from SA and GD Consistent ",
                    dataQualityContext.productDataObjectsFromEPH.get(0).PRODUCT_WORK_PUB_DATE
                            .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).PRODUCT_WORK_PUB_DATE));
        }
        if (dataQualityContext.productDataObjectsFromEPH.get(0).ELECTRONIC_RIGHTS_IND!=null
                || dataQualityContext.productDataObjectsFromEPHGD.get(0).ELECTRONIC_RIGHTS_IND !=null) {
            assertTrue("Expecting the Product details from SA and GD Consistent ",
                    dataQualityContext.productDataObjectsFromEPH.get(0).ELECTRONIC_RIGHTS_IND
                            .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).ELECTRONIC_RIGHTS_IND));
        }
        if (dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE!=null
                || dataQualityContext.productDataObjectsFromEPHGD.get(0).OPEN_ACCESS_JNL_TYPE_CODE !=null) {
            assertTrue("Expecting the Product details from SA and GD Consistent ",
                    dataQualityContext.productDataObjectsFromEPH.get(0).OPEN_ACCESS_JNL_TYPE_CODE
                            .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).OPEN_ACCESS_JNL_TYPE_CODE));
        }

        if (dataQualityContext.productDataObjectsFromEPH.get(0).PMC!=null
                || dataQualityContext.productDataObjectsFromEPHGD.get(0).PMC !=null) {
            assertTrue("Expecting the Product details from SA and GD Consistent ",
                    dataQualityContext.productDataObjectsFromEPH.get(0).PMC
                            .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).PMC));
        }

        if (dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TYPE!=null
                || dataQualityContext.productDataObjectsFromEPHGD.get(0).WORK_TYPE !=null) {
            assertTrue("Expecting the Product details from SA and GD Consistent ",
                    dataQualityContext.productDataObjectsFromEPH.get(0).WORK_TYPE
                            .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).WORK_TYPE));
        }

        if (dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS!=null
                || dataQualityContext.productDataObjectsFromEPHGD.get(0).WORK_STATUS !=null) {
            assertTrue("Expecting the Product details from SA and GD Consistent ",
                    dataQualityContext.productDataObjectsFromEPH.get(0).WORK_STATUS
                            .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).WORK_STATUS));
        }

        if (dataQualityContext.productDataObjectsFromEPH.get(0).IMPRINT!=null
                || dataQualityContext.productDataObjectsFromEPHGD.get(0).IMPRINT !=null) {
            assertTrue("Expecting the Product details from SA and GD Consistent ",
                    dataQualityContext.productDataObjectsFromEPH.get(0).IMPRINT
                            .equals(dataQualityContext.productDataObjectsFromEPHGD.get(0).IMPRINT));
        }
    }
}
