package com.eph.automation.testing.steps.sdrm;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.SDRMDLContext;
import com.eph.automation.testing.models.dao.SDRMDataLake.SDRMDLAccessObject;
import com.eph.automation.testing.services.db.sdrmsql.SDRMDataChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SDRMDataChecksSteps {

    private static String sql;
    private static List<String> ids;

    @Given("^We get the (.*) random SDRM ISBN ids (.*)$")
    public void getRandomisbnIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random sdrm isbn ids...");
        switch (tableName) {
            case "sdrm_inbound_part":
                sql = String.format(SDRMDataChecksSQL.GET_RANDOM_SDRM_ISBN_INBOUND, numberOfRecords);
                break;
            case "sdrm_transform_current_product_availability":
                sql = String.format(SDRMDataChecksSQL.GET_RANDOM_SDRM_CURRENT_PRODUCT_ISBN_INBOUND, numberOfRecords);
                break;
            case "sdrm_delta_current_product_availability":
                sql = String.format(SDRMDataChecksSQL.GET_RANDOM_SDRM_DELTA_CURRENT_ISBN_INBOUND, numberOfRecords);
                Log.info(sql);
                break;
            case "sdrm_transform_history_product_availability_part":
                sql = String.format(SDRMDataChecksSQL.GET_RANDOM_SDRM_CURRENT_DELTA_AND_DELTA_HISTORY_ISBN_INBOUND, numberOfRecords);
                break;
         case "sdrm_transform_latest_product_availability":
                sql = String.format(SDRMDataChecksSQL.GET_RANDOM_SDRM_DELTACURRENT_AND_DELTAEXCL_ISBN_INBOUND, numberOfRecords);
                break;
            case "sdrm_transform_file_history_product_availability_part":
                sql = String.format(SDRMDataChecksSQL.GET_RANDOM_SDRM_CURRENT_AND_PREV_FILE_HISTORY_ISBN_INBOUND, numberOfRecords);
                break;
            default:
                Log.info("No tables found");
        }

        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("isbn")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^Get the data from sdrm inbound table$")
    public void getSourceInboundRecords() {
        Log.info("We get the sdrm Source Inbound records...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_SOURCE_INBOUND_DATA, String.join("','",ids));
        SDRMDLContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Get the data from sdrm transform current product$")
    public void getRecordsFromCurrentProduct() {
        Log.info("We get the records from Current Product table...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_CURRENT_PRODUCT_DATA, String.join("','",ids));
        SDRMDLContext.recordsFromCurrentProductData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Get the data from sdrm transform product history$")
    public void getRecordsFromProductHistory() {
        Log.info("We get the records from Transform Product History table...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_TRANSFORM_PRODUCT_HISTORY_DATA, String.join("','",ids));
        Log.info(sql);
        SDRMDLContext.recordsFromProductHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the data from sdrm transform product file history$")
    public void getRecordsFromProductFileHistory() {
        Log.info("We get the records from Transform Product File History table...");
         sql = String.format(SDRMDataChecksSQL.GET_SDRM_TRANSFORM_PRODUCT_FILE_HISTORY_DATA, String.join("','",ids));
          Log.info(sql);
        SDRMDLContext.recordsFromProductFileHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the data from sdrm delta current product table$")
    public void getDeltaCurrentProductRecords() {
        Log.info("We get the sdrm Delta current product records...");
       Log.info(sql);
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_DELTA_CURRENT_PRODUCT_DATA, String.join("','",ids));
        SDRMDLContext.recordsFromDeltaCurrentProductData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the data from sdrm delta product history product$")
    public void getRecordsFromDeltaProductHistory() {
        Log.info("We get the records from Delta Product History table...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_DELTA_PRODUCT_HISTORY_DATA, String.join("','",ids));
        Log.info(sql);
        SDRMDLContext.recordsFromDeltaProductHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the data from sdrm delta current product and Delta Product History table$")
    public void getDeltaCurrentAndDeltaHistoryRecords() {
        Log.info("We get the Difference between sdrm Delta Current and sdrm Delta history table records...");
        sql = String.format(SDRMDataChecksSQL.GET_DIFF_BETWEEN_SDRM_CURRENT_DELTA_AND_DELTA_HISTORY_DATA, String.join("','",ids));
        Log.info(sql);
        SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);

    }

    @Then("^Get the data from sdrm history excl delta table$")
    public void getRecordsFromHistoryExclDelta() {
        Log.info("We get the records from sdrm History Excl Delta table...");
         sql = String.format(SDRMDataChecksSQL.GET_SDRM_HISTORY_EXCL_DELTA_DATA, String.join("','",ids));
        Log.info(sql);
        SDRMDLContext.recordsFromHistoryExclDeltaData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the data from sdrm delta current product and History Excl Delta table$")
    public void getDeltaCurrentAndHistoryExclDeltaRecords() {
        Log.info("We get the sdrm Delta Current Product And History Excl Delta table records...");
       sql = String.format(SDRMDataChecksSQL.GET_COMBINEOF_DELTACURRENT_AND_DELTAEXCL_DATA, String.join("','",ids));
        Log.info(sql);
        SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the data from sdrm transform latest product table$")
    public void getRecordsFromSDRMTransformLatestProduct() {
        Log.info("We get the records from SDMR Transform Latest Product table...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_TRANSFORM_LATEST_PRODUCT_DATA, String.join("','",ids));
        Log.info(sql);
        SDRMDLContext.recordsFromSDRMTransfromLatestProductData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }


    @When("^Get the data from difference between sdrm current and prev file history table$")
    public void getSDRMCurrentAndPrevFileHistory() {
        Log.info("We get the difference between sdrm Current And Prev File History...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_CURRENT_AND_PREV_FILE_HISTORY_DATA, String.join("','",ids));
        Log.info(sql);
        SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^we compare the records of SDRM Inbound and SDRM current product$")
    public void compareDataFullandCurrentProduct() {
        if (SDRMDLContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the isbn ids to compare the records between Full Load and Current Product...");
            for (int i = 0; i < SDRMDLContext.recordsFromInboundData.size(); i++) {

                SDRMDLContext.recordsFromInboundData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDRMDLContext.recordsFromCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn));

                Log.info("Full Load -> isbn => " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() +
                        "Current_Product -> isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn());
                if (SDRMDLContext.recordsFromInboundData.get(i).getisbn() != null ||
                        (SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() != null)) {
                    Assert.assertEquals("The isbn is =" + SDRMDLContext.recordsFromInboundData.get(i).getisbn() + " is missing/not found in Current_Product table",
                            SDRMDLContext.recordsFromInboundData.get(i).getisbn(),
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() +
                        " title => Full_Load =" + SDRMDLContext.recordsFromInboundData.get(i).gettitle() +
                        " Current_Product=" + SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle());

                if (SDRMDLContext.recordsFromInboundData.get(i).gettitle() != null ||
                        (SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle() != null)) {
                    Assert.assertEquals("The title is incorrect for isbn = " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromInboundData.get(i).gettitle(),
                            SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() +
                        " Rendition Format => Full_Load =" + SDRMDLContext.recordsFromInboundData.get(i).getrednitionFormat() +
                        " Current_Rendition_Format=" +  SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat());

                if (SDRMDLContext.recordsFromInboundData.get(i).getrednitionFormat() != null ||
                        (SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for isbn = " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromInboundData.get(i).getrednitionFormat(),
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() +
                        " Production Date => Full_Load =" + SDRMDLContext.recordsFromInboundData.get(i).getproductionDate() +
                        " Current_Production_Date=" +  SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate());

                if (SDRMDLContext.recordsFromInboundData.get(i).getproductionDate() != null ||
                        (SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate() != null)) {
                    Assert.assertEquals("The Production date is incorrect for isbn = " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromInboundData.get(i).getproductionDate(),
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() +
                        " EPR ID => Full_Load =" + SDRMDLContext.recordsFromInboundData.get(i).geteprId() +
                        " Current_eprId=" +  SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId());

                if (SDRMDLContext.recordsFromInboundData.get(i).geteprId() != null ||
                        (SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for isbn = " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromInboundData.get(i).geteprId(),
                            SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() +
                        " Product Type => Full_Load =" + SDRMDLContext.recordsFromInboundData.get(i).getproductType() +
                        " Current_Product_Type=" +  SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType());

                if (SDRMDLContext.recordsFromInboundData.get(i).getproductType() != null ||
                        (SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for isbn = " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromInboundData.get(i).getproductType(),
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() +
                        " uKey => Full_Load =" + SDRMDLContext.recordsFromInboundData.get(i).getuKey() +
                        " Current_uKey=" +  SDRMDLContext.recordsFromCurrentProductData.get(i).getuKey());


                if (SDRMDLContext.recordsFromInboundData.get(i).getuKey() != null ||
                        (SDRMDLContext.recordsFromCurrentProductData.get(i).getuKey() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for isbn = " + SDRMDLContext.recordsFromInboundData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromInboundData.get(i).getuKey(),
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getuKey());
                }
            }
        }
    }

    @And("^we compare the records of SDRM current product and SDRM transform product history$")
    public void compareDataCurrentProductandProductHistory() {
        if (SDRMDLContext.recordsFromCurrentProductData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the isbn ids to compare the records between Current Product and Product History...");
            for (int i = 0; i < SDRMDLContext.recordsFromCurrentProductData.size(); i++) {

                SDRMDLContext.recordsFromCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDRMDLContext.recordsFromProductHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn));

                Log.info("Current_Product -> isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        "Product_History -> isbn => " + SDRMDLContext.recordsFromProductHistoryData.get(i).getisbn());
                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() != null ||
                        (SDRMDLContext.recordsFromProductHistoryData.get(i).getisbn() != null)) {
                    Assert.assertEquals("The isbn is =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() + " is missing/not found in Current_Product table",
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn(),
                            SDRMDLContext.recordsFromProductHistoryData.get(i).getisbn());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " title => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle() +
                        " Product_History=" + SDRMDLContext.recordsFromProductHistoryData.get(i).gettitle());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle() != null ||
                        (SDRMDLContext.recordsFromProductHistoryData.get(i).gettitle() != null)) {
                    Assert.assertEquals("The title is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle(),
                            SDRMDLContext.recordsFromProductHistoryData.get(i).gettitle());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " Rendition Format => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat() +
                        " Product_History_Rendition_Format=" +  SDRMDLContext.recordsFromProductHistoryData.get(i).getrednitionFormat());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat() != null ||
                        (SDRMDLContext.recordsFromProductHistoryData.get(i).getrednitionFormat() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat(),
                            SDRMDLContext.recordsFromProductHistoryData.get(i).getrednitionFormat());
                }
                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " Production Date => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate() +
                        " Product_History_Production_Date=" +  SDRMDLContext.recordsFromProductHistoryData.get(i).getproductionDate());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate() != null ||
                        (SDRMDLContext.recordsFromProductHistoryData.get(i).getproductionDate() != null)) {
                    Assert.assertEquals("The Production date is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate(),
                            SDRMDLContext.recordsFromProductHistoryData.get(i).getproductionDate());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " EPR ID => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId() +
                        " Product_History_eprId=" +  SDRMDLContext.recordsFromProductHistoryData.get(i).geteprId());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId() != null ||
                        (SDRMDLContext.recordsFromProductHistoryData.get(i).geteprId() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId(),
                            SDRMDLContext.recordsFromProductHistoryData.get(i).geteprId());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " Product Type => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType() +
                        " Product_History_Product_Type=" +  SDRMDLContext.recordsFromProductHistoryData.get(i).getproductType());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType() != null ||
                        (SDRMDLContext.recordsFromProductHistoryData.get(i).getproductType() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType(),
                            SDRMDLContext.recordsFromProductHistoryData.get(i).getproductType());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " uKey => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType() +
                        " Product_History_uKey=" +  SDRMDLContext.recordsFromProductHistoryData.get(i).getproductType());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getuKey() != null ||
                        (SDRMDLContext.recordsFromProductHistoryData.get(i).getuKey() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getuKey(),
                            SDRMDLContext.recordsFromProductHistoryData.get(i).getuKey());
                }
            }
        }
    }

    @And("^we compare the records of SDRM current product and SDRM transform product file history$")
    public void compareDataCurrentProductandProductFileHistory() {
        if (SDRMDLContext.recordsFromCurrentProductData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Current product availability size "+ SDRMDLContext.recordsFromCurrentProductData.size());
            Log.info("Sorting the isbn ids to compare the records between Current Product and Product File History...");
            for (int i = 0; i < SDRMDLContext.recordsFromCurrentProductData.size(); i++) {

                SDRMDLContext.recordsFromCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDRMDLContext.recordsFromProductFileHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn));

                Log.info("Current_Product -> isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        "Product_File_History -> isbn => " + SDRMDLContext.recordsFromProductFileHistoryData.get(i).getisbn());
                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() != null ||
                        (SDRMDLContext.recordsFromProductFileHistoryData.get(i).getisbn() != null)) {
                    Assert.assertEquals("The isbn is =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() + " is missing/not found in Current_Product table",
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn(),
                            SDRMDLContext.recordsFromProductFileHistoryData.get(i).getisbn());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " title => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle() +
                        " Product_File_History=" + SDRMDLContext.recordsFromProductFileHistoryData.get(i).gettitle());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle() != null ||
                        (SDRMDLContext.recordsFromProductFileHistoryData.get(i).gettitle() != null)) {
                    Assert.assertEquals("The title is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).gettitle(),
                            SDRMDLContext.recordsFromProductFileHistoryData.get(i).gettitle());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " Rendition Format => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat() +
                        " Product_File_History_Rendition_Format=" +  SDRMDLContext.recordsFromProductFileHistoryData.get(i).getrednitionFormat());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat() != null ||
                        (SDRMDLContext.recordsFromProductFileHistoryData.get(i).getrednitionFormat() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getrednitionFormat(),
                            SDRMDLContext.recordsFromProductFileHistoryData.get(i).getrednitionFormat());
                }
                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " Inbound Ts => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getinboundTs() +
                        " Product_File_History_Inbound_Ts=" +  SDRMDLContext.recordsFromProductFileHistoryData.get(i).getinboundTs());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getinboundTs() != null ||
                        (SDRMDLContext.recordsFromProductFileHistoryData.get(i).getinboundTs() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getinboundTs(),
                            SDRMDLContext.recordsFromProductFileHistoryData.get(i).getinboundTs());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " Production Date => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate() +
                        " Product_File_History_Production_Date=" +  SDRMDLContext.recordsFromProductFileHistoryData.get(i).getproductionDate());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate() != null ||
                        (SDRMDLContext.recordsFromProductFileHistoryData.get(i).getproductionDate() != null)) {
                    Assert.assertEquals("The Production date is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getproductionDate(),
                            SDRMDLContext.recordsFromProductFileHistoryData.get(i).getproductionDate());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " EPR ID => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId() +
                        " Product_File_History_eprId=" +  SDRMDLContext.recordsFromProductFileHistoryData.get(i).geteprId());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId() != null ||
                        (SDRMDLContext.recordsFromProductFileHistoryData.get(i).geteprId() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).geteprId(),
                            SDRMDLContext.recordsFromProductFileHistoryData.get(i).geteprId());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " Product Type => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType() +
                        " Product_File_History_Product_Type=" +  SDRMDLContext.recordsFromProductFileHistoryData.get(i).getproductType());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType() != null ||
                        (SDRMDLContext.recordsFromProductFileHistoryData.get(i).getproductType() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getproductType(),
                            SDRMDLContext.recordsFromProductFileHistoryData.get(i).getproductType());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() +
                        " uKey => Current_Product =" + SDRMDLContext.recordsFromCurrentProductData.get(i).getuKey() +
                        " Product_File_History_uKey=" +  SDRMDLContext.recordsFromProductFileHistoryData.get(i).getuKey());

                if (SDRMDLContext.recordsFromCurrentProductData.get(i).getuKey() != null ||
                        (SDRMDLContext.recordsFromProductFileHistoryData.get(i).getuKey() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for isbn = " + SDRMDLContext.recordsFromCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromCurrentProductData.get(i).getuKey(),
                            SDRMDLContext.recordsFromProductFileHistoryData.get(i).getuKey());
                }
            }
        }
    }

    @And("^we compare the records of SDRM Delta current and Delta History table with SDRM History Excl Delta table$")
    public void compareExclDelta() {
        if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Difference between Delta Current product and Delta History availability size "+ SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.size());
            Log.info("Sorting the isbn ids to compare the records between Delta Current Product and Delta History with History Excl Delta...");
            for (int i = 0; i < SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.size(); i++) {

                SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDRMDLContext.recordsFromHistoryExclDeltaData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn));

                Log.info("Delta_Current_Product and History -> isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() +
                        "History Excl Delta -> isbn => " + SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getisbn());
                if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() != null ||
                        (SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getisbn() != null)) {
                    Assert.assertEquals("The isbn is =" + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() + " is missing/not found in Current_Product table",
                            SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn(),
                            SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getisbn());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() +
                        " title => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).gettitle() +
                        " History Excl Delta=" + SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).gettitle());

                if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).gettitle() != null ||
                        (SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).gettitle() != null)) {
                    Assert.assertEquals("The title is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).gettitle(),
                            SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).gettitle());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() +
                        " Rendition Format => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getrednitionFormat() +
                        " History Excl Delta_Rendition_Format=" +  SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getrednitionFormat());

                if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getrednitionFormat() != null ||
                        (SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getrednitionFormat() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getrednitionFormat(),
                            SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getrednitionFormat());
                }
                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() +
                        " Inbound Ts => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getinboundTs() +
                        " History Excl Delta_Inbound_Ts=" +  SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getinboundTs());

                if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getinboundTs() != null ||
                        (SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getinboundTs() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getinboundTs(),
                            SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getinboundTs());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() +
                        " Production Date => Delta_Current_Product and History=" + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getproductionDate() +
                        " History Excl Delta_Production_Date=" +  SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getproductionDate());

                if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getproductionDate() != null ||
                        (SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getproductionDate() != null)) {
                    Assert.assertEquals("The Production date is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getproductionDate(),
                            SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getproductionDate());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() +
                        " EPR ID => Delta_Current_Product and History=" + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).geteprId() +
                        " History Excl Delta_eprId=" +  SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).geteprId());

                if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).geteprId() != null ||
                        (SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).geteprId() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).geteprId(),
                            SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).geteprId());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() +
                        " Product Type => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getproductType() +
                        " History Excl Delta_Product_Type=" +  SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getproductType());

                if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getproductType() != null ||
                        (SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getproductType() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getproductType(),
                            SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getproductType());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() +
                        " uKey => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getuKey() +
                        " History Excl Delta_uKey=" +  SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getuKey());

                if (SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getuKey() != null ||
                        (SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getuKey() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getuKey(),
                            SDRMDLContext.recordsFromHistoryExclDeltaData.get(i).getuKey());
                }
            }

        }
    }

    @And("^we compare the records of SDRM Delta current and History Excl delta table with SDRM transform latest product table$")
    public void compareLatestRecords() {
        if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Difference between Delta Current product and Delta History availability size "+ SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.size());
            Log.info("Sorting the isbn ids to compare the records between Delta Current Product and Delta History with History Excl Delta...");
            for (int i = 0; i < SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.size(); i++) {

                SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDRMDLContext.recordsFromSDRMTransfromLatestProductData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn));

                Log.info("Delta_Current_Product and History -> isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() +
                        "History Excl Delta -> isbn => " + SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getisbn());
                if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() != null ||
                        (SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getisbn() != null)) {
                    Assert.assertEquals("The isbn is =" + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() + " is missing/not found in Current_Product table",
                            SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn(),
                            SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getisbn());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() +
                        " title => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).gettitle() +
                        " History Excl Delta=" + SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).gettitle());

                if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).gettitle() != null ||
                        (SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).gettitle() != null)) {
                    Assert.assertEquals("The title is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).gettitle(),
                            SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).gettitle());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() +
                        " Rendition Format => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getrednitionFormat() +
                        " History Excl Delta_Rendition_Format=" +  SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getrednitionFormat());

                if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getrednitionFormat() != null ||
                        (SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getrednitionFormat() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getrednitionFormat(),
                            SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getrednitionFormat());
                }
                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() +
                        " Inbound Ts => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getinboundTs() +
                        " History Excl Delta_Inbound_Ts=" +  SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getinboundTs());

                if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getinboundTs() != null ||
                        (SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getinboundTs() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getinboundTs(),
                            SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getinboundTs());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() +
                        " Production Date => Delta_Current_Product and History=" + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getproductionDate() +
                        " History Excl Delta_Production_Date=" +  SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getproductionDate());

                if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getproductionDate() != null ||
                        (SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getproductionDate() != null)) {
                    Assert.assertEquals("The Production date is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getproductionDate(),
                            SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getproductionDate());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() +
                        " EPR ID => Delta_Current_Product and History=" + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).geteprId() +
                        " History Excl Delta_eprId=" +  SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).geteprId());

                if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).geteprId() != null ||
                        (SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).geteprId() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).geteprId(),
                            SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).geteprId());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() +
                        " Product Type => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getproductType() +
                        " History Excl Delta_Product_Type=" +  SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getproductType());

                if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getproductType() != null ||
                        (SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getproductType() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getproductType(),
                            SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getproductType());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() +
                        " uKey => Delta_Current_Product and History =" + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getuKey() +
                        " History Excl Delta_uKey=" +  SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getuKey());

                if (SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getuKey() != null ||
                        (SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getuKey() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getuKey(),
                            SDRMDLContext.recordsFromSDRMTransfromLatestProductData.get(i).getuKey());
                }
            }
        }
    }

    @And("^we compare the records of difference between SDRM Current and Prev file history with SDRM Delta current Product$")
    public void compareDifferencebetweenSDRMCurrentAndPrevFileHistory() {
        if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("inbound size "+ SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.size());
            Log.info("Sorting the isbn ids to compare the records between sdrm Current and Prev file history with sdrm Delta current Product...");
            for (int i = 0; i < SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.size(); i++) {

                SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDRMDLContext.recordsFromDeltaCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn));

                Log.info("Current and Prev File History -> isbn => " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() +
                        "Delta Current_Product -> isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn());
                if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() != null ||
                        (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() != null)) {
                    Assert.assertEquals("The isbn is =" + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() + " is missing/not found in Current_Product table",
                            SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn(),
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() +
                        " title => Current and Prev File History =" + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).gettitle() +
                        " Delta Current_Product=" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).gettitle());

                if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).gettitle() != null ||
                        (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).gettitle() != null)) {
                    Assert.assertEquals("The title is incorrect for isbn = " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).gettitle(),
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).gettitle());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() +
                        " Rendition Format => Current and Prev File History =" + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getrednitionFormat() +
                        " Delta_Current_Rendition_Format=" +  SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getrednitionFormat());

                if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getrednitionFormat() != null ||
                        (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getrednitionFormat() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for isbn = " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getrednitionFormat(),
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getrednitionFormat());
                }
                Log.info("isbn => " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() +
                        " Inbound Ts => Current and Prev File History =" + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getinboundTs() +
                        "Delta Current_Inbound_Ts=" +  SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getinboundTs());

                if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getinboundTs() != null ||
                        (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getinboundTs() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for isbn = " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getinboundTs(),
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getinboundTs());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() +
                        " Production Date => Current and Prev File History =" + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getproductionDate() +
                        " Delta Current_Production_Date=" +  SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductionDate());

                if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getproductionDate() != null ||
                        (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductionDate() != null)) {
                    Assert.assertEquals("The Production date is incorrect for isbn = " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getproductionDate(),
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductionDate());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() +
                        " EPR ID => Current and Prev File History =" + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).geteprId() +
                        " Delta Current_eprId=" +  SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).geteprId());

                if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).geteprId() != null ||
                        (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).geteprId() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for isbn = " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).geteprId(),
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).geteprId());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() +
                        " Product Type => Current and Prev File History =" + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getproductType() +
                        " Delta Current_Product_Type=" +  SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductType());

                if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getproductType() != null ||
                        (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductType() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for isbn = " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getproductType(),
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductType());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() +
                        " Product Type => Current and Prev File History =" + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getdeltaMode() +
                        " Delta Current_Product_Type=" +  SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getdeltaMode());

                if (SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getdeltaMode() != null ||
                        (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getdeltaMode() != null)) {
                    Assert.assertEquals("The deltaMode is incorrect for isbn = " + SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getdeltaMode(),
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getdeltaMode());
                }
            }
        }
    }

    @And("^compare the records of SDRM Delta current product and SDRM Delta product history$")
    public void compareDataDeltaCurrentProductandDeltaProductHistory() {
        if (SDRMDLContext.recordsFromDeltaCurrentProductData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the isbn ids to compare the records between Delta Current Product and Delta Product History...");
            for (int i = 0; i < SDRMDLContext.recordsFromDeltaCurrentProductData.size(); i++) {

                SDRMDLContext.recordsFromDeltaCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn)); //sort primarykey data in the lists
                SDRMDLContext.recordsFromDeltaProductHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getisbn));

                Log.info("Delta_Current_Product -> isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() +
                        "Delta_Product_History -> isbn => " + SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getisbn());
                if (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() != null ||
                        (SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getisbn() != null)) {
                    Assert.assertEquals("The isbn is =" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() + " is missing/not found in Current_Product table",
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn(),
                            SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getisbn());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() +
                        " title => Delta_Current_Product =" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).gettitle() +
                        " Delta_Product_History=" + SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).gettitle());

                if (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).gettitle() != null ||
                        (SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).gettitle() != null)) {
                    Assert.assertEquals("The title is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).gettitle(),
                            SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).gettitle());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() +
                        " Rendition Format => Delta_Current_Product =" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getrednitionFormat() +
                        " Delta_Product_History_Rendition_Format=" +  SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getrednitionFormat());

                if (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getrednitionFormat() != null ||
                        (SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getrednitionFormat() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getrednitionFormat(),
                            SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getrednitionFormat());
                }
                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() +
                        " Inbound Ts => Delta_Current_Product =" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getinboundTs() +
                        " Delta_Product_History_Inbound_Ts=" +  SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getinboundTs());

                if (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getinboundTs() != null ||
                        (SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getinboundTs() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getinboundTs(),
                            SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getinboundTs());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() +
                        " Production Date => Delta_Current_Product =" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductionDate() +
                        " Delta_Product_History_Production_Date=" +  SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getproductionDate());

                if (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductionDate() != null ||
                        (SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getproductionDate() != null)) {
                    Assert.assertEquals("The Production date is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductionDate(),
                            SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getproductionDate());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() +
                        " EPR ID => Delta_Current_Product =" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).geteprId() +
                        " Delta_Product_History_eprId=" +  SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).geteprId());

                if (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).geteprId() != null ||
                        (SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).geteprId() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).geteprId(),
                            SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).geteprId());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() +
                        " Product Type => Delta_Current_Product =" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductType() +
                        " Delta_Product_History_Product_Type=" +  SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getproductType());

                if (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductType() != null ||
                        (SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getproductType() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getproductType(),
                            SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getproductType());
                }

                Log.info("isbn => " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() +
                        " uKey => Delta_Current_Product =" + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getuKey() +
                        " Delta_Product_History_uKey=" +  SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getuKey());

                if (SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getuKey() != null ||
                        (SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getuKey() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for isbn = " + SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getisbn() ,
                            SDRMDLContext.recordsFromDeltaCurrentProductData.get(i).getuKey(),
                            SDRMDLContext.recordsFromDeltaProductHistoryData.get(i).getuKey());
                }
            }
        }
    }
}
