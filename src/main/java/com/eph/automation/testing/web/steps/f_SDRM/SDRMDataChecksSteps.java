package com.eph.automation.testing.web.steps.f_SDRM;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.SDAccessDLContext;
import com.eph.automation.testing.models.contexts.SDRMDLContext;
import com.eph.automation.testing.models.dao.SDRMDataLake.SDRMDLAccessObject;
import com.eph.automation.testing.services.db.SDBooksDataLakeSQL.SDBooksDataChecksSQL;
import com.eph.automation.testing.services.db.SDRMDataLakeSQL.SDRMDataChecksSQL;
import com.google.common.base.Joiner;
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

    public SDRMDLContext dataQualitySDRMContext;


    public SDAccessDLContext dataQualitySDContext;

    private static String sql;
    private static List<String> Ids;
    private SDBooksDataChecksSQL sdBookObj = new SDBooksDataChecksSQL();




    @Given("^We get the (.*) random SDRM ISBN ids (.*)$")
    public void getRandomISBNIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random SDRM ISBN Ids...");

        switch (tableName) {
            case "sdrm_inbound_part":
                System.out.println("source bound table is matched");
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
            case "sdbooks_transform_history_excl_delta":
                sql = String.format(SDBooksDataChecksSQL.GET_RANDOM_ISBN_DIFF_DELTA_CURR_HIST_URL, numberOfRecords);
                break;
            case "sdrm_transform_latest_product_availability":
                sql = String.format(SDRMDataChecksSQL.GET_RANDOM_SDRM_DELTACURRENT_AND_DELTAEXCL_ISBN_INBOUND, numberOfRecords);
                break;
            case "sdrm_transform_file_history_product_availability_part":
                sql = String.format(SDRMDataChecksSQL.GET_RANDOM_SDRM_CURRENT_AND_PREV_FILE_HISTORY_ISBN_INBOUND, numberOfRecords);
                break;
        }

        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("ISBN")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^Get the data from sdrm inbound table$")
    public void getSourceInboundRecords() {
        Log.info("We get the SDRM Source Inbound records...");
         sql = String.format(SDRMDataChecksSQL.GET_SDRM_SOURCE_INBOUND_DATA, Joiner.on("','").join(Ids));
        dataQualitySDRMContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Get the data from sdrm transform current product$")
    public void getRecordsFromCurrentProduct() {
        Log.info("We get the records from Current Product table...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_CURRENT_PRODUCT_DATA, Joiner.on("','").join(Ids));
        dataQualitySDRMContext.recordsFromCurrentProductData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Get the data from sdrm transform product history$")
    public void getRecordsFromProductHistory() {
        Log.info("We get the records from Transform Product History table...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_TRANSFORM_PRODUCT_HISTORY_DATA, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDRMContext.recordsFromProductHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the data from sdrm transform product file history$")
    public void getRecordsFromProductFileHistory() {
        Log.info("We get the records from Transform Product File History table...");
         sql = String.format(SDRMDataChecksSQL.GET_SDRM_TRANSFORM_PRODUCT_FILE_HISTORY_DATA, Joiner.on("','").join(Ids));
          Log.info(sql);
        dataQualitySDRMContext.recordsFromProductFileHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the data from sdrm delta current product table$")
    public void getDeltaCurrentProductRecords() {
        Log.info("We get the SDRM Delta current product records...");
       Log.info(sql);
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_DELTA_CURRENT_PRODUCT_DATA, Joiner.on("','").join(Ids));
        dataQualitySDRMContext.recordsFromDeltaCurrentProductData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the data from sdrm delta product history product$")
    public void getRecordsFromDeltaProductHistory() {
        Log.info("We get the records from Delta Product History table...");
          sql = String.format(SDRMDataChecksSQL.GET_SDRM_DELTA_PRODUCT_HISTORY_DATA, Joiner.on("','").join(Ids));
            Log.info(sql);
        dataQualitySDRMContext.recordsFromDeltaProductHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the data from sdrm delta current product and Delta Product History table$")
    public void getDeltaCurrentAndDeltaHistoryRecords() {
        Log.info("We get the Difference between SDRM Delta Current and SDRM Delta history table records...");
        sql = String.format(SDRMDataChecksSQL.GET_DIFF_BETWEEN_SDRM_CURRENT_DELTA_AND_DELTA_HISTORY_DATA, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);

    }

    @Then("^Get the data from sdrm history excl delta table$")
    public void getRecordsFromHistoryExclDelta() {
        Log.info("We get the records from SDRM History Excl Delta table...");
         sql = String.format(SDRMDataChecksSQL.GET_SDRM_HISTORY_EXCL_DELTA_DATA, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDRMContext.recordsFromHistoryExclDeltaData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @When("^Get the data from sdrm delta current product and History Excl Delta table$")
    public void getDeltaCurrentAndHistoryExclDeltaRecords() {
        Log.info("We get the SDRM Delta Current Product And History Excl Delta table records...");
       sql = String.format(SDRMDataChecksSQL.GET_COMBINEOF_DELTACURRENT_AND_DELTAEXCL_DATA, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the data from sdrm transform latest product table$")
    public void getRecordsFromSDRMTransformLatestProduct() {
        Log.info("We get the records from SDMR Transform Latest Product table...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_TRANSFORM_LATEST_PRODUCT_DATA, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }


    @When("^Get the data from difference between sdrm current and prev file history table$")
    public void getSDRMCurrentAndPrevFileHistory() {
        Log.info("We get the difference between SDRM Current And Prev File History...");
        sql = String.format(SDRMDataChecksSQL.GET_SDRM_CURRENT_AND_PREV_FILE_HISTORY_DATA, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData = DBManager.getDBResultAsBeanList(sql, SDRMDLAccessObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of SDRM Inbound and SDRM current product$")
    public void compareDataFullandCurrentProduct() {
        if (dataQualitySDRMContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between Full Load and Current Product...");
            for (int i = 0; i < dataQualitySDRMContext.recordsFromInboundData.size(); i++) {

                dataQualitySDRMContext.recordsFromInboundData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDRMContext.recordsFromCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN));

                Log.info("Full Load -> ISBN => " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() +
                        "Current_Product -> ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN());
                if (dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() != null ||
                        (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() + " is missing/not found in Current_Product table",
                            dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN(),
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() +
                        " TITLE => Full_Load =" + dataQualitySDRMContext.recordsFromInboundData.get(i).getTITLE() +
                        " Current_Product=" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE());

                if (dataQualitySDRMContext.recordsFromInboundData.get(i).getTITLE() != null ||
                        (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE() != null)) {
                    Assert.assertEquals("The TITLE is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromInboundData.get(i).getTITLE(),
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() +
                        " Rendition Format => Full_Load =" + dataQualitySDRMContext.recordsFromInboundData.get(i).getRENDITION_FORMAT() +
                        " Current_Rendition_Format=" +  dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT());

                if (dataQualitySDRMContext.recordsFromInboundData.get(i).getRENDITION_FORMAT() != null ||
                        (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromInboundData.get(i).getRENDITION_FORMAT(),
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT());
                }
                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() +
                        " Inbound Ts => Full_Load =" + dataQualitySDRMContext.recordsFromInboundData.get(i).getINBOUND_TS() +
                        " Current_Inbound_Ts=" +  dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getINBOUND_TS());

                if (dataQualitySDRMContext.recordsFromInboundData.get(i).getINBOUND_TS() != null ||
                        (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getINBOUND_TS() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromInboundData.get(i).getINBOUND_TS(),
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getINBOUND_TS());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() +
                        " Production Date => Full_Load =" + dataQualitySDRMContext.recordsFromInboundData.get(i).getPRODUCTION_DATE() +
                        " Current_Production_Date=" +  dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE());

                if (dataQualitySDRMContext.recordsFromInboundData.get(i).getPRODUCTION_DATE() != null ||
                        (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE() != null)) {
                    Assert.assertEquals("The Production date is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromInboundData.get(i).getPRODUCTION_DATE(),
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() +
                        " EPR ID => Full_Load =" + dataQualitySDRMContext.recordsFromInboundData.get(i).getEPR_ID() +
                        " Current_EPR_ID=" +  dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID());

                if (dataQualitySDRMContext.recordsFromInboundData.get(i).getEPR_ID() != null ||
                        (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromInboundData.get(i).getEPR_ID(),
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() +
                        " Product Type => Full_Load =" + dataQualitySDRMContext.recordsFromInboundData.get(i).getPRODUCT_TYPE() +
                        " Current_Product_Type=" +  dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE());

                if (dataQualitySDRMContext.recordsFromInboundData.get(i).getPRODUCT_TYPE() != null ||
                        (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromInboundData.get(i).getPRODUCT_TYPE(),
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() +
                        " U_KEY => Full_Load =" + dataQualitySDRMContext.recordsFromInboundData.get(i).getU_KEY() +
                        " Current_U_KEY=" +  dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getU_KEY());


                if (dataQualitySDRMContext.recordsFromInboundData.get(i).getU_KEY() != null ||
                        (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromInboundData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromInboundData.get(i).getU_KEY(),
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getU_KEY());
                }
            }
        }
    }

    @And("^Compare the records of SDRM current product and SDRM transform product history$")
    public void compareDataCurrentProductandProductHistory() {
        if (dataQualitySDRMContext.recordsFromCurrentProductData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {

            System.out.println("inbound size "+ dataQualitySDRMContext.recordsFromCurrentProductData.size());

            Log.info("Sorting the ISBN Ids to compare the records between Current Product and Product History...");
            for (int i = 0; i < dataQualitySDRMContext.recordsFromCurrentProductData.size(); i++) {

                dataQualitySDRMContext.recordsFromCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDRMContext.recordsFromProductHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN));

                Log.info("Current_Product -> ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        "Product_History -> ISBN => " + dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getISBN());
                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() != null ||
                        (dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() + " is missing/not found in Current_Product table",
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN(),
                            dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " TITLE => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE() +
                        " Product_History=" + dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getTITLE());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE() != null ||
                        (dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getTITLE() != null)) {
                    Assert.assertEquals("The TITLE is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE(),
                            dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getTITLE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " Rendition Format => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT() +
                        " Product_History_Rendition_Format=" +  dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getRENDITION_FORMAT());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT() != null ||
                        (dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getRENDITION_FORMAT() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT(),
                            dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getRENDITION_FORMAT());
                }
                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " Production Date => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE() +
                        " Product_History_Production_Date=" +  dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getPRODUCTION_DATE());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE() != null ||
                        (dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getPRODUCTION_DATE() != null)) {
                    Assert.assertEquals("The Production date is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE(),
                            dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getPRODUCTION_DATE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " EPR ID => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID() +
                        " Product_History_EPR_ID=" +  dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getEPR_ID());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID() != null ||
                        (dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getEPR_ID() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID(),
                            dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getEPR_ID());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " Product Type => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE() +
                        " Product_History_Product_Type=" +  dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getPRODUCT_TYPE());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE() != null ||
                        (dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getPRODUCT_TYPE() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE(),
                            dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getPRODUCT_TYPE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " U_KEY => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE() +
                        " Product_History_U_KEY=" +  dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getPRODUCT_TYPE());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getU_KEY() != null ||
                        (dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getU_KEY(),
                            dataQualitySDRMContext.recordsFromProductHistoryData.get(i).getU_KEY());
                }
            }
        }
    }

    @And("^Compare the records of SDRM current product and SDRM transform product file history$")
    public void compareDataCurrentProductandProductFileHistory() {
        if (dataQualitySDRMContext.recordsFromCurrentProductData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            System.out.println("Current product availability size "+ dataQualitySDRMContext.recordsFromCurrentProductData.size());
            Log.info("Sorting the ISBN Ids to compare the records between Current Product and Product File History...");
            for (int i = 0; i < dataQualitySDRMContext.recordsFromCurrentProductData.size(); i++) {

                dataQualitySDRMContext.recordsFromCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDRMContext.recordsFromProductFileHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN));

                Log.info("Current_Product -> ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        "Product_File_History -> ISBN => " + dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getISBN());
                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() != null ||
                        (dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() + " is missing/not found in Current_Product table",
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN(),
                            dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " TITLE => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE() +
                        " Product_File_History=" + dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getTITLE());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE() != null ||
                        (dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getTITLE() != null)) {
                    Assert.assertEquals("The TITLE is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getTITLE(),
                            dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getTITLE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " Rendition Format => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT() +
                        " Product_File_History_Rendition_Format=" +  dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getRENDITION_FORMAT());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT() != null ||
                        (dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getRENDITION_FORMAT() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getRENDITION_FORMAT(),
                            dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getRENDITION_FORMAT());
                }
                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " Inbound Ts => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getINBOUND_TS() +
                        " Product_File_History_Inbound_Ts=" +  dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getINBOUND_TS());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getINBOUND_TS() != null ||
                        (dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getINBOUND_TS() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getINBOUND_TS(),
                            dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getINBOUND_TS());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " Production Date => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE() +
                        " Product_File_History_Production_Date=" +  dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getPRODUCTION_DATE());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE() != null ||
                        (dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getPRODUCTION_DATE() != null)) {
                    Assert.assertEquals("The Production date is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCTION_DATE(),
                            dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getPRODUCTION_DATE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " EPR ID => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID() +
                        " Product_File_History_EPR_ID=" +  dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getEPR_ID());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID() != null ||
                        (dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getEPR_ID() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getEPR_ID(),
                            dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getEPR_ID());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " Product Type => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE() +
                        " Product_File_History_Product_Type=" +  dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getPRODUCT_TYPE());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE() != null ||
                        (dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getPRODUCT_TYPE() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getPRODUCT_TYPE(),
                            dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getPRODUCT_TYPE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() +
                        " U_KEY => Current_Product =" + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getU_KEY() +
                        " Product_File_History_U_KEY=" +  dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getU_KEY());

                if (dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getU_KEY() != null ||
                        (dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromCurrentProductData.get(i).getU_KEY(),
                            dataQualitySDRMContext.recordsFromProductFileHistoryData.get(i).getU_KEY());
                }
            }
        }
    }


    @And("^Compare the records of SDRM Delta current and Delta History table with SDRM History Excl Delta table$")
    public void compareDataDeltaCurrentProductandDeltaHistorywithHistoryExclDelta() {
        if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            System.out.println("Difference between Delta Current product and Delta History availability size "+ dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.size());
            Log.info("Sorting the ISBN Ids to compare the records between Delta Current Product and Delta History with History Excl Delta...");
            for (int i = 0; i < dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.size(); i++) {

                dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDRMContext.recordsFromHistoryExclDeltaData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN));

                Log.info("Delta_Current_Product and History -> ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() +
                        "History Excl Delta -> ISBN => " + dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getISBN());
                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() != null ||
                        (dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() + " is missing/not found in Current_Product table",
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN(),
                            dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() +
                        " TITLE => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getTITLE() +
                        " History Excl Delta=" + dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getTITLE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getTITLE() != null ||
                        (dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getTITLE() != null)) {
                    Assert.assertEquals("The TITLE is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getTITLE(),
                            dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getTITLE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() +
                        " Rendition Format => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getRENDITION_FORMAT() +
                        " History Excl Delta_Rendition_Format=" +  dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getRENDITION_FORMAT());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getRENDITION_FORMAT() != null ||
                        (dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getRENDITION_FORMAT() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getRENDITION_FORMAT(),
                            dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getRENDITION_FORMAT());
                }
                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() +
                        " Inbound Ts => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getINBOUND_TS() +
                        " History Excl Delta_Inbound_Ts=" +  dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getINBOUND_TS());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getINBOUND_TS() != null ||
                        (dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getINBOUND_TS() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getINBOUND_TS(),
                            dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getINBOUND_TS());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() +
                        " Production Date => Delta_Current_Product and History=" + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getPRODUCTION_DATE() +
                        " History Excl Delta_Production_Date=" +  dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getPRODUCTION_DATE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getPRODUCTION_DATE() != null ||
                        (dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getPRODUCTION_DATE() != null)) {
                    Assert.assertEquals("The Production date is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getPRODUCTION_DATE(),
                            dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getPRODUCTION_DATE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() +
                        " EPR ID => Delta_Current_Product and History=" + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getEPR_ID() +
                        " History Excl Delta_EPR_ID=" +  dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getEPR_ID());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getEPR_ID() != null ||
                        (dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getEPR_ID() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getEPR_ID(),
                            dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getEPR_ID());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() +
                        " Product Type => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getPRODUCT_TYPE() +
                        " History Excl Delta_Product_Type=" +  dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getPRODUCT_TYPE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getPRODUCT_TYPE() != null ||
                        (dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getPRODUCT_TYPE() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getPRODUCT_TYPE(),
                            dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getPRODUCT_TYPE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() +
                        " U_KEY => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getU_KEY() +
                        " History Excl Delta_U_KEY=" +  dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getU_KEY());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getU_KEY() != null ||
                        (dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndDeltaHistoryData.get(i).getU_KEY(),
                            dataQualitySDRMContext.recordsFromHistoryExclDeltaData.get(i).getU_KEY());
                }


            }

        }
    }

    @And("^Compare the records of SDRM Delta current and History Excl delta table with SDRM transform latest product table$")
    public void compareDataDeltaCurrentProductandHistoryExclDeltawithSDRMTransformLatestProduct() {
        if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            System.out.println("Difference between Delta Current product and Delta History availability size "+ dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.size());
            Log.info("Sorting the ISBN Ids to compare the records between Delta Current Product and Delta History with History Excl Delta...");
            for (int i = 0; i < dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.size(); i++) {

                dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN));

                Log.info("Delta_Current_Product and History -> ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() +
                        "History Excl Delta -> ISBN => " + dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getISBN());
                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() != null ||
                        (dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() + " is missing/not found in Current_Product table",
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN(),
                            dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() +
                        " TITLE => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getTITLE() +
                        " History Excl Delta=" + dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getTITLE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getTITLE() != null ||
                        (dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getTITLE() != null)) {
                    Assert.assertEquals("The TITLE is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getTITLE(),
                            dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getTITLE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() +
                        " Rendition Format => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getRENDITION_FORMAT() +
                        " History Excl Delta_Rendition_Format=" +  dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getRENDITION_FORMAT());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getRENDITION_FORMAT() != null ||
                        (dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getRENDITION_FORMAT() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getRENDITION_FORMAT(),
                            dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getRENDITION_FORMAT());
                }
                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() +
                        " Inbound Ts => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getINBOUND_TS() +
                        " History Excl Delta_Inbound_Ts=" +  dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getINBOUND_TS());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getINBOUND_TS() != null ||
                        (dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getINBOUND_TS() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getINBOUND_TS(),
                            dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getINBOUND_TS());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() +
                        " Production Date => Delta_Current_Product and History=" + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getPRODUCTION_DATE() +
                        " History Excl Delta_Production_Date=" +  dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getPRODUCTION_DATE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getPRODUCTION_DATE() != null ||
                        (dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getPRODUCTION_DATE() != null)) {
                    Assert.assertEquals("The Production date is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getPRODUCTION_DATE(),
                            dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getPRODUCTION_DATE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() +
                        " EPR ID => Delta_Current_Product and History=" + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getEPR_ID() +
                        " History Excl Delta_EPR_ID=" +  dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getEPR_ID());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getEPR_ID() != null ||
                        (dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getEPR_ID() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getEPR_ID(),
                            dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getEPR_ID());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() +
                        " Product Type => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getPRODUCT_TYPE() +
                        " History Excl Delta_Product_Type=" +  dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getPRODUCT_TYPE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getPRODUCT_TYPE() != null ||
                        (dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getPRODUCT_TYPE() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getPRODUCT_TYPE(),
                            dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getPRODUCT_TYPE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() +
                        " U_KEY => Delta_Current_Product and History =" + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getU_KEY() +
                        " History Excl Delta_U_KEY=" +  dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getU_KEY());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getU_KEY() != null ||
                        (dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentAndHistoryExclDeltaData.get(i).getU_KEY(),
                            dataQualitySDRMContext.recordsFromSDRMTransfromLatestProductData.get(i).getU_KEY());
                }


            }

        }
    }

    @And("^Compare the records of difference between SDRM Current and Prev file history with SDRM Delta current Product$")
    public void compareDifferencebetweenSDRMCurrentAndPrevFileHistory() {
        if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {

            System.out.println("inbound size "+ dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.size());

            Log.info("Sorting the ISBN Ids to compare the records between SDRM Current and Prev file history with SDRM Delta current Product...");
            for (int i = 0; i < dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.size(); i++) {

                dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDRMContext.recordsFromDeltaCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN));

                Log.info("Current and Prev File History -> ISBN => " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() +
                        "Delta Current_Product -> ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN());
                if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() + " is missing/not found in Current_Product table",
                            dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN(),
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() +
                        " TITLE => Current and Prev File History =" + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getTITLE() +
                        " Delta Current_Product=" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getTITLE());

                if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getTITLE() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getTITLE() != null)) {
                    Assert.assertEquals("The TITLE is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getTITLE(),
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getTITLE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() +
                        " Rendition Format => Current and Prev File History =" + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getRENDITION_FORMAT() +
                        " Delta_Current_Rendition_Format=" +  dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getRENDITION_FORMAT());

                if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getRENDITION_FORMAT() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getRENDITION_FORMAT() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getRENDITION_FORMAT(),
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getRENDITION_FORMAT());
                }
                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() +
                        " Inbound Ts => Current and Prev File History =" + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getINBOUND_TS() +
                        "Delta Current_Inbound_Ts=" +  dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getINBOUND_TS());

                if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getINBOUND_TS() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getINBOUND_TS() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getINBOUND_TS(),
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getINBOUND_TS());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() +
                        " Production Date => Current and Prev File History =" + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getPRODUCTION_DATE() +
                        " Delta Current_Production_Date=" +  dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCTION_DATE());

                if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getPRODUCTION_DATE() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCTION_DATE() != null)) {
                    Assert.assertEquals("The Production date is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getPRODUCTION_DATE(),
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCTION_DATE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() +
                        " EPR ID => Current and Prev File History =" + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getEPR_ID() +
                        " Delta Current_EPR_ID=" +  dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getEPR_ID());

                if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getEPR_ID() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getEPR_ID() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getEPR_ID(),
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getEPR_ID());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() +
                        " Product Type => Current and Prev File History =" + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getPRODUCT_TYPE() +
                        " Delta Current_Product_Type=" +  dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCT_TYPE());

                if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getPRODUCT_TYPE() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCT_TYPE() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getPRODUCT_TYPE(),
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCT_TYPE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() +
                        " Product Type => Current and Prev File History =" + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getDELTA_MODE() +
                        " Delta Current_Product_Type=" +  dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getDELTA_MODE());

                if (dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getDELTA_MODE() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getDELTA_MODE() != null)) {
                    Assert.assertEquals("The DELTA_MODE is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromSDRMCurrentAndPrevFileHistoryData.get(i).getDELTA_MODE(),
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getDELTA_MODE());
                }

            }

        }
    }

    @And("^Compare the records of SDRM Delta current product and SDRM Delta product history$")
    public void compareDataDeltaCurrentProductandDeltaProductHistory() {
        if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ISBN Ids to compare the records between Delta Current Product and Delta Product History...");
            for (int i = 0; i < dataQualitySDRMContext.recordsFromDeltaCurrentProductData.size(); i++) {

                dataQualitySDRMContext.recordsFromDeltaCurrentProductData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN)); //sort primarykey data in the lists
                dataQualitySDRMContext.recordsFromDeltaProductHistoryData.sort(Comparator.comparing(SDRMDLAccessObject::getISBN));

                Log.info("Delta_Current_Product -> ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() +
                        "Delta_Product_History -> ISBN => " + dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getISBN());
                if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getISBN() != null)) {
                    Assert.assertEquals("The ISBN is =" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() + " is missing/not found in Current_Product table",
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN(),
                            dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getISBN());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() +
                        " TITLE => Delta_Current_Product =" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getTITLE() +
                        " Delta_Product_History=" + dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getTITLE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getTITLE() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getTITLE() != null)) {
                    Assert.assertEquals("The TITLE is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getTITLE(),
                            dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getTITLE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() +
                        " Rendition Format => Delta_Current_Product =" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getRENDITION_FORMAT() +
                        " Delta_Product_History_Rendition_Format=" +  dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getRENDITION_FORMAT());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getRENDITION_FORMAT() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getRENDITION_FORMAT() != null)) {
                    Assert.assertEquals("The Rendition Format is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getRENDITION_FORMAT(),
                            dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getRENDITION_FORMAT());
                }
                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() +
                        " Inbound Ts => Delta_Current_Product =" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getINBOUND_TS() +
                        " Delta_Product_History_Inbound_Ts=" +  dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getINBOUND_TS());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getINBOUND_TS() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getINBOUND_TS() != null)) {
                    Assert.assertEquals("The Inbound ts is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getINBOUND_TS(),
                            dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getINBOUND_TS());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() +
                        " Production Date => Delta_Current_Product =" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCTION_DATE() +
                        " Delta_Product_History_Production_Date=" +  dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getPRODUCTION_DATE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCTION_DATE() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getPRODUCTION_DATE() != null)) {
                    Assert.assertEquals("The Production date is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCTION_DATE(),
                            dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getPRODUCTION_DATE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() +
                        " EPR ID => Delta_Current_Product =" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getEPR_ID() +
                        " Delta_Product_History_EPR_ID=" +  dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getEPR_ID());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getEPR_ID() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getEPR_ID() != null)) {
                    Assert.assertEquals("The EPR Id is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getEPR_ID(),
                            dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getEPR_ID());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() +
                        " Product Type => Delta_Current_Product =" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCT_TYPE() +
                        " Delta_Product_History_Product_Type=" +  dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getPRODUCT_TYPE());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCT_TYPE() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getPRODUCT_TYPE() != null)) {
                    Assert.assertEquals("The Product Type is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getPRODUCT_TYPE(),
                            dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getPRODUCT_TYPE());
                }

                Log.info("ISBN => " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() +
                        " U_KEY => Delta_Current_Product =" + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getU_KEY() +
                        " Delta_Product_History_U_KEY=" +  dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getU_KEY());

                if (dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getU_KEY() != null ||
                        (dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getU_KEY() != null)) {
                    Assert.assertEquals("The U_Key is incorrect for ISBN = " + dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getISBN() ,
                            dataQualitySDRMContext.recordsFromDeltaCurrentProductData.get(i).getU_KEY(),
                            dataQualitySDRMContext.recordsFromDeltaProductHistoryData.get(i).getU_KEY());
                }

            }

        }
    }

}
