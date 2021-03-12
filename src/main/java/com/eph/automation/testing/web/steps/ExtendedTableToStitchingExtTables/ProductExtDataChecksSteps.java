package com.eph.automation.testing.web.steps.ExtendedTableToStitchingExtTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.StitchingExtContext;
import com.eph.automation.testing.models.dao.StitchingExtended.*;
import com.eph.automation.testing.services.db.StitchingExtendedSQL.StitchingExtDataChecksSQL;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ProductExtDataChecksSteps {

    public StitchingExtContext dataQualityStitchContext;
    private static String sql;
    private static List<String> Ids;
    private StitchingExtDataChecksSQL StchObj = new StitchingExtDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");



    @Given("^We get the (.*) random Prod Ext Availability EPR ids (.*)$")
    public void getRandomProdExtAvailabilityEPRIds(String numberOfRecords, String tableName) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Prod Ext Availability EPR Ids...");
        switch (tableName) {
            case "product_extended_availability":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_PROD_EXTENDED_AVAILABILITY, numberOfRecords);
                List<Map<?, ?>> randomProdExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomProdExtendedEPRIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the records from Prod extended Availability table$")
    public void getRecordsFromProdExtendedAvailabilityTable() {
        Log.info("We get the records from Prod Extended Avaiability Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_PROD_EXT_AVAIL_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromProdExtAvail = DBManager.getDBResultAsBeanList(sql, ProductExtAccessObject.class, Constants.AWS_URL);
    }

    public void getType(String ProdAvailId){
        Log.info("We get the type from Prod Stitching Extended Availability Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_PROD_EXT_AVAIL_JSON_REC, ProdAvailId);
       // Log.info(sql);
        dataQualityStitchContext.recFromProdStitchAvailExtended = DBManager.getDBResultAsBeanList(sql, ProductExtAccessObject.class, Constants.EPH_URL);
    }

  /*  public void getIndividualRecFromProdExtAvailTable(String prodAvailId) {
        Log.info("We get the Individual records from Prod Extended Availability Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_PROD_EXT_AVAIL_REC,prodAvailId);
        Log.info(sql);
        dataQualityStitchContext.recordsFromProdExtAvails = DBManager.getDBResultAsBeanList(sql, ProductExtAccessObject.class, Constants.AWS_URL);
    }*/

    @Then("^Get the records from product extended stitching table$")
    public void getProdExtendedAvailJSONRec(String prodAvailId) {
        Log.info("We get the JSON from Prod Ext Stitching Avail...");
        Log.info(prodAvailId);
        sql = String.format(StitchingExtDataChecksSQL.GET_PROD_EXT_AVAIL_JSON_REC, prodAvailId);
        Log.info(sql);
        List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        StitchingExtContext.recordsFromProdStitching = new Gson().fromJson(jsonValue.get(0).get("json"), ProdExtJsonObject.class);
    }

    @And("^Compare Product Extended Availability and Product Extended Stitching JSON$")
    public void compareProdExtAvailabilityAndStitchingProdExtAvail() {
        if (dataQualityStitchContext.recordsFromProdExtAvail.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < dataQualityStitchContext.recordsFromProdExtAvail.size(); i++) {
                String prodAvailId = dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id();
              //  getIndividualRecFromProdExtAvailTable(prodAvailId);
                getProdExtendedAvailJSONRec(prodAvailId);
                getType(prodAvailId);
                if (dataQualityStitchContext.recordsFromProdStitching.getAvailabilityExtended().getApplications() != null) {
                    ArrayList<Applications> extprodAvail_temp = new ArrayList(Arrays.asList(dataQualityStitchContext.recordsFromProdStitching.getAvailabilityExtended().getApplications()));
                    for (int j = 0; j < extprodAvail_temp.size(); j++) {
                        String sourceAppName = dataQualityStitchContext.recordsFromProdExtAvail.get(i).getapplication_name();
                        String trgetAppName = extprodAvail_temp.get(j).getApplicationName();
                        String sourceAvailFormat = dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_format();
                        String trgetAvailFormat = extprodAvail_temp.get(j).getAvailabilityFormat();

                        if (sourceAvailFormat==null) {
                            if (sourceAppName.equals(trgetAppName)) {
                                Log.info("Prod_Ext_Availability -> EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_JSON_Avail -> EPR => " + dataQualityStitchContext.recordsFromProdStitching.getId());
                                if (dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromProdStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() + " is missing/not found in Prod_Stitching table",
                                            dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromProdStitching.getId());
                                }

                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> prod_type => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getproduct_type() +
                                        " Prod_JSON_Avail -> Type => " + dataQualityStitchContext.recFromProdStitchAvailExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromProdExtAvail.get(i).getproduct_type() != null ||
                                        (dataQualityStitchContext.recFromProdStitchAvailExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() + " is missing/not found in Prod_Stitching table",
                                            dataQualityStitchContext.recordsFromProdExtAvail.get(i).getproduct_type(),
                                            dataQualityStitchContext.recFromProdStitchAvailExtended.get(0).gettype());
                                }
                                System.out.println(prodAvailId + " = sourceApp-->" + sourceAppName + " AND " + "targetApp------>" + trgetAppName);

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> App_Name => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getapplication_name() +
                                        " Prod_JSON_Avail -> App_Name => " + extprodAvail_temp.get(j).getApplicationName());
                                Assert.assertEquals("The App_Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getapplication_name(),
                                        extprodAvail_temp.get(j).getApplicationName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> delta_answer_code_uk => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getdelta_answer_code_uk() +
                                        " Prod_JSON_Avail -> delta_answer_code_uk => " + extprodAvail_temp.get(j).getDeltaAnswerCodeUK());
                                Assert.assertEquals("The delta_answer_code_uk is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getdelta_answer_code_uk(),
                                        extprodAvail_temp.get(j).getDeltaAnswerCodeUK());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> delta_answer_code_us => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getdelta_answer_code_us() +
                                        " Prod_JSON_Avail -> delta_answer_code_us => " + extprodAvail_temp.get(j).getDeltaAnswerCodeUS());
                                Assert.assertEquals("The delta_answer_code_us is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getdelta_answer_code_us(),
                                        extprodAvail_temp.get(j).getDeltaAnswerCodeUS());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> PublicationStatusANZ => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getpublication_status_anz() +
                                        " Prod_JSON_Avail -> PublicationStatusANZ => " + extprodAvail_temp.get(j).getPublicationStatusANZ());
                                Assert.assertEquals("The PublicationStatusANZ is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getpublication_status_anz(),
                                        extprodAvail_temp.get(j).getPublicationStatusANZ());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> AvailabilityStartDate => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_start_date() +
                                        " Prod_JSON_Avail -> AvailabilityStartDate => " + extprodAvail_temp.get(j).getAvailabilityStartDate());
                                Assert.assertEquals("The AvailabilityStartDate is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_start_date(),
                                        extprodAvail_temp.get(j).getAvailabilityStartDate());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> AvailabilityStatus => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_status() +
                                        " Prod_JSON_Avail -> AvailabilityStatus => " + extprodAvail_temp.get(j).getAvailabilityStatus());
                                Assert.assertEquals("The AvailabilityStatus is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_status(),
                                        extprodAvail_temp.get(j).getAvailabilityStatus());

                                /*Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> AvailabilityFormat => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_format() +
                                        " Prod_JSON_Avail -> AvailabilityFormat => " + extprodAvail_temp.get(j).getAvailabilityFormat());
                                Assert.assertEquals("The AvailabilityFormat is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_format(),
                                        extprodAvail_temp.get(j).getAvailabilityFormat());*/

                                System.out.println("--Satisfy Success--");
                                j = 0;
                                break;
                            } else {
                                j = j++;
                            }
                        } else if(sourceAvailFormat!=null) {

                            if (sourceAvailFormat.equals(trgetAvailFormat)) {

                                Log.info("Prod_Ext_Availability -> EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_JSON_Avail -> EPR => " + dataQualityStitchContext.recordsFromProdStitching.getId());
                                if (dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() != null ||
                                        (dataQualityStitchContext.recordsFromProdStitching.getId() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() + " is missing/not found in Prod_Stitching table",
                                            dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                            dataQualityStitchContext.recordsFromProdStitching.getId());
                                }

                                Log.info(" EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> prod_type => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getproduct_type() +
                                        " Prod_JSON_Avail -> Type => " + dataQualityStitchContext.recFromProdStitchAvailExtended.get(0).gettype());
                                if (dataQualityStitchContext.recordsFromProdExtAvail.get(i).getproduct_type() != null ||
                                        (dataQualityStitchContext.recFromProdStitchAvailExtended.get(0).gettype() != null)) {
                                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() + " is missing/not found in Prod_Stitching table",
                                            dataQualityStitchContext.recordsFromProdExtAvail.get(i).getproduct_type(),
                                            dataQualityStitchContext.recFromProdStitchAvailExtended.get(0).gettype());
                                }
                                System.out.println(prodAvailId + " = sourceApp-->" + sourceAppName + " AND " + "targetApp------>" + trgetAppName);

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> App_Name => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getapplication_name() +
                                        " Prod_JSON_Avail -> App_Name => " + extprodAvail_temp.get(j).getApplicationName());
                                Assert.assertEquals("The App_Name is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getapplication_name(),
                                        extprodAvail_temp.get(j).getApplicationName());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> delta_answer_code_uk => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getdelta_answer_code_uk() +
                                        " Prod_JSON_Avail -> delta_answer_code_uk => " + extprodAvail_temp.get(j).getDeltaAnswerCodeUK());
                                Assert.assertEquals("The delta_answer_code_uk is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getdelta_answer_code_uk(),
                                        extprodAvail_temp.get(j).getDeltaAnswerCodeUK());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> delta_answer_code_us => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getdelta_answer_code_us() +
                                        " Prod_JSON_Avail -> delta_answer_code_us => " + extprodAvail_temp.get(j).getDeltaAnswerCodeUS());
                                Assert.assertEquals("The delta_answer_code_us is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getdelta_answer_code_us(),
                                        extprodAvail_temp.get(j).getDeltaAnswerCodeUS());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> PublicationStatusANZ => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getpublication_status_anz() +
                                        " Prod_JSON_Avail -> PublicationStatusANZ => " + extprodAvail_temp.get(j).getPublicationStatusANZ());
                                Assert.assertEquals("The PublicationStatusANZ is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getpublication_status_anz(),
                                        extprodAvail_temp.get(j).getPublicationStatusANZ());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> AvailabilityStartDate => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_start_date() +
                                        " Prod_JSON_Avail -> AvailabilityStartDate => " + extprodAvail_temp.get(j).getAvailabilityStartDate());
                                Assert.assertEquals("The AvailabilityStartDate is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_start_date(),
                                        extprodAvail_temp.get(j).getAvailabilityStartDate());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> AvailabilityStatus => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_status() +
                                        " Prod_JSON_Avail -> AvailabilityStatus => " + extprodAvail_temp.get(j).getAvailabilityStatus());
                                Assert.assertEquals("The AvailabilityStatus is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_status(),
                                        extprodAvail_temp.get(j).getAvailabilityStatus());

                                Log.info("EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id() +
                                        " Prod_Ext_Availability -> AvailabilityFormat => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_format() +
                                        " Prod_JSON_Avail -> AvailabilityFormat => " + extprodAvail_temp.get(j).getAvailabilityFormat());
                                Assert.assertEquals("The AvailabilityFormat is incorrect for EPR => " + dataQualityStitchContext.recordsFromProdExtAvail.get(i).getepr_id(),
                                        dataQualityStitchContext.recordsFromProdExtAvail.get(i).getavailability_format(),
                                        extprodAvail_temp.get(j).getAvailabilityFormat());

                                System.out.println("--Satisfy Success--");
                                j = 0;
                                break;
                            } else {
                                j = j++;
                            }
                        }
                    }
                }
            }
        }
    }
}


