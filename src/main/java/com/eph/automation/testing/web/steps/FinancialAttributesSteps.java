package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.FinancialAttribsContext;
import com.eph.automation.testing.models.dao.FinancialAttribsDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.FinAttrSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FinancialAttributesSteps {

    @StaticInjection
    public FinancialAttribsContext financialAttribs;
    public String sql;
    private static List<FinancialAttribsDataObject> id;

    private String numberOfRecords;
    private List<Map<?, ?>> manifestationIds;
    private static List<String> ids;
    private static List<String> workid;
    private static List<String> isbns;
    public String fWorkID;
    private static List<WorkDataObject> refreshDate;

    public static List<FinancialAttribsDataObject> endDatedFinAttr;
    public static List<FinancialAttribsDataObject> stgNewRecord;

    @Given("^We know the number of financial attributes in DQ$")
    public void getFinAttrCountDQ(){
        if (System.getProperty("LOAD") != null) {
            if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = FinAttrSQL.PMX_STG_DQ_WORKS_COUNT_NoErr_Full;
                Log.info(sql);
                financialAttribs.dqCount = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
                Log.info("The DQ count is: " + financialAttribs.dqCount.get(0).dqCount);
            } else {
                sql = WorkCountSQL.GET_REFRESH_DATE;
                refreshDate = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                        Constants.EPH_URL);

                sql = FinAttrSQL.PMX_STG_DQ_WORKS_COUNT_NoErr.replace("PARAM1", refreshDate.get(1).refresh_timestamp);
                Log.info(sql);
                financialAttribs.dqCount = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
                Log.info("The DQ count is: " + financialAttribs.dqCount.get(0).dqCount);
            }
        }
        else{
            sql = WorkCountSQL.GET_REFRESH_DATE;
            refreshDate = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                    Constants.EPH_URL);

            sql = FinAttrSQL.PMX_STG_DQ_WORKS_COUNT_NoErr.replace("PARAM1", refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            financialAttribs.dqCount = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
            Log.info("The DQ count is: " + financialAttribs.dqCount.get(0).dqCount);

/*            sql = FinAttrSQL.PMX_STG_DQ_WORKS_COUNT_NoErr_Full;
            Log.info(sql);
            financialAttribs.dqCount = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
            Log.info("The DQ count is: " + financialAttribs.dqCount.get(0).dqCount);*/
            }

    }

    @When("^We get the financial attributes from SA$")
    public void getFinAttrCountSA(){
        sql = FinAttrSQL.Get_SA_count;
        Log.info(sql);
        financialAttribs.saCount = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
        Log.info("The SA count is: " + financialAttribs.saCount.get(0).saCount);
    }

    @When("^We get the financial attributes from GD$")
    public void getFinAttrCountGD(){
        sql = FinAttrSQL.Get_GD_count;
        Log.info(sql);
        financialAttribs.gdCount = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
        Log.info("The GD count is: " + financialAttribs.gdCount.get(0).gdCount);
    }

    @When("^We get the financial attributes from AE$")
    public void getFinAttrCountAE(){
        sql = FinAttrSQL.Get_GD_count;
        Log.info(sql);
        financialAttribs.gdCount = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
        Log.info("The GD count is: " + financialAttribs.gdCount.get(0).gdCount);

        sql = FinAttrSQL.Get_AE_count;
        financialAttribs.aeCount = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
        Log.info("The AE count is: " + financialAttribs.aeCount.get(0).aeCount);
    }

    @Then("^The financial attributes between (.*) and (.*) are equal$")
    public void compareCount(String source, String target){
        if (source.equalsIgnoreCase("DQ")){
           Assert.assertEquals("The count between DQ and SA does not match!", financialAttribs.dqCount.get(0).dqCount,
                   financialAttribs.saCount.get(0).saCount);
        }else if (target.equalsIgnoreCase("GD")){
            Assert.assertEquals("The count between SA and GD does not match!", financialAttribs.saCount.get(0).saCount,
                    financialAttribs.gdCount.get(0).gdCount);
        }else {
            Assert.assertEquals("The count between SA and GD + AE does not match!", financialAttribs.saCount.get(0).saCount,
                    financialAttribs.gdCount.get(0).gdCount + financialAttribs.aeCount.get(0).aeCount);
        }
    }

    @Given("^We have a number of financial attributes to check$")
    public void getID(){

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber")!=null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        }else {
            numberOfRecords = "5";
        }
        Log.info("numberOfRecords = " + numberOfRecords);


            if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            sql = FinAttrSQL.gettingSourceRef.replace("PARAM1", numberOfRecords);
            }else {
                sql = WorkCountSQL.GET_REFRESH_DATE;
                refreshDate = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                        Constants.EPH_URL);
                sql = FinAttrSQL.gettingSourceRefDelta.replace("PARAM1", numberOfRecords)
                .replace("PARAM2",refreshDate.get(1).refresh_timestamp);
            }

        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomISBNIds.stream().map(m -> (BigDecimal) m.get("random_value")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

        sql = String.format(FinAttrSQL.gettingWorkID, Joiner.on("','").join(ids));
        List<Map<?, ?>> randomWorkID = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        workid = randomWorkID.stream().map(m -> (String) m.get("work_id")).collect(Collectors.toList());
        Log.info(workid.toString());
    }

    @When("^We get the data for financial attributes$")
    public void getFinancialData(){
        if(!ids.isEmpty()) {
            sql = String.format(FinAttrSQL.GET_STG_DQ_WORKS_DATA, Joiner.on("','").join(ids));
            financialAttribs.financialDataFromStg = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
        }

        sql = String.format(FinAttrSQL.GET_SA_FinAttr_DATA, Joiner.on("','").join(workid));
        financialAttribs.financialDataFromSA = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);

        sql = String.format(FinAttrSQL.GET_SA_FinAttr_DATA_All, Joiner.on("','").join(workid));
        financialAttribs.financialDataFromSAAll = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);

        sql = String.format(FinAttrSQL.GET_GD_FinnAttr_DATA, Joiner.on("','").join(workid));
        financialAttribs.financialDataFromGD = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);


    }

    @Then("^The data between DQ and SA is identical$")
    public void checkFinancialData(){
        if (financialAttribs.financialDataFromStg == null) {
            Log.info("No new records added in Financial attributes");
        }else if (financialAttribs.financialDataFromSA.isEmpty()) {
            Log.info("There is no changed data for Financial attributes");
        }else {
            for (int i = 0; i < financialAttribs.financialDataFromStg.size(); i++) {
                sql = FinAttrSQL.Get_work_id.replace("PARAM1", financialAttribs.financialDataFromStg.get(i).PMX_SOURCE_REFERENCE);
                Log.info(sql);
                financialAttribs.workID = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
                fWorkID = financialAttribs.workID.get(0).workID;
                Log.info(sql);
                sql = String.format(FinAttrSQL.GET_SA_FinAttr_DATA.replace("PARAM1", fWorkID));
                Log.info(sql);
                financialAttribs.financialDataFromSA = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);


                Assert.assertEquals("The classname is incorrect for id=" + financialAttribs.id,
                        "WorkFinancialAttributes", financialAttribs.financialDataFromSA.get(0).B_CLASSNAME);

                Log.info(ids.get(i));
                Log.info(financialAttribs.financialDataFromStg.get(i).opco);
                Log.info(financialAttribs.financialDataFromStg.get(i).resp_centre);

                if (financialAttribs.financialDataFromStg.get(i).opco != null
                        && financialAttribs.financialDataFromStg.get(i).resp_centre != null) {
                    sql = FinAttrSQL.GET_FinnAttr_ID.replace("PARAM1", ids.get(i) +
                            financialAttribs.financialDataFromStg.get(i).opco +
                            financialAttribs.financialDataFromStg.get(i).resp_centre
                    );
                    Log.info(sql);
                    financialAttribs.financialID = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
                    if (financialAttribs.financialID.isEmpty()) {
                        Log.info("No financial Id found!");
                    } else {
                        Log.info("Financial attribute in dq is: " + financialAttribs.financialID.get(0).fin_attribs_id);
                        Log.info("Financial attribute in sa is: " + financialAttribs.financialDataFromSA.get(0).fin_attribs_id);
                        Assert.assertEquals("Expecting the financial attribute id details from STG and SA Consistent for id=" + ids.get(i),
                                financialAttribs.financialID.get(0).fin_attribs_id
                                , (financialAttribs.financialDataFromSA.get(0).fin_attribs_id));
                    }
                }

                if (financialAttribs.financialDataFromStg.get(i).opco != null
                        || financialAttribs.financialDataFromSA.get(0).gl_company != null) {
                    Log.info(financialAttribs.financialDataFromStg.get(i).opco);
                    Log.info(financialAttribs.financialDataFromSA.get(0).gl_company);
                    assertTrue("Expecting the OPCO details from STG and SA Consistent for id=" + ids.get(i),
                            financialAttribs.financialDataFromStg.get(i).opco
                                    .equals(financialAttribs.financialDataFromSA.get(0).gl_company));
                }

                if (financialAttribs.financialDataFromStg.get(i).resp_centre != null
                        || financialAttribs.financialDataFromSA.get(0).cost_resp_centre != null) {
                    Log.info(financialAttribs.financialDataFromStg.get(i).resp_centre);
                    Log.info(financialAttribs.financialDataFromSA.get(0).cost_resp_centre);
                    assertTrue("Expecting the cost resp centre details from STG and SA Consistent for id=" + ids.get(i),
                            financialAttribs.financialDataFromStg.get(i).resp_centre
                                    .equals(financialAttribs.financialDataFromSA.get(0).cost_resp_centre));
                }

                if (financialAttribs.financialDataFromStg.get(i).resp_centre != null
                        || financialAttribs.financialDataFromSA.get(0).revenue_resp_centre != null) {
                    assertTrue("Expecting the revenue resp centre details from STG and SA Consistent for id=" + ids.get(i),
                            financialAttribs.financialDataFromStg.get(i).resp_centre
                                    .equals(financialAttribs.financialDataFromSA.get(0).revenue_resp_centre));
                }

                if (financialAttribs.financialDataFromStg.get(i).getPMX_SOURCE_REFERENCE() != null
                        || financialAttribs.financialDataFromSA.get(0).external_reference != null) {
                    assertEquals("Expecting the revenue resp centre details from STG and SA Consistent for id=" + ids.get(i),
                            financialAttribs.financialDataFromStg.get(i).getPMX_SOURCE_REFERENCE() +
                                    financialAttribs.financialDataFromStg.get(i).opco +
                                    financialAttribs.financialDataFromStg.get(i).resp_centre,
                            financialAttribs.financialDataFromSA.get(0).external_reference);
                }
            }
        }
    }

    @And("^The data between SA and GD is identical$")
    public void checkFinancialDataGD(){
        for (int i=0; i<financialAttribs.financialDataFromSAAll.size();i++) {
            if (financialAttribs.financialDataFromSA.isEmpty() && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
                Log.info("There is no changed data for Financial attributes");
            }else{
                Assert.assertEquals("The classname is incorrect for id=" + financialAttribs.financialDataFromSAAll.get(i).fin_attribs_id,
                    "WorkFinancialAttributes", financialAttribs.financialDataFromGD.get(i).B_CLASSNAME);

            Log.info("Fin attr in sa is: " + financialAttribs.financialDataFromSAAll.get(i).fin_attribs_id);
            Log.info("Fin attr in gd is: " + financialAttribs.financialDataFromGD.get(i).fin_attribs_id);
                Assert.assertEquals("Expecting the financial attribute id details from SA and GD Consistent for id=" + financialAttribs.financialDataFromSAAll.get(i).fin_attribs_id,
                        financialAttribs.financialDataFromSAAll.get(i).fin_attribs_id
                                ,(financialAttribs.financialDataFromGD.get(i).fin_attribs_id));


            if (financialAttribs.financialDataFromSAAll.get(i).gl_company != null
                    || financialAttribs.financialDataFromGD.get(i).gl_company != null) {
                assertTrue("Expecting the OPCO details from SA and GD Consistent for id=" + financialAttribs.financialDataFromSAAll.get(i).fin_attribs_id,
                        financialAttribs.financialDataFromSAAll.get(i).gl_company
                                .equals(financialAttribs.financialDataFromGD.get(i).gl_company));
            }

            if (financialAttribs.financialDataFromSAAll.get(i).cost_resp_centre != null
                    || financialAttribs.financialDataFromGD.get(i).cost_resp_centre != null) {
                assertTrue("Expecting the cost resp centre details from SA and GD Consistent for id=" + financialAttribs.financialDataFromSAAll.get(i).fin_attribs_id,
                        financialAttribs.financialDataFromSAAll.get(i).cost_resp_centre
                                .equals(financialAttribs.financialDataFromGD.get(i).cost_resp_centre));
            }

            if (financialAttribs.financialDataFromSAAll.get(i).revenue_resp_centre != null
                    || financialAttribs.financialDataFromGD.get(i).revenue_resp_centre != null) {
                assertTrue("Expecting the revenue resp centre details from SA and GD Consistent for id=" + financialAttribs.financialDataFromSAAll.get(i).fin_attribs_id,
                        financialAttribs.financialDataFromSAAll.get(i).revenue_resp_centre
                                .equals(financialAttribs.financialDataFromGD.get(i).revenue_resp_centre));
            }

            if (financialAttribs.financialDataFromSAAll.get(i).external_reference != null
                    || financialAttribs.financialDataFromGD.get(i).external_reference != null) {
                assertTrue("Expecting the revenue resp centre details from SA and GD Consistent for id=" + financialAttribs.financialDataFromSAAll.get(i).fin_attribs_id,
                        financialAttribs.financialDataFromSAAll.get(i).external_reference
                                .equals(financialAttribs.financialDataFromGD.get(i).external_reference));
            }
        }
        }
    }

    @Given("^We have end-dated financial attribute in GD$")
    public void getEndDatedFA() {
        if (System.getProperty("LOAD") != null) {
            if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                Log.info("There is no delta load performed");
            } else {
                sql = FinAttrSQL.GET_GD_FinnAttr_DATA_End_Date;
                endDatedFinAttr = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class,
                        Constants.EPH_URL);
                if (endDatedFinAttr.isEmpty()){
                    Log.info("No records were updated");
                } else {
                    Log.info("There are end dated records");
                }
            }
        }else{
            sql = FinAttrSQL.GET_GD_FinnAttr_DATA_End_Date;
            endDatedFinAttr = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class,
                    Constants.EPH_URL);
            if (endDatedFinAttr.isEmpty()){
                Log.info("No records were updated");
            } else {
                Log.info("There are end dated records");
            }
         //   Log.info("There is no delta load performed");
        }
    }

    @When("^We get the new data for the same record from STG$")
    public void getNewStgData(){
        if (System.getProperty("LOAD") != null) {
            if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                Log.info("There is no delta load performed");
            } else {
                if (endDatedFinAttr.isEmpty()){
                    Log.info("No financial attributes were updated");
                } else {
                    sql = FinAttrSQL.GET_STG_DQ_WORKS_DATA_Delta.replace("PARAM1",
                            endDatedFinAttr.get(0).PMX_SOURCE_REFERENCE);
                    stgNewRecord = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class,
                            Constants.EPH_URL);
                }
            }
        }else{
            if (endDatedFinAttr.isEmpty()){
                Log.info("No financial attributes were updated");
            } else {
                sql = FinAttrSQL.GET_STG_DQ_WORKS_DATA_Delta.replace("PARAM1",
                        endDatedFinAttr.get(0).PMX_SOURCE_REFERENCE);
                stgNewRecord = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class,
                        Constants.EPH_URL);
            }
           // Log.info("There is no delta load performed");
        }
    }

    @Then("^There is difference in the record values$")
    public void compareNewId(){
        if (System.getProperty("LOAD") != null) {
            if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                Log.info("There is no delta load performed");
            } else {
                if (endDatedFinAttr.isEmpty()){
                    Log.info("No identifiers were updated");
                } else {
                    if (stgNewRecord.get(0).opco.equalsIgnoreCase(endDatedFinAttr.get(0).gl_company)){
                        Log.info("OPCO details are the same. Checking resp_centre data...");
                        Assert.assertNotEquals("There is no data change!",stgNewRecord.get(0).resp_centre,
                                endDatedFinAttr.get(0).cost_resp_centre);

                    } else {
                        Log.info("There is a change in the OPCO data");
                    }
                }
            }
        }else{
            if (endDatedFinAttr.isEmpty()){
                Log.info("No identifiers were updated");
            } else {
                if (stgNewRecord.get(0).opco.equalsIgnoreCase(endDatedFinAttr.get(0).gl_company)){
                    Log.info("OPCO details are the same. Checking resp_centre data...");
                    Assert.assertNotEquals("There is no data change!",stgNewRecord.get(0).resp_centre,
                            endDatedFinAttr.get(0).cost_resp_centre);
                } else {
                    Log.info("There is a change in the OPCO data");
                }
            }
           // Log.info("There is no delta load performed");
        }
    }
}
