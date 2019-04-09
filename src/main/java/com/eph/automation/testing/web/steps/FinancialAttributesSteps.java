package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.FinancialAttribsContext;
import com.eph.automation.testing.models.dao.FinancialAttribsDataObject;
import com.eph.automation.testing.services.db.sql.FinAttrSQL;
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


    @Given("^We have a number of financial attributes to check$")
    public void getID(){

        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //numberOfRecords = "5";
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = FinAttrSQL.gettingSourceRef.replace("PARAM1", numberOfRecords);
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
        sql = String.format(FinAttrSQL.GET_STG_DQ_WORKS_DATA, Joiner.on("','").join(ids));
        financialAttribs.financialDataFromStg = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);

        sql = String.format(FinAttrSQL.GET_SA_FinAttr_DATA_All, Joiner.on("','").join(workid));
        financialAttribs.financialDataFromSAAll = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);

        sql = String.format(FinAttrSQL.GET_GD_FinnAttr_DATA, Joiner.on("','").join(workid));
        financialAttribs.financialDataFromGD = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);


    }

    @Then("^The data between DQ and SA is identical$")
    public void checkFinancialData(){
        for (int i=0; i<financialAttribs.financialDataFromStg.size();i++) {
            sql=FinAttrSQL.Get_work_id.replace("PARAM1", financialAttribs.financialDataFromStg.get(i).PMX_SOURCE_REFERENCE);
            Log.info(sql);
            financialAttribs.workID = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
            fWorkID = financialAttribs.workID.get(0).workID;

            sql = String.format(FinAttrSQL.GET_SA_FinAttr_DATA.replace("PARAM1",fWorkID));
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
                }else {
                    Log.info("Financial attribute in dq is: " + financialAttribs.financialID.get(0).fin_attribs_id);
                    Log.info("Financial attribute in sa is: " + financialAttribs.financialDataFromSA.get(0).fin_attribs_id);
                    Assert.assertEquals("Expecting the financial attribute id details from STG and SA Consistent for id=" + ids.get(i),
                            financialAttribs.financialID.get(0).fin_attribs_id
                                    ,(financialAttribs.financialDataFromSA.get(0).fin_attribs_id));
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
                Log.info(financialAttribs.financialDataFromSA.get(0).cost_resp_centre );
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
        }
    }

    @And("^The data between SA and GD is identical$")
    public void checkFinancialDataGD(){
        for (int i=0; i<financialAttribs.financialDataFromSAAll.size();i++) {
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
        }
    }
}