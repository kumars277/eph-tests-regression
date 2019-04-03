package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.FinancialAttribsContext;
import com.eph.automation.testing.models.dao.FinancialAttribsDataObject;
import com.eph.automation.testing.services.db.sql.FinAttrSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class FinancialAttributesSteps {

    @StaticInjection
    public FinancialAttribsContext financialAttribs;
    public String sql;
    private static List<FinancialAttribsDataObject> id;
    private static List<FinancialAttribsDataObject> workid;

    @Given("^We have to check a number of financial attributes$")
    public void getID(){
        sql = FinAttrSQL.gettingSourceRef;
        id = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
        financialAttribs.identifier = id.get(0).random_value;
        Log.info
                ("\nWork id is " + financialAttribs.identifier);

        sql = FinAttrSQL.gettingWorkID.replace("PARAM1",financialAttribs.identifier);
        workid = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
        financialAttribs.workID = workid.get(0).work_id;
        Log.info
                ("\nWork id is " + financialAttribs.workID);

    }

    @When("^We get the data for financial attributes$")
    public void getFinancialData(){
        sql = FinAttrSQL.GET_STG_DQ_WORKS_DATA.replace("PARAM1", financialAttribs.identifier);
        financialAttribs.financialDataFromStg = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);


        sql = FinAttrSQL.GET_SA_FinAttr_DATA.replace("PARAM1", financialAttribs.workID);
        financialAttribs.financialDataFromSA = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);


        sql = FinAttrSQL.GET_GD_FinnAttr_DATA.replace("PARAM1", financialAttribs.workID);
        financialAttribs.financialDataFromGD = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);

        sql = FinAttrSQL.GET_FinnAttr_ID.replace("PARAM1", financialAttribs.identifier +
                financialAttribs.financialDataFromStg.get(0).opco +
                financialAttribs.financialDataFromStg.get(0).resp_centre
                );
        Log.info(sql);
        financialAttribs.financialID = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
        Log.info(financialAttribs.financialID.get(0).fin_attribs_id);

    }

    @Then("^The data between DQ and SA is identical$")
    public void checkFinancialData(){

        Assert.assertEquals("The classname is incorrect for id="+financialAttribs.id,
                "WorkFinancialAttributes", financialAttribs.financialDataFromSA.get(0).B_CLASSNAME);

        if (financialAttribs.financialID.get(0).fin_attribs_id!=null
                || financialAttribs.financialDataFromSA.get(0).fin_attribs_id !=null) {
            assertTrue("Expecting the financial attribute id details from STG and SA Consistent for id="+ financialAttribs.identifier,
                    financialAttribs.financialID.get(0).fin_attribs_id
                            .equals(financialAttribs.financialDataFromSA.get(0).fin_attribs_id ));
        }

        if (financialAttribs.financialDataFromStg.get(0).opco!=null
                || financialAttribs.financialDataFromSA.get(0).gl_company !=null) {
            assertTrue("Expecting the OPCO details from STG and SA Consistent for id="+ financialAttribs.identifier,
                    financialAttribs.financialDataFromStg.get(0).opco
                            .equals(financialAttribs.financialDataFromSA.get(0).gl_company ));
        }

        if (financialAttribs.financialDataFromStg.get(0).resp_centre!=null
                || financialAttribs.financialDataFromSA.get(0).cost_resp_centre !=null) {
            assertTrue("Expecting the cost resp centre details from STG and SA Consistent for id="+ financialAttribs.identifier,
                    financialAttribs.financialDataFromStg.get(0).resp_centre
                            .equals(financialAttribs.financialDataFromSA.get(0).cost_resp_centre ));
        }

        if (financialAttribs.financialDataFromStg.get(0).resp_centre!=null
                || financialAttribs.financialDataFromSA.get(0).revenue_resp_centre !=null) {
            assertTrue("Expecting the revenue resp centre details from STG and SA Consistent for id="+ financialAttribs.identifier,
                    financialAttribs.financialDataFromStg.get(0).resp_centre
                            .equals(financialAttribs.financialDataFromSA.get(0).revenue_resp_centre ));
        }

    }

    @And("^The data between SA and GD is identical$")
    public void checkFinancialDataGD(){

        Assert.assertEquals("The classname is incorrect for id="+financialAttribs.id,
                "WorkFinancialAttributes", financialAttribs.financialDataFromGD.get(0).B_CLASSNAME);

        if (financialAttribs.financialDataFromSA.get(0).fin_attribs_id!=null
                || financialAttribs.financialDataFromGD.get(0).fin_attribs_id !=null) {
            assertTrue("Expecting the financial attribute id details from STG and SA Consistent for id="+ financialAttribs.identifier,
                    financialAttribs.financialDataFromSA.get(0).fin_attribs_id
                            .equals(financialAttribs.financialDataFromGD.get(0).fin_attribs_id ));
        }

        if (financialAttribs.financialDataFromSA.get(0).gl_company!=null
                || financialAttribs.financialDataFromGD.get(0).gl_company !=null) {
            assertTrue("Expecting the OPCO details from STG and SA Consistent for id="+ financialAttribs.identifier,
                    financialAttribs.financialDataFromSA.get(0).gl_company
                            .equals(financialAttribs.financialDataFromGD.get(0).gl_company ));
        }

        if (financialAttribs.financialDataFromSA.get(0).cost_resp_centre!=null
                || financialAttribs.financialDataFromGD.get(0).cost_resp_centre !=null) {
            assertTrue("Expecting the cost resp centre details from STG and SA Consistent for id="+ financialAttribs.identifier,
                    financialAttribs.financialDataFromSA.get(0).cost_resp_centre
                            .equals(financialAttribs.financialDataFromGD.get(0).cost_resp_centre ));
        }

        if (financialAttribs.financialDataFromSA.get(0).revenue_resp_centre!=null
                || financialAttribs.financialDataFromGD.get(0).revenue_resp_centre !=null) {
            assertTrue("Expecting the revenue resp centre details from STG and SA Consistent for id="+ financialAttribs.identifier,
                    financialAttribs.financialDataFromSA.get(0).revenue_resp_centre
                            .equals(financialAttribs.financialDataFromGD.get(0).revenue_resp_centre ));
        }

    }
}
