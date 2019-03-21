package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.WorksIdentifierSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import com.eph.automation.testing.helper.Log;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class WorksIdSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    public String sql;
    private static List<WorkDataObject> data;
    private static List<WorkDataObject> dataFromSTG;
    private static List<WorkDataObject> dataFromSA;
    private static List<WorkDataObject> dataFromSAId;
    private static List<WorkDataObject> dataFromSAFtype;
    private static List<WorkDataObject> dataFromGDFtype;
    private static List<WorkDataObject> dataFromGDId;

    @Given("^We have a work from type (.*) to check by (.*)$")
    public void getProductNum(String type, String id){
        sql = WorksIdentifierSQL.getRandomProductNum
                .replace("PARAM1", id)
                .replace("PARAM2", type);
        data = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        System.out.print(sql);
        dataQualityContext.productIdFromStg = data.get(0).random_value;
        Log.info("\n The product number is " + dataQualityContext.productIdFromStg);
    }

    @When("^We get the data from Staging, SA and Work Identifiers using (.*)")
    public void getIdentifiers(String id){
        sql = WorksIdentifierSQL.getIdentifiers
                .replace("PARAM1",id)
                .replace("PARAM2", dataQualityContext.productIdFromStg);
        dataFromSTG = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);


        sql = WorksIdentifierSQL.getEphWorkID
                .replace("PARAM1", dataFromSTG.get(0).PRODUCT_WORK_ID);
        dataFromSA = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

        sql = WorksIdentifierSQL.getIdentifierDataFromSA
                .replace("PARAM1", dataFromSA.get(0).WORK_ID);
        dataFromSAId = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

        sql = WorksIdentifierSQL.getIdentifierDataFromGD
                .replace("PARAM1", dataFromSA.get(0).WORK_ID);
        dataFromGDId = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    @Then("^All of the identifiers are stored")
    public void checkIdCount(){
        ArrayList<String> dataFromSTGCount = new ArrayList<String>();

        if (dataFromSTG.get(0).JOURNAL_NUMBER!=null){
            dataFromSTGCount.add(dataFromSTG.get(0).JOURNAL_NUMBER);
        }
        if (dataFromSTG.get(0).ISSN_L!=null){
            dataFromSTGCount.add(dataFromSTG.get(0).ISSN_L);
        }
        if (dataFromSTG.get(0).JOURNAL_ACRONYM!=null){
            dataFromSTGCount.add(dataFromSTG.get(0).JOURNAL_ACRONYM);
        }
        if (dataFromSTG.get(0).DAC_KEY!=null){
            dataFromSTGCount.add(dataFromSTG.get(0).DAC_KEY);
        }
        if (dataFromSTG.get(0).PROJECT_NUM!=null){
            dataFromSTGCount.add(dataFromSTG.get(0).PROJECT_NUM);
        }

        Log.info("\nIdentifiers are "+dataFromSTGCount.size());
        Log.info("\nIdentifiers are "+dataFromSAId.size());

        Assert.assertEquals("There are missing identifiers", dataFromSTGCount.size(),dataFromSAId.size());
    }

    @And("^The identifiers data is correct$")
    public void checkIdData(){

        Assert.assertTrue("Load ID is empty!", dataFromSAId.get(0).B_LOADID != null);

        Assert.assertTrue("F_EVENT is empty!", dataFromSAId.get(0).F_EVENT != null);

        Assert.assertEquals("The classname is incorrect for id="+dataQualityContext.productIdFromStg,
                "WorkIdentifier", dataFromSAId.get(0).B_CLASSNAME);

            if (dataFromSTG.get(0).JOURNAL_NUMBER != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "ELSEVIER JOURNAL NUMBER");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Assert.assertEquals("The JOURNAL_NUMBER is incorrect for id="+dataQualityContext.productIdFromStg,
                        "ELSEVIER JOURNAL NUMBER", dataFromSAFtype.get(0).F_TYPE);
                Log.info("Journal number is correct");
            }
            if (dataFromSTG.get(0).ISSN_L != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "ISSN-L");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Assert.assertEquals("The ISSN_L is incorrect for id="+dataQualityContext.productIdFromStg,
                        "ISSN-L", dataFromSAFtype.get(0).F_TYPE);
                Log.info("ISSN_L is correct");
            }
            if (dataFromSTG.get(0).JOURNAL_ACRONYM != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "JOURNAL ACRONYM");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Assert.assertEquals("The JOURNAL_ACRONYM is incorrect for id="+dataQualityContext.productIdFromStg,
                        "JOURNAL ACRONYM", dataFromSAFtype.get(0).F_TYPE);
                Log.info("Journal acronym is correct");
            }
            if (dataFromSTG.get(0).DAC_KEY != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "DAC-K");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Assert.assertEquals("The DAC_KEY is incorrect for id="+dataQualityContext.productIdFromStg,
                        "DAC-K", dataFromSAFtype.get(0).F_TYPE);
                Log.info("DAC_KEY is correct");
            }
            if (dataFromSTG.get(0).PROJECT_NUM != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "PROJECT-NUM");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Assert.assertEquals("The PROJECT_NUM is incorrect for id="+dataQualityContext.productIdFromStg,
                        "PROJECT-NUM", dataFromSAFtype.get(0).F_TYPE);
                Log.info("PROJECT_NUM is correct");
            }

    }

    @And("^The identifiers data between SA and GD is identical$")
    public void checkIdDataSAGD(){
        //Assert.assertEquals("There are missing identifiers", dataFromGDId.size(),dataFromSAId.size());


        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataFromSAId.get(0).F_EVENT
                        .equals(dataFromGDId.get(0).F_EVENT));
        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataFromSAId.get(0).B_CLASSNAME
                        .equals(dataFromGDId.get(0).B_CLASSNAME));

        if (dataFromSTG.get(0).JOURNAL_NUMBER!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","ELSEVIER JOURNAL NUMBER");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The Journal Number is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "ELSEVIER JOURNAL NUMBER",dataFromGDFtype.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).ISSN_L!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","ISSN-L");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The ISSN_L is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "ISSN-L",dataFromGDFtype.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).JOURNAL_ACRONYM!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","JOURNAL ACRONYM");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The JOURNAL_ACRONYM is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "JOURNAL ACRONYM",dataFromGDFtype.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).DAC_KEY!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","DAC-K");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The DAC_KEY is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "DAC-K",dataFromGDFtype.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).PROJECT_NUM!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","PROJECT-NUM");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
            Assert.assertEquals("The PROJECT_NUM is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "PROJECT-NUM",dataFromGDFtype.get(0).F_TYPE);
        }
    }
}
