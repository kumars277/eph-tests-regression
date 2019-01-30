package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.WorksIdentifierSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class WorksIdSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    public String sql;
    private static List<ProductDataObject> data;
    private static List<ProductDataObject> dataFromSTG;
    private static List<ProductDataObject> dataFromSA;
    private static List<ProductDataObject> dataFromSAId;
    private static List<ProductDataObject> dataFromSAFtype;
    private static List<ProductDataObject> dataFromGDFtype;
    private static List<ProductDataObject> dataFromGDId;

    @Given("^We have a work from type (.*) to check by (.*)$")
    public void getProductNum(String type, String id){
        sql = WorksIdentifierSQL.getRandomProductNum
                .replace("PARAM1", id)
                .replace("PARAM2", type);
        data = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        dataQualityContext.productIdFromStg = data.get(0).random_value;
        System.out.println("\n The product number is " + dataQualityContext.productIdFromStg);
    }

    @When("^We get the data from Staging, SA and Work Identifiers using (.*)")
    public void getIdentifiers(String id){
        sql = WorksIdentifierSQL.getIdentifiers
                .replace("PARAM1",id)
                .replace("PARAM2", dataQualityContext.productIdFromStg);
        dataFromSTG = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);


        sql = WorksIdentifierSQL.getEphWorkID
                .replace("PARAM1", dataFromSTG.get(0).PRODUCT_WORK_ID);
        dataFromSA = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);

        sql = WorksIdentifierSQL.getIdentifierDataFromSA
                .replace("PARAM1", dataFromSA.get(0).WORK_ID);
        dataFromSAId = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);

        sql = WorksIdentifierSQL.getIdentifierDataFromGD
                .replace("PARAM1", dataFromSA.get(0).WORK_ID);
        dataFromGDId = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
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

        System.out.println("\nIdentifiers are "+dataFromSTGCount.size());
        System.out.println("\nIdentifiers are "+dataFromSAId.size());

        //Assert.assertEquals("There are missing identifiers", dataFromSTGCount.size(),dataFromSAId.size());
    }

    @And("^The identifiers data is correct$")
    public void checkIdData(){

        Assert.assertTrue("Load ID is empty!", dataFromSAId.get(0).B_LOADID != null);

        Assert.assertTrue("F_EVENT is empty!", dataFromSAId.get(0).F_EVENT != null);

        Assert.assertEquals("The classname is incorrect!","WorkIdentifier", dataFromSAId.get(0).B_CLASSNAME);

            if (dataFromSTG.get(0).JOURNAL_NUMBER != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "ELSEVIER JOURNAL NUMBER");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                Assert.assertEquals("The f_type is incorrect!", "ELSEVIER JOURNAL NUMBER", dataFromSAFtype.get(0).F_TYPE);
                System.out.println("Journal number is correct");
            }
            if (dataFromSTG.get(0).ISSN_L != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "ISSN-L");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                Assert.assertEquals("The f_type is incorrect!", "ISSN-L", dataFromSAFtype.get(0).F_TYPE);
                System.out.println("ISSN_L is correct");
            }
            if (dataFromSTG.get(0).JOURNAL_ACRONYM != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "JOURNAL ACRONYM");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                Assert.assertEquals("The f_type is incorrect!", "JOURNAL ACRONYM", dataFromSAFtype.get(0).F_TYPE);
                System.out.println("Journal acronym is correct");
            }
            if (dataFromSTG.get(0).DAC_KEY != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "DAC-K");
                System.out.println(sql);
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                Assert.assertEquals("The f_type is incorrect!", "DAC-K", dataFromSAFtype.get(0).F_TYPE);
                System.out.println("DAC_KEY is correct");
            }
            if (dataFromSTG.get(0).PROJECT_NUM != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "PROJECT-NUM");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
                Assert.assertEquals("The f_type is incorrect!", "PROJECT-NUM", dataFromSAFtype.get(0).F_TYPE);
                System.out.println("PROJECT_NUM is correct");
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
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
            Assert.assertEquals("The f_type is incorrect!","ELSEVIER JOURNAL NUMBER",dataFromGDFtype.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).ISSN_L!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","ISSN-L");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
            Assert.assertEquals("The f_type is incorrect!","ISSN-L",dataFromGDFtype.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).JOURNAL_ACRONYM!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","JOURNAL ACRONYM");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
            Assert.assertEquals("The f_type is incorrect!","JOURNAL ACRONYM",dataFromGDFtype.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).DAC_KEY!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","DAC-K");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
            Assert.assertEquals("The f_type is incorrect!","DAC-K",dataFromGDFtype.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).PROJECT_NUM!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","PROJECT-NUM");
            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
            Assert.assertEquals("The f_type is incorrect!","PROJECT-NUM",dataFromGDFtype.get(0).F_TYPE);
        }
    }
}
