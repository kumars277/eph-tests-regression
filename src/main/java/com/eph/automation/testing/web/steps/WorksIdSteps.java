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
    private static List<ProductDataObject> dataFromGDId;

    @Given("^We have a work from type (.*) to check by (.*)$")
    public void getProductNum(String type, String id){
        sql = WorksIdentifierSQL.getRandomProductNum
                .replace("PARAM1", id)
                .replace("PARAM2", type);
        System.out.println(sql+"\n");
        data = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);
        dataQualityContext.productIdFromStg = data.get(0).random_value;
        System.out.println("\n The product number is " + dataQualityContext.productIdFromStg);
    }

    @When("^We get the data from Staging, SA and Work Identifiers using (.*)")
    public void getIdentifiers(String id){
        sql = WorksIdentifierSQL.getIdentifiers
                .replace("PARAM1",id)
                .replace("PARAM2", dataQualityContext.productIdFromStg);
        System.out.println(sql+"\n");
        dataFromSTG = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);


        sql = WorksIdentifierSQL.getEphWorkID
                .replace("PARAM1", dataQualityContext.productIdFromStg);
        dataFromSA = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);

        sql = WorksIdentifierSQL.getIdentifierDataFromSA
                .replace("PARAM1", dataFromSA.get(0).WORK_ID);
                //.replace("PARAM1", "EPR-W-1000SX");
        System.out.println(sql+"\n");
        dataFromSAId = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_SIT_URL);

        sql = WorksIdentifierSQL.getIdentifierDataFromGD
                .replace("PARAM1", dataFromSA.get(0).WORK_ID);
                //.replace("PARAM1", "EPR-W-1000SX");
        System.out.println(sql+"\n");
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

        System.out.println("Identifiers are "+dataFromSTGCount.size());
        System.out.println("Identifiers are "+dataFromSAId.size());

        Assert.assertEquals("There are missing identifiers", dataFromSTGCount.size(),dataFromSAId.size());
    }

    @And("^The identifiers data is correct$")
    public void checkIdData(){

        Assert.assertTrue("Load ID is empty!", dataFromSAId.get(0).B_LOADID != null);

        Assert.assertTrue("F_EVENT is empty!", dataFromSAId.get(0).F_EVENT != null);

        Assert.assertEquals("The classname is incorrect!","WorkIdentifier", dataFromSAId.get(0).B_CLASSNAME);

        if (dataFromSTG.get(0).JOURNAL_NUMBER!=null){
            Assert.assertEquals("The f_type is incorrect!","ELSEVIER JOURNAL NUMBER",dataFromSAId.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).ISSN_L!=null){
            Assert.assertEquals("The f_type is incorrect!","ISSN_L",dataFromSAId.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).JOURNAL_ACRONYM!=null){
            Assert.assertEquals("The f_type is incorrect!","JOURNAL ACRONYM",dataFromSAId.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).DAC_KEY!=null){
            Assert.assertEquals("The f_type is incorrect!","DAC-K",dataFromSAId.get(0).F_TYPE);
        }
        if (dataFromSTG.get(0).PROJECT_NUM!=null){
            Assert.assertEquals("The f_type is incorrect!","PROJECT-NUM",dataFromSAId.get(0).F_TYPE);
        }
    }

    @And("^The identifiers data between SA and GD is identical$")
    public void checkIdDataSAGD(){
        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataFromSAId.get(0).F_EVENT
                        .equals(dataFromGDId.get(0).F_EVENT));
        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataFromSAId.get(0).B_CLASSNAME
                        .equals(dataFromGDId.get(0).B_CLASSNAME));
        if (dataFromSAId.get(0).F_TYPE!=null) {
            assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                    dataFromSAId.get(0).F_TYPE
                            .equals(dataFromGDId.get(0).F_TYPE));
        }
    }
}
