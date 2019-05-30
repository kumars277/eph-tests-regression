package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.eph.automation.testing.services.db.sql.WorksIdentifierSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    private static List<WorkDataObject> stgCount;
    private static List<WorkDataObject> saCount;
    private static List<WorkDataObject> gdCount;
    private static List<WorkDataObject> refreshDate;
    private static List<WorkDataObject> endDatedID;
    private static List<WorkDataObject> pmxSource;
    private static List<WorkDataObject> stgNewID;

    @Given("^We have a work from type (.*) to check$")
    public void getProductNum(String type){
        sql = WorksIdentifierSQL.getRandomProductNum
                .replace("PARAM1", type);
        data = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        System.out.print(sql);
        dataQualityContext.productIdFromStg = data.get(0).random_value;
        Log.info("\n The product number is " + dataQualityContext.productIdFromStg);
    }

    @When("^We get the data from Staging, SA and Work Identifiers")
    public void getIdentifiers(){
        sql = WorksIdentifierSQL.getIdentifiers
                .replace("PARAM1", dataQualityContext.productIdFromStg);
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

       Assert.assertEquals("The number of non-null identifiers don't match", dataFromSTGCount.size(),dataFromSAId.size());
    }

    @And("^The identifiers data is correct$")
    public void checkIdData(){

        Assert.assertTrue("Load ID is empty!", dataFromSAId.get(0).B_LOADID != null);

        Assert.assertTrue("F_EVENT is empty!", dataFromSAId.get(0).F_EVENT != null);

        Assert.assertEquals("The classname is incorrect for id="+dataQualityContext.productIdFromStg,
                "WorkIdentifier", dataFromSAId.get(0).B_CLASSNAME);

        sql = String.format(WorksIdentifierSQL.GET_F_WWORK, dataFromSTG.get(0).PRODUCT_WORK_ID);
        Log.info(sql);

        List<Map<String, Object>> workIdObject = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        String F_WORK  =  ((String) (workIdObject.get(0).get("F_WWORK")));

        Log.info("The Work id in stage is " + dataFromSTG.get(0).PRODUCT_WORK_ID );
        Log.info("The work id in Map table is " + F_WORK);
        Log.info("The Work id in SA is " + dataFromSAId.get(0).F_WWORK );

        Assert.assertEquals("The Work ID's don't match ",F_WORK, dataFromSAId.get(0).F_WWORK);

            if (dataFromSTG.get(0).JOURNAL_NUMBER != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "ELSEVIER JOURNAL NUMBER");

                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Assert.assertEquals("The  type is incorrect for id="+dataQualityContext.productIdFromStg,
                        "ELSEVIER JOURNAL NUMBER", dataFromSAFtype.get(0).F_TYPE);

                Assert.assertEquals("The JOURNAL_NUMBER  is incorrect for id="+dataQualityContext.productIdFromStg,
                        dataFromSTG.get(0).JOURNAL_NUMBER, dataFromSAFtype.get(0).IDENTIFER);

                Log.info("The value in STG is : " + dataFromSTG.get(0).JOURNAL_NUMBER);
                Log.info("The value in SA is : " +  dataFromSAFtype.get(0).IDENTIFER);

                Log.info("Journal number is correct");
            }
            if (dataFromSTG.get(0).ISSN_L != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "ISSN-L");

                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Assert.assertEquals("The type is incorrect for id="+dataQualityContext.productIdFromStg,
                        "ISSN-L", dataFromSAFtype.get(0).F_TYPE);

                Assert.assertEquals("The ISSN_L is incorrect for id="+dataQualityContext.productIdFromStg,
                        dataFromSTG.get(0).ISSN_L, dataFromSAFtype.get(0).IDENTIFER);

                Log.info("The value in STG is : " + dataFromSTG.get(0).ISSN_L);
                Log.info("The value in SA is : " + dataFromSAFtype.get(0).IDENTIFER);

                Log.info("ISSN_L is correct");
            }
            if (dataFromSTG.get(0).JOURNAL_ACRONYM != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "JOURNAL ACRONYM");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

                Assert.assertEquals("The type is incorrect for id="+dataQualityContext.productIdFromStg,
                        "JOURNAL ACRONYM", dataFromSAFtype.get(0).F_TYPE);

                Assert.assertEquals("The JOURNAL_ACRONYM is incorrect for id="+dataQualityContext.productIdFromStg,
                        dataFromSTG.get(0).JOURNAL_ACRONYM, dataFromSAFtype.get(0).IDENTIFER);

                Log.info("The value in STG is : " + dataFromSTG.get(0).JOURNAL_ACRONYM);
                Log.info("The value in SA is : " + dataFromSAFtype.get(0).IDENTIFER);
                Log.info("Journal acronym is correct");
            }
            if (dataFromSTG.get(0).DAC_KEY != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "DAC-K");
                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

                Assert.assertEquals("The type is incorrect for id="+dataQualityContext.productIdFromStg,
                        "DAC-K", dataFromSAFtype.get(0).F_TYPE);

                Assert.assertEquals("The DAC_KEY is incorrect for id="+dataQualityContext.productIdFromStg,
                        dataFromSTG.get(0).DAC_KEY, dataFromSAFtype.get(0).IDENTIFER);

                Log.info("The value in STG is : " + dataFromSTG.get(0).DAC_KEY);
                Log.info("The value in SA is : " + dataFromSAFtype.get(0).IDENTIFER);
                Log.info("DAC_KEY is correct");
            }
            if (dataFromSTG.get(0).PROJECT_NUM != null) {
                sql = WorksIdentifierSQL.getTypeId
                        .replace("PARAM1", dataFromSA.get(0).WORK_ID)
                        .replace("PARAM2", "PROJECT-NUM");

                dataFromSAFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

                Assert.assertEquals("The type is incorrect for id="+dataQualityContext.productIdFromStg,
                        "PROJECT-NUM", dataFromSAFtype.get(0).F_TYPE);

                Assert.assertEquals("The PROJECT_NUM is incorrect for id="+dataQualityContext.productIdFromStg,
                        dataFromSTG.get(0).PROJECT_NUM, dataFromSAFtype.get(0).IDENTIFER);

                Log.info("The value in STG is : " + dataFromSTG.get(0).PROJECT_NUM);
                Log.info("The value in SA is : " + dataFromSAFtype.get(0).IDENTIFER);

                Log.info("PROJECT_NUM is correct");
            }

    }

    @And("^The identifiers data between SA and GD is identical$")
    public void checkIdDataSAGD(){
        Assert.assertEquals("There are missing identifiers", dataFromGDId.size(),dataFromSAId.size());

        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataFromSAId.get(0).F_EVENT
                        .equals(dataFromGDId.get(0).F_EVENT));
        assertTrue("Expecting the Product details from PMX and EPH Consistent ",
                dataFromSAId.get(0).B_CLASSNAME
                        .equals(dataFromGDId.get(0).B_CLASSNAME));

        assertTrue("Expecting the EPH Work ID to be consistent  ",
                dataFromSAId.get(0).F_WWORK.equals(dataFromGDId.get(0).F_WWORK));

        if (dataFromSTG.get(0).JOURNAL_NUMBER!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","ELSEVIER JOURNAL NUMBER");

            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

            Assert.assertEquals("The type is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "ELSEVIER JOURNAL NUMBER",dataFromGDFtype.get(0).F_TYPE);

            Assert.assertEquals("The Journal Number is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    dataFromSTG.get(0).JOURNAL_NUMBER,dataFromGDFtype.get(0).IDENTIFER);

        }
        if (dataFromSTG.get(0).ISSN_L!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","ISSN-L");

            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

            Assert.assertEquals("The type is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "ISSN-L",dataFromGDFtype.get(0).F_TYPE);

            Assert.assertEquals("The ISSN_L is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    dataFromSTG.get(0).ISSN_L,dataFromGDFtype.get(0).IDENTIFER);

        }
        if (dataFromSTG.get(0).JOURNAL_ACRONYM!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","JOURNAL ACRONYM");

            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

            Assert.assertEquals("The type is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "JOURNAL ACRONYM",dataFromGDFtype.get(0).F_TYPE);

            Assert.assertEquals("The JOURNAL_ACRONYM is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    dataFromSTG.get(0).JOURNAL_ACRONYM,dataFromGDFtype.get(0).IDENTIFER);
        }
        if (dataFromSTG.get(0).DAC_KEY!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","DAC-K");

            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

            Assert.assertEquals("The type is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "DAC-K",dataFromGDFtype.get(0).F_TYPE);

            Assert.assertEquals("The DAC_KEY is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    dataFromSTG.get(0).DAC_KEY,dataFromGDFtype.get(0).IDENTIFER);

        }
        if (dataFromSTG.get(0).PROJECT_NUM!=null){
            sql=WorksIdentifierSQL.getTypeIdGD
                    .replace("PARAM1",dataFromSA.get(0).WORK_ID)
                    .replace("PARAM2","PROJECT-NUM");

            dataFromGDFtype = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

            Assert.assertEquals("The type is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    "PROJECT-NUM",dataFromGDFtype.get(0).F_TYPE);

            Assert.assertEquals("The PROJECT_NUM is incorrect for id="+dataFromSA.get(0).WORK_ID,
                    dataFromSTG.get(0).PROJECT_NUM,dataFromGDFtype.get(0).IDENTIFER);

        }
    }

    @Given("^We know the work identifiers count in staging from (.*)$")
    public void getSTGCount(String type){
        if (System.getProperty("LOAD") != null) {
            if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = WorksIdentifierSQL.COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_WORK_TABLE
                        .replace("PARAM1", type);
                System.out.print(sql);
                stgCount = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Log.info("\n The count in stg for " + type + " is " + stgCount.get(0).count);
            }else {
                sql = WorkCountSQL.GET_REFRESH_DATE;
                refreshDate =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                        Constants.EPH_URL);

                sql = WorksIdentifierSQL.COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_WORK_DELTA
                        .replace("PARAM1", type)
                        .replace("PARAM2",refreshDate.get(0).refresh_timestamp);
                System.out.print(sql);
                stgCount = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
                Log.info("\n The count in stg for " + type + " is " + stgCount.get(0).count);
            }
        }else{
            sql = WorksIdentifierSQL.COUNT_OF_RECORDS_WITH_ISBN_IN_EPH_STG_WORK_TABLE
                    .replace("PARAM1", type);
            System.out.print(sql);
            stgCount = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
            Log.info("\n The count in stg for " + type + " is" + stgCount.get(0).count);
        }
    }

    @When("^We get the work identifier count from SA and GD (.*)$")
    public void getSACount(String type){
        sql = WorksIdentifierSQL.COUNT_SA_WORK_IDENTIFIER
                .replace("PARAM1", type);
        System.out.print(sql);
        saCount = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        Log.info("\n The count in SA for " + type + " is " + saCount.get(0).count);

        sql = WorksIdentifierSQL.COUNT_GD_WORK_IDENTIFIER
                .replace("PARAM1", type);
        System.out.print(sql);
        gdCount = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        Log.info("\n The count in GD for " + type + " is " + gdCount.get(0).count);
    }

    @Then("^The counts between staging and SA are matching$")
    public void compareSTGtoSA(){
        Assert.assertEquals("The counts between STG and SA do not match!",
                stgCount.get(0).count,saCount.get(0).count);
    }


    @And("^The counts between SA and GD are matching$")
    public void compareSAtoGD(){
        Assert.assertEquals("The counts between SA and GD do not match!",
                saCount.get(0).count,gdCount.get(0).count);
    }

    @Given("^We have an end-dated identifier in GD$")
    public void getEndDatedID() {
        if (System.getProperty("LOAD") != null) {
            if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                Log.info("There is no delta load performed");
            } else {
                sql = WorksIdentifierSQL.getEndDatedIdentifierDataFromGD;
                endDatedID = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                        Constants.EPH_URL);
                if (endDatedID.isEmpty()){
                    Log.info("No identifiers were updated");
                } else {
                    sql = WorksIdentifierSQL.getPmxSourceRef.replace("PARAM1",
                            endDatedID.get(0).F_WWORK);
                    pmxSource = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                            Constants.EPH_URL);

                }
            }
        }else{
                Log.info("There is no delta load performed");
            }
    }


    @When("^We get the data for the identifier from staging$")
    public void getNewStgData(){
        if (System.getProperty("LOAD") != null) {
            if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                Log.info("There is no delta load performed");
            } else {
                if (endDatedID.isEmpty()){
                    Log.info("No identifiers were updated");
                } else {
                    sql = WorksIdentifierSQL.getIdentifiers.replace("PARAM1",
                            pmxSource.get(0).WORK_ID);
                    stgNewID = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                            Constants.EPH_URL);
                }
            }
        }else{
            Log.info("There is no delta load performed");
        }
    }

    @Then("^There is a change to the work identifier$")
    public void compareNewId(){
        if (System.getProperty("LOAD") != null) {
        if (System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            Log.info("There is no delta load performed");
        } else {
            if (endDatedID.isEmpty()){
                Log.info("No identifiers were updated");
            } else {
                if (endDatedID.get(0).F_TYPE.equalsIgnoreCase("ISSN-L")){
                    Assert.assertNotEquals("The identifiers are the same",endDatedID.get(0).IDENTIFIER,
                            stgNewID.get(0).ISSN_L);
                } else if (endDatedID.get(0).F_TYPE.equalsIgnoreCase("PROJECT-NUM")){
                    Assert.assertNotEquals("The identifiers are the same",endDatedID.get(0).IDENTIFIER,
                            stgNewID.get(0).PROJECT_NUM);
                }else if (endDatedID.get(0).F_TYPE.equalsIgnoreCase("ELSEVIER JOURNAL NUMBER")){
                    Assert.assertNotEquals("The identifiers are the same",endDatedID.get(0).IDENTIFIER,
                            stgNewID.get(0).JOURNAL_NUMBER);
                }else if (endDatedID.get(0).F_TYPE.equalsIgnoreCase("JOURNAL ACRONYM")){
                    Assert.assertNotEquals("The identifiers are the same",endDatedID.get(0).IDENTIFIER,
                            stgNewID.get(0).JOURNAL_ACRONYM);
                } else{
                    Assert.assertNotEquals("The identifiers are the same",endDatedID.get(0).IDENTIFIER,
                            stgNewID.get(0).DAC_KEY);
                }
            }
        }
    }else{
        Log.info("There is no delta load performed");
    }
    }

}
