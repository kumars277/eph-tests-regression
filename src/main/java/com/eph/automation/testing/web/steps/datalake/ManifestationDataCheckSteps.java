package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.ManifestationDataDLObject;
import com.eph.automation.testing.services.db.DataLakeSql.ManifestationDataChecksSQL;
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


public class ManifestationDataCheckSteps {

    @StaticInjection
    public DataQualityDLContext dataQualityDLContext;
    private static String sql;
    private static List<String> Ids;


    @Given("^We get (.*) random manifestation ids of (.*)")
    public void getRandomManifestationIds(String numberOfRecords, String tableName) {
        Log.info("Get random records ..");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);
        if(tableName.contentEquals("gd_manifestation")){
            sql = String.format(ManifestationDataChecksSQL.GET_RANDOM_MANIFESTATION_ID, numberOfRecords);
            Log.info(sql);
            List<Map<?, ?>> randomManifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Ids = randomManifestationIds.stream().map(m -> (String) m.get("MANIFESTATION_ID")).collect(Collectors.toList());
        }else if(tableName.contentEquals("gh_manifestation")){
            sql = String.format(ManifestationDataChecksSQL.GET_RANDOM_MANIFESTATION_ID_GH, numberOfRecords);
            Log.info(sql);
            List<Map<?, ?>> randomManifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Ids = randomManifestationIds.stream().map(m -> (String) m.get("MANIFESTATION_ID")).collect(Collectors.toList());
        }else if(tableName.contentEquals("gh_manifestation_identifier")){
            sql = String.format(ManifestationDataChecksSQL.GET_RANDOM_MANI_IDENTIFIER_ID_GH, numberOfRecords);
            Log.info(sql);
            List<Map<?, ?>> randomManiIdentifierIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Ids = randomManiIdentifierIds.stream().map(m -> (BigDecimal) m.get("MANIF_IDENTIFIER_ID")).map(String::valueOf).collect(Collectors.toList());
        }else if(tableName.contentEquals("gd_manifestation_identifier")){
        sql = String.format(ManifestationDataChecksSQL.GET_RANDOM_MANI_IDENTIFIER_ID_GD, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomManiIdentifierIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        Ids = randomManiIdentifierIds.stream().map(m -> (BigDecimal) m.get("MANIF_IDENTIFIER_ID")).map(String::valueOf).collect(Collectors.toList());
    }


        Log.info(Ids.toString());
    }

    @When("^We get the gd manifestation records from EPH$")
    public void getManifestationEPH() {
        Log.info("We get the manifestation records from EPH..");
        sql = String.format(ManifestationDataChecksSQL.GET_DATA_MANIFESTATION_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbManifestationDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the gd manifestation records from DL$")
    public void getManifestationDL() {
        Log.info("We get the manifestation records from DL..");
        sql = String.format(ManifestationDataChecksSQL.GET_DATA_MANIFESTATION_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbManifestationDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare gd manifestation records in EPH and DL$")
    public void compareGDManifestationDataEPHtoDL() {
        if(dataQualityDLContext.tbManifestationDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the manifestation records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbManifestationDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbManifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataDLObject::getMANIFESTATION_ID));
                dataQualityDLContext.tbManifestationDataObjectsFromDL.sort(Comparator.comparing(ManifestationDataDLObject::getMANIFESTATION_ID));

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The MANIFESTATION_ID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_ID());

                }
                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                    Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID());
                }
                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_ID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_ID().equals("null"))) {
                    Assert.assertEquals("The S_MANIFESTATION_ID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_ID());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_KEY_TITLE().equals("null"))) {
                    Assert.assertEquals("The MANIFESTATION_KEY_TITLE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_KEY_TITLE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_KEY_TITLE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_KEY_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_MANIFESTATION_KEY_TITLE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_KEY_TITLE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_KEY_TITLE());
                }


                //Converting Boolean to String
                String inter_edition_indicator_eph = String.valueOf(dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getINTER_EDITION_FLAG());
                String inter_edition_indicator_dl = String.valueOf(dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getINTER_EDITION_FLAG());
                if (inter_edition_indicator_eph != null || (!inter_edition_indicator_dl.equals("null"))) {
                    Assert.assertEquals("The INTER_EDITION_FLAG is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            inter_edition_indicator_eph, inter_edition_indicator_dl);
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getFIRST_PUB_DATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getFIRST_PUB_DATE().equals("null"))) {
                    Assert.assertEquals("The FIRST_PUB_DATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getFIRST_PUB_DATE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getFIRST_PUB_DATE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getLAST_PUB_DATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getLAST_PUB_DATE().equals("null"))) {
                    Assert.assertEquals("The LAST_PUB_DATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getLAST_PUB_DATE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getLAST_PUB_DATE());
                }

                 if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_STATUS() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_STATUS().equals("null"))) {
                    Assert.assertEquals("The F_STATUS is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_STATUS(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_STATUS());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_FORMAT_TYPE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_FORMAT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_FORMAT_TYPE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_FORMAT_TYPE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_FORMAT_TYPE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_WWORK().equals("null"))) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_WWORK());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_T_EVENT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT());

                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_SELF_ONE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_SELF_ONE());
                }

            }}

    }
    @When("^We get the gh manifestation records from EPH$")
    public void getGHManifestationEPH() {
        Log.info("We get the work records from EPH..");
        sql = String.format(ManifestationDataChecksSQL.GET_DATA_GH_MANIFESTATION_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbManifestationDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the gh manifestation records from DL$")
    public void getGHManifestationDL() {
        Log.info("We get the work records from DL..");
        sql = String.format(ManifestationDataChecksSQL.GET_DATA_GH_MANIFESTATION_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbManifestationDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare gh manifestation records in EPH and DL$")
    public void compareGHManifestationDataEPHtoDL() {
        if (dataQualityDLContext.tbManifestationDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the manifestation records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbManifestationDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbManifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataDLObject::getMANIFESTATION_ID));
                dataQualityDLContext.tbManifestationDataObjectsFromDL.sort(Comparator.comparing(ManifestationDataDLObject::getMANIFESTATION_ID));

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The MANIFESTATION_ID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_ID());

                }
                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                    Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                    Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_ID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_ID().equals("null"))) {
                    Assert.assertEquals("The S_MANIFESTATION_ID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_ID());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_KEY_TITLE().equals("null"))) {
                    Assert.assertEquals("The MANIFESTATION_KEY_TITLE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_KEY_TITLE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_KEY_TITLE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_KEY_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_MANIFESTATION_KEY_TITLE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_KEY_TITLE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_KEY_TITLE());
                }


                //Converting Boolean to String
                String inter_edition_indicator_eph = String.valueOf(dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getINTER_EDITION_FLAG());
                String inter_edition_indicator_dl = String.valueOf(dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getINTER_EDITION_FLAG());
                if (inter_edition_indicator_eph != null || (!inter_edition_indicator_dl.equals("null"))) {
                    Assert.assertEquals("The INTER_EDITION_FLAG is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            inter_edition_indicator_eph, inter_edition_indicator_dl);
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getFIRST_PUB_DATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getFIRST_PUB_DATE().equals("null"))) {
                    Assert.assertEquals("The FIRST_PUB_DATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getFIRST_PUB_DATE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getFIRST_PUB_DATE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getLAST_PUB_DATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getLAST_PUB_DATE().equals("null"))) {
                    Assert.assertEquals("The LAST_PUB_DATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getLAST_PUB_DATE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getLAST_PUB_DATE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_STATUS() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_STATUS().equals("null"))) {
                    Assert.assertEquals("The F_STATUS is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_STATUS(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_STATUS());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_FORMAT_TYPE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_FORMAT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_FORMAT_TYPE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_FORMAT_TYPE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_FORMAT_TYPE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_WWORK().equals("null"))) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_WWORK());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_T_EVENT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT());

                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_SELF_ONE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_SELF_ONE());
                }

            }
        }

    }

    @When("^We get the gh manifestation identifier records from EPH$")
    public void getGHManiIdentifierEPH() {
        Log.info("We get the manifestation identifier records from EPH..");
        sql = String.format(ManifestationDataChecksSQL.GET_DATA_GH_MANI_IDENTIFIER_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbManifestationDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.EPH_URL);
    }

    @Then ("^We get the gh manifestation identifier records from DL$")
    public void getGHManiIdentifierDL() {
        Log.info("We get the manifestation identifier records from DL..");
        sql = String.format(ManifestationDataChecksSQL.GET_DATA_GH_MANI_IDENTIFIER_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbManifestationDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.AWS_URL);
    }

    @And ("^Compare gh manifestation identifier records in EPH and DL$")
    public void compareGHManifIdentifierDataEPHtoDL() {
        if (dataQualityDLContext.tbManifestationDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the manifestation identifier records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbManifestationDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbManifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataDLObject::getMANIF_IDENTIFIER_ID));
                dataQualityDLContext.tbManifestationDataObjectsFromDL.sort(Comparator.comparing(ManifestationDataDLObject::getMANIF_IDENTIFIER_ID));

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIF_IDENTIFIER_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The MANIF_IDENTIFIER_ID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIF_IDENTIFIER_ID());

                }
                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                    Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                    Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getIDENTIFIER() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getIDENTIFIER().equals("null"))) {
                    Assert.assertEquals("The IDENTIFIER is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getIDENTIFIER(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getIDENTIFIER());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_IDENTIFIER() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_IDENTIFIER().equals("null"))) {
                    Assert.assertEquals("The S_IDENTIFIER is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_IDENTIFIER(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_IDENTIFIER());
                }


                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_MANIFESTATION() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_MANIFESTATION().equals("null"))) {
                    Assert.assertEquals("The F_MANIFESTATION is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_MANIFESTATION(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_MANIFESTATION());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT());

                }
            }
        }

    }

    @When("^We get the gd manifestation identifier records from EPH$")
    public void getManiIdentifierEPH() {
        Log.info("We get the manifestation identifier records from EPH..");
        sql = String.format(ManifestationDataChecksSQL.GET_DATA_GD_MANI_IDENTIFIER_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbManifestationDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.EPH_URL);
    }

    @Then ("^We get the gd manifestation identifier records from DL$")
    public void getManiIdentifierDL() {
        Log.info("We get the manifestation identifier records from DL..");
        sql = String.format(ManifestationDataChecksSQL.GET_DATA_GD_MANI_IDENTIFIER_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbManifestationDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.AWS_URL);
    }

    @And ("^Compare gd manifestation identifier records in EPH and DL$")
    public void compareManifIdentifierDataEPHtoDL() {
        if (dataQualityDLContext.tbManifestationDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the manifestation identifier records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbManifestationDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbManifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataDLObject::getMANIF_IDENTIFIER_ID));
                dataQualityDLContext.tbManifestationDataObjectsFromDL.sort(Comparator.comparing(ManifestationDataDLObject::getMANIF_IDENTIFIER_ID));

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIF_IDENTIFIER_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The MANIF_IDENTIFIER_ID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIF_IDENTIFIER_ID());

                }
                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                    Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID());
                }


                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getIDENTIFIER() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getIDENTIFIER().equals("null"))) {
                    Assert.assertEquals("The IDENTIFIER is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getIDENTIFIER(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getIDENTIFIER());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_IDENTIFIER() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_IDENTIFIER().equals("null"))) {
                    Assert.assertEquals("The S_IDENTIFIER is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_IDENTIFIER(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getS_IDENTIFIER());
                }


                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_MANIFESTATION() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_MANIFESTATION().equals("null"))) {
                    Assert.assertEquals("The F_MANIFESTATION is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_MANIFESTATION(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_MANIFESTATION());
                }

                if (dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT());

                }
            }
        }

    }



}
