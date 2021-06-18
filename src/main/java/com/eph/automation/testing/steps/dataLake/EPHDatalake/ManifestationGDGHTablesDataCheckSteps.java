package com.eph.automation.testing.steps.dataLake.EPHDatalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityEPHDLContext;
import com.eph.automation.testing.models.dao.EPHDataLake.ManifestationDataDLObject;
import com.eph.automation.testing.services.db.EPHDataLakeSql.GetDLDBUser;
import com.eph.automation.testing.services.db.EPHDataLakeSql.ManifestationGDGHTablesDataChecksSQL;
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


public class ManifestationGDGHTablesDataCheckSteps {

    @StaticInjection
    public DataQualityEPHDLContext dataQualityEPHDLContext;
    private static String sql;
    private static List<String> Ids;
    private ManifestationGDGHTablesDataChecksSQL manifObj = new ManifestationGDGHTablesDataChecksSQL();


    @Given("^We get (.*) random manifestation ids of (.*)")
    public void getRandomManifestationIds(String numberOfRecords, String tableName) {
        // numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random record...");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);

        switch (tableName){
            case "gd_manifestation":
                sql = String.format(ManifestationGDGHTablesDataChecksSQL.GET_RANDOM_MANIFESTATION_ID, numberOfRecords);
                List<Map<?, ?>> randomManifestationIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManifestationIds.stream().map(m -> (String) m.get("MANIFESTATION_ID")).collect(Collectors.toList());
                break;
            case "gh_manifestation":
                sql = String.format(ManifestationGDGHTablesDataChecksSQL.GET_RANDOM_MANIFESTATION_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomManifestationGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManifestationGHIds.stream().map(m -> (String) m.get("MANIFESTATION_ID")).collect(Collectors.toList());
                break;
            case "gh_manifestation_identifier":
                sql = String.format(ManifestationGDGHTablesDataChecksSQL.GET_RANDOM_MANI_IDENTIFIER_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomManiIdentifierGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManiIdentifierGHIds.stream().map(m -> (BigDecimal) m.get("MANIF_IDENTIFIER_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_manifestation_identifier":
                sql = String.format(ManifestationGDGHTablesDataChecksSQL.GET_RANDOM_MANI_IDENTIFIER_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomManiIdentifierIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManiIdentifierIds.stream().map(m -> (BigDecimal) m.get("MANIF_IDENTIFIER_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the manifestation records from EPH of (.*)$")
    public void getManifestationEPH(String tableName) {
        Log.info("We get the manifestation records from EPH..");
        sql = String.format(manifObj.manifestationBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the manifestation records from DL of (.*)$")
    public void getManifestationDL(String tableName) {
        Log.info("We get the manifestation records from DL..");
        sql = String.format(manifObj.manifestationBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbManifestationDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare manifestation records in EPH and DL of (.*)$")
    public void compareGDManifestationDataEPHtoDL(String tableName) {
        if(dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the gd manifestation records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataDLObject::getMANIFESTATION_ID));
                dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.sort(Comparator.comparing(ManifestationDataDLObject::getMANIFESTATION_ID));

                if(tableName.equals("gh_manifestation")){
                    dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.sort(Comparator.comparing(ManifestationDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The MANIFESTATION_ID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_ID());

                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_manifestation")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }


                    Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " B_UPDATE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " S_MANIFESTATION_ID => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_ID()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_ID());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_ID() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_ID()!= null)) {
                    Assert.assertEquals("The S_MANIFESTATION_ID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_ID());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " MANIFESTATION_KEY_TITLE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_KEY_TITLE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_KEY_TITLE()!= null)) {
                    Assert.assertEquals("The MANIFESTATION_KEY_TITLE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_KEY_TITLE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIFESTATION_KEY_TITLE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " S_MANIFESTATION_KEY_TITLE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_KEY_TITLE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_KEY_TITLE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_KEY_TITLE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_KEY_TITLE()!= null)) {
                    Assert.assertEquals("The S_MANIFESTATION_KEY_TITLE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_MANIFESTATION_KEY_TITLE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_MANIFESTATION_KEY_TITLE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " INTER_EDITION_FLAG => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getINTER_EDITION_FLAG()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getINTER_EDITION_FLAG());

                //Converting Boolean to String
                String inter_edition_indicator_eph = String.valueOf(dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getINTER_EDITION_FLAG());
                String inter_edition_indicator_dl = String.valueOf(dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getINTER_EDITION_FLAG());
                if (inter_edition_indicator_eph != null || (inter_edition_indicator_dl!= null)) {
                    Assert.assertEquals("The INTER_EDITION_FLAG is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            inter_edition_indicator_eph, inter_edition_indicator_dl);
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " FIRST_PUB_DATE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getFIRST_PUB_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getFIRST_PUB_DATE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getFIRST_PUB_DATE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getFIRST_PUB_DATE()!= null)) {
                    Assert.assertEquals("The FIRST_PUB_DATE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getFIRST_PUB_DATE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getFIRST_PUB_DATE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " LAST_PUB_DATE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getLAST_PUB_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getLAST_PUB_DATE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getLAST_PUB_DATE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getLAST_PUB_DATE()!= null)) {
                    Assert.assertEquals("The LAST_PUB_DATE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getLAST_PUB_DATE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getLAST_PUB_DATE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " T_EVENT_DESCRIPTION => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION()!= null)) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " F_TYPE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE()!= null)) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " F_STATUS => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_STATUS()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_STATUS());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_STATUS() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_STATUS()!= null)) {
                    Assert.assertEquals("The F_STATUS is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_STATUS(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_STATUS());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " F_FORMAT_TYPE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_FORMAT_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_FORMAT_TYPE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_FORMAT_TYPE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_FORMAT_TYPE()!= null)) {
                    Assert.assertEquals("The F_FORMAT_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_FORMAT_TYPE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_FORMAT_TYPE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " F_WWORK => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_WWORK()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_WWORK());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_WWORK()!= null)) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_WWORK());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " F_T_EVENT_TYPE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_T_EVENT_TYPE()!= null)) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT());

                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID()+
                        " F_SELF_ONE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_SELF_ONE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_SELF_ONE());


                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_SELF_ONE()!= null)) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_SELF_ONE());
                }
            }
        }
    }

    @When("^We get the manifestation identifier records from EPH of (.*)$")
    public void getManiIdentifierEPH(String tableName) {
        Log.info("We get the manifestation identifier records from EPH..");
        sql = String.format(manifObj.manifIdentifierBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.EPH_URL);
    }

    @Then ("^We get the manifestation identifier records from DL of (.*)$")
    public void getManiIdentifierDL(String tableName) {
        Log.info("We get the manifestation identifier records from DL..");
        sql = String.format(manifObj.manifIdentifierBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbManifestationDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ManifestationDataDLObject.class, Constants.AWS_URL);
    }

    @And ("^Compare manifestation identifier records in EPH and DL of (.*)$")
    public void compareManifIdentifierDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the gd manifestation identifier records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataDLObject::getMANIF_IDENTIFIER_ID));
                dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.sort(Comparator.comparing(ManifestationDataDLObject::getMANIF_IDENTIFIER_ID));

                if(tableName.equals("gh_manifestation_identifier")){
                    dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.sort(Comparator.comparing(ManifestationDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.sort(Comparator.comparing(ManifestationDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIF_IDENTIFIER_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The MANIF_IDENTIFIER_ID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getMANIF_IDENTIFIER_ID());

                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gd_manifestation_identifier")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " B_UPDDATE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " IDENTIFIER => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getIDENTIFIER()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getIDENTIFIER());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getIDENTIFIER() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getIDENTIFIER()!= null)) {
                    Assert.assertEquals("The IDENTIFIER is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getIDENTIFIER(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getIDENTIFIER());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " S_IDENTIFIER => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_IDENTIFIER()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_IDENTIFIER());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_IDENTIFIER() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_IDENTIFIER()!= null)) {
                    Assert.assertEquals("The S_IDENTIFIER is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getS_IDENTIFIER(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getS_IDENTIFIER());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " EFFECTIVE_START_DATE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " EFFECTIVE_END_DATE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " F_TYPE => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE()!= null)) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_TYPE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " F_MANIFESTATION => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_MANIFESTATION()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_MANIFESTATION());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_MANIFESTATION() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_MANIFESTATION()!= null)) {
                    Assert.assertEquals("The F_MANIFESTATION is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_MANIFESTATION(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_MANIFESTATION());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIF_IDENTIFIER_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getMANIFESTATION_ID(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbManifestationDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

}
