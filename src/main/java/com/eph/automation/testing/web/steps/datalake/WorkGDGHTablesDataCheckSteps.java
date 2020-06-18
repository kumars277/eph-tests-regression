package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.*;
import com.eph.automation.testing.services.db.DataLakeSql.GetDLDBUser;
import com.eph.automation.testing.services.db.DataLakeSql.WorksGDGHTablesDataChecksSQL;
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


public class WorkGDGHTablesDataCheckSteps {

    @StaticInjection
    public DataQualityDLContext dataQualityDLContext;
    private static String sql;
    private static List<String> Ids;
    public WorksGDGHTablesDataChecksSQL workObj = new WorksGDGHTablesDataChecksSQL();


    @Given("^We get (.*) random work ids of (.*)")
    public void getRandomWorkIds(String numberOfRecords, String tableName) {
        Log.info("Get random records ..");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);
        switch (tableName) {
            case "gd_wwork":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkIds.stream().map(m -> (String) m.get("WORK_ID")).collect(Collectors.toList());
                break;

            case "gh_wwork":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomWorkGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkGHIds.stream().map(m -> (String) m.get("WORK_ID")).collect(Collectors.toList());
                break;

            case "gd_work_financial_attribs":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_FIN_ATTR_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomWorkFinAttrGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkFinAttrGDIds.stream().map(m -> (BigDecimal) m.get("WORK_FIN_ATTRIBS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_work_financial_attribs":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_FIN_ATTR_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomGHWorkFinAttrGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGHWorkFinAttrGDIds.stream().map(m -> (BigDecimal) m.get("WORK_FIN_ATTRIBS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_work_identifier":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_IDENTFIER_ID_GD, numberOfRecords);
                Log.info(sql);
                List<Map<?, ?>> randomWorkIdentifierGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkIdentifierGDIds.stream().map(m -> (BigDecimal) m.get("WORK_IDENTIFIER_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_work_identifier":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_IDENTFIER_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomGHWorkIdentifierGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGHWorkIdentifierGDIds.stream().map(m -> (BigDecimal) m.get("WORK_IDENTIFIER_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_work_relationship":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_RELATIONSHIP_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomRelationIdentifierGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomRelationIdentifierGDIds.stream().map(m -> (BigDecimal) m.get("WORK_RELATIONSHIP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_work_relationship":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_RELATIONSHIP_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomGHRelationWorkGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGHRelationWorkGDIds.stream().map(m -> (BigDecimal) m.get("WORK_RELATIONSHIP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_work_subject_area_link":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_SUBJECT_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomWorkSubjectGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkSubjectGDIds.stream().map(m -> (BigDecimal) m.get("WORK_SUBJECT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_work_subject_area_link":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_SUBJECT_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomWorkSubjectGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkSubjectGHIds.stream().map(m -> (BigDecimal) m.get("WORK_SUBJECT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_work_copyright_owner_link":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_COPYRIGHTS_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomWorkCopyrightGdIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkCopyrightGdIds.stream().map(m -> (BigDecimal) m.get("WORK_COPYRIGHTS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_work_copyright_owner_link":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_COPYRIGHTS_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomWorkCopyrightGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkCopyrightGHIds.stream().map(m -> (BigDecimal) m.get("WORK_COPYRIGHTS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_work_relationship_edition":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_RELATION_EDITION_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomWorkRelEditIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkRelEditIds.stream().map(m -> (BigDecimal) m.get("WORK_COPYRIGHTS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_work_relationship_edition":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_RELATION_EDITION_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomWorkRelEditGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkRelEditGHIds.stream().map(m -> (BigDecimal) m.get("WORK_COPYRIGHTS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the work records from EPH of (.*)$")
    public void getWorksEPH(String tableName) {
        Log.info("We get the work records from EPH..");
        sql = String.format(workObj.workDataBuildSql(Constants.EPH_SCHEMA,tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the work records from DL of (.*)$")
    public void getWorksDL(String tableName) {
        Log.info("We get the work records from DL..");
        sql = String.format(workObj.workDataBuildSql(GetDLDBUser.getDataBase(),tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare work records in EPH and DL of (.*)$")
    public void compareWorkDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the work financial attribs in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_ID));
                if(tableName.equals("gh_wwork")){
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                }
                String workId = dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID();
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_ID is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_ID());

                }
                Log.info("ID => " + workId +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_wwork")){
                    Log.info("ID => " + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID() +
                            " B_TOBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());


                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }


                    Log.info("ID => " + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID() +
                            " B_FROMBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }
                }else{
                    Log.info("ID => " + workId +
                            " B_BATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + workId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }
                Log.info("ID => " + workId +
                        " B_CREDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => " + workId +
                        " B_UPDDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => " + workId +
                        " B_CREATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => " + workId +
                        " B_UPDATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => " + workId +
                        " S_WORK_ID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID().equals("null"))) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID());
                }

                Log.info("ID => " + workId +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => " + workId +
                        " WORK_TITLE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE());
                }

                Log.info("ID => " + workId +
                        " S_WORK_TITLE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE());


                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE());
                }

                Log.info("ID => " + workId +
                        " WORK_SUB_TITLE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_SUB_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE());
                }

                Log.info("ID => " + workId +
                        " S_WORK_SUB_TITLE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_SUB_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE());

                }

                Log.info("ID => " + workId +
                        " WORK_SHORT_TITLE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SHORT_TITLE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SHORT_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SHORT_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SHORT_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_SHORT_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SHORT_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SHORT_TITLE());
                }

                Log.info("ID => " + workId +
                        " S_WORK_SHORT_TITLE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_SHORT_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE());
                }

                //Converting Boolean to String
                String electro_right_indicator_eph = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getELECTRO_RIGHTS_INDICATOR());
                String electro_right_indicator_dl = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getELECTRO_RIGHTS_INDICATOR());
                if(electro_right_indicator_eph.equals("f")){
                    electro_right_indicator_eph = "false";
                }else{
                    electro_right_indicator_eph = "true";
                }

                Log.info("ID => " + workId +
                        " ELECTRO_RIGHTS_INDICATOR => EPH=" + electro_right_indicator_eph +
                        " DL=" + electro_right_indicator_dl);

                if (electro_right_indicator_eph != null || (!electro_right_indicator_dl.equals("null"))) {
                    Assert.assertEquals("The ELECTRO_RIGHTS_INDICATOR is incorrect for id=" + workId,
                            electro_right_indicator_eph, electro_right_indicator_dl);
                }

                Log.info("ID => " + workId +
                        " VOLUME => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME().equals("null"))) {
                    Assert.assertEquals("The VOLUME is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME());
                }

                Log.info("ID => " + workId +
                        " COPYRIGHT_YEAR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR().equals("null"))) {
                    Assert.assertEquals("The COPYRIGHT_YEAR is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR());
                }

                Log.info("ID => " + workId +
                        " EDITION_NUMBER => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER().equals("null"))) {
                    Assert.assertEquals("The EDITION_NUMBER is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER());
                }
                Log.info("ID => " + workId +
                        " T_SUMMARY_CHANGED => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_SUMMARY_CHANGED() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_SUMMARY_CHANGED());

                String summaryChanged_eph = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_SUMMARY_CHANGED());
                String summaryChanged_dl = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_SUMMARY_CHANGED());
                if(summaryChanged_eph.equals("f")){
                    summaryChanged_eph = "false";
                }else{
                    summaryChanged_eph = "true";
                }

                if (summaryChanged_eph != null || (!summaryChanged_dl.equals("null"))) {
                    Assert.assertEquals("The T_SUMMARY_CHANGED is incorrect for id=" + workId,
                            summaryChanged_eph, summaryChanged_dl);
                }

                Log.info("ID => " + workId +
                        " T_EVENT_DESCRIPTION => EPH=" + summaryChanged_eph +
                        " DL=" + summaryChanged_dl);

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                Log.info("ID => " + workId +
                        " F_TYPE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());
                }

                Log.info("ID => " + workId +
                        " F_STATUS => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS().equals("null"))) {
                    Assert.assertEquals("The F_STATUS is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS());
                }

                Log.info("ID => " + workId +
                        " f_accountable_product => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getf_accountable_product() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getf_accountable_product());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getf_accountable_product() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getf_accountable_product().equals("null"))) {
                    Assert.assertEquals("The f_accountable_product is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getf_accountable_product(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getf_accountable_product());
                }

                Log.info("ID => " + workId +
                        " F_PMC => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC().equals("null"))) {
                    Assert.assertEquals("The F_PMC is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC());
                }

                Log.info("ID => " + workId +
                        " F_OA_TYPE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_OA_TYPE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE());
                }


                Log.info("ID => " + workId +
                        " F_FAMILY => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY().equals("null"))) {
                    Assert.assertEquals("The F_FAMILY is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY());

                }

                Log.info("ID => " + workId +
                        " F_IMPRINT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT().equals("null"))) {
                    Assert.assertEquals("The IMPRINT is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT());
                }

                Log.info("ID => " + workId +
                        " F_SOCIETY_OWNERSHIP => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP().equals("null"))) {
                    Assert.assertEquals("The F_SOCIETY_OWNERSHIP is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP());
                }

                Log.info("ID => " + workId +
                        " F_SUBSCRIPTION_TYPE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE().equals("null"))) {
                    Assert.assertEquals("The SUBSCRIPTION_TYPE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE());
                }

                Log.info("ID => " + workId +
                        " F_LLANGUAGE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE().equals("null"))) {
                    Assert.assertEquals("The F_LLANGUAGE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE());
                }

                Log.info("ID => " + workId +
                        " F_T_EVENT_TYPE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                Log.info("ID => " + workId +
                        " F_EVENT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());
                }

                Log.info("ID => " + workId +
                        " F_SELF_ONE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE());
                }

                Log.info("ID => " + workId +
                        " F_SELF_TWO => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO());
                }

                Log.info("ID => " + workId +
                        " F_SELF_THREE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE());
                }


                Log.info("ID => " + workId +
                        " F_SELF_FOUR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR());
                }


                Log.info("ID => " + workId +
                        " F_SELF_FIVE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE());
                }


                Log.info("ID => " + workId +
                        " F_SELF_SIX => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX());
                }

                Log.info("ID => " + workId +
                        " F_SELF_SEVEN => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN());
                }
                Log.info(
                        "ID => " + workId +
                                " F_SELF_EIGHT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT() +
                                " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT());
                }
            }
        }

    }


    @When("^We get the work financial attribs from EPH of (.*)$")
    public void getWorkFinAttrEPH(String tableName) {
        Log.info("We get the work financial attribs records from EPH..");
        sql = String.format(workObj.workFinAttrBuildSql(Constants.EPH_SCHEMA,tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the work financial attribs from DL of (.*)$")
    public void getWorkFinAttrDL(String tableName) {
        Log.info("We get the work financial attribs records from DL..");
        sql = String.format(workObj.workFinAttrBuildSql(GetDLDBUser.getDataBase(),tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare work financial attribs in EPH and DL of (.*)$")
    public void compareWorkFiAttrDataEPHtoDL(String tableName) {

        if (dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found .....");
        } else {
            Log.info("Sorting the data to compare the gd work Financial Attributes in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_FIN_ATTRIBS_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_FIN_ATTRIBS_ID));
                if(tableName.equals("gh_work_financial_attribs")){
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                }
                String workFinAttrId = dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_FIN_ATTRIBS_ID();
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_FIN_ATTRIBS_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_FIN_ATTRIBS_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_FIN_ATTRIBS_ID is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_FIN_ATTRIBS_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_FIN_ATTRIBS_ID());

                }
                Log.info("ID => " + workFinAttrId +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_work_financial_attribs")){
                    Log.info("ID => " + workFinAttrId +
                            " B_FROMBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + workFinAttrId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => " + workFinAttrId +
                            " B_TOBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + workFinAttrId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => " + workFinAttrId +
                            " B_BATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + workFinAttrId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    }

                }

                Log.info("ID => " + workFinAttrId +
                        " B_CREDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => " + workFinAttrId +
                        " B_UPDDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => " + workFinAttrId +
                        " B_CREATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => " + workFinAttrId +
                        " B_UPDATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => " + workFinAttrId +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => " + workFinAttrId +
                        " EFFECTIVE_START_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => " + workFinAttrId +
                        " EFFECTIVE_END_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());


                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => " + workFinAttrId +
                        " F_GL_COMPANY => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_COMPANY() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_COMPANY());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_COMPANY() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_COMPANY().equals("null"))) {
                    Assert.assertEquals("The F_GL_COMPANY is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_COMPANY(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_COMPANY());
                }

                Log.info("ID => " + workFinAttrId +
                        " F_GL_COST_RESP_CENTRE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE().equals("null"))) {
                    Assert.assertEquals("The F_GL_COST_RESP_CENTRE is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE());

                }

                Log.info("ID => " + workFinAttrId +
                        " F_GL_REVENUE_RESP_CENTRE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE().equals("null"))) {
                    Assert.assertEquals("The F_GL_REVENUE_RESP_CENTRE is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE());
                }

                Log.info("ID => " + workFinAttrId +
                        " F_WWORK => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK().equals("null"))) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK());
                }


                Log.info("ID => " + workFinAttrId +
                        " F_EVENT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + workFinAttrId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());
                }

            }
        }
    }

    @When("^We get the work identifier from EPH of (.*)$")
    public void getWorkIdentifierEPH(String tableName) {
        Log.info("We get the gd work identifier records from EPH..");
        sql = String.format(workObj.workIdentifierBuildSql(Constants.EPH_SCHEMA,tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the work identifier from DL of (.*)$")
    public void getWorkIdentifierDL(String tableName) {
        Log.info("We get the gd work identifier records from DL..");
        sql = String.format(workObj.workIdentifierBuildSql(GetDLDBUser.getDataBase(),tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
    }


    @And("^Compare work identifier in EPH and DL of (.*)$")
    public void compareWorkIdentifierDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found .....");
        } else {
            Log.info("Sorting the data to compare the work Identifier in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_IDENTIFIER_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_IDENTIFIER_ID));
                if(tableName.equals("gh_work_identifier")){
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                }
                String workIdentifierId = dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_IDENTIFIER_ID();
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_IDENTIFIER_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_IDENTIFIER_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_IDENTIFIER_ID is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_IDENTIFIER_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_IDENTIFIER_ID());

                }
                Log.info("ID => " + workIdentifierId +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_work_identifier")){
                    Log.info("ID => " + workIdentifierId +
                            " B_FROMBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + workIdentifierId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => " + workIdentifierId +
                            " B_TOBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + workIdentifierId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => " + workIdentifierId +
                            " B_BATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + workIdentifierId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => " + workIdentifierId +
                        " B_CREDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => " + workIdentifierId +
                        " B_UPDDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => " + workIdentifierId +
                        " B_CREATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => " + workIdentifierId +
                        " B_UPDATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => " + workIdentifierId +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => " + workIdentifierId +
                        " IDENTIFIER => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getIDENTIFIER() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getIDENTIFIER());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getIDENTIFIER() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getIDENTIFIER().equals("null"))) {
                    Assert.assertEquals("The IDENTIFIER is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getIDENTIFIER(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getIDENTIFIER());
                }

                Log.info("ID => " + workIdentifierId +
                        " S_IDENTIFIER => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_IDENTIFIER() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_IDENTIFIER());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_IDENTIFIER() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_IDENTIFIER().equals("null"))) {
                    Assert.assertEquals("The S_IDENTIFIER is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_IDENTIFIER(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_IDENTIFIER());
                }
                Log.info("ID => " + workIdentifierId +
                        " EFFECTIVE_START_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => " + workIdentifierId +
                        " EFFECTIVE_END_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => " + workIdentifierId +
                        " F_TYPE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());
                }

                Log.info("ID => " + workIdentifierId +
                        " F_WWORK => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK().equals("null"))) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK());

                }

                Log.info("ID => " + workIdentifierId +
                        " F_EVENT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + workIdentifierId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());
                }

            }
        }
    }

    @When("^We get the work relationship from EPH of (.*)$")
    public void getWorkRelationEPH(String tableName) {
        Log.info("We get the work relationship records from EPH..");
        sql = String.format(workObj.workRelationBuildSql(Constants.EPH_SCHEMA,tableName), Joiner.on("','").join(Ids));
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
        Log.info(sql);
    }

    @Then("^We get the work relationship from DL of (.*)$")
    public void getWorkRelationDL(String tableName) {
        Log.info("We get the gd work relationship records from DL..");
        sql = String.format(workObj.workRelationBuildSql(GetDLDBUser.getDataBase(),tableName), Joiner.on("','").join(Ids));
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare work relationship in EPH and DL of (.*)$")
    public void compareWorkRelationDataEPHtoDL(String tableName) {

        if (dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found .....");
        } else {
            Log.info("Sorting the data to compare the gd work relationship in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_RELATIONSHIP_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_RELATIONSHIP_ID));
                if(tableName.equals("gh_work_relationship")){
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                }
                String workRelationId = dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_RELATIONSHIP_ID();
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_RELATIONSHIP_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_RELATIONSHIP_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_RELATIONSHIP_ID is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_RELATIONSHIP_ID());

                }
                Log.info("ID => " + workRelationId +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gd_work_relationship")){
                    Log.info("ID => " + workRelationId +
                            " B_BATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + workRelationId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }else{
                    Log.info("ID => " + workRelationId +
                            " B_FROMBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + workRelationId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => " + workRelationId +
                            " B_TOBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + workRelationId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }

                Log.info("ID => " + workRelationId +
                        " B_CREDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => " + workRelationId +
                        " B_UPDDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => " + workRelationId +
                        " B_CREATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => " + workRelationId +
                        " B_UPDATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => " + workRelationId +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => " + workRelationId +
                        " EFFECTIVE_START_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => " + workRelationId +
                        " EFFECTIVE_END_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => " + workRelationId +
                        " F_PARENT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PARENT() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PARENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PARENT().equals("null"))) {
                    Assert.assertEquals("The F_PARENT is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PARENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PARENT());
                }

                Log.info("ID => " + workRelationId +
                        " F_CHILD => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_CHILD() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_CHILD());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_CHILD() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_CHILD().equals("null"))) {
                    Assert.assertEquals("The F_CHILD is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_CHILD(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_CHILD());

                }

                Log.info("ID => " + workRelationId +
                        " F_RELATIONSHIP_TYPE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_RELATIONSHIP_TYPE is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE());
                }

                Log.info("ID => " + workRelationId +
                        " F_EVENT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_RELATIONSHIP_TYPE is incorrect for id=" + workRelationId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }

    }

    @When("^We get the work subject from EPH of (.*)$")
    public void getWorkSubjectEPH(String tableName) {
        Log.info("We get the work subject records from EPH..");
        sql = String.format(workObj.workSubjectBuildSql(Constants.EPH_SCHEMA,tableName), Joiner.on("','").join(Ids));
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
        Log.info(sql);
    }

    @Then("^We get the work subject from DL of (.*)$")
    public void getWorkSubjectDL(String tableName) {
        Log.info("We get the work subject records records from DL..");
        sql = String.format(workObj.workSubjectBuildSql(GetDLDBUser.getDataBase(),tableName), Joiner.on("','").join(Ids));
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare work subject in EPH and DL of (.*)$")
    public void compareWorkSubjectDataEPHtoDL(String tableName) {

        if (dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found .....");
        } else {
            Log.info("Sorting the data to compare the work subject in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_SUBJECT_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_SUBJECT_ID));

                if(tableName.equals("gh_work_subject_area_link")){
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                }

                String workSubjectId = dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUBJECT_ID();
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUBJECT_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUBJECT_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_SUBJECT_ID is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUBJECT_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUBJECT_ID());
                }
                Log.info("ID => " + workSubjectId +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gd_work_subject_area_link")){
                    Log.info("ID => " + workSubjectId +
                            " B_BATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + workSubjectId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }else{
                    Log.info("ID => " + workSubjectId +
                            " B_FROMBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + workSubjectId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => " + workSubjectId +
                            " B_TOBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + workSubjectId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }
                Log.info("ID => " + workSubjectId +
                        " B_CREDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => " + workSubjectId +
                        " B_UPDDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => " + workSubjectId +
                        " B_CREATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => " + workSubjectId +
                        " B_UPDATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => " + workSubjectId +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => " + workSubjectId +
                        " EFFECTIVE_START_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }
                Log.info("ID => " + workSubjectId +
                        " EFFECTIVE_END_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }
                Log.info("ID => " + workSubjectId +
                        " PPRIMARY => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getPPRIMARY() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getPPRIMARY());

                String primary_eph = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getPPRIMARY());
                String primary_dl = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getPPRIMARY());

                if (primary_eph != null ||
                        (!primary_dl.equals("null"))) {
                    Assert.assertEquals("The PPRIMARY is incorrect for id=" + workSubjectId,
                            primary_eph,primary_dl);
                }
                Log.info("ID => " + workSubjectId +
                        " F_SUBJECT_AREA => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBJECT_AREA() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBJECT_AREA());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBJECT_AREA() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBJECT_AREA().equals("null"))) {
                    Assert.assertEquals("The F_SUBJECT_AREA is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBJECT_AREA(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBJECT_AREA());

                }
                Log.info("ID => " + workSubjectId +
                        " F_WWORK => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK().equals("null"))) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + workSubjectId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK());
                }
            }
        }
    }

    @When("^We get the work copyright from EPH of (.*)$")
    public void getWorkCopyrightEPH(String tableName) {
        Log.info("We get the work copyright records from EPH..");
        sql = String.format(workObj.workCopyrightBuildSql(Constants.EPH_SCHEMA,tableName), Joiner.on("','").join(Ids));
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
        Log.info(sql);
    }

    @Then("^We get the work copyright from DL of (.*)$")
    public void getWorkCopyrightDL(String tableName) {
        Log.info("We get the work copyright records from DL..");
        sql = String.format(workObj.workCopyrightBuildSql(GetDLDBUser.getDataBase(),tableName), Joiner.on("','").join(Ids));
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare work copyright in EPH and DL of (.*)$")
    public void compareWorkCopyrightDataEPHtoDL(String tableName) {

        if (dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found .....");
        } else {
            Log.info("Sorting the data to compare the work copyright in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_COPYRIGHTS_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_COPYRIGHTS_ID));

                if(tableName.equals("gh_work_copyright_owner_link")){
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                }

                String workCopyRightId = dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_COPYRIGHTS_ID();
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_COPYRIGHTS_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_COPYRIGHTS_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_COPYRIGHTS_ID is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_COPYRIGHTS_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_COPYRIGHTS_ID());
                }
                Log.info("ID => " + workCopyRightId +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gd_work_copyright_owner_link")){
                    Log.info("ID => " + workCopyRightId +
                            " B_BATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + workCopyRightId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }else{
                    Log.info("ID => " + workCopyRightId +
                            " B_FROMBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + workCopyRightId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => " + workCopyRightId +
                            " B_TOBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + workCopyRightId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }
                Log.info("ID => " + workCopyRightId +
                        " B_CREDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => " + workCopyRightId +
                        " B_UPDDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => " + workCopyRightId +
                        " B_CREATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => " + workCopyRightId +
                        " B_UPDATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => " + workCopyRightId +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => " + workCopyRightId +
                        " EFFECTIVE_START_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }
                Log.info("ID => " + workCopyRightId +
                        " EFFECTIVE_END_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }
                Log.info("ID => " + workCopyRightId +
                        " F_COPYRIGHT_OWNER => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_COPYRIGHT_OWNER() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_COPYRIGHT_OWNER());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_COPYRIGHT_OWNER() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_COPYRIGHT_OWNER().equals("null"))) {
                    Assert.assertEquals("The F_COPYRIGHT_OWNER is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_COPYRIGHT_OWNER(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_COPYRIGHT_OWNER());

                }
                Log.info("ID => " + workCopyRightId +
                        " F_WWORK => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK().equals("null"))) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_WWORK());
                }
                Log.info("ID => " + workCopyRightId +
                        " F_EVENT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + workCopyRightId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the work relationship_edition from EPH of (.*)$")
    public void getWorksRelEditEPH(String tableName) {
        Log.info("We get the work records from EPH..");
        sql = String.format(workObj.workRelEditBuildSql(Constants.EPH_SCHEMA,tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the work relationship_edition from DL of (.*)$")
    public void getWorksRelEditDL(String tableName) {
        Log.info("We get the work records from DL..");
        sql = String.format(workObj.workRelEditBuildSql(GetDLDBUser.getDataBase(),tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
    }


    @And("^Compare work relationship_edition in EPH and DL of (.*)$")
    public void compareWorkRelEditDataEPHtoDL(String tableName) {

        if (dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found .....");
        } else {
            Log.info("Sorting the data to compare the work relation edit in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_REL_EDITION_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_REL_EDITION_ID));

                if(tableName.equals("gh_work_relationship_edition")){
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getB_FROMBATCHID));
                }

                String workRelEditId = dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_REL_EDITION_ID();
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_REL_EDITION_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_REL_EDITION_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_REL_EDITION_ID is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_REL_EDITION_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_REL_EDITION_ID());
                }
                Log.info("ID => " + workRelEditId +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gd_work_relationship_edition")){
                    Log.info("ID => " + workRelEditId +
                            " B_BATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + workRelEditId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }else{
                    Log.info("ID => " + workRelEditId +
                            " B_FROMBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + workRelEditId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => " + workRelEditId +
                            " B_TOBATCHID => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + workRelEditId,
                                dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }
                Log.info("ID => " + workRelEditId +
                        " B_CREDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => " + workRelEditId +
                        " B_UPDDATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => " + workRelEditId +
                        " B_CREATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => " + workRelEditId +
                        " B_UPDATOR => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => " + workRelEditId +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => " + workRelEditId +
                        " EFFECTIVE_START_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }
                Log.info("ID => " + workRelEditId +
                        " EFFECTIVE_END_DATE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }
                Log.info("ID => " + workRelEditId +
                        " F_ORIGINAL => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_ORIGINAL() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_ORIGINAL());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_ORIGINAL() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_ORIGINAL().equals("null"))) {
                    Assert.assertEquals("The F_ORIGINAL is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_ORIGINAL(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_ORIGINAL());

                }
                Log.info("ID => " + workRelEditId +
                        " F_EDITION => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EDITION() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EDITION());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EDITION() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EDITION().equals("null"))) {
                    Assert.assertEquals("The F_EDITION is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EDITION(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EDITION());
                }

                Log.info("ID => " + workRelEditId +
                        " F_RELATIONSHIP_TYPE => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_RELATIONSHIP_TYPE is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE());
                }
                Log.info("ID => " + workRelEditId +
                        " F_EVENT => EPH=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() +
                        " DL=" + dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + workRelEditId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }





}
