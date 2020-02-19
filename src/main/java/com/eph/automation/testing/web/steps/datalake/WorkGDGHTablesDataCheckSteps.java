package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.*;
import com.eph.automation.testing.services.db.DataLakeSql.WorksGDGHTablesDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class WorkGDGHTablesDataCheckSteps {

    @StaticInjection
    public DataQualityDLContext dataQualityDLContext;
    private static String sql;
    private static List<String> Ids;


    @Given("^We get (.*) random work ids of (.*)")
    public void getRandomWorkIds(String numberOfRecords, String tableName) {
        Log.info("Get random records ..");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);
        switch (tableName){
            case "gd_wwork":
            sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_ID_GD, numberOfRecords);
            Log.info(sql);
            List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            Ids = randomWorkIds.stream().map(m -> (String) m.get("WORK_ID")).collect(Collectors.toList());
            break;

            case "gh_wwork":
                sql = String.format(WorksGDGHTablesDataChecksSQL.GET_RANDOM_WORK_ID_GH, numberOfRecords);
                Log.info(sql);
                List<Map<?, ?>> randomWorkGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkGHIds.stream().map(m -> (String) m.get("WORK_ID")).collect(Collectors.toList());
                break;
        }
        Log.info(Ids.toString());
    }

    @When("^We get the gd work records from EPH$")
    public void getWorksEPH() {
        Log.info("We get the work records from EPH..");
        sql = String.format(WorksGDGHTablesDataChecksSQL.GET_DATA_WORKS_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the gd work records from DL$")
    public void getWorksDL() {
        Log.info("We get the work records from DL..");
        sql = String.format(WorksGDGHTablesDataChecksSQL.GET_DATA_WORKS_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare gd work records in EPH and DL$")
    public void compareGDWorkDataEPHtoDL() {
        if(dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the work records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_ID));
                String workId = dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID();
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_ID is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_ID());

                }
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                    Assert.assertEquals("The B_BATCHID is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());
                }
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID().equals("null"))) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_SUB_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_SUB_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE());

                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SHORT_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SHORT_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_SHORT_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SHORT_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SHORT_TITLE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_SHORT_TITLE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE());
                }

                //Converting Boolean to String
                String electro_right_indicator_eph = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getELECTRO_RIGHTS_INDICATOR());
                String electro_right_indicator_dl = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getELECTRO_RIGHTS_INDICATOR());
                if (electro_right_indicator_eph != null || (!electro_right_indicator_dl.equals("null"))) {
                    Assert.assertEquals("The ELECTRO_RIGHTS_INDICATOR is incorrect for id=" + workId,
                            electro_right_indicator_eph, electro_right_indicator_dl);
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME().equals("null"))) {
                    Assert.assertEquals("The VOLUME is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR().equals("null"))) {
                    Assert.assertEquals("The COPYRIGHT_YEAR is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER().equals("null"))) {
                    Assert.assertEquals("The EDITION_NUMBER is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER());
                }

                String summaryChanged_eph = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_SUMMARY_CHANGED());
                String summaryChanged_dl = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_SUMMARY_CHANGED());
                if (summaryChanged_eph != null || (!summaryChanged_dl.equals("null"))) {
                    Assert.assertEquals("The T_SUMMARY_CHANGED is incorrect for id=" + workId,
                            summaryChanged_eph, summaryChanged_dl);
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS().equals("null"))) {
                    Assert.assertEquals("The F_STATUS is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getf_accountable_product() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getf_accountable_product().equals("null"))) {
                    Assert.assertEquals("The f_accountable_product is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getf_accountable_product(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getf_accountable_product());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC().equals("null"))) {
                    Assert.assertEquals("The F_PMC is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_OA_TYPE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY().equals("null"))) {
                    Assert.assertEquals("The F_FAMILY is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY());

                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT().equals("null"))) {
                    Assert.assertEquals("The IMPRINT is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP().equals("null"))) {
                    Assert.assertEquals("The F_SOCIETY_OWNERSHIP is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE().equals("null"))) {
                    Assert.assertEquals("The SUBSCRIPTION_TYPE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE().equals("null"))) {
                    Assert.assertEquals("The F_LLANGUAGE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO());
                }
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE());
                }
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR());
                }
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN());
                }

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workId,
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT());
                }
            }}

    }

    @When("^We get the gh work records from EPH$")
    public void getGhWorksEPH() {
        Log.info("We get the gh work records from EPH..");
        sql = String.format(WorksGDGHTablesDataChecksSQL.GET_DATA_GH_WORKS_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the gh work records from DL$")
    public void getGhWorksDL() {
        Log.info("We get the work records from DL..");
        sql = String.format(WorksGDGHTablesDataChecksSQL.GET_DATA_GH_WORKS_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare gh work records in EPH and DL$")
    public void compareGhWorkDataEPHtoDL() {

        if(dataQualityDLContext.tbWorkDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found .....");
        }else{
            Log.info("Sorting the data to compare the gh work records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbWorkDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_ID));
                dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_ID));

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The WORK_ID is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_ID());

                }
                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " B_TOBATCHID => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());


                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                    Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_TOBATCHID());
                }


                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " B_FROMBATCHID => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                    Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_FROMBATCHID());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }


                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " B_UPDDATE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }


                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " S_WORK_ID => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID().equals("null"))) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " WORK_TITLE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_TITLE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " S_WORK_TITLE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_TITLE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " WORK_SUB_TITLE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_SUB_TITLE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " S_WORK_SUB_TITLE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_SUB_TITLE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE());

                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " WORK_SHORT_TITLE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SHORT_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SHORT_TITLE().equals("null"))) {
                    Assert.assertEquals("The WORK_SHORT_TITLE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SHORT_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SHORT_TITLE());
                }
                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " S_WORK_SHORT_TITLE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_SHORT_TITLE is incorrect for id=" + Ids.get(i),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE());
                }
                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " ELECTRO_RIGHTS_INDICATOR => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getELECTRO_RIGHTS_INDICATOR()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getELECTRO_RIGHTS_INDICATOR());
                //Converting Boolean to String
                String electro_right_indicator_eph = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getELECTRO_RIGHTS_INDICATOR());
                String electro_right_indicator_dl = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getELECTRO_RIGHTS_INDICATOR());
                if (electro_right_indicator_eph != null || (!electro_right_indicator_dl.equals("null"))) {
                    Assert.assertEquals("The ELECTRO_RIGHTS_INDICATOR is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            electro_right_indicator_eph, electro_right_indicator_dl);
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " VOLUME => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME().equals("null"))) {
                    Assert.assertEquals("The VOLUME is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " COPYRIGHT_YEAR => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR().equals("null"))) {
                    Assert.assertEquals("The COPYRIGHT_YEAR is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " EDITION_NUMBER => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER().equals("null"))) {
                    Assert.assertEquals("The EDITION_NUMBER is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER());
                }
                String summaryChanged_eph = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_SUMMARY_CHANGED());
                String summaryChanged_dl = String.valueOf(dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_SUMMARY_CHANGED());

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " summaryChanged => EPH="+summaryChanged_eph+
                        " DL="+summaryChanged_dl);
                if (summaryChanged_eph != null || (!summaryChanged_dl.equals("null"))) {
                    Assert.assertEquals("The T_SUMMARY_CHANGED is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            summaryChanged_eph, summaryChanged_dl);
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " T_EVENT_DESCRIPTION => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_TYPE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_STATUS => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS().equals("null"))) {
                    Assert.assertEquals("The F_STATUS is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " f_accountable_product => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getf_accountable_product()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getf_accountable_product());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getf_accountable_product() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getf_accountable_product().equals("null"))) {
                    Assert.assertEquals("The f_accountable_product is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getf_accountable_product(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getf_accountable_product());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_PMC => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC().equals("null"))) {
                    Assert.assertEquals("The F_PMC is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_OA_TYPE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_OA_TYPE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_FAMILY => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY().equals("null"))) {
                    Assert.assertEquals("The F_FAMILY is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY());

                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_IMPRINT => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT().equals("null"))) {
                    Assert.assertEquals("The IMPRINT is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SOCIETY_OWNERSHIP => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP().equals("null"))) {
                    Assert.assertEquals("The F_SOCIETY_OWNERSHIP is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SUBSCRIPTION_TYPE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE().equals("null"))) {
                    Assert.assertEquals("The SUBSCRIPTION_TYPE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE());
                }


                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_LLANGUAGE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE().equals("null"))) {
                    Assert.assertEquals("The F_LLANGUAGE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_T_EVENT_TYPE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_EVENT => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());
                }


                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SELF_ONE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE());
                }


                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SELF_TWO => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO());
                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SELF_THREE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE());
                }
                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SELF_FOUR => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR());


                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR().equals("null"))) {
                    Assert.assertEquals("The F_SELF_FOUR is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SELF_FIVE => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_FIVE is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE());
                }

                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SELF_SIX => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX()+
                        " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX().equals("null"))) {
                    Assert.assertEquals("The F_SELF_SIX is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX());
                }


                Log.info("ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SELF_SEVEN => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN()+
                                " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN().equals("null"))) {
                    Assert.assertEquals("The F_SELF_SEVEN is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN());
                }


                Log.info(
                        "ID => "+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID()+
                        " F_SELF_EIGHT => EPH="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT()+
                                " DL="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT());

                if (dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT().equals("null"))) {
                    Assert.assertEquals("The F_SELF_EIGHT is incorrect for id=" + dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_ID(),
                            dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT(),
                            dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT());
                }
            }
        }

    }


}
