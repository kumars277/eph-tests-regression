package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.EventDataDLObject;
import com.eph.automation.testing.services.db.DataLakeSql.EventGDGHTablesDataChecksSQL;
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


public class EventGDGHTablesDataCheckSteps {

    @StaticInjection
    public DataQualityDLContext dataQualityDLContext;
    private static String sql;
    private static List<String> Ids;


    @Given("^We get (.*) random event ids of (.*)")
    public void getRandomEventIds(String numberOfRecords, String tableName) {
        Log.info("Get random records ..");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);
        switch (tableName){
            case "gd_event":
                sql = String.format(EventGDGHTablesDataChecksSQL.GET_RANDOM_EVENT_ID_GD, numberOfRecords);
                Log.info(sql);
                List<Map<?, ?>> randomEventIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomEventIds.stream().map(m -> (BigDecimal) m.get("EVENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
            Log.info(Ids.toString());
    }

    @When("^We get the gd event records from EPH$")
    public void getWorksEPH() {
        Log.info("We get the event records from EPH..");
        sql = String.format(EventGDGHTablesDataChecksSQL.GET_DATA_EVENT_EPH, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbEventDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the gd event records from DL$")
    public void getEventsDL() {
        Log.info("We get the event records from DL..");
        sql = String.format(EventGDGHTablesDataChecksSQL.GET_DATA_EVENT_DL, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbEventDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare gd event records in EPH and DL$")
    public void compareGDEventDataEPHtoDL() {
        if(dataQualityDLContext.tbEventDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
        Log.info("Sorting the data to compare the event records in EPH and DATA LAKE ..");//sort data in the lists
        for (int i = 0; i < dataQualityDLContext.tbEventDataObjectsFromEPH.size(); i++) {
            dataQualityDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getEVENT_ID));
            dataQualityDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getEVENT_ID));

            String eventId = dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEVENT_ID();

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEVENT_ID() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEVENT_ID().equals("null"))) {  //In data lake null considering or getting as String
                Assert.assertEquals("The EVENT_ID is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEVENT_ID(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEVENT_ID());

            }
            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                Assert.assertEquals("The B_BATCHID is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());
            }
            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                Assert.assertEquals("The B_CREDATE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                    (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                Assert.assertEquals("The B_UPDATE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                Assert.assertEquals("The B_CREATOR is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                Assert.assertEquals("The B_UPDATOR is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDDATE() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDDATE().equals("null"))) {
                Assert.assertEquals("The DDATE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDDATE(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDDATE());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTTIMESTAMP() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTTIMESTAMP().equals("null"))) {
                Assert.assertEquals("The TTIMESTAMP is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTTIMESTAMP(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTTIMESTAMP());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDESCRIPTION() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDESCRIPTION().equals("null"))) {
                Assert.assertEquals("The DESCRIPTION is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDESCRIPTION(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDESCRIPTION());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTHIRD_PARTY() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTHIRD_PARTY().equals("null"))) {
                Assert.assertEquals("The THIRD_PARTY is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTHIRD_PARTY(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTHIRD_PARTY());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getWORKFLOW_ID() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getWORKFLOW_ID().equals("null"))) {
                Assert.assertEquals("The WORKFLOW_ID is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getWORKFLOW_ID(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getWORKFLOW_ID());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_EVENT_TYPE() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_EVENT_TYPE().equals("null"))) {
                Assert.assertEquals("The F_EVENT_TYPE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_EVENT_TYPE(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_EVENT_TYPE());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_WORKFLOW_SOURCE() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_WORKFLOW_SOURCE().equals("null"))) {
                Assert.assertEquals("The F_WORKFLOW_SOURCE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_WORKFLOW_SOURCE(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_WORKFLOW_SOURCE());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_ONE().equals("null"))) {
                Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_ONE());
            }

            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_TWO() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_TWO().equals("null"))) {
                Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_TWO());
            }
            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_THREE() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_THREE().equals("null"))) {
                Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_THREE());
            }
            if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_FOUR() != null ||
                    (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_FOUR().equals("null"))) {
                Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                        dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                        dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_FOUR());
            }

        }}

    }



}
