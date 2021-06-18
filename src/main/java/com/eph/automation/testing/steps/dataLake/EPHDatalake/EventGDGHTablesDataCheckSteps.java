package com.eph.automation.testing.steps.dataLake.EPHDatalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityEPHDLContext;
import com.eph.automation.testing.models.dao.EPHDataLake.EventDataDLObject;
import com.eph.automation.testing.services.db.EPHDataLakeSql.EventGDGHTablesDataChecksSQL;
import com.eph.automation.testing.services.db.EPHDataLakeSql.GetDLDBUser;
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
    public DataQualityEPHDLContext dataQualityEPHDLContext;
    private static String sql;
    private static List<String> Ids;
    private EventGDGHTablesDataChecksSQL eventObj = new EventGDGHTablesDataChecksSQL();


    @Given("^We get (.*) random event ids of (.*)")
    public void getRandomEventIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random record...");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);
        switch (tableName){
            case "gd_event":
                sql = String.format(EventGDGHTablesDataChecksSQL.GET_RANDOM_EVENT_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomEventIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomEventIds.stream().map(m -> (BigDecimal) m.get("EVENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_subject_area":
                sql = String.format(EventGDGHTablesDataChecksSQL.GET_RANDOM_SUB_AREA_ID, numberOfRecords);
                List<Map<?, ?>> randomSubjAreaIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomSubjAreaIds.stream().map(m -> (BigDecimal) m.get("SUBJECT_AREA_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_subject_area":
                sql = String.format(EventGDGHTablesDataChecksSQL.GET_RANDOM_SUB_AREA_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomSubjAreaGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomSubjAreaGHIds.stream().map(m -> (BigDecimal) m.get("SUBJECT_AREA_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_copyright_owner":
                sql = String.format(EventGDGHTablesDataChecksSQL.GET_RANDOM_COPYRIGHT_ID, numberOfRecords);
                List<Map<?, ?>> randomCopyrightIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomCopyrightIds.stream().map(m -> (BigDecimal) m.get("COPY_RIGHT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_copyright_owner":
                sql = String.format(EventGDGHTablesDataChecksSQL.GET_RANDOM_COPYRIGHT_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomGHCopyrightIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGHCopyrightIds.stream().map(m -> (BigDecimal) m.get("COPY_RIGHT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the gd event records from EPH$")
    public void getEventsEPH() {
        Log.info("We get the event records from EPH..");
        // sql = String.format(EventGDGHTablesDataChecksSQL.GET_DATA_EVENT_EPH, Joiner.on("','").join(Ids));
        sql = String.format(eventObj.eventBuildSQLQuery(Constants.EPH_SCHEMA),Joiner.on("','").join(Ids)) ;
        Log.info(sql);
        dataQualityEPHDLContext.tbEventDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the gd event records from DL$")
    public void getEventsDL() {
        Log.info("We get the event records from DL..");
        //sql = String.format(EventGDGHTablesDataChecksSQL.GET_DATA_EVENT_DL, Joiner.on("','").join(Ids));
        sql = String.format(eventObj.eventBuildSQLQuery(GetDLDBUser.getDataBase()),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbEventDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare gd event records in EPH and DL$")
    public void compareGDEventDataEPHtoDL() {
        if(dataQualityEPHDLContext.tbEventDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the gd event records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbEventDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getEVENT_ID));
                dataQualityEPHDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getEVENT_ID));

                String eventId = dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEVENT_ID();

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEVENT_ID() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEVENT_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The EVENT_ID is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEVENT_ID(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEVENT_ID());

                }
                Log.info("ID => "+eventId+
                        " EVENT_ID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                Log.info("ID => "+eventId+
                        " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                    Assert.assertEquals("The B_BATCHID is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());
                }
                Log.info("ID => "+eventId+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => "+eventId+
                        " B_UPDDATE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+eventId+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => "+eventId+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => "+eventId+
                        " DDATE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getDDATE());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getDDATE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getDDATE()!= null)) {
                    Assert.assertEquals("The DDATE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getDDATE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getDDATE());
                }
                Log.info("ID => "+eventId+
                        " TTIMESTAMP => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getTTIMESTAMP()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getTTIMESTAMP());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getTTIMESTAMP() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getTTIMESTAMP()!= null)) {
                    Assert.assertEquals("The TTIMESTAMP is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getTTIMESTAMP(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getTTIMESTAMP());
                }
                Log.info("ID => "+eventId+
                        " DESCRIPTION => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getDESCRIPTION()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getDESCRIPTION());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getDESCRIPTION() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getDESCRIPTION()!= null)) {
                    Assert.assertEquals("The DESCRIPTION is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getDESCRIPTION(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getDESCRIPTION());
                }
                Log.info("ID => "+eventId+
                        " THIRD_PARTY => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getTHIRD_PARTY()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getTHIRD_PARTY());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getTHIRD_PARTY() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getTHIRD_PARTY()!= null)) {
                    Assert.assertEquals("The THIRD_PARTY is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getTHIRD_PARTY(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getTHIRD_PARTY());
                }
                Log.info("ID => "+eventId+
                        " WORKFLOW_ID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getWORKFLOW_ID()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getWORKFLOW_ID());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getWORKFLOW_ID() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getWORKFLOW_ID()!= null)) {
                    Assert.assertEquals("The WORKFLOW_ID is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getWORKFLOW_ID(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getWORKFLOW_ID());
                }
                Log.info("ID => "+eventId+
                        " F_EVENT_TYPE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_EVENT_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_EVENT_TYPE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_EVENT_TYPE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_EVENT_TYPE()!= null)) {
                    Assert.assertEquals("The F_EVENT_TYPE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_EVENT_TYPE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_EVENT_TYPE());
                }
                Log.info("ID => "+eventId+
                        " F_WORKFLOW_SOURCE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_WORKFLOW_SOURCE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_WORKFLOW_SOURCE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_WORKFLOW_SOURCE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_WORKFLOW_SOURCE()!= null)) {
                    Assert.assertEquals("The F_WORKFLOW_SOURCE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_WORKFLOW_SOURCE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_WORKFLOW_SOURCE());
                }
                Log.info("ID => "+eventId+
                        " F_SELF_ONE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_ONE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_ONE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_ONE()!= null)) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_ONE());
                }
                Log.info("ID => "+eventId+
                        " F_SELF_TWO => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_TWO()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_TWO());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_TWO() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_TWO()!= null)) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_TWO());
                }
                Log.info("ID => "+eventId+
                        " F_SELF_THREE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_THREE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_THREE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_THREE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_THREE()!= null)) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_THREE());
                }
                Log.info("ID => "+eventId+
                        " F_SELF_FOUR => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_FOUR()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_FOUR());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_FOUR() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_FOUR()!= null)) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_FOUR());
                }

            }}

    }

    @When("^We get the subject area records from EPH of (.*)$")
    public void getSubjAreaEPH(String tableName) {
        Log.info("We get the Subject Area records from EPH..");
        sql = String.format(eventObj.subAreaBuildSQLQuery(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids)) ;
        Log.info(sql);
        dataQualityEPHDLContext.tbEventDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the subject area records from DL of (.*)$")
    public void getSubjAreaDL(String tableName) {
        Log.info("We get the Subject Area records from DL..");
        sql = String.format(eventObj.subAreaBuildSQLQuery(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbEventDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare subject area records in EPH and DL of (.*)$")
    public void compareSubAreaDataEPHtoDL(String tableName) {
        if(dataQualityEPHDLContext.tbEventDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the subject area records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbEventDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getSUBJECT_AREA_ID));
                dataQualityEPHDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getSUBJECT_AREA_ID));
                if(tableName.equals("gh_subject_area")){
                    dataQualityEPHDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getB_FROMBATCHID));
                }
                String subAreaId = dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getSUBJECT_AREA_ID();
                if (subAreaId != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEVENT_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The EVENT_ID is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEVENT_ID(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEVENT_ID());

                }
                Log.info("ID => "+subAreaId+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_subject_area")){
                    Log.info("ID => "+subAreaId+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + subAreaId,
                                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }
                    Log.info("ID => "+subAreaId+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + subAreaId,
                                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+subAreaId+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + subAreaId,
                                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+subAreaId+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => "+subAreaId+
                        " B_UPDDATE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+subAreaId+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => "+subAreaId+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => "+subAreaId+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => "+subAreaId+
                        " CODE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getCODE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getCODE());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getCODE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getCODE()!= null)) {
                    Assert.assertEquals("The CODE is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getCODE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getCODE());
                }
                Log.info("ID => "+subAreaId+
                        " NAME => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getNAME()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getNAME());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getNAME() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getNAME()!= null)) {
                    Assert.assertEquals("The NAME is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getNAME(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getNAME());
                }
                Log.info("ID => "+subAreaId+
                        " F_TYPE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_TYPE()!= null)) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_TYPE());
                }
                Log.info("ID => "+subAreaId+
                        " F_PARENT_SUBJECT_AREA => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_PARENT_SUBJECT_AREA()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_PARENT_SUBJECT_AREA());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_PARENT_SUBJECT_AREA() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_PARENT_SUBJECT_AREA()!= null)) {
                    Assert.assertEquals("The F_PARENT_SUBJECT_AREA is incorrect for id=" + subAreaId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getF_PARENT_SUBJECT_AREA(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getF_PARENT_SUBJECT_AREA());
                }

            }}

    }


    @When("^We get the copyright records from EPH of (.*)$")
    public void getCopyrightEPH(String tableName) {
        Log.info("We get the Copyright Owner records from EPH..");
        sql = String.format(eventObj.copyRightBuildSQLQuery(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids)) ;
        Log.info(sql);
        dataQualityEPHDLContext.tbEventDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the copyright records from DL of (.*)$")
    public void getCopyrightDL(String tableName) {
        Log.info("We get the Copyright Owner records from DL..");
        sql = String.format(eventObj.copyRightBuildSQLQuery(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbEventDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare copyright records in EPH and DL of (.*)$")
    public void compareCopyRightDataEPHtoDL(String tableName) {
        if(dataQualityEPHDLContext.tbEventDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the gd copyright records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbEventDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getCOPY_RIGHT_ID));
                dataQualityEPHDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getCOPY_RIGHT_ID));
                if(tableName.equals("gh_copyright_owner")){
                    dataQualityEPHDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getB_FROMBATCHID));
                }

                String copyRightId = dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getCOPY_RIGHT_ID();

                if (copyRightId != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getCOPY_RIGHT_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The COPY_RIGHT_ID is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getCOPY_RIGHT_ID(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getCOPY_RIGHT_ID());

                }
                Log.info("ID => "+copyRightId+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_copyright_owner")){
                    Log.info("ID => "+copyRightId+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + copyRightId,
                                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }else{
                    Log.info("ID => "+copyRightId+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + copyRightId,
                                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+copyRightId+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + copyRightId,
                                dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }

                Log.info("ID => "+copyRightId+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => "+copyRightId+
                        " B_UPDDATE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+copyRightId+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => "+copyRightId+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => "+copyRightId+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => "+copyRightId+
                        " NAME => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getNAME()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getNAME());


                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getNAME() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getNAME()!= null)) {
                    Assert.assertEquals("The NAME is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getNAME(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getNAME());
                }
                Log.info("ID => "+copyRightId+
                        " S_NAME => EPH="+ dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getS_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getS_NAME());

                if (dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getS_NAME() != null ||
                        (dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getS_NAME()!= null)) {
                    Assert.assertEquals("The S_NAME is incorrect for id=" + copyRightId,
                            dataQualityEPHDLContext.tbEventDataObjectsFromEPH.get(i).getS_NAME(),
                            dataQualityEPHDLContext.tbEventDataObjectsFromDL.get(i).getS_NAME());
                }

            }
        }

    }




}
