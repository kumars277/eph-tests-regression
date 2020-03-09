package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.EventDataDLObject;
import com.eph.automation.testing.services.db.DataLakeSql.EventGDGHTablesDataChecksSQL;
import com.eph.automation.testing.services.db.DataLakeSql.GetDLDBUser;
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
    private EventGDGHTablesDataChecksSQL eventObj = new EventGDGHTablesDataChecksSQL();


    @Given("^We get (.*) random event ids of (.*)")
    public void getRandomEventIds(String numberOfRecords, String tableName) {
        Log.info("Get random records ..");

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
        dataQualityDLContext.tbEventDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the gd event records from DL$")
    public void getEventsDL() {
        Log.info("We get the event records from DL..");
        //sql = String.format(EventGDGHTablesDataChecksSQL.GET_DATA_EVENT_DL, Joiner.on("','").join(Ids));
        sql = String.format(eventObj.eventBuildSQLQuery(GetDLDBUser.getDataBase()),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbEventDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare gd event records in EPH and DL$")
    public void compareGDEventDataEPHtoDL() {
        if(dataQualityDLContext.tbEventDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the gd event records in EPH and DATA LAKE ..");//sort data in the lists
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
                Log.info("ID => "+eventId+
                        " EVENT_ID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                Log.info("ID => "+eventId+
                        " B_BATCHID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                    Assert.assertEquals("The B_BATCHID is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());
                }
                Log.info("ID => "+eventId+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => "+eventId+
                        " B_UPDDATE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+eventId+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => "+eventId+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => "+eventId+
                        " DDATE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDDATE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDDATE());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDDATE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDDATE().equals("null"))) {
                    Assert.assertEquals("The DDATE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDDATE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDDATE());
                }
                Log.info("ID => "+eventId+
                        " TTIMESTAMP => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTTIMESTAMP()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTTIMESTAMP());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTTIMESTAMP() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTTIMESTAMP().equals("null"))) {
                    Assert.assertEquals("The TTIMESTAMP is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTTIMESTAMP(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTTIMESTAMP());
                }
                Log.info("ID => "+eventId+
                        " DESCRIPTION => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDESCRIPTION()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDESCRIPTION());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDESCRIPTION() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The DESCRIPTION is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getDESCRIPTION(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getDESCRIPTION());
                }
                Log.info("ID => "+eventId+
                        " THIRD_PARTY => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTHIRD_PARTY()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTHIRD_PARTY());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTHIRD_PARTY() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTHIRD_PARTY().equals("null"))) {
                    Assert.assertEquals("The THIRD_PARTY is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getTHIRD_PARTY(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getTHIRD_PARTY());
                }
                Log.info("ID => "+eventId+
                        " WORKFLOW_ID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getWORKFLOW_ID()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getWORKFLOW_ID());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getWORKFLOW_ID() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getWORKFLOW_ID().equals("null"))) {
                    Assert.assertEquals("The WORKFLOW_ID is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getWORKFLOW_ID(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getWORKFLOW_ID());
                }
                Log.info("ID => "+eventId+
                        " F_EVENT_TYPE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_EVENT_TYPE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_EVENT_TYPE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_EVENT_TYPE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_EVENT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_EVENT_TYPE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_EVENT_TYPE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_EVENT_TYPE());
                }
                Log.info("ID => "+eventId+
                        " F_WORKFLOW_SOURCE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_WORKFLOW_SOURCE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_WORKFLOW_SOURCE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_WORKFLOW_SOURCE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_WORKFLOW_SOURCE().equals("null"))) {
                    Assert.assertEquals("The F_WORKFLOW_SOURCE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_WORKFLOW_SOURCE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_WORKFLOW_SOURCE());
                }
                Log.info("ID => "+eventId+
                        " F_SELF_ONE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_ONE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_ONE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_ONE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_ONE());
                }
                Log.info("ID => "+eventId+
                        " F_SELF_TWO => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_TWO()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_TWO());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_TWO() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_TWO().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_TWO());
                }
                Log.info("ID => "+eventId+
                        " F_SELF_THREE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_THREE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_THREE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_THREE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_THREE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_THREE());
                }
                Log.info("ID => "+eventId+
                        " F_SELF_FOUR => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_FOUR()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_FOUR());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_FOUR() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_FOUR().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + eventId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_SELF_FOUR());
                }

            }}

    }

    @When("^We get the subject area records from EPH of (.*)$")
    public void getSubjAreaEPH(String tableName) {
        Log.info("We get the Subject Area records from EPH..");
        sql = String.format(eventObj.subAreaBuildSQLQuery(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids)) ;
        Log.info(sql);
        dataQualityDLContext.tbEventDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the subject area records from DL of (.*)$")
    public void getSubjAreaDL(String tableName) {
        Log.info("We get the Subject Area records from DL..");
        sql = String.format(eventObj.subAreaBuildSQLQuery(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbEventDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare subject area records in EPH and DL of (.*)$")
    public void compareSubAreaDataEPHtoDL(String tableName) {
        if(dataQualityDLContext.tbEventDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the subject area records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbEventDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getSUBJECT_AREA_ID));
                dataQualityDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getSUBJECT_AREA_ID));
                if(tableName.equals("gh_subject_area")){
                    dataQualityDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getB_FROMBATCHID));
                }
                String subAreaId = dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getSUBJECT_AREA_ID();
                if (subAreaId != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEVENT_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The EVENT_ID is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEVENT_ID(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEVENT_ID());

                }
                Log.info("ID => "+subAreaId+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_subject_area")){
                    Log.info("ID => "+subAreaId+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + subAreaId,
                                dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }
                    Log.info("ID => "+subAreaId+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + subAreaId,
                                dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+subAreaId+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + subAreaId,
                                dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+subAreaId+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => "+subAreaId+
                        " B_UPDDATE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+subAreaId+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => "+subAreaId+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => "+subAreaId+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => "+subAreaId+
                        " CODE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getCODE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getCODE());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getCODE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getCODE().equals("null"))) {
                    Assert.assertEquals("The CODE is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getCODE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getCODE());
                }
                Log.info("ID => "+subAreaId+
                        " NAME => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getNAME()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getNAME());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getNAME() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getNAME().equals("null"))) {
                    Assert.assertEquals("The NAME is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getNAME(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getNAME());
                }
                Log.info("ID => "+subAreaId+
                        " F_TYPE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_TYPE());
                }
                Log.info("ID => "+subAreaId+
                        " F_PARENT_SUBJECT_AREA => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_PARENT_SUBJECT_AREA()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_PARENT_SUBJECT_AREA());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_PARENT_SUBJECT_AREA() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_PARENT_SUBJECT_AREA().equals("null"))) {
                    Assert.assertEquals("The F_PARENT_SUBJECT_AREA is incorrect for id=" + subAreaId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getF_PARENT_SUBJECT_AREA(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getF_PARENT_SUBJECT_AREA());
                }

            }}

    }


    @When("^We get the copyright records from EPH of (.*)$")
    public void getCopyrightEPH(String tableName) {
        Log.info("We get the Copyright Owner records from EPH..");
        sql = String.format(eventObj.copyRightBuildSQLQuery(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids)) ;
        Log.info(sql);
        dataQualityDLContext.tbEventDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the copyright records from DL of (.*)$")
    public void getCopyrightDL(String tableName) {
        Log.info("We get the Copyright Owner records from DL..");
        sql = String.format(eventObj.copyRightBuildSQLQuery(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbEventDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, EventDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare copyright records in EPH and DL of (.*)$")
    public void compareCopyRightDataEPHtoDL(String tableName) {
        if(dataQualityDLContext.tbEventDataObjectsFromDL.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the gd copyright records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbEventDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getCOPY_RIGHT_ID));
                dataQualityDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getCOPY_RIGHT_ID));
                if(tableName.equals("gh_copyright_owner")){
                    dataQualityDLContext.tbEventDataObjectsFromEPH.sort(Comparator.comparing(EventDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbEventDataObjectsFromDL.sort(Comparator.comparing(EventDataDLObject::getB_FROMBATCHID));
                }

                String copyRightId = dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getCOPY_RIGHT_ID();

                if (copyRightId != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getCOPY_RIGHT_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The COPY_RIGHT_ID is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getCOPY_RIGHT_ID(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getCOPY_RIGHT_ID());

                }
                Log.info("ID => "+copyRightId+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_copyright_owner")){
                    Log.info("ID => "+copyRightId+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + copyRightId,
                                dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }else{
                    Log.info("ID => "+copyRightId+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + copyRightId,
                                dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+copyRightId+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + copyRightId,
                                dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }

                Log.info("ID => "+copyRightId+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => "+copyRightId+
                        " B_UPDDATE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+copyRightId+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => "+copyRightId+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                Log.info("ID => "+copyRightId+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => "+copyRightId+
                        " NAME => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getNAME()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getNAME());


                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getNAME() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getNAME().equals("null"))) {
                    Assert.assertEquals("The NAME is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getNAME(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getNAME());
                }
                Log.info("ID => "+copyRightId+
                        " S_NAME => EPH="+dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getS_NAME()+
                        " DL="+dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getS_NAME());

                if (dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getS_NAME() != null ||
                        (!dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getS_NAME().equals("null"))) {
                    Assert.assertEquals("The S_NAME is incorrect for id=" + copyRightId,
                            dataQualityDLContext.tbEventDataObjectsFromEPH.get(i).getS_NAME(),
                            dataQualityDLContext.tbEventDataObjectsFromDL.get(i).getS_NAME());
                }

            }
        }

    }




}
