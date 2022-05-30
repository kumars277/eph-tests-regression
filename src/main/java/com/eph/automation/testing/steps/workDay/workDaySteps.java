package com.eph.automation.testing.steps.workDay;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.And;
import cucumber.api.java.en.When;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.workdaySQL.workDayChecksSQL;
import com.eph.automation.testing.models.contexts.WorkdayDLContext;
import com.eph.automation.testing.models.dao.workDayObjects.workDayDLAccessObject;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class workDaySteps {

    private static String wdInboundCountSql;
    private static int wdInboundCount;
    private static int wdRefViewCount;
    private static String wdRefViewSql;
    private static String sql;
    private static List<String> ids;


    @Given("^Get the total count of workday_data_orgdata$")
    public static void getWorkdayIndoundCount() {
        Log.info("Getting workday inbound table count...");
        wdInboundCountSql = workDayChecksSQL.GET_WORKDAY_INBOUND_COUNT_SQL;
        List<Map<String, Object>> wdInboundSrcTableCount = DBManager.getDLResultMap(wdInboundCountSql, Constants.AWS_URL);
        wdInboundCount = ((Long) wdInboundSrcTableCount.get(0).get("Source_Count")).intValue();
        Log.info(wdInboundCountSql);
    }

    @Then("^Get total count of workday_reference_v$")
    public static void getWorkdayRefCount() {
        Log.info("Getting workday_reference_v table count...");
        wdRefViewSql = workDayChecksSQL.GET_WORKDAY_REF_COUNT_SQL;
        List<Map<String, Object>> wdRefTableCount = DBManager.getDLResultMap(wdRefViewSql, Constants.AWS_URL);
        wdRefViewCount = ((Long) wdRefTableCount.get(0).get("Target_Count")).intValue();
        Log.info(wdRefViewSql);
    }

    @And("^Compare count of workday_data_orgdata and workday_reference_v$")
    public void compareInboundRefCount() {
        Log.info("The count for workday_data_orgdata => " + wdInboundCount + " and in workday_reference_v$ => " + wdRefViewCount);
        Assert.assertEquals("The counts are not equal when compared with workday_data_orgdata and workday_reference_v", wdRefViewCount, wdInboundCount);
    }

    @Given("^Get the (.*) random ids from workday_data_orgdata$")
    public void getRandomIds(String numberOfRecords) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random workday inbound emplid ids...");
        sql = String.format(workDayChecksSQL.GET_WORKDAY_INBOUND_IDS_SQL, numberOfRecords);
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^Records from inbound table workday_data_orgdata$")
    public void getInboundRecords() {
        Log.info("We get the workday_data_orgdata records...");
        sql = String.format(workDayChecksSQL.GET_WORKDAY_INBOUND_RECS_SQL, String.join("','",ids));
        WorkdayDLContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, workDayDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When("^Records from workday_reference_v$")
    public void getWorkDayRefRecords() {
        Log.info("We get the workday_reference_v records...");
        sql = String.format(workDayChecksSQL.GET_WORKDAY_REF_RECS_SQL, String.join("','",ids));
        WorkdayDLContext.recordsFromWorkDayRefData = DBManager.getDBResultAsBeanList(sql, workDayDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare the records for workday_data_orgdata and workday_reference_v$")
    public void compareRecords() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (WorkdayDLContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < WorkdayDLContext.recordsFromInboundData.size(); i++) {


                Log.info("comparing inbound and workday reference records...");
                WorkdayDLContext.recordsFromInboundData.sort(Comparator.comparing(workDayDLAccessObject::getsourceref)); //sort primarykey data in the lists
                WorkdayDLContext.recordsFromWorkDayRefData.sort(Comparator.comparing(workDayDLAccessObject::getsourceref));

                String[] workDayRef = {"getsourceref", "getgiven_name", "getfamily_name","getpeoplehub_id",
                        "getemail","getend_date"};
                for (String strTemp : workDayRef) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;

                    workDayDLAccessObject objectToCompare1 = WorkdayDLContext.recordsFromInboundData.get(i);
                    workDayDLAccessObject objectToCompare2 = WorkdayDLContext.recordsFromWorkDayRefData.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("sourceref => " + WorkdayDLContext.recordsFromInboundData.get(i).getsourceref() +
                            " " + strTemp + " => workday_org_data = " + method.invoke(objectToCompare1) +
                            " workday_ref_view = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in workday_ref_view for sourceref:" + WorkdayDLContext.recordsFromInboundData.get(i).getsourceref(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }


}


