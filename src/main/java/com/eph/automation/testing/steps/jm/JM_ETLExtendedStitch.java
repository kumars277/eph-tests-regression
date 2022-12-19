package com.eph.automation.testing.steps.jm;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JMETL.JMETL_ExtendedAccessDLContext;
import com.eph.automation.testing.models.dao.JMDataLake.JM_ETLExtendedDLAccessObject;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JM_ETLExtendedCountDataChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import com.google.common.base.Joiner;

import java.util.*;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;

public class JM_ETLExtendedStitch {
    private static String JMExtendedSQLCurrentCount;
    private static int JMEXtCurrentCount;
    private static String JMsourceSQLCount;
    private static int JMSourceCurrentCount;
    private static String sql;
    private static List<String> Ids;

    @Given("^Get the total count of JM ETL Extended Table$")
    public void getJMETLExtendedCount() {
        Log.info("Getting JM ETL Extended Table Count...");
        JMExtendedSQLCurrentCount = JM_ETLExtendedCountDataChecksSQL.GET_JM_EXT_TableCount;
        Log.info(JMExtendedSQLCurrentCount);
        List<Map<String, Object>> JM_ETLEXTCurrentTableCount = DBManager.getDBResultMap(JMExtendedSQLCurrentCount, Constants.AWS_URL);
        JMEXtCurrentCount = ((Long) JM_ETLEXTCurrentTableCount.get(0).get("Count")).intValue();
    }

    @When("^We get the total count of Stitching table$")
    public void getJMStitchCount() {
        Log.info("Getting JM Stitch data Count...");
        JMsourceSQLCount = JM_ETLExtendedCountDataChecksSQL.GET_JMStitched_Count;
        Log.info(JMsourceSQLCount);
        List<Map<String, Object>> JM_ETLEXTSourceTableCount = DBManager.getDBResultMap(JMsourceSQLCount, Constants.AWS_URL);
        JMSourceCurrentCount = ((Long) JM_ETLEXTSourceTableCount.get(0).get("Count")).intValue();
    }

    @And("^Compare count of JM Stitch data and JM ETL Extended table are identical$")
    public void compareJMETLExtendedtoStitchedDataCounts() {
        Log.info("The count for JM ETL Extended data => " + JMEXtCurrentCount + " and in JM Stitch data   => " + JMSourceCurrentCount);
        Assert.assertEquals("The JM ETL Extended and JM Stitch counts are not equal ", JMEXtCurrentCount, JMSourceCurrentCount);
    }

    @Given("^Get the IDs (.*) of JM ETL Extended data$")
    public void getSomeRandomIds(String numberOfRecords) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for JM ETL Extended Tables....");
        List<Map<?, ?>> randomIds;
        sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_ETLExtended_IDs, numberOfRecords);
        randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("EPR_ID")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the Data from JMETL Extended Tables$")
    public void getJMETLExtendedsourceRecords() {
        Log.info("We get the JM ETL Extended source records...");
        sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JMETL_EXT_Records, Joiner.on("','").join(Ids));
        JMETL_ExtendedAccessDLContext.recordsFromSource = DBManager.getDBResultAsBeanList(sql, JM_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And ("^Data from the Stitching table$")
    public void getJMStitchRecords() {
        Log.info("We get the JM Stitch records...");
        sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_JM_ETLEXT_Stitched_Records, Joiner.on("','").join(Ids));
        JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment = DBManager.getDBResultAsBeanList(sql, JM_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare data of JM ETL Extended Tables and Stitching tables are identical$")
    public void compareJMandStitchRecords() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JMETL_ExtendedAccessDLContext.recordsFromSource.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records...");
            for (int i = 0; i < JMETL_ExtendedAccessDLContext.recordsFromSource.size(); i++) {
                JMETL_ExtendedAccessDLContext.recordsFromSource.sort(Comparator.comparing(JM_ETLExtendedDLAccessObject::getepr_id)); //sort primarykey data in the lists
                JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.sort(Comparator.comparing(JM_ETLExtendedDLAccessObject::getepr_id));
                String[] recordsComp = {"getepr_id", "getsource","getproduct_type","getlast_updated_date","getapplication_name","getdelta_answer_code_uk","getdelta_answer_code_us","getpublication_status_anz","getavailability_format"};
                for (String strTemp : recordsComp) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;

                    JM_ETLExtendedDLAccessObject objectToCompare1 = JMETL_ExtendedAccessDLContext.recordsFromSource.get(i);
                    JM_ETLExtendedDLAccessObject objectToCompare2 = JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("EPRID => " + JMETL_ExtendedAccessDLContext.recordsFromSource.get(i).getepr_id() +
                            " " + strTemp + " => source = " + method.invoke(objectToCompare1) +
                            " target = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in target table for EPRID:" + JMETL_ExtendedAccessDLContext.recordsFromSource.get(i).getepr_id(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }

}
