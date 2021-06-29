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
import org.junit.Assert;
import com.google.common.base.Joiner;

import java.util.*;
import java.io.FileReader;
import java.io.BufferedReader;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;

public class JM_ETLExtendedCount_DataChecksSteps {
    public JMETL_ExtendedAccessDLContext dataQualityBCSContext;
    private static String JMExtendedSQLCurrentCount;
    private static int JMEXtCurrentCount;
    private static String JMsourceSQLCount;
    private static int JMSourceCurrentCount;
    private static String sql;
    private static List<String> Ids;


    static Object[][] expectedIdentifiersFields;
    static Object[][] loadedIdentifiersFields;


    @Given("^Get the total count of JM ETL Extended Tables (.*)$")
    public void getJMExtendedCount(String tableName) {
        switch (tableName) {
            case "jnl_new_fulfilment_system":
                Log.info("Getting jm Extended new Fullfilment system Table Count...");
                JMExtendedSQLCurrentCount = JM_ETLExtendedCountDataChecksSQL.GET_JM_EXT_NEW_FULFIL_SYSTEM_COUNT;
                break;
            case "jnl_fulfilment_system":
                Log.info("Getting jm Extended Fullfilment system Table Count...");
                JMExtendedSQLCurrentCount = JM_ETLExtendedCountDataChecksSQL.GET_JM_EXT_FULFIL_SYSTEM_COUNT;
                break;

        }
        Log.info(JMExtendedSQLCurrentCount);
        List<Map<String, Object>> JM_ETLEXTCurrentTableCount = DBManager.getDBResultMap(JMExtendedSQLCurrentCount, Constants.AWS_URL);
        JMEXtCurrentCount = ((Long) JM_ETLEXTCurrentTableCount.get(0).get("Target_Count")).intValue();
    }

    @Given("^We know the total count of JM source tables (.*)$")
    public void getJMExtSourceCount(String tableName) {
        switch (tableName) {
            case "jnl_new_fulfilment_system":
                Log.info("Getting source Count...");
                JMsourceSQLCount = JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_EXT_NEW_FULFIL_SYSTEM_COUNT;
                break;
            case "jnl_fulfilment_system":
                Log.info("Getting source Count...");
                JMsourceSQLCount = JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_EXT_FULFIL_SYSTEM_COUNT;
                break;
        }
        Log.info(JMsourceSQLCount);
        List<Map<String, Object>> JM_ETLEXTSourceTableCount = DBManager.getDBResultMap(JMsourceSQLCount, Constants.AWS_URL);
        JMSourceCurrentCount = ((Long) JM_ETLEXTSourceTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of JM source Inbound and JM ETL Extended (.*) tables are identical$")
    public void compareJMETLExtendedCounts(String tableName) {
        Log.info("The count for table " + tableName + " => " + JMEXtCurrentCount + " and in Source  => " + JMSourceCurrentCount);
        Assert.assertEquals("The counts are not equal when compared with " + tableName + " and source ", JMEXtCurrentCount, JMSourceCurrentCount);
    }

    @Given("^Get the (.*) of JM ETL Extended data from Inbound Tables (.*)$")
    public void getRandomIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for jm ETL Extended Tables....");
        List<Map<?, ?>> randomIds;
        switch (tableName) {
            case "jnl_new_fulfilment_system":
                sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_EXT_NEW_FULFIL_SYSTEM_COUNT_RAND_ID, numberOfRecords);
                break;
            case "jnl_fulfilment_system":
                sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_EXT_FULFIL_SYSTEM_RAND_ID, numberOfRecords);
                break;
        }
        randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the Data from the Source Tables (.*)$")
    public void getsourceRecords(String tableName) {
        Log.info("We get the jm source records...");
        switch (tableName) {
            case "jnl_new_fulfilment_system":
                sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_EXT_NEW_FULFIL_SYSTEM_REC, Joiner.on("','").join(Ids));
                break;
            case "jnl_fulfilment_system":
                sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_EXT_FULFIL_SYSTEM_REC, Joiner.on("','").join(Ids));
                break;
        }
        JMETL_ExtendedAccessDLContext.recordsFromSource = DBManager.getDBResultAsBeanList(sql, JM_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Data from the JM ETL Extended Tables to compare source Check (.*)$")
    public void getTargetRecords(String tableName) {
        Log.info("We get the jm target records...");
        switch (tableName) {
            case "jnl_new_fulfilment_system":
                sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_JM_EXT_NEW_FULFIL_SYSTEM_REC, Joiner.on("','").join(Ids));
                break;
            case "jnl_fulfilment_system":
                sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_JM_EXT_FULFIL_SYSTEM_REC, Joiner.on("','").join(Ids));
                break;
        }
        JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment = DBManager.getDBResultAsBeanList(sql, JM_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare data of JM ETL Extended Tables and source (.*) tables are identical$")
    public void compareRecords(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JMETL_ExtendedAccessDLContext.recordsFromSource.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records...");
            for (int i = 0; i < JMETL_ExtendedAccessDLContext.recordsFromSource.size(); i++) {
                switch (tableName) {
                    case "jnl_new_fulfilment_system":
                        JMETL_ExtendedAccessDLContext.recordsFromSource.sort(Comparator.comparing(JM_ETLExtendedDLAccessObject::getepr_id)); //sort primarykey data in the lists
                        JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.sort(Comparator.comparing(JM_ETLExtendedDLAccessObject::getepr_id));
                        String[] recordsComp = {"getepr_id", "getproduct_type", /*"getlast_updated_date",*/ "getapplication_name", "getavailability_start_date", "getavailability_status", "getdelete_flag"};
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
                        break;

                    case "jnl_fulfilment_system":
                        JMETL_ExtendedAccessDLContext.recordsFromSource.sort(Comparator.comparing(JM_ETLExtendedDLAccessObject::getepr_id)); //sort primarykey data in the lists
                        JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.sort(Comparator.comparing(JM_ETLExtendedDLAccessObject::getepr_id));
                        String[] recordsomp = {"getissn",
                                "getapplication_code",
                                /*"getlast_updated_date",*/
                                "getepr_id",
                                "getproduct_type",
                                //"getavailability_start_date", "getavailability_status", "getdelete_flag"
                        };
                        for (String strTemp : recordsomp) {
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
                        break;
                }
            }
        }
    }
//Data Check with Excel

    //Method 1 using Hash Map
    @Given("^We have the expected data$")
    public void verifyData() throws Throwable {
        String splitBy = ",";
        String csvfile = "C:\\Users\\sureshkumard\\OneDrive - Reed Elsevier Group ICO Reed Elsevier Inc\\Desktop\\sample.csv";
            ArrayList<String> csvRows = new ArrayList<String>();
        List<String> reqId = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(csvfile));
        String line = "";
        HashMap<String, String> map = new HashMap<String, String>();
        HashMap<String, String> Dbval = new HashMap<String, String>();
        br.lines().skip(1);
        int i = 0;
        while ((line = br.readLine()) != null) {
            while ((line = br.readLine()) != null) {
                csvRows.add(line.toString());
                System.out.println(csvRows);
            }
        }
        for (i = 0; i < csvRows.size(); i++) {
            String[] testData = csvRows.get(i).split(splitBy);
            reqId.add(testData[2]);
        }
        sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_EXT_FULFIL_SYSTEM_REC, Joiner.on("','").join(reqId));
        JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment = DBManager.getDBResultAsBeanList(sql, JM_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(reqId.toString());
        Log.info(sql);
        for (int j = 0; j < csvRows.size(); j++) {
            String[] testData = csvRows.get(j).split(splitBy);
            map.put("issn", testData[0]);
            map.put("application_code", testData[1]);
            map.put("epr_id", testData[2]);
            map.put("product_type", testData[3]);
            Dbval.put("issn", JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(j).getissn());
            Dbval.put("application_code", JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(j).getapplication_code());
            Dbval.put("epr_id", JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(j).getepr_id());
            Dbval.put("product_type", JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(j).getproduct_type());
            ArrayList<String> firstMapValues = new ArrayList<>(map.values());
            ArrayList secondMapValues = new ArrayList<>(Dbval.values());
            Log.info("Excel => "+firstMapValues +" DB => "+secondMapValues);
            Assert.assertEquals(firstMapValues, secondMapValues);
        }
    }
//Method 2 using Objects 2d Array
        @Then("^Verify data with file upoad$")
        public void verifyDatassss() throws Throwable {
            String splitBy = ",";
            String csvfile = "C:\\Users\\sureshkumard\\OneDrive - Reed Elsevier Group ICO Reed Elsevier Inc\\Desktop\\sample.csv";
            ArrayList<String> csvRows = new ArrayList<String>();
            List<String> reqId = new ArrayList<String>();
            BufferedReader br = new BufferedReader(new FileReader(csvfile));
            String line = "";
            while ((line = br.readLine()) != null) {
                while ((line = br.readLine()) != null) {
                    csvRows.add(line.toString());
                }
            }
            for (int i = 0; i < csvRows.size(); i++) {
                String[] testDataId = csvRows.get(i).split(splitBy);
                reqId.add(testDataId[2]);
            }
            sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_SOURCE_JM_EXT_FULFIL_SYSTEM_REC, Joiner.on("','").join(reqId));
            JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment = DBManager.getDBResultAsBeanList(sql, JM_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
            Log.info(reqId.toString());
            Log.info(sql);
            for (int i = 0; i < JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.size(); i++) {
                String[] testDataVals = csvRows.get(i).split(splitBy);
                expectedIdentifiersFields = new Object[][]{{"issn", testDataVals[0]}, {"application_code", testDataVals[1]}, {"epr_id", testDataVals[2]},
                        {"product_type", testDataVals[3]}};
                loadedIdentifiersFields =
                        new Object[][]{{"issn", JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(i).getissn()}, {"application_code", JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(i).getapplication_code()},
                                {"epr_id", JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(i).getepr_id()}, {"product_type", JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(i).getproduct_type()}};
                for (int j = 0; j < loadedIdentifiersFields.length; j++) {
                    Log.info(loadedIdentifiersFields[j][0] + " = DB => " + loadedIdentifiersFields[j][1] + " CSV => " + expectedIdentifiersFields[j][1]);
                     String ExcelVal = String.valueOf(expectedIdentifiersFields[j][1]);
                     String DBVal = String.valueOf(loadedIdentifiersFields[j][1]);
                    Assert.assertEquals("Expected all columns to be with correct values loaded but for epr_id = "
                                    + JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(i).getepr_id() + " there are not equal values for the field " + expectedIdentifiersFields[j][0],
                            ExcelVal, DBVal);
                }

            }

        }


    //Count Check with Excel using Method 2
    @And("^Check the Count from Excel$")
    public void verifyCount()throws Throwable {
        String splitBy = ",";
        String csvfile = "C:\\Users\\sureshkumard\\OneDrive - Reed Elsevier Group ICO Reed Elsevier Inc\\Desktop\\sample.csv";
        ArrayList<String> csvRows = new ArrayList<String>();
        List<String> reqId = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(csvfile));
        String line = "";
        while ((line = br.readLine()) != null) {
            while ((line = br.readLine()) != null) {
                csvRows.add(line.toString());
            }
        }
        for (int i = 0; i < csvRows.size(); i++) {
            String[] testDataId = csvRows.get(i).split(splitBy);
                           reqId.add(testDataId[2]);
        }
        int expecTedCount = reqId.size();
        sql = String.format(JM_ETLExtendedCountDataChecksSQL.GET_JM_EXT_FULFIL_SYSTEM_Count_REC, Joiner.on("','").join(reqId));
        JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment = DBManager.getDBResultAsBeanList(sql, JM_ETLExtendedDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
        int actualCount = JMETL_ExtendedAccessDLContext.recordsFromJMFulFilment.get(0).geteprIdCount();
        Log.info("Expected Count: "+expecTedCount+" And Actual Count: "+actualCount);
        Assert.assertEquals("The excpected Count not matching with Actual",expecTedCount,actualCount);
    }



}



       /////////////////////////////////////////
        /*expectedIdentifiersFields = new Object[][]{{"issn", "03765040"}, {"application_code", "Delta"}, {"epr_id", "EPR-10GW8S"},
                {"product_type", "SUB"}};
        sql = JM_ETLExtendedCountDataChecksSQL.GET_JM_EXT_FULFIL_SYSTEM_Single_REC;
        JMETL_ExtendedAccessDLContext.recordsFromSingleJMFulFilment = DBManager.getDBResultAsBeanList(sql, JM_ETLExtendedDLAccessObject.class, Constants.AWS_URL);

        loadedIdentifiersFields = new Object[][]{{"issn", JMETL_ExtendedAccessDLContext.recordsFromSingleJMFulFilment.get(0).getissn()}, {"application_code", JMETL_ExtendedAccessDLContext.recordsFromSingleJMFulFilment.get(0).getapplication_code()},
                {"epr_id", JMETL_ExtendedAccessDLContext.recordsFromSingleJMFulFilment.get(0).getepr_id()},
                {"product_type", JMETL_ExtendedAccessDLContext.recordsFromSingleJMFulFilment.get(0).getproduct_type()}};

        for (int i = 0; i < expectedIdentifiersFields.length; i++) {
            Assert.assertEquals("Expected all columns to be with correct values loaded but for identifier with id = "
                            + JMETL_ExtendedAccessDLContext.recordsFromSingleJMFulFilment.get(0).getissn() + " there are not equal values for the field " + expectedIdentifiersFields[i][0],
                    expectedIdentifiersFields[i][1], loadedIdentifiersFields[i][1]);
        }*/






















