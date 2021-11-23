package com.eph.automation.testing.steps.erms;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.ErmsEtlAccessDLContext;
import com.eph.automation.testing.services.db.ermsDataLakeSQL.ErmsEtlChecksSql;
import com.eph.automation.testing.models.dao.erms.ErmsDLAccessObject;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;


public class ERMSEtlChecksSteps {

    private static String ermsCurrentSQLCount;
    private static String ermsInboundSQLCount;
    private static String ermsLatestSQLCount;
    private static String ermsDuplicateLatestSQLCount;
    private static String ermsDeltaAndExclSQLCount;
    private static String ermstransformPartitionSQLCount;
    private static String ermstransformSQLCount;
    private static int ermsInboundCount;
    private static int ermsCurrentCount;
    private static int ermsTransformFileCount;
    private static int ermsTransformPartCount;
    private static int ermsLatestCount;
    private static int ermsDeltaCurrAndExclCount;
    private static int ermsDuplicateLatestCount;
    private static String noTablemsg = "no table found";
    private static String sql;
    private static List<String> ids;

    @Given("^Get the total count of ERMS Data from Inbound Load (.*)$")
    public static void getCountERMSInboundTables(String tableName){
        switch (tableName){
            case "erms_transform_current_work_identifier":
                Log.info("Getting Inbound Count for work_identifier...");
                ermsInboundSQLCount = ErmsEtlChecksSql.GET_WORK_IDENTIFIER_INBOUND_COUNT;
                break;

            case "erms_transform_current_work_person_role":
                Log.info("Getting Inbound Count for work person role...");
                ermsInboundSQLCount = ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_INBOUND_COUNT;
                break;

            default:
                Log.info(noTablemsg);

        }
        Log.info(ermsInboundSQLCount);
        List<Map<String, Object>> ermsInboundTableCount = DBManager.getDBResultMap(ermsInboundSQLCount, Constants.AWS_URL);
        ermsInboundCount = ((Long) ermsInboundTableCount.get(0).get("Source_Count")).intValue();
    }


    @Then("^We know the total count of Current ERMS ETL data (.*)$")
    public static void getCountERMSCurrentTables(String tableName){
        switch (tableName){
            case "erms_transform_current_work_identifier":
                Log.info("Getting Inbound Count for work_identifier...");
                ermsCurrentSQLCount = ErmsEtlChecksSql.GET_WORK_IDENTIFIER_CURRENT_COUNT;
                break;

            case "erms_transform_current_work_person_role":
                Log.info("Getting Inbound Count for work person role...");
                ermsCurrentSQLCount = ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_CURRENT_COUNT;
                break;

            default:
                Log.info(noTablemsg);

        }
        Log.info(ermsCurrentSQLCount);
        List<Map<String, Object>> ermsCurrentTableCount = DBManager.getDBResultMap(ermsCurrentSQLCount, Constants.AWS_URL);
        ermsCurrentCount = ((Long) ermsCurrentTableCount.get(0).get("Target_Count")).intValue();
    }



    @And("^Compare count of ERMS Inbound load with current ERMS ETL table are identical (.*)$")
    public void compareInboundAndCurrentCounts(String tableName){
        Log.info("The count for table "+tableName+" => " + ermsCurrentCount + " and in Inbound  => " + ermsInboundCount);
        Assert.assertEquals("The counts are not equal when compared with "+tableName+" and Inbound ", ermsCurrentCount, ermsInboundCount);
    }

    @And("^Get the (.*) random EPR ids from the table ERMS inbound (.*)$")
    public void getRandomErmsEprIds(String numberOfRecords,String tableName){
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids from ERMS Inbound Tables....");
        List<Map<?, ?>> randomids;
        switch (tableName) {
            case "erms_transform_current_work_identifier":
                sql = String.format(ErmsEtlChecksSql.GET_RANDOM_WORK_IDENTIFIER_ID_INBOUND, numberOfRecords);
                break;
            case "erms_transform_current_work_person_role":
                sql = String.format(ErmsEtlChecksSql.GET_RANDOM_WORK_PERSON_ID_INBOUND, numberOfRecords);
                break;
            default:
                Log.info(noTablemsg);
        }
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^Get the data from the ERMS inbound tables (.*)$")
    public static void getERMSInboundRecords(String tableName) {
        Log.info("We get the erms inbound records...");
        switch (tableName) {
            case "erms_transform_current_work_identifier":
                sql = String.format(ErmsEtlChecksSql.GET_WORK_IDENTIFIER_REC_INBOUND_DATA, String.join("','",ids));
                break;
            case "erms_transform_current_work_person_role":
                sql = String.format(ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_INBOUND_DATA, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        ErmsEtlAccessDLContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, ErmsDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Get the data from the ERMS transform current tables (.*)$")
    public static void getERMSCurrentRecords(String tableName) {
        Log.info("We get the erms current records...");
        switch (tableName) {
            case "erms_transform_current_work_identifier":
                sql = String.format(ErmsEtlChecksSql.GET_WORK_IDENTIFIER_REC_CURRENT_DATA, String.join("','",ids));
                break;
            case "erms_transform_current_work_person_role":
                sql = String.format(ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_CURRENT_DATA, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        ErmsEtlAccessDLContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, ErmsDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^we compare the records of ERMS Inbound and ERMS current tables (.*)$")
    public void compareErmsInboundandCurrent(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (ErmsEtlAccessDLContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between Inbound and current...");
            for (int i = 0; i < ErmsEtlAccessDLContext.recordsFromInboundData.size(); i++) {
                switch (tableName) {
                    case "erms_transform_current_work_identifier":
                        Log.info("comparing inbound and etl_accountable_product_current_v records...");
                        ErmsEtlAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(ErmsDLAccessObject::getu_key)); //sort primarykey data in the lists
                        ErmsEtlAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(ErmsDLAccessObject::getu_key));

                        String[] allWorkIdentifierCol = {"getepr_id", "geterms_id", "getu_key"};
                        for (String strTemp : allWorkIdentifierCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            ErmsDLAccessObject objectToCompare1 = ErmsEtlAccessDLContext.recordsFromInboundData.get(i);
                            ErmsDLAccessObject objectToCompare2 = ErmsEtlAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("epr_id => " + ErmsEtlAccessDLContext.recordsFromInboundData.get(i).getepr_id() +
                                    " " + strTemp + " => ERMS Inbound = " + method.invoke(objectToCompare1) +
                                    " work_identifier = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_identifier for uKey:" + ErmsEtlAccessDLContext.recordsFromInboundData.get(i).getepr_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "erms_transform_current_work_person_role":
                        Log.info("erms_transform_current_work_person_role records.... ");
                        ErmsEtlAccessDLContext.recordsFromInboundData.sort(Comparator.comparing(ErmsDLAccessObject::getepr_id)); //sort primarykey data in the lists
                        ErmsEtlAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(ErmsDLAccessObject::getepr_id));

                        String[] allWorkPersonRoleCol = {"getepr_id", "getu_key", "getwork_source_ref", "geterms_person_ref", "getperson_source_ref", "getf_role", "getemail","getname","getstaff_user","geteffective_start_date","geteffective_end_date","getmodified_date","getis_deleted"};
                        for (String strTemp : allWorkPersonRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            ErmsDLAccessObject objectToCompare1 = ErmsEtlAccessDLContext.recordsFromInboundData.get(i);
                            ErmsDLAccessObject objectToCompare2 = ErmsEtlAccessDLContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("work_ID => " + ErmsEtlAccessDLContext.recordsFromInboundData.get(i).getepr_id() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " work_person_role = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in work_person_role for workId:" + ErmsEtlAccessDLContext.recordsFromInboundData.get(i).getepr_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;
                    default:
                        Log.info(noTablemsg);
                }
            }
        }
    }


    @Then("^We know the total count of erms transform file (.*)$")
    public static void getCountERMSTransformFileTables(String tableName){
        switch (tableName){
            case "erms_transform_file_history_work_identifier_part":
                Log.info("Getting tranform file Count for work_identifier...");
                ermstransformSQLCount = ErmsEtlChecksSql.GET_WORK_IDENTIFIER_TRANSFORM_FILE_COUNT;
                break;

            case "erms_transform_file_history_work_person_role_part":
                Log.info("Getting transform file Count for work person role...");
                ermstransformSQLCount = ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_TRANSFORM_FILE_COUNT;
                break;

            default:
                Log.info(noTablemsg);

        }
        Log.info(ermstransformSQLCount);
        List<Map<String, Object>> ermsTransFileTableCount = DBManager.getDBResultMap(ermstransformSQLCount, Constants.AWS_URL);
        ermsTransformFileCount = ((Long) ermsTransFileTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of ERMS current and the ERMS transform file table are identical (.*)(.*)$")
    public void compareCurrentAndTransformFileCounts(String srcTable, String trgtTable){
        Log.info("The count for "+srcTable+" => " + ermsCurrentCount + " and in "+trgtTable+" => " +ermsTransformFileCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable,ermsTransformFileCount , ermsCurrentCount);
    }

    @Given("^Get the (.*) random EPR ids from the current table (.*)$")
    public void getRandomErmsCurrIds(String numberOfRecords,String tableName){
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids from ERMS Current Tables....");
        List<Map<?, ?>> randomids;
        switch (tableName) {
            case "erms_transform_current_work_identifier":
                sql = String.format(ErmsEtlChecksSql.GET_RANDOM_WORK_IDENTIFIER_ID_CURRENT, numberOfRecords);
                break;
            case "erms_transform_current_work_person_role":
                sql = String.format(ErmsEtlChecksSql.GET_RANDOM_WORK_PERSON_ID_CURRENT, numberOfRecords);
                break;
            default:
                Log.info(noTablemsg);
        }
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }


    @Then("^Get the data from the ERMS transform files tables (.*)$")
    public static void getERMStransformFileRecords(String tableName) {
        Log.info("We get the erms transformFile records...");
        switch (tableName) {
            case "erms_transform_file_history_work_identifier_part":
                sql = String.format(ErmsEtlChecksSql.GET_WORK_IDENTIFIER_TRANSFORM_FILE_REC, String.join("','",ids));
                break;
            case "erms_transform_file_history_work_person_role_part":
                sql = String.format(ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_TRANSFORM_FILE_REC, String.join("','",ids));
                break;
            default:
                Log.info(noTablemsg);
        }
        ErmsEtlAccessDLContext.recFromTransformFile = DBManager.getDBResultAsBeanList(sql, ErmsDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^we compare the records of ERMS Current and ERMS tranform file tables (.*) and (.*)$")
    public void compareErmsCurrentandTransFile(String srctableName, String trgtTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (ErmsEtlAccessDLContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between Current and transform_file...");
            for (int i = 0; i < ErmsEtlAccessDLContext.recordsFromCurrent.size(); i++) {
                switch (srctableName) {
                    case "erms_transform_current_work_identifier":
                        Log.info("comparing "+srctableName+" and "+trgtTable+" records...");
                        ErmsEtlAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(ErmsDLAccessObject::getu_key)); //sort primarykey data in the lists
                        ErmsEtlAccessDLContext.recFromTransformFile.sort(Comparator.comparing(ErmsDLAccessObject::getu_key));

                        String[] allWorkIdentifierCol = {"getepr_id", "geterms_id", "getu_key"};
                        for (String strTemp : allWorkIdentifierCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            ErmsDLAccessObject objectToCompare1 = ErmsEtlAccessDLContext.recordsFromCurrent.get(i);
                            ErmsDLAccessObject objectToCompare2 = ErmsEtlAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("epr_id => " + ErmsEtlAccessDLContext.recordsFromCurrent.get(i).getepr_id() +
                                    " " + strTemp + " => "+srctableName+" = " + method.invoke(objectToCompare1) +
                                    " "+trgtTable+" = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+trgtTable+" for uKey:" + ErmsEtlAccessDLContext.recordsFromCurrent.get(i).getepr_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "erms_transform_current_work_person_role":
                        Log.info("comparing "+srctableName+" and "+trgtTable+" records...");
                        ErmsEtlAccessDLContext.recordsFromCurrent.sort(Comparator.comparing(ErmsDLAccessObject::getepr_id)); //sort primarykey data in the lists
                        ErmsEtlAccessDLContext.recFromTransformFile.sort(Comparator.comparing(ErmsDLAccessObject::getepr_id));

                        String[] allWorkPersonRoleCol = {"getepr_id", "getu_key", "getwork_source_ref", "geterms_person_ref", "getperson_source_ref", "getf_role", "getemail","getname","getstaff_user","geteffective_start_date","geteffective_end_date","getmodified_date","getis_deleted"};
                        for (String strTemp : allWorkPersonRoleCol) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            ErmsDLAccessObject objectToCompare1 = ErmsEtlAccessDLContext.recordsFromCurrent.get(i);
                            ErmsDLAccessObject objectToCompare2 = ErmsEtlAccessDLContext.recFromTransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("work_ID => " + ErmsEtlAccessDLContext.recordsFromCurrent.get(i).getepr_id() +
                                    " " + strTemp + " => "+srctableName+" = " + method.invoke(objectToCompare1) +
                                    " "+trgtTable+" = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in "+trgtTable+" for workId:" + ErmsEtlAccessDLContext.recordsFromCurrent.get(i).getepr_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;
                    default:
                        Log.info(noTablemsg);
                }
            }
        }
    }



    @Then("^We know the total count of erms transform partition history (.*)$")
    public static void getCountERMSTransformPartitionHistTables(String tableName){
        switch (tableName){
            case "erms_transform_history_work_identifier_part":
                Log.info("Getting tranform Partition Count for work_identifier...");
                ermstransformPartitionSQLCount = ErmsEtlChecksSql.GET_WORK_IDENTIFIER_TRANSFORM_PARTITION_COUNT;
                break;

            case "erms_transform_history_work_person_role_part":
                Log.info("Getting transform Partition Count for work person role...");
                ermstransformPartitionSQLCount = ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_TRANSFORM_PARTITION_COUNT;
                break;

            default:
                Log.info(noTablemsg);

        }
        Log.info(ermstransformPartitionSQLCount);
        List<Map<String, Object>> ermsTransPartTableCount = DBManager.getDBResultMap(ermstransformPartitionSQLCount, Constants.AWS_URL);
        ermsTransformPartCount = ((Long) ermsTransPartTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of ERMS current and the ERMS transform partition history table are identical (.*)(.*)$")
    public void compareCurrentAndTransformPartitionCounts(String srcTable, String trgtTable){
        Log.info("The count for "+srcTable+" => "+ermsCurrentCount+" and the "+trgtTable+" => " +ermsTransformPartCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and "+trgtTable,ermsTransformPartCount , ermsCurrentCount);

    }

    @Then("^We know the total count of latest ERMS ETL data (.*)$")
    public static void getCountERMSLatestTables(String tableName){
        switch (tableName){
            case "erms_transform_latest_work_identifier":
                Log.info("Getting Latest Count for work_identifier...");
                ermsLatestSQLCount = ErmsEtlChecksSql.GET_WORK_IDENTIFIER_LATEST_COUNT;
                break;

            case "erms_transform_latest_work_person_role":
                Log.info("Getting Latest Count for work person role...");
                ermsLatestSQLCount = ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_LATEST_COUNT;
                break;

            default:
                Log.info(noTablemsg);

        }
        Log.info(ermsLatestSQLCount);
        List<Map<String, Object>> ermsLatestTableCount = DBManager.getDBResultMap(ermsLatestSQLCount, Constants.AWS_URL);
        ermsLatestCount = ((Long) ermsLatestTableCount.get(0).get("Target_Count")).intValue();
    }

    @Then("^We know the total count of erms delta current and exclude tables (.*)$")
    public static void getCountERMSDeltaAndExclTables(String tableName){
        switch (tableName){
            case "erms_transform_latest_work_identifier":
                Log.info("Getting Latest Count for work_identifier...");
                ermsDeltaAndExclSQLCount = ErmsEtlChecksSql.GET_WORK_IDENTIFIER_DELTA_AND_EXCL_COUNT;
                break;

            case "erms_transform_latest_work_person_role":
                Log.info("Getting Latest Count for work person role...");
                ermsDeltaAndExclSQLCount = ErmsEtlChecksSql.GET_WORK_PERSON_ROLE_DELTA_AND_EXCL_COUNT;
                break;

            default:
                Log.info(noTablemsg);

        }
        Log.info(ermsDeltaAndExclSQLCount);
        List<Map<String, Object>> ermsDeltCurrAndExclTableCount = DBManager.getDBResultMap(ermsDeltaAndExclSQLCount, Constants.AWS_URL);
        ermsDeltaCurrAndExclCount = ((Long) ermsDeltCurrAndExclTableCount.get(0).get("Source_Count")).intValue();
    }

    @And("^Compare count of ERMS latest with the delta current and exclude tables are identical (.*)$")
    public void compareLatestTableCounts(String srcTable){
        Log.info("The count for "+srcTable+" => "+ermsLatestCount+" and the sum of delta and Exclude => " +ermsDeltaCurrAndExclCount);
        Assert.assertEquals("The counts are not equal when compared with "+srcTable+" and the sum of delta and Exclude",ermsDeltaCurrAndExclCount, ermsLatestCount);
    }


    @Given("^Get the ERMS Duplicate count in latest tables (.*)$")
    public static void getDuplicateCount(String tableName){
        switch (tableName){
            case "erms_transform_latest_work_identifier":
                Log.info("Getting Duplicate Acc Prod Latest Table Count...");
                ermsDuplicateLatestSQLCount = ErmsEtlChecksSql.GET_DUPLICATES_LATEST_WORK_IDENTIFIER_COUNT;
                break;
            case "erms_transform_latest_work_person_role":
                Log.info("Getting Duplicate MANIF Latest Table Count...");
                ermsDuplicateLatestSQLCount = ErmsEtlChecksSql.GET_DUPLICATES_LATEST_WORK_PERSON_ROLE_COUNT;
                break;
            default:
                Log.info(noTablemsg);

        }
        Log.info(ermsDuplicateLatestSQLCount);
        List<Map<String, Object>> ermsDupLatestTableCount = DBManager.getDBResultMap(ermsDuplicateLatestSQLCount, Constants.AWS_URL);
       ermsDuplicateLatestCount = ((Long) ermsDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the ERMS latest count should be equal to Zero (.*)$")
    public void checkDupCountErmsZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + ermsDuplicateLatestCount);
        Assert.assertEquals("There are Duplicate Count of "+tableName,0,ermsDuplicateLatestCount);

    }


}
