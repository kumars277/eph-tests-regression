package com.eph.automation.testing.steps.erms;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.ermsDataLakeSQL.ErmsEtlCountChecksSql;
import com.eph.automation.testing.services.db.sdrmsql.SDRMDataChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class ERMSCountChecksSteps {

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

    @Given("^Get the total count of ERMS Data from Inbound Load (.*)$")
    public static void getCountERMSInboundTables(String tableName){
        switch (tableName){
            case "erms_transform_current_work_identifier":
                Log.info("Getting Inbound Count for work_identifier...");
                ermsInboundSQLCount = ErmsEtlCountChecksSql.GET_WORK_IDENTIFIER_INBOUND_COUNT;
                break;

            case "erms_transform_current_work_person_role":
                Log.info("Getting Inbound Count for work person role...");
                ermsInboundSQLCount = ErmsEtlCountChecksSql.GET_WORK_PERSON_ROLE_INBOUND_COUNT;
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
                ermsCurrentSQLCount = ErmsEtlCountChecksSql.GET_WORK_IDENTIFIER_CURRENT_COUNT;
                break;

            case "erms_transform_current_work_person_role":
                Log.info("Getting Inbound Count for work person role...");
                ermsCurrentSQLCount = ErmsEtlCountChecksSql.GET_WORK_PERSON_ROLE_CURRENT_COUNT;
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


    @Then("^We know the total count of erms transform file (.*)$")
    public static void getCountERMSTransformFileTables(String tableName){
        switch (tableName){
            case "erms_transform_file_history_work_identifier_part":
                Log.info("Getting tranform file Count for work_identifier...");
                ermstransformSQLCount = ErmsEtlCountChecksSql.GET_WORK_IDENTIFIER_TRANSFORM_FILE_COUNT;
                break;

            case "erms_transform_file_history_work_person_role_part":
                Log.info("Getting transform file Count for work person role...");
                ermstransformSQLCount = ErmsEtlCountChecksSql.GET_WORK_PERSON_ROLE_TRANSFORM_FILE_COUNT;
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


    @Then("^We know the total count of erms transform partition history (.*)$")
    public static void getCountERMSTransformPartitionHistTables(String tableName){
        switch (tableName){
            case "erms_transform_history_work_identifier_part":
                Log.info("Getting tranform Partition Count for work_identifier...");
                ermstransformPartitionSQLCount = ErmsEtlCountChecksSql.GET_WORK_IDENTIFIER_TRANSFORM_PARTITION_COUNT;
                break;

            case "erms_transform_history_work_person_role_part":
                Log.info("Getting transform Partition Count for work person role...");
                ermstransformPartitionSQLCount = ErmsEtlCountChecksSql.GET_WORK_PERSON_ROLE_TRANSFORM_PARTITION_COUNT;
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
                ermsLatestSQLCount = ErmsEtlCountChecksSql.GET_WORK_IDENTIFIER_LATEST_COUNT;
                break;

            case "erms_transform_latest_work_person_role":
                Log.info("Getting Latest Count for work person role...");
                ermsLatestSQLCount = ErmsEtlCountChecksSql.GET_WORK_PERSON_ROLE_LATEST_COUNT;
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
                ermsDeltaAndExclSQLCount = ErmsEtlCountChecksSql.GET_WORK_IDENTIFIER_DELTA_AND_EXCL_COUNT;
                break;

            case "erms_transform_latest_work_person_role":
                Log.info("Getting Latest Count for work person role...");
                ermsDeltaAndExclSQLCount = ErmsEtlCountChecksSql.GET_WORK_PERSON_ROLE_DELTA_AND_EXCL_COUNT;
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
                ermsDuplicateLatestSQLCount = ErmsEtlCountChecksSql.GET_DUPLICATES_LATEST_WORK_IDENTIFIER_COUNT;
                break;
            case "erms_transform_latest_work_person_role":
                Log.info("Getting Duplicate MANIF Latest Table Count...");
                ermsDuplicateLatestSQLCount = ErmsEtlCountChecksSql.GET_DUPLICATES_LATEST_WORK_PERSON_ROLE_COUNT;
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
