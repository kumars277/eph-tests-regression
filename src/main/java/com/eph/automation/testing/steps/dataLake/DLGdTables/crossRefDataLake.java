package com.eph.automation.testing.steps.dataLake.DLGdTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.GDTablesDLSQLContext;
import com.eph.automation.testing.models.dao.GDTableDLSQL.GDTableDLSQLObject;
import com.eph.automation.testing.services.db.gdSQLDLSQL.crossRefDLSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class crossRefDataLake {
    private static String sql;
    private static List<String> Ids;
    private static String sqlcrossRefgdDLCount;
    private static String sqlGdWorkMAnifProdIdentifierCounts;
    private static int crossRefgdDLCount;
    private static int gdTablePosgreSQLCount;
    private static int gdWorkMAnifProdIdentifierSQLCount;
    private static String sqlGdPostgresCounts;
    private static int IdentifierCrossRefSQLCount;
    private static String sqlIdentifierCrossRefCounts;


    @Given("^We get the count (.*) from semarchy_eph_mdm$")
    public void getTheCountfromSemarchy(String table) {
        // Log.info("Getting the count from the PostgreSQL");
        switch (table) {
            case "gd_manifestation":
                sqlGdPostgresCounts = crossRefDLSQL.GET_MANIF_GD_SQL_COUNT;
                break;
            case "gd_manifestation_identifier":
                sqlGdPostgresCounts = crossRefDLSQL.GET_MANIF_ID_GD_SQL_COUNT;
                break;
            case "gd_product":
                sqlGdPostgresCounts = crossRefDLSQL.GET_PROD_GD_SQL_COUNT;
                break;
            case "gd_product_identifier":
                sqlGdPostgresCounts = crossRefDLSQL.GET_PROD_IDENTIF_GD_SQL_COUNT;
                break;
            case "gd_work_identifier":
                sqlGdPostgresCounts = crossRefDLSQL.GET_WORK_IDENTF_GD_SQL_COUNT;
                break;
            case "gd_wwork":
                sqlGdPostgresCounts = crossRefDLSQL.GET_WORK_SQL_COUNT;
                break;
        }
        Log.info(sqlGdPostgresCounts);
        List<Map<String, Object>> gdTableSqlCount = DBManager.getDBResultMap(sqlGdPostgresCounts, Constants.EPH_URL);
        gdTablePosgreSQLCount = ((Long) gdTableSqlCount.get(0).get("Source_Count")).intValue();
    }


    @Then("^Get the count of (.*) from the CrossRef Schema from DL$")
    public void getCountcrossRefGdTables(String SemarchyTable) {
        // Log.info("Getting the count from the DL");
        switch (SemarchyTable) {
            case "gd_manifestation":
                sqlcrossRefgdDLCount = crossRefDLSQL.GET_MANIF_GD_DL_COUNT;
                break;
            case "gd_manifestation_identifier":
                sqlcrossRefgdDLCount = crossRefDLSQL.GET_MANIF_ID_GD_DL_COUNT;
                break;
            case "gd_product":
                sqlcrossRefgdDLCount = crossRefDLSQL.GET_PROD_GD_DL_COUNT;
                break;
            case "gd_product_identifier":
                sqlcrossRefgdDLCount = crossRefDLSQL.GET_PROD_IDENTIF_GD_DL_COUNT;
                break;
            case "gd_work_identifier":
                sqlcrossRefgdDLCount = crossRefDLSQL.GET_WORK_IDENTF_GD_DL_COUNT;
                break;
            case "gd_wwork":
                sqlcrossRefgdDLCount = crossRefDLSQL.GET_WORK_DL_COUNT;
                break;
        }
        Log.info(sqlcrossRefgdDLCount);
        List<Map<String, Object>> crossRefgdDLTableCount = DBManager.getDBResultMap(sqlcrossRefgdDLCount, Constants.AWS_URL);
        crossRefgdDLCount = ((Long) crossRefgdDLTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^we compare crossRef Gd and Semarchy GD counts are identical (.*)$")
    public void compareSemarchyandCrossRefCounts(String srctable) {
        Log.info("The count for " + srctable + " in SQL => " + gdTablePosgreSQLCount + " and in Cross_Ref  => " + crossRefgdDLCount);
        Assert.assertEquals("The counts are not equal for " + srctable + " in Posgres and CrossRef DL ", crossRefgdDLCount, gdTablePosgreSQLCount);
    }

    @Given("^Get (.*) random ids of (.*) from the Semarchy$")
    public void getIds(String numberOfRecords, String SourceTable) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (SourceTable) {
            case "gd_manifestation":
                sql = String.format(crossRefDLSQL.GET_GD_MANIF_IDS, numberOfRecords);
                List<Map<?, ?>> randomManifIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManifIds.stream().map(m -> (String) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_manifestation_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_MANIF_IDENTIF_IDS, numberOfRecords);
                List<Map<?, ?>> randomManifIdentifierIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManifIdentifierIds.stream().map(m -> (BigDecimal) m.get("manif_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product":
                sql = String.format(crossRefDLSQL.GET_GD_PRODUCT_IDS, numberOfRecords);
                List<Map<?, ?>> randomPriceIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomPriceIds.stream().map(m -> (String) m.get("product_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_PRODUCT_IDENTIF_IDS, numberOfRecords);
                List<Map<?, ?>> randomProdIdentifIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProdIdentifIds.stream().map(m -> (BigDecimal) m.get("product_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_WORK_IDENTIFIER_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkIdentifierIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkIdentifierIds.stream().map(m -> (BigDecimal) m.get("work_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_wwork":
                sql = String.format(crossRefDLSQL.GET_GD_WORK_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkIds.stream().map(m -> (String) m.get("work_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "eph_identifier_cross_reference":
                sql = String.format(crossRefDLSQL.GET_IDENTIFIER, numberOfRecords);
                List<Map<?, ?>> randomIdentifiersIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomIdentifiersIds.stream().map(m -> (String) m.get("identifier")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the records for (.*) from semarchy$")
    public void getRecordsSemarchy(String SemarchyTable) {
        Log.info("We get the records from Semarchy..");
        switch (SemarchyTable) {
            case "gd_manifestation":
                sql = String.format(crossRefDLSQL.GET_GD_MANIFESTATION, Joiner.on("','").join(Ids));
                break;
            case "gd_manifestation_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_product":
                sql = String.format(crossRefDLSQL.GET_GD_PRODUCT, Joiner.on("','").join(Ids));
                break;
            case "gd_product_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_PROD_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_work_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_WORK_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_wwork":
                sql = String.format(crossRefDLSQL.GET_GD_WWORK, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        GDTablesDLSQLContext.recordsFromSql = DBManager.getDBResultAsBeanList(sql, GDTableDLSQLObject.class, Constants.EPH_URL);
    }

    @When("^Get the records for (.*) from the crossRef DL$")
    public void getRecordsCrossRefDL(String SemarchyTable) {
        Log.info("We get the records from CrossRef..");
        switch (SemarchyTable) {
            case "gd_manifestation":
                sql = String.format(crossRefDLSQL.GET_GD_MANIFESTATION_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_manifestation_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_MANIFESTATION_IDENTIFIER_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_product":
                sql = String.format(crossRefDLSQL.GET_GD_PRODUCT_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_product_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_PROD_IDENTIFIER_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_identifier":
                sql = String.format(crossRefDLSQL.GET_GD_WORK_IDENTIFIER_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_wwork":
                sql = String.format(crossRefDLSQL.GET_GD_WWORK_DL, Joiner.on("','").join(Ids));
                break;

        }
        Log.info(sql);
        GDTablesDLSQLContext.recordsFromDL = DBManager.getDBResultAsBeanList(sql, GDTableDLSQLObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of (.*) from Semarchy and crossRef DL$")
    public void compareSemarchyAndDL(String SourceTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.recordsFromSql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare Records in Semarchy and DL ..");
            for (int i = 0; i < GDTablesDLSQLContext.recordsFromSql.size(); i++) {
                switch (SourceTable) {
                    case "gd_manifestation":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getmanifestation_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getmanifestation_id));
                        String[] manif = {"getmanifestation_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "gets_manifestation_id", "getexternal_reference", "getmanifestation_key_title", "gets_manifestation_key_title", "getinter_edition_flag",
                                "getfirst_pub_date", "getlast_pub_date", "gett_event_description", "getf_type", "getf_status", "getf_format_type", "getf_wwork", "getf_t_event_type", "getf_event", "getf_self_one"};
                        for (String strTemp : manif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getinter_edition_flag") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id(),
                                            flagVal1, flagVal2);


                                }
                            } else {
                                Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_manifestation_identifier":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getmanif_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getmanif_identifier_id));
                        String[] manifIdentif = {"getmanif_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getidentifier", "gets_identifier", "geteffective_start_date", "geteffective_end_date",
                                "getf_type", "getf_manifestation", "getf_event", "getf_type", "getlead_indicator"};
                        for (String strTemp : manifIdentif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getlead_indicator") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id(),
                                            flagVal1, flagVal2);

                                }


                            } else {
                                Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_id));
                        String[] product = {"getproduct_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getname", "gets_name", "getshort_name", "gets_short_name",
                                "getseparately_sale_indicator", "gettrial_allowed_indicator", "getrestricted_sale_indicator", "getlaunch_date", "getcontent_from_date",
                                "getcontent_to_date", "getcontent_date_offset", "gett_summary_changed", "gett_event_description", "getf_type", "getf_status", "getf_status", "getf_accountable_product"
                                , "getf_tax_code", "getf_revenue_model", "getf_revenue_account", "getf_wwork", "getf_manifestation", "getf_t_event_type", "getf_event", "getf_self_one", "getf_self_two", "getf_self_three", "getf_self_four", "getf_self_five", "getf_self_six", "getf_self_seven"};
                        for (String strTemp : product) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prodId => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getseparately_sale_indicator" || method.getName() == "gettrial_allowed_indicator" || method.getName() == "getrestricted_sale_indicator" ||
                                    method.getName() == "gett_summary_changed") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("prodId => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id(),
                                            flagVal1, flagVal2);

                                }


                            } else {
                                Log.info("prodId => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_identifier":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_identifier_id));
                        String[] prodIdentifId = {"getproduct_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getidentifier", "gets_identifier", "getf_event", "getf_product", "geteffective_start_date", "geteffective_end_date", "getf_type"};
                        for (String strTemp : prodIdentifId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_identifier_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_identifier_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_identifier_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_identifier":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_identifier_id));
                        String[] workIdentifier = {"getwork_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getidentifier", "gets_identifier", "getf_wwork", "getf_type", "getf_event"};
                        for (String strTemp : workIdentifier) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_Identif_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_identifier_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_Identif_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_wwork":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_id));
                        String[] work = {"getwork_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "gets_work_id", "getexternal_reference", "getwork_title", "gets_work_title", "getwork_sub_title",
                                "gets_work_sub_title", "getwork_short_title", "gets_work_short_title", "getelectro_rights_indicator", "getvolume_name", "getcopyright_year", "getedition_number", "getplanned_launch_date", "getactual_launch_date", "getplanned_discontinue_date",
                                "getactual_discontinue_date", "gett_summary_changed", "gett_event_description", "getf_type", "getf_status", "getf_accountable_product", "getf_pmc", "getf_family", "getf_imprint",
                                "getf_legal_ownership", "getf_subscription_type", "getf_llanguage", "getf_t_event_type", "getf_event", "getf_self_one", "getf_self_two", "getf_self_three", "getf_self_four", "getf_self_five", "getf_self_six", "getf_self_seven", "getf_self_eight", "getf_self_nine", "getf_self_ten"};
                        for (String strTemp : work) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "gett_summary_changed" || method.getName() == "getelectro_rights_indicator") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("work_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id(),
                                            flagVal1, flagVal2);
                                }
                            } else {
                                Log.info("work_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                }
            }
        }
    }

    @Given("^We get the count from the work,manif and product identifiers$")
    public void getCountfromGD() {
        // Log.info("Getting the count from the PostgreSQL");
        sqlGdWorkMAnifProdIdentifierCounts = crossRefDLSQL.GD_WORK_MANIF_PROD_IDENTIFIERS_COUNT;
        Log.info(sqlGdWorkMAnifProdIdentifierCounts);
        List<Map<String, Object>> GdWorkMAnifProdIdentifierCountsTableSqlCount = DBManager.getDBResultMap(sqlGdWorkMAnifProdIdentifierCounts, Constants.AWS_URL);
        gdWorkMAnifProdIdentifierSQLCount = ((Long) GdWorkMAnifProdIdentifierCountsTableSqlCount.get(0).get("Source_Count")).intValue();
    }

    @Given("^Get the count from eph_identifier_cross_reference$")
    public void getCountfromcrossRefIdentifier() {
        sqlIdentifierCrossRefCounts = crossRefDLSQL.EPH_IDENTIFIER_CROSS_REF_COUNT;
        Log.info(sqlIdentifierCrossRefCounts);
        List<Map<String, Object>> IdentifierCrossRefTableSqlCount = DBManager.getDBResultMap(sqlIdentifierCrossRefCounts, Constants.AWS_URL);
        IdentifierCrossRefSQLCount = ((Long) IdentifierCrossRefTableSqlCount.get(0).get("Target_Count")).intValue();
    }

    @And("^we compare work,manifestation and product identifiers counts with eph_identifier_cross_reference$")
    public void compareIdentifierCrossRefCounts() {
        Log.info("The count for prod,manif and work Identifiers => " + gdWorkMAnifProdIdentifierSQLCount + " and in eph_identifier_cross_reference  => " + IdentifierCrossRefSQLCount);
        Assert.assertEquals("The counts are not equal for prod,manif and work Identifiers and eph_identifier_cross_reference", gdWorkMAnifProdIdentifierSQLCount, IdentifierCrossRefSQLCount);
    }


    @When("^We get the records from the work,manif and product identifiers$")
    public void getRecfromGdTableIdentifiers() {
        Log.info("We get the records from GD table work, manif and prod identifiers..");
        sql = String.format(crossRefDLSQL.GET_IDENTIFIER_RECS, Joiner.on("','").join(Ids));
        Log.info(sql);
        GDTablesDLSQLContext.recordsFromGDTableIdentifiers = DBManager.getDBResultAsBeanList(sql, GDTableDLSQLObject.class, Constants.AWS_URL);
    }

    @When("^Get the records from (.*)$")
    public void getRecfrom(String table) {
        Log.info("We get the records from "+table);
        sql = String.format(crossRefDLSQL.GET_CROSS_IDENTIFIER_RECS, Joiner.on("','").join(Ids));
        Log.info(sql);
        GDTablesDLSQLContext.recordsFromCrossRefIdentifier = DBManager.getDBResultAsBeanList(sql, GDTableDLSQLObject.class, Constants.AWS_URL);
    }


    @And("^Compare the records of work,manif and product identifiers with (.*)$")
    public void compareGDAndCrossRefRecs(String SourceTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.recordsFromGDTableIdentifiers.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare Records for " + SourceTable);
            for (int i = 0; i < GDTablesDLSQLContext.recordsFromGDTableIdentifiers.size(); i++) {
                GDTablesDLSQLContext.recordsFromGDTableIdentifiers.sort(Comparator.comparing(GDTableDLSQLObject::getidentifier)); //sort data in the lists
                GDTablesDLSQLContext.recordsFromCrossRefIdentifier.sort(Comparator.comparing(GDTableDLSQLObject::getidentifier));
                String[] manif = {"getidentifier", "getidentifier_type", "getrecord_level", "getepr", "getproduct_type", "getmanifestation_type"
                        , "getwork_type"};
                for (String strTemp : manif) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromGDTableIdentifiers.get(i);
                    GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromCrossRefIdentifier.get(i);
                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);
                    Log.info("identifier => " + GDTablesDLSQLContext.recordsFromGDTableIdentifiers.get(i).getidentifier() +
                            " " + strTemp + " => GD Tables=" + method.invoke(objectToCompare1) +
                            " " + strTemp + " cross_ref_identifier=" + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                        " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromGDTableIdentifiers.get(i).getidentifier(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }
}





