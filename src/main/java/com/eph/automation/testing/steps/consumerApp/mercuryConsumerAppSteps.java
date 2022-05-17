package com.eph.automation.testing.steps.consumerApp;


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.GDTablesDLSQLContext;
import com.eph.automation.testing.models.dao.consumerApp.r12Objects;
import com.eph.automation.testing.services.db.consumerAppSQL.mercuryChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class mercuryConsumerAppSteps {
    private static String mercuryPrintCountSQL;
    private static String sourceEPHSQL;
    private static int sourceEPHCount;
    private static int mercuryPrintCount;
    private static String sql;
    private static List<String> ids;

    @Given("^Get the total count from EPH (.*)$")
    public static void sourceEPHCount (String tableNanem) {
        switch (tableNanem){
            case "extract_mercury_print_v":
                sourceEPHSQL = mercuryChecksSQL.GET_SOURCE_EPH_COUNT;
                break;
        }

        Log.info(sourceEPHSQL);
        List<Map<String, Object>> SrcEPHTableCnt = DBManager.getDBResultMap(sourceEPHSQL, Constants.AWS_URL);
        sourceEPHCount = ((Long) SrcEPHTableCnt.get(0).get("source_count")).intValue();
    }

    @Then("^We know the total count from mercury view (.*)$")
    public static void getmercuryPrintCount(String tableNanem) {
        switch (tableNanem){
            case "extract_mercury_print_v":
                mercuryPrintCountSQL = mercuryChecksSQL.GET_MERCURY_PRINT_COUNT;
                break;

        }

        Log.info(mercuryPrintCountSQL);
        List<Map<String, Object>> mercuryPrintTableCnt = DBManager.getDBResultMap(mercuryPrintCountSQL, Constants.AWS_URL);
        mercuryPrintCount = ((Long) mercuryPrintTableCnt.get(0).get("Target_count")).intValue();
    }

    @And("^Compare counts of EPH and MercuryPrint view (.*)$")
    public void compCounts(String table){
        Log.info("The count for EPH => " + sourceEPHCount + " and in "+table+ "=> " + mercuryPrintCount);
        Assert.assertEquals("The counts are not equal when compared for "+table, sourceEPHCount,mercuryPrintCount );
    }

 @Given("^Get the (.*) random ids from EPH (.*)$")
    public void getRandomIds(String numberOfRecords,String tableName){
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids from Mercury Tables....");
        List<Map<?, ?>> randomids;
        switch (tableName) {
            case "extract_mercury_print_v":
                sql = String.format(mercuryChecksSQL.GET_RANDOM_ID_MERCURY, numberOfRecords);
                break;

        }
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("product_id")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

      @When("^We Get the records from the source EPH (.*)$")
    public static void getSrcEPH(String tableName) {
        Log.info("We get source records...");
        switch (tableName) {
            case "extract_mercury_print_v":
                sql = String.format(mercuryChecksSQL.GET_EPH_SOURCE_DATA, String.join("','",ids));
                break;

        }
        GDTablesDLSQLContext.sourceEPHMercury = DBManager.getDBResultAsBeanList(sql, r12Objects.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @When("^We Get the records from the Mercury Print (.*)$")
    public static void getMecuryViewRec(String tableName) {
        Log.info("We get Target records...");
        switch (tableName) {
            case "extract_mercury_print_v":
                sql = String.format(mercuryChecksSQL.GET_MERCURY_PRINT_RECS, String.join("','",ids));
                break;

        }
        GDTablesDLSQLContext.mercuryPrintView = DBManager.getDBResultAsBeanList(sql, r12Objects.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^compare the rec of both EPH and Mercury Print views (.*)$")
    public void compareRecsEPHAndMercury(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.sourceEPHMercury.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records...");
            for (int i = 0; i < GDTablesDLSQLContext.sourceEPHMercury.size(); i++) {
                switch (tableName) {
                    case "extract_mercury_print_v":
                        Log.info("comparing eph vs mercury records...");
                        GDTablesDLSQLContext.sourceEPHMercury.sort(Comparator.comparing(r12Objects::getproduct_id)); //sort primarykey data in the lists
                        GDTablesDLSQLContext.mercuryPrintView.sort(Comparator.comparing(r12Objects::getproduct_id));

                        String[] r12FullData = {"getschemaversion", "getcreated", "getupdated", "getproduct_id",
                                "getproduct_full_name", "getproduct_name", "getwork_title", "getwork_type", "getwork_typerollup", "getwork_status", "getwork_statusrollup", "getmanifestation_id", "getjournal_number", "getjournal_acronym", "getoperating_company", "getproduct_type", "getissn", "getpublication_status", "getetax_code", "getfulfilment_system", "getwarehouse_primary", "getwarehouse_backissue", "getnumber_of_issues", "getnumber_of_issues", "getddp_eligible", "getpts_journal_indicator", "getyear_of_first_issue", "getfirst_issue_name", "getfirst_volume_name", "getdefault_start", "getshort_title", "getrc_code", "getpmc_code"};
                        for (String strTemp : r12FullData) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            r12Objects objectToCompare1 = GDTablesDLSQLContext.sourceEPHMercury.get(i);
                            r12Objects objectToCompare2 = GDTablesDLSQLContext.mercuryPrintView.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("product_id => " + GDTablesDLSQLContext.sourceEPHMercury.get(i).getproduct_id() +
                                    " " + strTemp + " => EPH = " + method.invoke(objectToCompare1) +
                                    " Mercury Print = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Mercury_Print for product_id:" + GDTablesDLSQLContext.sourceEPHMercury.get(i).getproduct_id(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                }
            }
        }


    }
}
