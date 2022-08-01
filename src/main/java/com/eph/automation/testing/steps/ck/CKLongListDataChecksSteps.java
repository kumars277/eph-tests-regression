package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.CK.CKAccessDLContext;
import com.eph.automation.testing.models.dao.CK.CKCurrentTablesDataObject;
import com.eph.automation.testing.models.dao.CK.CKInboundSourceTableDataObject;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKLongListDataChecksSQL;
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

public class CKLongListDataChecksSteps {
    private static String sql;
    private static List<String> ids;

    //    CK LongList Data Checks
    @Given("^We get the (.*) random CK LongList View ids of (.*)$")
    public static void getRandomidsFromLongListView(String numberOfRecords, String DPPLongListView) {
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for CK LongList View....");
        List<Map<?, ?>> randomids;
        sql = String.format(CKLongListDataChecksSQL.GET_LongList_VIEW_IDs, DPPLongListView, numberOfRecords);
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("U_KEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^We get the LongList View Records from (.*)$")
    public static void getLongListViewData(String DPPLongListView) {
        Log.info("We get the CK LongList View records...");
        sql = String.format(CKLongListDataChecksSQL.GET_LongList_VIEW_Data, DPPLongListView, String.join("','", ids));
        CKAccessDLContext.CKCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKCurrentTablesDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We get the LongList Table records from (.*)$")
    public static void getDataforLongListTableCheck(String DPPLongListTable) {
        Log.info("We get the records from Current CK LongList table Check...");
        sql = String.format(CKLongListDataChecksSQL.GET_LongList_TABLE_Data, DPPLongListTable, String.join("','", ids));
        CKAccessDLContext.CKInboundSourceTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKInboundSourceTableDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare records in LongList View and Table of (.*)$")
    public void compareLongListViewandTable(String DPPLongListTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (CKAccessDLContext.CKCurrentTableDataObjectList.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between LongList View and Table...");
            for (int i = 0; i < CKAccessDLContext.CKCurrentTableDataObjectList.size(); i++) {
                Log.info("comparing inbound source and Current records...");
                CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getmaster_isbn)); //sort master_isbn data in the lists
                CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getmaster_isbn));

                String[] LongListCol = {"geteph_work_id","getmaster_isbn","gete_isbn","geteph_manifestation_title","getplanned_publication","getactual_publication","geth300_date","getwork_type","getwork_status","getpmg_code","getpmc_code","getpmg_name","getpmc_name","getedition","getopco","getopco_name","getauthors","getprevious_edition_work_id","getprevious_edition_master_isbn"};
                for (String strTemp : LongListCol) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;

                    CKCurrentTablesDataObject objectToCompare1 = CKAccessDLContext.CKCurrentTableDataObjectList.get(i);
                    CKInboundSourceTableDataObject objectToCompare2 = CKAccessDLContext.CKInboundSourceTableDataObjectList.get(i);

                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    Log.info("master_isbn => " + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getmaster_isbn() +
                            " " + strTemp + " => View = " + method.invoke(objectToCompare1) +
                            " Table = " + method2.invoke(objectToCompare2));
                    if (method.invoke(objectToCompare1) != null ||
                            (method2.invoke(objectToCompare2) != null)) {
                        Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in EBTD Inbound for Master_ISBN:" + CKAccessDLContext.CKCurrentTableDataObjectList.get(i).getmaster_isbn(),
                                method.invoke(objectToCompare1),
                                method2.invoke(objectToCompare2));
                    }
                }
            }
        }
    }
}
