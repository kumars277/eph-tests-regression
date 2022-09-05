package com.eph.automation.testing.steps.ck;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.CK.CKAccessDLContext;
import com.eph.automation.testing.models.dao.CK.CKCurrentTablesDataObject;
import com.eph.automation.testing.models.dao.CK.CKInboundSourceTableDataObject;
import com.eph.automation.testing.services.db.CKDataLakeSQL.CKEBTDInboundDataChecksSQL;
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

public class CKEBTDInboundDataChecksSteps {
    private static String sql;
    private static List<String> ids;
    //    CK EBTD Inbound Data Checks
    @Given("^We get the (.*) random CK EBTD View ids of (.*)$")
    public static void getRandomidsFromEBTDInboundView(String numberOfRecords, String DPPEBTDView) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random ids for CK EBTD View....");
        List<Map<?, ?>> randomids;
        sql = String.format(CKEBTDInboundDataChecksSQL.GET_EBTD_Inbound_VIEW_IDs, DPPEBTDView, numberOfRecords);
        randomids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        ids = randomids.stream().map(m -> (String) m.get("U_KEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^We get the CK EBTD View Records from (.*)$")
    public static void getEBTDViewRecordData(String DPPEBTDView) {
        Log.info("We get the CK Inbound records...");
        sql = String.format(CKEBTDInboundDataChecksSQL.GET_EBTD_Inbound_VIEW_Data, DPPEBTDView, String.join("','", ids));
        CKAccessDLContext.CKCurrentTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKCurrentTablesDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We get the CK EBTD Table records from (.*)$")
    public static void getDataforEBTDTableCheck(String DPPEBTDTable) {
        Log.info("We get the records from Current CK EBTD Inbound table for Inbound Check...");
        sql = String.format(CKEBTDInboundDataChecksSQL.GET_EBTD_Inbound_TABLE_Data, DPPEBTDTable, String.join("','", ids));
        CKAccessDLContext.CKInboundSourceTableDataObjectList = DBManager.getDBResultAsBeanList(sql, CKInboundSourceTableDataObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare CK records in EBTD View and Table of (.*)$")
    public void compareEBTDViewAndTable(String DPPEBTDTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (CKAccessDLContext.CKCurrentTableDataObjectList.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the ids to compare the records between EBTD Inbound View and Table...");
            for (int i = 0; i < CKAccessDLContext.CKCurrentTableDataObjectList.size(); i++) {
                Log.info("comparing inbound source and Current records...");
                CKAccessDLContext.CKCurrentTableDataObjectList.sort(Comparator.comparing(CKCurrentTablesDataObject::getmaster_isbn)); //sort master_isbn data in the lists
                CKAccessDLContext.CKInboundSourceTableDataObjectList.sort(Comparator.comparing(CKInboundSourceTableDataObject::getmaster_isbn));

                String[] EBTDCol = {"gete_isbn","getmaster_isbn","geth300_date"};
                for (String strTemp : EBTDCol) {
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
