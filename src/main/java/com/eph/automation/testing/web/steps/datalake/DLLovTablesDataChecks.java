package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.configuration.*;
import com.eph.automation.testing.helper.*;
import com.eph.automation.testing.models.contexts.*;
import com.eph.automation.testing.models.dao.datalake.*;
import com.eph.automation.testing.services.db.DataLakeSql.*;
import com.google.common.base.Joiner;
import cucumber.api.java.en.*;
import org.junit.Assert;

import java.util.*;
import java.util.stream.*;


public class DLLovTablesDataChecks {
    private static String sql;
    private static List<String> lovIds;
    public static LovTablesDLContext LovTablesDLContext;

    @Given("^We get (.*) random Lov Codes of (.*)")
    public void getRandomLovIds(String numberOfRecords, String tableName) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);

        sql = String.format(DLLovTablesDataChecksSQL.GET_RANDOM_LOV_CODE, tableName, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomLovIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        lovIds = randomLovIds.stream().map(m -> (String) m.get("LOV_CODE")).collect(Collectors.toList());
        Log.info(lovIds.toString());
    }

    @When("^We get the Lov (.*) records from EPH$")
    public void getLovEPH(String tableName) {
        Log.info("We get the Lov records from EPH..");
        sql = String.format(DLLovTablesDataChecksSQL.GET_DATA_LOV_EPH, tableName, Joiner.on("','").join(lovIds));
        Log.info(sql);
        LovTablesDLContext.LovTableDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, LovTableDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the Lov (.*) records from DL$")
    public void getWorksDL(String tableName) {
        Log.info("We get the Lov records from DL..");
        sql = String.format(DLLovTablesDataChecksSQL.GET_DATA_LOV_DL, tableName,  Joiner.on("','").join(lovIds));
        Log.info(sql);
        LovTablesDLContext.LovTableDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, LovTableDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Lov (.*) records in EPH and DL$")
    public void compareLovDataEPHtoDL(String tableName) {
        if (LovTablesDLContext.LovTableDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the " + tableName + " records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < LovTablesDLContext.LovTableDataObjectsFromEPH.size(); i++) {
                LovTablesDLContext.LovTableDataObjectsFromEPH.sort(Comparator.comparing(LovTableDLObject::getCODE));
                LovTablesDLContext.LovTableDataObjectsFromDL.sort(Comparator.comparing(LovTableDLObject::getCODE));
                String lovCode = LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCODE();
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCODE() != null ||
                        (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getCODE().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The LOV Code is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCODE(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getCODE());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if (String.valueOf(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_BATCHID()) != null ||
                        (!String.valueOf(LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_BATCHID()).equals("null"))) {
                    Assert.assertEquals("The B_BATCHID is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_BATCHID(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_BATCHID());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!String.valueOf(LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CREDATE()).equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CREATOR(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CREATOR());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_DESCRIPTION() != null ||
                        (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_DESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_DESCRIPTION(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_DESCRIPTION());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_START_DATE() != null ||
                        (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_START_DATE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_START_DATE(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_START_DATE());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_END_DATE() != null ||
                        (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_END_DATE().equals("null"))) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_END_DATE(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_END_DATE());
                }
                else{
                    Log.info(tableName + " record info matches between EPH and DataLake");
                }
            }
        }
    }
}
