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
    public DLLovTablesDataChecksSQL lovObj = new DLLovTablesDataChecksSQL();

    @Given("^We get (.*) random Lov Codes of (.*)")
    public void getRandomLovIds(String numberOfRecords, String tableName) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);

        sql = String.format(DLLovTablesDataChecksSQL.getRandomLovCodes(Constants.EPH_SCHEMA), tableName, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomLovIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        lovIds = randomLovIds.stream().map(m -> (String) m.get("LOV_CODE")).collect(Collectors.toList());
        Log.info(lovIds.toString());
    }

    @When("^We get the Lov (.*) records from EPH$")
    public void getLovEPH(String tableName) {
        Log.info("We get the Lov records from EPH..");
        sql = String.format(lovObj.EPHlovDataBuildSql(Constants.EPH_SCHEMA, tableName), tableName, Joiner.on("','").join(lovIds));
        Log.info(sql);
        LovTablesDLContext.LovTableDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, LovTableDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the Lov (.*) records from DL$")
    public void getWorksDL(String tableName) {
        Log.info("We get the Lov records from DL..");
        sql = String.format(lovObj.DLlovDataBuildSql(GetDLDBUser.getDataBase(), tableName), tableName,  Joiner.on("','").join(lovIds));
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

                switch (tableName) {
                    case "gd_x_lov_event_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getLEVEL_2_EVENT() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getLEVEL_2_EVENT().equals("null"))) {
                            Assert.assertEquals("The LEVEL_2_EVENT is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getLEVEL_2_EVENT(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getLEVEL_2_EVENT());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getLEVEL_3_EVENT() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getLEVEL_3_EVENT().equals("null"))) {
                            Assert.assertEquals("The LEVEL_3_EVENT is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getLEVEL_3_EVENT(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getLEVEL_3_EVENT());
                        }
                        break;

                    case "gd_x_lov_identifier_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_WORK() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_WORK().equals("null"))) {
                            Assert.assertEquals("The VALID_AT_WORK is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_WORK(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_WORK());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_MANIFESTATION() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_MANIFESTATION().equals("null"))) {
                            Assert.assertEquals("The VALID_AT_MANIFESTATION is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_MANIFESTATION(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_MANIFESTATION());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_PRODUCT() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_PRODUCT().equals("null"))) {
                            Assert.assertEquals("The VALID_AT_PRODUCT is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_PRODUCT(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_PRODUCT());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_BOOKS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_JOURNALS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS());
                        }
                        break;

                    case "gd_x_lov_manif_status":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS().equals("null"))) {
                            Assert.assertEquals("The ROLL_UP_STATUS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_BOOKS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_JOURNALS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS());
                        }
                        break;

                    case "gd_x_lov_manif_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_TYPE() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_TYPE().equals("null"))) {
                            Assert.assertEquals("The ROLL_UP_TYPE is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_TYPE(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_TYPE());
                        }
                    case "gd_x_lov_pmc":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getF_PMG() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getF_PMG().equals("null"))) {
                            Assert.assertEquals("The F_PMG is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getF_PMG(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getF_PMG());
                        }
                        break;

                    case "gd_x_lov_product_status":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS().equals("null"))) {
                            Assert.assertEquals("The ROLL_UP_STATUS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_BOOKS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_JOURNALS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_DIGITAL_PACKAGE() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_DIGITAL_PACKAGE().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_DIGITAL_PACKAGE is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_DIGITAL_PACKAGE(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_DIGITAL_PACKAGE());
                        }
                        break;

                    case "gd_x_lov_relationship_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getPARENT_DESCRIPTION() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getPARENT_DESCRIPTION().equals("null"))) {
                            Assert.assertEquals("The PARENT_DESCRIPTION is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getPARENT_DESCRIPTION(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getPARENT_DESCRIPTION());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCHILD_DESCRIPTION() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getCHILD_DESCRIPTION().equals("null"))) {
                            Assert.assertEquals("The CHILD_DESCRIPTION is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCHILD_DESCRIPTION(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getCHILD_DESCRIPTION());
                        }
                        break;

                    case "gd_x_lov_work_status":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS().equals("null"))) {
                            Assert.assertEquals("The ROLL_UP_STATUS is incorrect for id=" + lovCode,
                                    com.eph.automation.testing.models.contexts.LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_BOOKS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS().equals("null"))) {
                            Assert.assertEquals("The VALID_FOR_JOURNALS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS());
                        }
                        break;

                    case "gd_x_lov_work_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_TYPE() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_TYPE().equals("null"))) {
                            Assert.assertEquals("The ROLL_UP_TYPE is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_TYPE(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_TYPE());
                        }
                        break;

                    case "gd_x_lov_society_ownership":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_OWNERSHIP() != null ||
                                (!LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_OWNERSHIP().equals("null"))) {
                            Assert.assertEquals("The ROLL_UP_OWNERSHIP is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_OWNERSHIP(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_OWNERSHIP());
                        }
                        break;
                }

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
            }
        }
    }
}
