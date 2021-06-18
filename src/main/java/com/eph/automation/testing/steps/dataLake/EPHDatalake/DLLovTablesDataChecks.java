package com.eph.automation.testing.steps.dataLake.EPHDatalake;

import com.eph.automation.testing.configuration.*;
import com.eph.automation.testing.helper.*;
import com.eph.automation.testing.models.contexts.*;
import com.eph.automation.testing.models.dao.EPHDataLake.*;
import com.eph.automation.testing.services.db.EPHDataLakeSql.*;
import com.google.common.base.Joiner;
import cucumber.api.java.en.*;
import org.junit.Assert;

import java.util.*;
import java.util.stream.*;

/**
 * Created by Thomas Kruck on 15/02/2020
 */

public class DLLovTablesDataChecks {
    private static String sql;
    private static List<String> lovIds;
    public static LovTablesDLContext LovTablesDLContext;
    public DLLovTablesDataChecksSQL lovObj = new DLLovTablesDataChecksSQL();
    public String validCheck;

    @Given("^We get (.*) random Lov Codes of (.*)")
    public void getRandomLovIds(String numberOfRecords, String tableName) {
          numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random record...");
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
        switch (tableName){
            case "gd_x_lov_event_type":
                sql = String.format(lovObj.gd_x_Lov_event_type(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_identifier_type":
                sql = String.format(lovObj.gd_x_lov_identifier_type(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_manif_status":
                sql = String.format(lovObj.gd_x_lov_manif_status(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_manif_type":
                sql = String.format(lovObj.gd_x_lov_manif_type(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_pmc":
                sql = String.format(lovObj.gd_x_lov_pmc(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_product_status":
                sql = String.format(lovObj.gd_x_lov_product_status(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_relationship_type":
                sql = String.format(lovObj.gd_x_lov_relationship_type(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_work_type":
                sql = String.format(lovObj.gd_x_lov_work_type(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_work_status":
                sql = String.format(lovObj.gd_x_lov_work_status(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_society_ownership":
                sql = String.format(lovObj.gd_x_lov_society_ownership(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
                break;
            default :
                sql = String.format(lovObj.gd_x_lov_core_sql(Constants.EPH_SCHEMA), tableName, Joiner.on("','").join(lovIds));
        }
        Log.info(sql);
        LovTablesDLContext.LovTableDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, LovTableDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the Lov (.*) records from DL$")
    public void getWorksDL(String tableName) {
        Log.info("We get the Lov records from DL..");
        switch (tableName){
            case "gd_x_lov_event_type":
                sql = String.format(lovObj.gd_x_Lov_event_type( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_identifier_type":
                sql = String.format(lovObj.gd_x_lov_identifier_type( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_manif_status":
                sql = String.format(lovObj.gd_x_lov_manif_status( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_manif_type":
                sql = String.format(lovObj.gd_x_lov_manif_type( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_pmc":
                sql = String.format(lovObj.gd_x_lov_pmc( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_product_status":
                sql = String.format(lovObj.gd_x_lov_product_status( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_relationship_type":
                sql = String.format(lovObj.gd_x_lov_relationship_type( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_work_type":
                sql = String.format(lovObj.gd_x_lov_work_type( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_work_status":
                sql = String.format(lovObj.gd_x_lov_work_status( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            case "gd_x_lov_society_ownership":
                sql = String.format(lovObj.gd_x_lov_society_ownership( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
                break;
            default :
                sql = String.format(lovObj.gd_x_lov_core_sql( GetDLDBUser.getDataBase()), tableName, Joiner.on("','").join(lovIds));
        }
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

//              Switch cases for tables with specific columns
                switch (tableName) {
                    case "gd_x_lov_event_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getLEVEL_2_EVENT() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getLEVEL_2_EVENT()!= null)) {
                            Assert.assertEquals("The LEVEL_2_EVENT is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getLEVEL_2_EVENT(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getLEVEL_2_EVENT());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getLEVEL_3_EVENT() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getLEVEL_3_EVENT()!= null)) {
                            Assert.assertEquals("The LEVEL_3_EVENT is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getLEVEL_3_EVENT(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getLEVEL_3_EVENT());
                        }
                        break;

                    case "gd_x_lov_identifier_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_WORK() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_WORK()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_WORK().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_WORK().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_AT_WORK is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_WORK());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_MANIFESTATION() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_MANIFESTATION()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_MANIFESTATION().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_MANIFESTATION().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_AT_MANIFESTATION is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_MANIFESTATION());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_PRODUCT() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_PRODUCT()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_PRODUCT().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_AT_PRODUCT().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_AT_PRODUCT is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_AT_PRODUCT());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_BOOKS is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_JOURNALS is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS());
                        }
                        break;

                    case "gd_x_lov_manif_status":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS()!= null)) {
                            Assert.assertEquals("The ROLL_UP_STATUS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_BOOKS is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_JOURNALS is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS());
                        }
                        break;

                    case "gd_x_lov_manif_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_TYPE() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_TYPE()!= null)) {
                            Assert.assertEquals("The ROLL_UP_TYPE is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_TYPE(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_TYPE());
                        }
                        break;

                    case "gd_x_lov_pmc":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getF_PMG() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getF_PMG()!= null)) {
                            Assert.assertEquals("The F_PMG is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getF_PMG(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getF_PMG());
                        }
                        break;

                    case "gd_x_lov_product_status":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS()!= null)) {
                            Assert.assertEquals("The ROLL_UP_STATUS is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_BOOKS is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_JOURNALS is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_DIGITAL_PACKAGE() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_DIGITAL_PACKAGE()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_DIGITAL_PACKAGE().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_DIGITAL_PACKAGE().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_DIGITAL_PACKAGE is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_DIGITAL_PACKAGE());
                        }
                        break;

                    case "gd_x_lov_relationship_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getPARENT_DESCRIPTION() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getPARENT_DESCRIPTION()!= null)) {
                            Assert.assertEquals("The PARENT_DESCRIPTION is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getPARENT_DESCRIPTION(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getPARENT_DESCRIPTION());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCHILD_DESCRIPTION() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getCHILD_DESCRIPTION()!= null)) {
                            Assert.assertEquals("The CHILD_DESCRIPTION is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCHILD_DESCRIPTION(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getCHILD_DESCRIPTION());
                        }
                        break;

                    case "gd_x_lov_work_status":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS()!= null)) {
                            Assert.assertEquals("The ROLL_UP_STATUS is incorrect for id=" + lovCode,
                                    com.eph.automation.testing.models.contexts.LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_STATUS(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_STATUS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_BOOKS().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_BOOKS is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_BOOKS());
                        }
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS()!= null)) {
                            if(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS().equals("t")){
                                validCheck = "true";
                            }
                            else if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getVALID_FOR_JOURNALS().equals("f")){
                                validCheck = "false";
                            }
                            Assert.assertEquals("The VALID_FOR_JOURNALS is incorrect for id=" + lovCode,
                                    validCheck,
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getVALID_FOR_JOURNALS());
                        }
                        break;

                    case "gd_x_lov_work_type":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_TYPE() != null ||
                                (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_TYPE()!= null)) {
                            Assert.assertEquals("The ROLL_UP_TYPE is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_TYPE(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_TYPE());
                        }
                        break;

                    case "gd_x_lov_society_ownership":
                        if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_OWNERSHIP() != null ||
                                LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_OWNERSHIP()!= null) {
                            Assert.assertEquals("The ROLL_UP_OWNERSHIP is incorrect for id=" + lovCode,
                                    LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getROLL_UP_OWNERSHIP(),
                                    LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getROLL_UP_OWNERSHIP());
                        }
                        break;
                }

//                Columns which apply to all tables

                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCODE() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getCODE()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The LOV Code is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getCODE(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getCODE());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if (String.valueOf(LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_BATCHID()) != null ||
                        (String.valueOf(LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_BATCHID())!= null)) {
                    Assert.assertEquals("The B_BATCHID is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_BATCHID(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_BATCHID());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CREDATE())!= null) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_CREATOR(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_CREATOR());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getB_UPDATOR());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_DESCRIPTION() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_DESCRIPTION()!= null)) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_DESCRIPTION(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_DESCRIPTION());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_START_DATE() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_START_DATE()!= null)) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_START_DATE(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_START_DATE());
                }
                if (LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_END_DATE() != null ||
                        (LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_END_DATE()!= null)) {
                    Assert.assertEquals("The S_WORK_ID is incorrect for id=" + lovCode,
                            LovTablesDLContext.LovTableDataObjectsFromEPH.get(i).getL_END_DATE(),
                            LovTablesDLContext.LovTableDataObjectsFromDL.get(i).getL_END_DATE());
                }
            }
        }
    }
}
