package com.eph.automation.testing.web.steps.EPHDatalake.DPPTables;

/**
 * Created by Thomas Kruck on 12/03//2020
 */

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DPPTablesDLContext;
import com.eph.automation.testing.models.dao.EPHDataLake.DPPTables.DPPTablesDLObject;
import com.eph.automation.testing.services.db.EPHDataLakeSql.DPPTables.DPPTablesEPHtoDLSQL;
import com.eph.automation.testing.services.db.EPHDataLakeSql.GetDLDBUser;
import com.google.common.base.*;
import cucumber.api.java.en.*;
import org.junit.Assert;
import java.util.*;
import java.util.stream.Collectors;

import static com.eph.automation.testing.configuration.Constants.DPP_SCHEMA;

public class DPPTablesEPHtoDLSteps {
    private static String sqlDL;
    private static String sqlEPH;
    private static int EPHDPPCount;
    private static int DLDPPCount;
    private static String sql;
    private static List<String> Ids;
    public DPPTablesEPHtoDLSQL DPPObj = new DPPTablesEPHtoDLSQL();

    @Given("^We know the number of DPP (.*) data in EPH")
    public void getDPPEPHCount(String tableName) {
        switch (tableName){
            case "comment":
                sqlEPH = DPPTablesEPHtoDLSQL.EPHCommentCount;
                break;
            case "eph_user":
                sqlEPH = DPPTablesEPHtoDLSQL.EPH_EPHUser;
                break;
            case "package":
                sqlEPH = DPPTablesEPHtoDLSQL.EPHPackage;
                break;
            case "package_approval":
                sqlEPH = DPPTablesEPHtoDLSQL.EPHPackageApproval;
                break;
            case "package_approvals":
                sqlEPH = DPPTablesEPHtoDLSQL.EPHPackageApprovals;
                break;
            case "package_have_items":
                sqlEPH = DPPTablesEPHtoDLSQL.EPHPackageHaveItems;
                break;
            case "package_item":
                sqlEPH = DPPTablesEPHtoDLSQL.EPHPackageItem;
                break;
            case "package_item_audit":
                sqlEPH = DPPTablesEPHtoDLSQL.EPHPackageItemAudit;
                break;
        }
        Log.info(sqlEPH);
        List<Map<String, Object>> DPPCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.WFT_URL);
        EPHDPPCount = ((Long) DPPCountEPH.get(0).get("count")).intValue();
        Log.info(tableName + " in EPH Count: " + EPHDPPCount);
    }

    @When("^The DPP (.*) data is in the DL$")
    public void getDPPDLLCount(String tableName) {
        switch (tableName){
            case "comment":
                sqlDL = DPPTablesEPHtoDLSQL.DLCommentCount;
                break;
            case "eph_user":
                sqlDL = DPPTablesEPHtoDLSQL.DL_EPHUser;
                break;
            case "package":
                sqlDL = DPPTablesEPHtoDLSQL.DLPackage;
                break;
            case "package_approval":
                sqlDL = DPPTablesEPHtoDLSQL.DLPackageApproval;
                break;
            case "package_approvals":
                sqlDL = DPPTablesEPHtoDLSQL.DLPackageApprovals;
                break;
            case "package_have_items":
                sqlDL = DPPTablesEPHtoDLSQL.DLPackageHaveItems;
                break;
            case "package_item":
                sqlDL = DPPTablesEPHtoDLSQL.DLPackageItem;
                break;
            case "package_item_audit":
                sqlDL = DPPTablesEPHtoDLSQL.DLPackageItemAudit;
                break;
        }
        Log.info(sqlDL);
        List<Map<String, Object>> DPPCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLDPPCount = ((Long) DPPCountDL.get(0).get("count")).intValue();
        Log.info(tableName + " data in DL: " + DLDPPCount);

    }

    @Then("^The DPP count for (.*) table between EPH and DL are identical$")
    public void compareEPHtoDL(String tableName) {
        Assert.assertEquals("The counts between " + tableName + " is not equal", EPHDPPCount, DLDPPCount);
        Log.info("The count for " + tableName + " table between EPH: " + EPHDPPCount + " and DL: " + DLDPPCount + " is equal");
    }

    @Given("^We get (.*) random DPP ids of (.*)")
    public void getRandomDPPId(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random records ..");
        switch (tableName) {

            case "package_approvals":
                sql = String.format(DPPTablesEPHtoDLSQL.getRandomDPPPackageIDs(), tableName, numberOfRecords);
                List<Map<?, ?>> DPPpkgApprovalsIDs = DBManager.getDBResultMap(sql, Constants.WFT_URL);
                Ids = DPPpkgApprovalsIDs.stream().map(m -> (Long) m.get("PACKAGE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "package_have_items":
                sql = String.format(DPPTablesEPHtoDLSQL.getRandomDPPPackageIDs(), tableName, numberOfRecords);
                List<Map<?, ?>> DPPItemIDs = DBManager.getDBResultMap(sql, Constants.WFT_URL);
                Ids = DPPItemIDs.stream().map(m -> (Long) m.get("PACKAGE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            default:
                sql = String.format(DPPTablesEPHtoDLSQL.getRandomDPPIDs(), tableName, numberOfRecords);
                List<Map<?, ?>> DPPIDs = DBManager.getDBResultMap(sql, Constants.WFT_URL);
                Ids = DPPIDs.stream().map(m -> (Long) m.get("ID")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the DPP (.*) records from EPH$")
    public void getDPPEPH(String tableName) {  //db->0=EPHDB,1=AWSDB
        Log.info("We get the DPP records from EPH..");
        switch (tableName) {
            case "comment":
                sql = String.format(DPPObj.DPPComment(0), tableName, Joiner.on("','").join(Ids));
                break;
            case "eph_user":
                sql = String.format(DPPObj.DPPEPHUser(0), tableName, Joiner.on("','").join(Ids));
                break;
            case "package":
                sql = String.format(DPPObj.DPPPackage(0), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_approval":
                sql = String.format(DPPObj.DPPPackageApproval(0), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_approvals":
                sql = String.format(DPPObj.DPPPackageApprovals(0), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_have_items":
                sql = String.format(DPPObj.DPPPackageHaveItems(0), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_item":
                sql = String.format(DPPObj.DPPPackageItem(0), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_item_audit":
                sql = String.format(DPPObj.DPPPackageItemAudit(0), tableName, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        DPPTablesDLContext.DPPTableDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, DPPTablesDLObject.class, Constants.WFT_URL);
   }

    @Then("^We get the DPP (.*) records from DL$")
    public void getDPPDL(String tableName) {  //db->0=EPHDB,1=AWSDB
        Log.info("We get the DPP records from DL..");
        switch (tableName) {
            case "comment":
                sql = String.format(DPPObj.DPPComment(1), tableName, Joiner.on("','").join(Ids));
                break;
            case "eph_user":
                sql = String.format(DPPObj.DPPEPHUser(1), tableName, Joiner.on("','").join(Ids));
                break;
            case "package":
                sql = String.format(DPPObj.DPPPackage(1), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_approval":
                sql = String.format(DPPObj.DPPPackageApproval(1), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_approvals":
                sql = String.format(DPPObj.DPPPackageApprovals(1), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_have_items":
                sql = String.format(DPPObj.DPPPackageHaveItems(1), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_item":
                sql = String.format(DPPObj.DPPPackageItem(1), tableName, Joiner.on("','").join(Ids));
                break;
            case "package_item_audit":
                sql = String.format(DPPObj.DPPPackageItemAudit(1), tableName, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        DPPTablesDLContext.DPPTableDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, DPPTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare DPP (.*) records in EPH and DL$")
    public void compareDPPDataEPHtoDL(String tableName) {
        if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the " + tableName + " records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < DPPTablesDLContext.DPPTableDataObjectsFromEPH.size(); i++) {
                String DPPID = DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getID();

//               Tables package_approvals and package_have_items have 2 exclusive columns
                if (tableName.equals("package_approvals")){
                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.sort(Comparator.comparing(DPPTablesDLObject::getPACKAGE_ID));
                    DPPTablesDLContext.DPPTableDataObjectsFromDL.sort(Comparator.comparing(DPPTablesDLObject::getPACKAGE_ID));
                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.sort(Comparator.comparing(DPPTablesDLObject::getPACKAGE_APPROVAL_ID));
                    DPPTablesDLContext.DPPTableDataObjectsFromDL.sort(Comparator.comparing(DPPTablesDLObject::getPACKAGE_APPROVAL_ID));

                    if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID() != null ||
                            (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID().equals("null"))) {
                        Log.info("package ID => "+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                " PACKAGE_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID());
                        Assert.assertEquals("The PACKAGE_ID "+DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                " is missing/incorrect in DL",
                                DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID(),
                                DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID());
                    }
                    if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_APPROVAL_ID() != null ||
                            (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_APPROVAL_ID().equals("null"))) {
                        Log.info("package ID => "+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                " PACKAGE_APPROVAL_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_APPROVAL_ID()+
                                " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_APPROVAL_ID());
                        Assert.assertEquals("The PACKAGE_APPROVAL_ID is incorrect for package_id=" + DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID(),
                                DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_APPROVAL_ID(),
                                DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_APPROVAL_ID());
                    }
                }
                    else if (tableName.equals("package_have_items")){
                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.sort(Comparator.comparing(DPPTablesDLObject::getPACKAGE_ID));
                    DPPTablesDLContext.DPPTableDataObjectsFromDL.sort(Comparator.comparing(DPPTablesDLObject::getPACKAGE_ID));
                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.sort(Comparator.comparing(DPPTablesDLObject::getPACKAGE_ITEM_ID));
                    DPPTablesDLContext.DPPTableDataObjectsFromDL.sort(Comparator.comparing(DPPTablesDLObject::getPACKAGE_ITEM_ID));
                    if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID() != null ||
                                (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID().equals("null"))) {
                            Log.info("package ID => "+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                    " PACKAGE_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                    " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID());
                            Assert.assertEquals("The PACKAGE_ID "+DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                            "is incorrect/missing in DL" ,
                                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID(),
                                    DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID());
                        }
                        if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ITEM_ID() != null ||
                                (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ITEM_ID().equals("null"))) {
                            Log.info("Package ID => "+  DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                    " PACKAGE_ITEM_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ITEM_ID()+
                                    " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ITEM_ID());
                            Assert.assertEquals("The PACKAGE_ITEM_ID is incorrect for package_id=" +  DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID(),
                                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ITEM_ID(),
                                    DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ITEM_ID());
                        }
                }
                    else {
                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.sort(Comparator.comparing(DPPTablesDLObject::getID));
                    DPPTablesDLContext.DPPTableDataObjectsFromDL.sort(Comparator.comparing(DPPTablesDLObject::getID));
                        //                    Case switches for specific table columns
                    switch (tableName) {
                        case "comment":
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getREASON() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getREASON().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " REASON => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getREASON()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getREASON());
                                Assert.assertEquals("The REASON is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getREASON(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getREASON());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getREMARKS() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getREMARKS().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " REMARKS => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getREMARKS()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getREMARKS());
                                Assert.assertEquals("The REMARKS is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getREMARKS(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getREMARKS());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " STATUS => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS());
                                Assert.assertEquals("The STATUS is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS());
                            }
                            break;
                        case "eph_user":
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEMAIL() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEMAIL().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " EMAIL => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEMAIL()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEMAIL());
                                Assert.assertEquals("The EMAIL is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEMAIL(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEMAIL());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getNAME() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getNAME().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " NAME => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getNAME()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getNAME());
                                Assert.assertEquals("The NAME is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getNAME(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getNAME());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPEOPLE_HUB_ID() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPEOPLE_HUB_ID().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " PEOPLE_HUB_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPEOPLE_HUB_ID()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPEOPLE_HUB_ID());
                                Assert.assertEquals("The PEOPLE_HUB_ID is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPEOPLE_HUB_ID(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPEOPLE_HUB_ID());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPMG() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPMG().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " PMG => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPMG()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPMG());
                                Assert.assertEquals("The PMG is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPMG(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPMG());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getROLE() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getROLE().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " ROLE => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getROLE()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getROLE());
                                Assert.assertEquals("The ROLE is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getROLE(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getROLE());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getUPN_KEY() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getUPN_KEY().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " UPN_KEY => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getUPN_KEY()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getUPN_KEY());
                                Assert.assertEquals("The UPN_KEY is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getUPN_KEY(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getUPN_KEY());
                            }
                            break;
                        case "package":
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_EPR_ID() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_EPR_ID().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " PACKAGE_EPR_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_EPR_ID()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_EPR_ID());
                                Assert.assertEquals("The PACKAGE_EPR_ID is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_EPR_ID(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_EPR_ID());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " STATUS => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS());
                                Assert.assertEquals("The STATUS is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getTITLE() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getTITLE().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " TITLE => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getTITLE()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getTITLE());
                                Assert.assertEquals("The TITLE is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getTITLE(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getTITLE());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getVERSION() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getVERSION().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " VERSION => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getVERSION()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getVERSION());
                                Assert.assertEquals("The VERSION is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getVERSION(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getVERSION());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getYEAR() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getYEAR().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " YEAR => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getYEAR()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getYEAR());
                                Assert.assertEquals("The YEAR is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getYEAR(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getYEAR());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getWORKFLOW_TYPE() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getWORKFLOW_TYPE().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " WORKFLOW_TYPE => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getWORKFLOW_TYPE()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getWORKFLOW_TYPE());
                                Assert.assertEquals("The WORKFLOW_TYPE is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getWORKFLOW_TYPE(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getWORKFLOW_TYPE());
                            }
                            break;

                        case "package_approval":
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getOVERRIDDEN() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getOVERRIDDEN().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " OVERRIDDEN => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getOVERRIDDEN()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getOVERRIDDEN());

                                String OverRiddenSql = String.valueOf(DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getOVERRIDDEN());
                                if(OverRiddenSql.equals("f")){
                                    OverRiddenSql = "false";
                                }else{
                                    OverRiddenSql = "true";
                                }
                                Assert.assertEquals("The OVERRIDDEN is incorrect for id=" + DPPID,
                                        OverRiddenSql,DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getOVERRIDDEN());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " STATUS => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS());
                                Assert.assertEquals("The STATUS is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPH_USER_ID() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPH_USER_ID().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " EPH_USER_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPH_USER_ID()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPH_USER_ID());
                                Assert.assertEquals("The EPH_USER_ID is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPH_USER_ID(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPH_USER_ID());
                            }
                            break;
                        case "package_item":
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPR_ID() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPR_ID().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " EPR_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPR_ID()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPR_ID());
                                Assert.assertEquals("The EPR_ID is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPR_ID(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPR_ID());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getIS_NEW_ADDITION() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getIS_NEW_ADDITION().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " IS_NEW_ADDITION => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getIS_NEW_ADDITION()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getIS_NEW_ADDITION());
                                String newAdditionSql = String.valueOf(DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getIS_NEW_ADDITION());
                                if(newAdditionSql.equals("f")){
                                    newAdditionSql = "false";
                                }else{
                                    newAdditionSql = "true";
                                }
                                Assert.assertEquals("The IS_NEW_ADDITION is incorrect for id=" + DPPID,
                                        newAdditionSql,DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getIS_NEW_ADDITION());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getISSN() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getISSN().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " ISSN => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getISSN()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getISSN());
                                Assert.assertEquals("The ISSN is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getISSN(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getISSN());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getJOURNAL_NUMBER() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getJOURNAL_NUMBER().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " JOURNAL_NUMBER => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getJOURNAL_NUMBER()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getJOURNAL_NUMBER());
                                Assert.assertEquals("The JOURNAL_NUMBER is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getJOURNAL_NUMBER(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getJOURNAL_NUMBER());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getOWNERSHIP_TYPE() != null ||
                                (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getOWNERSHIP_TYPE().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " OWNERSHIP_TYPE => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getOWNERSHIP_TYPE()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getOWNERSHIP_TYPE());
                                Assert.assertEquals("The OWNERSHIP_TYPE is incorrect for id=" + DPPID,
                                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getOWNERSHIP_TYPE(),
                                    DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getOWNERSHIP_TYPE());
                        }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPMG_CODE() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPMG_CODE().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " PMG_CODE => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPMG_CODE()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPMG_CODE());
                                Assert.assertEquals("The PMG_CODE is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPMG_CODE(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPMG_CODE());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPUBLISHER() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPUBLISHER().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " PUBLISHER => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPUBLISHER()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPUBLISHER());
                                Assert.assertEquals("The PUBLISHER is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPUBLISHER(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPUBLISHER());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPUBLISHING_DIRECTOR() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPUBLISHING_DIRECTOR().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " PUBLISHING_DIRECTOR => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPUBLISHING_DIRECTOR()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPUBLISHING_DIRECTOR());
                                Assert.assertEquals("The PUBLISHING_DIRECTOR is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPUBLISHING_DIRECTOR(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPUBLISHING_DIRECTOR());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " STATUS => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS());
                                Assert.assertEquals("The STATUS is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getSTATUS(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getSTATUS());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getTITLE() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getTITLE().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " TITLE => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getTITLE()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getTITLE());
                                Assert.assertEquals("The getTITLE is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getTITLE(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getTITLE());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getVERSION() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getVERSION().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " VERSION => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getVERSION()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getVERSION());
                                Assert.assertEquals("The VERSION is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getVERSION(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getVERSION());
                            }
                            break;
                        case "package_item_audit":
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getAFTER_VALUE() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getAFTER_VALUE().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " AFTER_VALUE => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getAFTER_VALUE()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getAFTER_VALUE());
                                Assert.assertEquals("The AFTER_VALUE is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getAFTER_VALUE(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getAFTER_VALUE());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getBEFORE_VALUE() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getBEFORE_VALUE().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " BEFORE_VALUE => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getBEFORE_VALUE()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getBEFORE_VALUE());
                                Assert.assertEquals("The BEFORE_VALUE is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getBEFORE_VALUE(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getBEFORE_VALUE());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPR_ID() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPR_ID().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " EPR_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPR_ID()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPR_ID());
                                Assert.assertEquals("The EPR_ID is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getEPR_ID(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getEPR_ID());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getFIELD_NAME() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getFIELD_NAME().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " FIELD_NAME => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getFIELD_NAME()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getFIELD_NAME());
                                Assert.assertEquals("The FIELD_NAME is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getFIELD_NAME(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getFIELD_NAME());
                            }
                            if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_STATUS() != null ||
                                    (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_STATUS().equals("null"))) {
                                Log.info("ID => "+ DPPID+
                                        " PACKAGE_STATUS => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_STATUS()+
                                        " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_STATUS());
                                Assert.assertEquals("The PACKAGE_STATUS is incorrect for id=" + DPPID,
                                        DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_STATUS(),
                                        DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_STATUS());
                            }
                        if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID() != null ||
                                (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID().equals("null"))) {
                            Log.info("ID => "+ DPPID+
                                    " PACKAGE_ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID()+
                                    " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID());
                            Assert.assertEquals("The PACKAGE_ID is incorrect for id=" + DPPID,
                                    DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getPACKAGE_ID(),
                                    DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getPACKAGE_ID());
                        }
                            break;
                    }

//                    Core columns for all tables (except package_approvals and package_have_items)

                    if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getID() != null ||
                            (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getID().equals("null"))) {
                        Log.info("ID => "+ DPPID+
                                " ID => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getID()+
                                " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getID());
                        Assert.assertEquals("The ID is incorrect for id=" + DPPID,
                                DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getID(),
                                DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getID());
                    }
                    if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getCREATED_TIME() != null ||
                            (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getCREATED_TIME().equals("null"))) {
                        Log.info("ID => "+ DPPID+
                                " CREATED_TIME => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getCREATED_TIME()+
                                " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getCREATED_TIME());
                        Assert.assertEquals("The CREATED_TIME is incorrect for id=" + DPPID,
                                DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getCREATED_TIME(),
                                DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getCREATED_TIME());
                    }
                    if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getLAST_UPDATE_TIME() != null ||
                            (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getLAST_UPDATE_TIME().equals("null"))) {
                        Log.info("ID => "+ DPPID+
                                " LAST_UPDATE_TIME => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getLAST_UPDATE_TIME()+
                                " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getLAST_UPDATE_TIME());
                        Assert.assertEquals("The LAST_UPDATE_TIME is incorrect for id=" + DPPID,
                                DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getLAST_UPDATE_TIME(),
                                DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getLAST_UPDATE_TIME());
                    }
                    if (DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getLAST_UPDATE_USER() != null ||
                            (!DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getLAST_UPDATE_USER().equals("null"))) {
                        Log.info("ID => "+ DPPID+
                                " LAST_UPDATE_USER => EPH="+ DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getLAST_UPDATE_USER()+
                                " DL="+ DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getLAST_UPDATE_USER());

                        Assert.assertEquals("The LAST_UPDATE_USER is incorrect for id=" + DPPID,
                                DPPTablesDLContext.DPPTableDataObjectsFromEPH.get(i).getLAST_UPDATE_USER(),
                                DPPTablesDLContext.DPPTableDataObjectsFromDL.get(i).getLAST_UPDATE_USER());
                    }
                }
            }

        }
    }
}
