package com.eph.automation.testing.web.steps.JMDataLake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.JMDataLake.JMTablesDLObject;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMTablesDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import com.eph.automation.testing.models.contexts.DataQualityJMContext;
import org.junit.Assert;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class JMTablesDataChecksSteps {

    public DataQualityJMContext dataQualityJMContext;
    private static String sql;
    private static List<String> Ids;
    private JMTablesDataChecksSQL jmObj = new JMTablesDataChecksSQL();

  // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
  // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get (.*) random ids of (.*)$")
    public void getRandomIds(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random record...");
        switch (tableName) {
            case "JMF_ALLOCATION_CHANGE":
                sql = String.format(JMTablesDataChecksSQL.GET_ALLOCATION_IDS, numberOfRecords);
                List<Map<?, ?>> randomAllocationIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomAllocationIds.stream().map(m -> (Integer) m.get("ALLOCATION_CHANGE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "JMF_APPROVAL_REQUEST":
                sql = String.format(JMTablesDataChecksSQL.GET_APPROVAL_REQ_ID, numberOfRecords);
                List<Map<?, ?>> randomApprovalIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomApprovalIds.stream().map(m -> (Integer) m.get("APPROVAL_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "JMF_CHRONICLE_SCENARIO":
                sql = String.format(JMTablesDataChecksSQL.GET_CHRONICLE_SCE_ID, numberOfRecords);
                List<Map<?, ?>> randomChronicleIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomChronicleIds.stream().map(m -> (String) m.get("CHRONICLE_SCENARIO_CODE")).collect(Collectors.toList());
                break;

            case "JMF_CHRONICLE_STATUS":
                sql = String.format(JMTablesDataChecksSQL.GET_CHRONICLE_STATUS_ID, numberOfRecords);
                List<Map<?, ?>> randomChronicleStatusIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomChronicleStatusIds.stream().map(m -> (String) m.get("CHRONICLE_STATUS_CODE")).collect(Collectors.toList());
                break;

            case "JMF_FAMILY_RESOURCE_DETAILS":
                sql = String.format(JMTablesDataChecksSQL.GET_FAMILY_RESOURCE_ID, numberOfRecords);
                List<Map<?, ?>> randomFamilyIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomFamilyIds.stream().map(m -> (Integer) m.get("FAMILY_RESOURCE_DETAILS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_FINANCIAL_INFORMATION":
                sql = String.format(JMTablesDataChecksSQL.GET_PRODUCT_WORK_ID, numberOfRecords);
                List<Map<?, ?>> randomPrdtWorkIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomPrdtWorkIds.stream().map(m -> (Integer) m.get("PRODUCT_WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_LEGAL_INFORMATION":
                sql = String.format(JMTablesDataChecksSQL.GET_LEGAL_PRODUCT_WORK_ID, numberOfRecords);
                List<Map<?, ?>> randomLegalPrdtWorkIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomLegalPrdtWorkIds.stream().map(m -> (Integer) m.get("PRODUCT_WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_MANIFESTATION_ELECTRONIC_DETAILS":
                sql = String.format(JMTablesDataChecksSQL.GET_MANIF_PRODUCT_ID, numberOfRecords);
                List<Map<?, ?>> randomPrdtManifIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomPrdtManifIds.stream().map(m -> (Integer) m.get("PRODUCT_MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_MANIFESTATION_PRINT_DETAILS":
                sql = String.format(JMTablesDataChecksSQL.GET_MANIF_PRINT_ID, numberOfRecords);
                List<Map<?, ?>> randomManifPrintIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomManifPrintIds.stream().map(m -> (Integer) m.get("PRODUCT_MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_PARTY_IN_PRODUCT":
                sql = String.format(JMTablesDataChecksSQL.GET_PARTY_PROD_ID, numberOfRecords);
                List<Map<?, ?>> randomPartyPrdtIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomPartyPrdtIds.stream().map(m -> (Integer) m.get("PARTY_IN_PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_PRODUCT_AVAILABILITY":
                sql = String.format(JMTablesDataChecksSQL.GET_PRODUCT_AVAIL_ID, numberOfRecords);
                List<Map<?, ?>> randomPrdtAvailIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomPrdtAvailIds.stream().map(m -> (Integer) m.get("PRODUCT_AVAILABILITY_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_PRODUCT_CHRONICLE":
                sql = String.format(JMTablesDataChecksSQL.GET_PRODUCT_CHRONICLE_ID, numberOfRecords);
                List<Map<?, ?>> randomPrdtChroIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomPrdtChroIds.stream().map(m -> (Integer) m.get("PRODUCT_CHRONICLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_PRODUCT_FAMILY":
                sql = String.format(JMTablesDataChecksSQL.GET_PRODUCT_FAMILY_ID, numberOfRecords);
                List<Map<?, ?>> randomPrdtFamilyIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomPrdtFamilyIds.stream().map(m -> (Integer) m.get("PRODUCT_FAMILY_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_PRODUCT_MANIFESTATION":
                sql = String.format(JMTablesDataChecksSQL.GET_PRODUCT_MANIF_ID, numberOfRecords);
                List<Map<?, ?>> randomProdManifIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomProdManifIds.stream().map(m -> (Integer) m.get("PRODUCT_MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_PRODUCT_SUBJECT_AREA":
                sql = String.format(JMTablesDataChecksSQL.GET_PRODUCT_SUBJ_ID, numberOfRecords);
                List<Map<?, ?>> randomProdSubjIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomProdSubjIds.stream().map(m -> (Integer) m.get("PRODUCT_SUBJECT_AREA_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_REVIEW_COMMENT":
                sql = String.format(JMTablesDataChecksSQL.GET_REVIEW_COMMENTS_ID, numberOfRecords);
                List<Map<?, ?>> randomReviewComIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomReviewComIds.stream().map(m -> (Integer) m.get("REVIEW_COMMENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_PRODUCT_WORK":
                sql = String.format(JMTablesDataChecksSQL.GET_PROD_WORK_ID, numberOfRecords);
                List<Map<?, ?>> randomProdWorkIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomProdWorkIds.stream().map(m -> (Integer) m.get("PRODUCT_WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_APPROVAL_ATTACHMENT":
                sql = String.format(JMTablesDataChecksSQL.GET_APPROVAL_ID, numberOfRecords);
                List<Map<?, ?>> randomApprovalAttachIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomApprovalAttachIds.stream().map(m -> (Integer) m.get("APPROVAL_ATTACHMENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "JMF_PRODUCTION_INFORMATION":
                sql = String.format(JMTablesDataChecksSQL.GET_PRODUCTION_INFORMATION_ID, numberOfRecords);
                List<Map<?, ?>> randomProductWorkIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
                Ids = randomProductWorkIds.stream().map(m -> (Integer) m.get("PRODUCT_WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;


        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the JMF Allocation records from JMF MySQL of (.*)")
    public void getRecordsFromJMF(String tableName) {
        Log.info("We get the Allocations Change records from JMF MySql..");
        sql = String.format(jmObj.getAllocationChangesSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Allocation records from DL of (.*)$")
    public void getRecordsFromDL(String tableName) {
        Log.info("We get the Allocations Change records from JMF DL..");
        sql = String.format(jmObj.getAllocationChangesSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Allocation records in JMF MySQL and DL of (.*)$")
    public void compareJMapplicationChangeDataSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the allocation_change records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getALLOCATION_CHANGE_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getALLOCATION_CHANGE_ID));

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " ALLOCATION_CHANGE_ID => MysqL=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID() != null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The ALLOCATION_CHANGE_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+" is missing/not found in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID());

                }
                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " ALLOCATION_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE() != null)) {
                    Assert.assertEquals("The ALLOCATION_TYPE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE());
                }
                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_FILTER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER() != null)) {
                    Assert.assertEquals("The PMG_FILTER is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER());
                }
                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMC_FILTER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER() != null)) {
                    Assert.assertEquals("The PMC_FILTER is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER());
                }
                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PPC_FILTER_EMAIL => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL() != null)) {
                    Assert.assertEquals("The PPC_FILTER_EMAIL is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL());
                }


                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PPC_FILTER_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME() != null)) {
                    Assert.assertEquals("The PPC_FILTER_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMC_CHANGE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND() != null)) {
                    Assert.assertEquals("The PMC_CHANGE_IND is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PPC_CHANGE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND() != null)) {
                    Assert.assertEquals("The PPC_CHANGE_IND is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMX_PMGCODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE() != null)) {
                    Assert.assertEquals("The PMX_PMGCODE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE());
                }
                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMX_PMG_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE() != null)) {
                    Assert.assertEquals("The PMX_PMG_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_NAME());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMX_PMG_F_BUSINESS_UNIT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT() != null)) {
                    Assert.assertEquals("The PMX_PMG_F_BUSINESS_UNIT is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_BUS_CTRL_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME() != null)) {
                    Assert.assertEquals("The PMG_BUS_CTRL_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME());
                }


                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_BUS_CTRL_EMAIL => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL() != null)) {
                    Assert.assertEquals("The PMG_BUS_CTRL_EMAIL is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_PUBDIR_NAME_OLD => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD() != null)) {
                    Assert.assertEquals("The PMG_PUBDIR_NAME_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD());
                }


                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_PUBDIR_EMAIL_OLD => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD() != null)) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD());
                }


                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_PUBDIR_PEOPLE_HUB_ID_OLD => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD() != null)) {
                    Assert.assertEquals("The PMG_PUBDIR_PEOPLE_HUB_ID_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD());
                }


                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_PUBDIR_NAME_NEW => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW() != null)) {
                    Assert.assertEquals("The PMG_PUBDIR_NAME_NEW is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_PUBDIR_EMAIL_NEW => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW() != null)) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_NEW is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());
                }


                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " PMG_PUBDIR_PEOPLE_HUB_ID_NEW => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_NEW() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW() != null)) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_NEW is incorrect for ALLOCATION_CHANGE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for ALLOCATION_CHANGE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

                Log.info("ALLOCATION_CHANGE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " EPH_PMG_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE() != null)) {
                    Assert.assertEquals("The EPH_PMG_CODE is incorrect for ALLOCATION_CHANGE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE());
                }


            }
        }
    }

    @Given("^We get (.*) random application key of (.*)")
    public void getApplicationkey(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random record...");
        sql = String.format(JMTablesDataChecksSQL.GET_APPLICATION_KEY, numberOfRecords);
        List<Map<?, ?>> randomAllocationIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
        Ids = randomAllocationIds.stream().map(m -> (String) m.get("APPLICATION_PROPERTY_KEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the JMF Application properties records from JMF MySQL of (.*)")
    public void getAppPropertyFromJMF(String tableName) {
        Log.info("We get the Application Property records from JMF MySql..");
        sql = String.format(jmObj.getAppPropertySql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);
    }

    @Then("^We get the JMF Application properties records from DL of (.*)")
    public void getAppPropertyFromDL(String tableName) {
        Log.info("We get the Application Property records from JMF DL..");
        sql = String.format(jmObj.getAppPropertySql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Application properties records in JMF MySQL and DL of (.*)$")
    public void compareJMappPropertyDataSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the application_property records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getAPPLICATION_PROPERTY_KEY)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getAPPLICATION_PROPERTY_KEY));

                Log.info("APP_PROP_KEY => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY() +
                        " APPLICATION_PROPERTY_KEY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_KEY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_KEY() != null)) {
                    Assert.assertEquals("The APPLICATION_PROPERTY_KEY is incorrect for app_prop_key=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_KEY());
                }

                Log.info("APP_PROP_KEY => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY() +
                        " APPLICATION_PROPERTY_VALUE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_VALUE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_VALUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_VALUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_VALUE() != null)) {
                    Assert.assertEquals("The APPLICATION_PROPERTY_VALUE is incorrect for app_prop_key=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_VALUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_VALUE());
                }

                Log.info("APP_PROP_KEY => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for app_prop_key=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

            }
        }
    }

    @When("^We get the JMF Approval Request records from JMF MySQL of (.*)")
    public void getAppRequestFromJMF(String tableName) {
        Log.info("We get the Application Request records from JMF MySql..");
        sql = String.format(jmObj.getAppRequestSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);
    }

    @Then("^We get the JMF Approval Request records from DL of (.*)")
    public void getgetAppRequestFromDL(String tableName) {
        Log.info("We get the Application Request records from JMF DL..");
        sql = String.format(jmObj.getAppRequestSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Approval Request records in JMF MySQL and DL of (.*)$")
    public void compareJMappRequestDataSQLtoDL(String tableName) {

        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the approval_request records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getAPPROVAL_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getAPPROVAL_ID));

                Log.info("APPROVAL_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() +
                        " APPROVAL_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ID() != null)) {
                    Assert.assertEquals("The APPROVAL_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ID());
                }

                Log.info("APPROVAL_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() +
                        " F_PRODUCT_CHRONICLE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_CHRONICLE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_CHRONICLE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_VALUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_VALUE() != null)) {
                    Assert.assertEquals("The F_PRODUCT_CHRONICLE is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_CHRONICLE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_CHRONICLE());
                }

                Log.info("APPROVAL_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() +
                        " APPROVAL_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_TYPE() != null)) {
                    Assert.assertEquals("The APPROVAL_TYPE is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_TYPE());
                }

                Log.info("APPROVAL_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() +
                        " APPROVER_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVER_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVER_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVER_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVER_NAME() != null)) {
                    Assert.assertEquals("The APPROVER_NAME is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVER_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVER_NAME());
                }

                Log.info("APPROVAL_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() +
                        " APPROVAL_GIVEN_INDICATOR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_GIVEN_INDICATOR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_GIVEN_INDICATOR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_GIVEN_INDICATOR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_GIVEN_INDICATOR() != null)) {
                    Assert.assertEquals("The APPROVAL_GIVEN_INDICATOR is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_GIVEN_INDICATOR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_GIVEN_INDICATOR());
                }

                Log.info("APPROVAL_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() +
                        " APPROVAL_REQUEST_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_REQUEST_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_REQUEST_DATE());
                //Date ReqdateSql=formatter1.parse(dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_REQUEST_DATE());
                //Date ReqdateDL = formatter2.parse(dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_REQUEST_DATE());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_REQUEST_DATE()!=null ||
                        dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_REQUEST_DATE()!=null) {
                   // Assert.assertTrue("APPROVAL_REQUEST_DATE are not equal",ReqdateSql.compareTo(ReqdateDL)==0);
                    Assert.assertEquals("The APPROVAL_REQUEST_DATE is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_REQUEST_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_REQUEST_DATE());
                }

                Log.info("APPROVAL_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() +
                        " APPROVAL_RESPONSE_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_RESPONSE_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_RESPONSE_DATE());
                //Date ResponsedateSql=formatter1.parse(dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_REQUEST_DATE());
                //Date ResponsedateDL = formatter2.parse(dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_REQUEST_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_RESPONSE_DATE()!= null
                        || dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_RESPONSE_DATE()!= null) {
                    Assert.assertEquals("The APPROVAL_RESPONSE_DATE is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_RESPONSE_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_RESPONSE_DATE());
                    //Assert.assertTrue("APPROVAL_RESPONSE_DATE are not equal",ResponsedateSql.compareTo(ResponsedateDL)==0);
                }
            }
        }
    }

    @When("^We get the JMF Chronicle Scenario records from JMF MySQL of (.*)")
    public void getChronicleRecFromJMF(String tableName) {
        Log.info("We get the Chronicle Scenario records from JMF MySql..");
        sql = String.format(jmObj.getChronicleSceSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Chronicle Scenario records from DL of (.*)$")
    public void getChronicleRecFromDL(String tableName) {
        Log.info("We get the Chronicle Scenario records from JMF DL..");
        sql = String.format(jmObj.getChronicleSceSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Chronicle Scenario records in JMF MySQL and DL of (.*)$")
    public void compareJMChronSceDataSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the chronicle_scenario records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getCHRONICLE_SCENARIO_CODE)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getCHRONICLE_SCENARIO_CODE));

                Log.info("CHRO_SCE_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() +
                        " CHRO_SCE_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE() != null)) {
                    Assert.assertEquals("The CHRO_SCE_CODE =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE());
                }
                Log.info("CHRO_SCE_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() +
                        " CHRONICLE_SCENARIO_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_NAME() != null)) {
                    Assert.assertEquals("The CHRONICLE_SCENARIO_NAME is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_NAME());
                }

                Log.info("CHRO_SCE_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() +
                        " CHRONICLE_SCENARIO_DESCRIPTION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_DESCRIPTION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_DESCRIPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_DESCRIPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_DESCRIPTION() != null)) {
                    Assert.assertEquals("The CHRONICLE_SCENARIO_DESCRIPTION is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_DESCRIPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_DESCRIPTION());
                }

                Log.info("CHRO_SCE_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() +
                        " CHRONICLE_SCENARIO_EVOLUTIONARY_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND() != null)) {
                    Assert.assertEquals("The CHRONICLE_SCENARIO_EVOLUTIONARY_IND is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND());
                }

                Log.info("CHRO_SCE_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }


    @When("^We get the JMF Chronicle Status records from JMF MySQL of (.*)")
    public void getChronicleStatusRecFromJMF(String tableName) {
        Log.info("We get the Chronicle Status records from JMF MySql..");
        sql = String.format(jmObj.getChronicleStatusSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Chronicle Status records from DL of (.*)$")
    public void getChronicleStatusRecFromDL(String tableName) {
        Log.info("We get the Chronicle Status records from JMF DL..");
        sql = String.format(jmObj.getChronicleStatusSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Chronicle Status records in JMF MySQL and DL of (.*)$")
    public void compareJMChronStatusDataSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the chronicle_status records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getCHRONICLE_STATUS_CODE)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getCHRONICLE_STATUS_CODE));

                Log.info("CHRO_STATUS_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() +
                        " CHRO_STATUS_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE() != null)) {
                    Assert.assertEquals("The CHRO_STATUS_CODE =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE());
                }
                Log.info("CHRO_STATUS_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() +
                        " CHRONICLE_STATUS_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_NAME() != null)) {
                    Assert.assertEquals("The CHRONICLE_STATUS_NAME is incorrect for CHRO_STATUS_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_NAME());
                }

                Log.info("CHRO_STATUS_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() +
                        " CHRONICLE_STATUS_DESCRIPTION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_DESCRIPTION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_DESCRIPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_DESCRIPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_DESCRIPTION() != null)) {
                    Assert.assertEquals("The CHRONICLE_STATUS_DESCRIPTION is incorrect for CHRO_STATUS_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_DESCRIPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_DESCRIPTION());
                }

                Log.info("CHRO_STATUS_CODE => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for CHRO_STATUS_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Family Resource records from JMF MySQL of (.*)")
    public void getFamilyResourceRecFromJMF(String tableName) {
        Log.info("We get the Family Resources records from JMF MySql..");
        sql = String.format(jmObj.getFamilyResourceSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Family Resource records from DL of (.*)$")
    public void getFamilyResourceRecFromDL(String tableName) {
        Log.info("We get the Family Resources records from JMF DL..");
        sql = String.format(jmObj.getFamilyResourceSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Family Resource records in JMF MySQL and DL of (.*)$")
    public void compareJMFamilyResourceDataSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the family_resource records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getFAMILY_RESOURCE_DETAILS_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getFAMILY_RESOURCE_DETAILS_ID));

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " FAMILY_RESOURCE_DETAILS_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFAMILY_RESOURCE_DETAILS_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFAMILY_RESOURCE_DETAILS_ID() != null)) {
                    Assert.assertEquals("The FAMILY_RESOURCE_DETAILS_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFAMILY_RESOURCE_DETAILS_ID());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " RESOURCE_KEY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_KEY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_KEY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_KEY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_KEY() != null)) {
                    Assert.assertEquals("The RESOURCE_KEY is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_KEY());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " INITIAL_VALUE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINITIAL_VALUE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINITIAL_VALUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINITIAL_VALUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINITIAL_VALUE() != null)) {
                    Assert.assertEquals("The INITIAL_VALUE is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINITIAL_VALUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINITIAL_VALUE());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " RESOURCE_CHANGE_INDICATOR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_CHANGE_INDICATOR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_CHANGE_INDICATOR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_CHANGE_INDICATOR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_CHANGE_INDICATOR() != null)) {
                    Assert.assertEquals("The RESOURCE_CHANGE_INDICATOR is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_CHANGE_INDICATOR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_CHANGE_INDICATOR());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " NEW_VALUE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNEW_VALUE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNEW_VALUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNEW_VALUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNEW_VALUE() != null)) {
                    Assert.assertEquals("The NEW_VALUE is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNEW_VALUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNEW_VALUE());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " F_PRODUCT_FAMILY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY() != null)) {
                    Assert.assertEquals("The F_PRODUCT_FAMILY is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " PEOPLEHUB_ID_OLD => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_OLD() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_OLD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_OLD() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID_OLD is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_OLD());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " PEOPLEHUB_ID_NEW => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_NEW() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_NEW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_NEW() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID_NEW is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_NEW());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Finanacial Info records from JMF MySQL of (.*)")
    public void getFinanceInfoRecFromJMF(String tableName) {
        Log.info("We get the Financial Info records from JMF MySql..");
        sql = String.format(jmObj.getFinancialInfoSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Finanacial Info records from DL of (.*)$")
    public void getFinanceInfoRecFromDL(String tableName) {
        Log.info("We get the Financial Info records from JMF DL..");
        sql = String.format(jmObj.getFinancialInfoSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Finanacial Info records in JMF MySQL and DL of (.*)$")
    public void compareJMFinanceInfoDataSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the financial_Information records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_WORK_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_WORK_ID));

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRODUCT_WORK_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_WORK_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BUSINESS_CONTROLLER_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_CONTROLLER_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_CONTROLLER_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_CONTROLLER_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_CONTROLLER_NAME() != null)) {
                    Assert.assertEquals("The BUSINESS_CONTROLLER_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_CONTROLLER_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_CONTROLLER_NAME());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BUSINESS_UNIT_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_UNIT_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_UNIT_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_UNIT_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_UNIT_CODE() != null)) {
                    Assert.assertEquals("The BUSINESS_UNIT_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_UNIT_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_UNIT_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PMG_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_CODE() != null)) {
                    Assert.assertEquals("The PMG_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PMC_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CODE() != null)) {
                    Assert.assertEquals("The PMC_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPCO_R12_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_ID() != null)) {
                    Assert.assertEquals("The OPCO_R12_ID is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_ID());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPCO_R12_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_CODE() != null)) {
                    Assert.assertEquals("The OPCO_R12_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " CONTRACT_OPCO_R12_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_OPCO_R12_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_OPCO_R12_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_OPCO_R12_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_OPCO_R12_CODE() != null)) {
                    Assert.assertEquals("The CONTRACT_OPCO_R12_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_OPCO_R12_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_OPCO_R12_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " RESPONSIBILITY_CENTRE_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESPONSIBILITY_CENTRE_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESPONSIBILITY_CENTRE_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESPONSIBILITY_CENTRE_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESPONSIBILITY_CENTRE_CODE() != null)) {
                    Assert.assertEquals("The RESPONSIBILITY_CENTRE_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESPONSIBILITY_CENTRE_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESPONSIBILITY_CENTRE_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " HFM_HIERARCHY_POSITION_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getHFM_HIERARCHY_POSITION_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getHFM_HIERARCHY_POSITION_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getHFM_HIERARCHY_POSITION_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getHFM_HIERARCHY_POSITION_CODE() != null)) {
                    Assert.assertEquals("The HFM_HIERARCHY_POSITION_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getHFM_HIERARCHY_POSITION_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getHFM_HIERARCHY_POSITION_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " CITY_OPCO_R12_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCITY_OPCO_R12_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCITY_OPCO_R12_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCITY_OPCO_R12_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCITY_OPCO_R12_CODE() != null)) {
                    Assert.assertEquals("The CITY_OPCO_R12_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCITY_OPCO_R12_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCITY_OPCO_R12_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " COUNTRY_OPCO_R12_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOUNTRY_OPCO_R12_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOUNTRY_OPCO_R12_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOUNTRY_OPCO_R12_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOUNTRY_OPCO_R12_CODE() != null)) {
                    Assert.assertEquals("The COUNTRY_OPCO_R12_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOUNTRY_OPCO_R12_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOUNTRY_OPCO_R12_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPCO_R12_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_NAME() != null)) {
                    Assert.assertEquals("The OPCO_R12_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_NAME());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPCO_R12_LEGAL_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_LEGAL_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_LEGAL_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_LEGAL_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_LEGAL_NAME() != null)) {
                    Assert.assertEquals("The OPCO_R12_LEGAL_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPCO_R12_LEGAL_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPCO_R12_LEGAL_NAME());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " REFUND_OPTION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREFUND_OPTION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREFUND_OPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREFUND_OPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREFUND_OPTION() != null)) {
                    Assert.assertEquals("The REFUND_OPTION is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREFUND_OPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREFUND_OPTION());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " REMINDER_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMINDER_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMINDER_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMINDER_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMINDER_DATE() != null)) {
                    Assert.assertEquals("The REMINDER_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMINDER_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMINDER_DATE().substring(0,10));
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " REMINDER_OPTION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMINDER_OPTION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMINDER_OPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMINDER_OPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMINDER_OPTION() != null)) {
                    Assert.assertEquals("The REMINDER_OPTION is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMINDER_OPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMINDER_OPTION());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " RENEWAL_OPTION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENEWAL_OPTION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENEWAL_OPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENEWAL_OPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENEWAL_OPTION() != null)) {
                    Assert.assertEquals("The RENEWAL_OPTION is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENEWAL_OPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENEWAL_OPTION());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " RENEWAL_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENEWAL_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENEWAL_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENEWAL_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENEWAL_DATE() != null)) {
                    Assert.assertEquals("The RENEWAL_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENEWAL_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENEWAL_DATE().substring(0,10));
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BUSINESS_CONTROLLER_EMAIL_ADDRESS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_CONTROLLER_EMAIL_ADDRESS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_CONTROLLER_EMAIL_ADDRESS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_CONTROLLER_EMAIL_ADDRESS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_CONTROLLER_EMAIL_ADDRESS() != null)) {
                    Assert.assertEquals("The BUSINESS_CONTROLLER_EMAIL_ADDRESS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUSINESS_CONTROLLER_EMAIL_ADDRESS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUSINESS_CONTROLLER_EMAIL_ADDRESS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " CLAIMS_HANDLING_OPTION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCLAIMS_HANDLING_OPTION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCLAIMS_HANDLING_OPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCLAIMS_HANDLING_OPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCLAIMS_HANDLING_OPTION() != null)) {
                    Assert.assertEquals("The CLAIMS_HANDLING_OPTION is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCLAIMS_HANDLING_OPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCLAIMS_HANDLING_OPTION());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " CLAIMS_HANDLING_END_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCLAIMS_HANDLING_END_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCLAIMS_HANDLING_END_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCLAIMS_HANDLING_END_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCLAIMS_HANDLING_END_DATE() != null)) {
                    Assert.assertEquals("The CLAIMS_HANDLING_END_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCLAIMS_HANDLING_END_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCLAIMS_HANDLING_END_DATE().substring(0,10));
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACKISSUES_HANDLING_OPTION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKISSUES_HANDLING_OPTION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKISSUES_HANDLING_OPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKISSUES_HANDLING_OPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKISSUES_HANDLING_OPTION() != null)) {
                    Assert.assertEquals("The BACKISSUES_HANDLING_OPTION is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKISSUES_HANDLING_OPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKISSUES_HANDLING_OPTION());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACKISSUES_HANDLING_END_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKISSUES_HANDLING_END_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKISSUES_HANDLING_END_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKISSUES_HANDLING_END_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKISSUES_HANDLING_END_DATE() != null)) {
                    Assert.assertEquals("The BACKISSUES_HANDLING_END_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKISSUES_HANDLING_END_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKISSUES_HANDLING_END_DATE().substring(0,10));
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

            }
        }
    }

    @When("^We get the JMF Legal Info records from JMF MySQL of (.*)")
    public void getLegalInfoRecordsFromJMF(String tableName) {
        Log.info("We get the Legal Info records from JMF MySql..");
        sql = String.format(jmObj.getLegalInfoSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Legal Info records from DL of (.*)$")
    public void getLegalInfoRecordsFromDL(String tableName) {
        Log.info("We get the Legal Info records from JMF DL..");
        sql = String.format(jmObj.getLegalInfoSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Legal Info records in JMF MySQL and DL of (.*)$")
    public void compareJMFLeagalInfoRecSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Legal records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_WORK_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_WORK_ID));

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRODUCT_WORK_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_WORK_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OWNERSHIP_BRAND_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOWNERSHIP_BRAND_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOWNERSHIP_BRAND_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOWNERSHIP_BRAND_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOWNERSHIP_BRAND_TYPE() != null)) {
                    Assert.assertEquals("The OWNERSHIP_BRAND_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOWNERSHIP_BRAND_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOWNERSHIP_BRAND_TYPE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " JOINT_OWNERSHIP_DETAILS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOINT_OWNERSHIP_DETAILS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOINT_OWNERSHIP_DETAILS());

                String jointOwnershipDetailSql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOINT_OWNERSHIP_DETAILS();

                if (jointOwnershipDetailSql != null || dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOINT_OWNERSHIP_DETAILS() != null) {

                    if(jointOwnershipDetailSql.isEmpty()){
                        jointOwnershipDetailSql = null;
                    }
                    Assert.assertEquals("The JOINT_OWNERSHIP_DETAILS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            jointOwnershipDetailSql,dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOINT_OWNERSHIP_DETAILS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_CONTRACT_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_DATE() != null)) {
                    Assert.assertEquals("The SOCIETY_CONTRACT_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_DATE().substring(0,10));
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_CONTRACT_EXPIRY_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE() != null)) {
                    Assert.assertEquals("The SOCIETY_CONTRACT_EXPIRY_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE().substring(0,10));
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " JOURNAL_COPYRIGHT_OWNER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_COPYRIGHT_OWNER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_COPYRIGHT_OWNER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_COPYRIGHT_OWNER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_COPYRIGHT_OWNER() != null)) {
                    Assert.assertEquals("The JOURNAL_COPYRIGHT_OWNER is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_COPYRIGHT_OWNER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_COPYRIGHT_OWNER());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " STANDARD_COPYRIGHT_STATEMENT_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTANDARD_COPYRIGHT_STATEMENT_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTANDARD_COPYRIGHT_STATEMENT_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTANDARD_COPYRIGHT_STATEMENT_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTANDARD_COPYRIGHT_STATEMENT_IND() != null)) {
                    Assert.assertEquals("The STANDARD_COPYRIGHT_STATEMENT_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTANDARD_COPYRIGHT_STATEMENT_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTANDARD_COPYRIGHT_STATEMENT_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NON_STANDARD_COPYRIGHT_STATEMENT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_STANDARD_COPYRIGHT_STATEMENT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_STANDARD_COPYRIGHT_STATEMENT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_STANDARD_COPYRIGHT_STATEMENT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_STANDARD_COPYRIGHT_STATEMENT() != null)) {
                    Assert.assertEquals("The NON_STANDARD_COPYRIGHT_STATEMENT is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_STANDARD_COPYRIGHT_STATEMENT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_STANDARD_COPYRIGHT_STATEMENT());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE() != null)) {
                    Assert.assertEquals("The OPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_ARTICLE_COPYRIGHT_TYPE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ARTICLE_DISCLAIMER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_DISCLAIMER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_DISCLAIMER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_DISCLAIMER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_DISCLAIMER() != null)) {
                    Assert.assertEquals("The ARTICLE_DISCLAIMER is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_DISCLAIMER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_DISCLAIMER());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLISHING_RIGHT_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLISHING_RIGHT_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLISHING_RIGHT_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLISHING_RIGHT_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLISHING_RIGHT_TYPE() != null)) {
                    Assert.assertEquals("The PUBLISHING_RIGHT_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLISHING_RIGHT_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLISHING_RIGHT_TYPE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRINT_RIGHTS_SECURED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINT_RIGHTS_SECURED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINT_RIGHTS_SECURED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINT_RIGHTS_SECURED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINT_RIGHTS_SECURED_IND() != null)) {
                    Assert.assertEquals("The PRINT_RIGHTS_SECURED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINT_RIGHTS_SECURED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINT_RIGHTS_SECURED_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EXCLUSIVE_RIGHTS_SECURED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEXCLUSIVE_RIGHTS_SECURED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEXCLUSIVE_RIGHTS_SECURED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEXCLUSIVE_RIGHTS_SECURED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEXCLUSIVE_RIGHTS_SECURED_IND() != null)) {
                    Assert.assertEquals("The EXCLUSIVE_RIGHTS_SECURED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEXCLUSIVE_RIGHTS_SECURED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEXCLUSIVE_RIGHTS_SECURED_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NO_EXCLUSIVE_PRINT_RIGHTS_REASON => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNO_EXCLUSIVE_PRINT_RIGHTS_REASON() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNO_EXCLUSIVE_PRINT_RIGHTS_REASON());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNO_EXCLUSIVE_PRINT_RIGHTS_REASON() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNO_EXCLUSIVE_PRINT_RIGHTS_REASON() != null)) {
                    Assert.assertEquals("The NO_EXCLUSIVE_PRINT_RIGHTS_REASON is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNO_EXCLUSIVE_PRINT_RIGHTS_REASON(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNO_EXCLUSIVE_PRINT_RIGHTS_REASON());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ELECTRONIC_PRINT_RIGHTS_SECURED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELECTRONIC_PRINT_RIGHTS_SECURED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELECTRONIC_PRINT_RIGHTS_SECURED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELECTRONIC_PRINT_RIGHTS_SECURED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELECTRONIC_PRINT_RIGHTS_SECURED_IND() != null)) {
                    Assert.assertEquals("The ELECTRONIC_PRINT_RIGHTS_SECURED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELECTRONIC_PRINT_RIGHTS_SECURED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELECTRONIC_PRINT_RIGHTS_SECURED_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON() != null)) {
                    Assert.assertEquals("The NO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNO_EXCLUSIVE_ELECTRONIC_RIGHTS_REASON());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " RIGHTS_RESTRICTIONS_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRIGHTS_RESTRICTIONS_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRIGHTS_RESTRICTIONS_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRIGHTS_RESTRICTIONS_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRIGHTS_RESTRICTIONS_IND() != null)) {
                    Assert.assertEquals("The RIGHTS_RESTRICTIONS_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRIGHTS_RESTRICTIONS_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRIGHTS_RESTRICTIONS_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " RIGHTS_RESTRICTIONS_LIST => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRIGHTS_RESTRICTIONS_LIST() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRIGHTS_RESTRICTIONS_LIST());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRIGHTS_RESTRICTIONS_LIST() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRIGHTS_RESTRICTIONS_LIST() != null)) {
                    Assert.assertEquals("The RIGHTS_RESTRICTIONS_LIST is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRIGHTS_RESTRICTIONS_LIST(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRIGHTS_RESTRICTIONS_LIST());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACKFILE_CONSENT_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKFILE_CONSENT_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKFILE_CONSENT_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKFILE_CONSENT_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKFILE_CONSENT_IND() != null)) {
                    Assert.assertEquals("The BACKFILE_CONSENT_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKFILE_CONSENT_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKFILE_CONSENT_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACKFILE_CONSENT_INFO => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKFILE_CONSENT_INFO() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKFILE_CONSENT_INFO());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKFILE_CONSENT_INFO() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKFILE_CONSENT_INFO() != null)) {
                    Assert.assertEquals("The BACKFILE_CONSENT_INFO is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKFILE_CONSENT_INFO(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKFILE_CONSENT_INFO());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACK_RIGHTS_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_RIGHTS_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_RIGHTS_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_RIGHTS_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_RIGHTS_TYPE() != null)) {
                    Assert.assertEquals("The BACK_RIGHTS_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_RIGHTS_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_RIGHTS_TYPE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACK_RIGHTS_START_VOLUME_AND_ISSUE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_RIGHTS_START_VOLUME_AND_ISSUE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_RIGHTS_START_VOLUME_AND_ISSUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_RIGHTS_START_VOLUME_AND_ISSUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_RIGHTS_START_VOLUME_AND_ISSUE() != null)) {
                    Assert.assertEquals("The BACK_RIGHTS_START_VOLUME_AND_ISSUE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_RIGHTS_START_VOLUME_AND_ISSUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_RIGHTS_START_VOLUME_AND_ISSUE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND() != null)) {
                    Assert.assertEquals("The NON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_EXCLUSIVE_ELECTRONIC_RIGHTS_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " POST_TERMINATION_ELECTRONIC_RIGHTS_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPOST_TERMINATION_ELECTRONIC_RIGHTS_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPOST_TERMINATION_ELECTRONIC_RIGHTS_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPOST_TERMINATION_ELECTRONIC_RIGHTS_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPOST_TERMINATION_ELECTRONIC_RIGHTS_TYPE() != null)) {
                    Assert.assertEquals("The POST_TERMINATION_ELECTRONIC_RIGHTS_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPOST_TERMINATION_ELECTRONIC_RIGHTS_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPOST_TERMINATION_ELECTRONIC_RIGHTS_TYPE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PERMISSION_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERMISSION_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERMISSION_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERMISSION_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERMISSION_TYPE() != null)) {
                    Assert.assertEquals("The PERMISSION_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERMISSION_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERMISSION_TYPE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PERMISSION_HANDLING_EMAIL_ADDRESS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERMISSION_HANDLING_EMAIL_ADDRESS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERMISSION_HANDLING_EMAIL_ADDRESS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERMISSION_HANDLING_EMAIL_ADDRESS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERMISSION_HANDLING_EMAIL_ADDRESS() != null)) {
                    Assert.assertEquals("The PERMISSION_HANDLING_EMAIL_ADDRESS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERMISSION_HANDLING_EMAIL_ADDRESS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERMISSION_HANDLING_EMAIL_ADDRESS());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_MEMBERSHIP_HELD_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MEMBERSHIP_HELD_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MEMBERSHIP_HELD_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MEMBERSHIP_HELD_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MEMBERSHIP_HELD_TYPE() != null)) {
                    Assert.assertEquals("The SOCIETY_MEMBERSHIP_HELD_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MEMBERSHIP_HELD_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MEMBERSHIP_HELD_TYPE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SPECIAL_ARRANGEMENTS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIAL_ARRANGEMENTS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIAL_ARRANGEMENTS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIAL_ARRANGEMENTS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIAL_ARRANGEMENTS() != null)) {
                    Assert.assertEquals("The SPECIAL_ARRANGEMENTS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIAL_ARRANGEMENTS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIAL_ARRANGEMENTS());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DESPATCH_METHOD => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDESPATCH_METHOD() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDESPATCH_METHOD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDESPATCH_METHOD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDESPATCH_METHOD() != null)) {
                    Assert.assertEquals("The DESPATCH_METHOD is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDESPATCH_METHOD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDESPATCH_METHOD());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " CONTRACT_SEEN_BY_ELS_ATTORNEY_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_SEEN_BY_ELS_ATTORNEY_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_SEEN_BY_ELS_ATTORNEY_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_SEEN_BY_ELS_ATTORNEY_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_SEEN_BY_ELS_ATTORNEY_IND() != null)) {
                    Assert.assertEquals("The CONTRACT_SEEN_BY_ELS_ATTORNEY_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_SEEN_BY_ELS_ATTORNEY_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_SEEN_BY_ELS_ATTORNEY_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " CONTRACT_SENT_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_SENT_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_SENT_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_SENT_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_SENT_IND() != null)) {
                    Assert.assertEquals("The CONTRACT_SENT_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCONTRACT_SENT_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCONTRACT_SENT_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DIRECT_BILLING_MEMBERSHIP => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDIRECT_BILLING_MEMBERSHIP() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDIRECT_BILLING_MEMBERSHIP());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDIRECT_BILLING_MEMBERSHIP() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDIRECT_BILLING_MEMBERSHIP() != null)) {
                    Assert.assertEquals("The DIRECT_BILLING_MEMBERSHIP is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDIRECT_BILLING_MEMBERSHIP(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDIRECT_BILLING_MEMBERSHIP());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DIRECT_BILLING_MEMBERSHIP_WITH_FEE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDIRECT_BILLING_MEMBERSHIP_WITH_FEE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDIRECT_BILLING_MEMBERSHIP_WITH_FEE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDIRECT_BILLING_MEMBERSHIP_WITH_FEE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDIRECT_BILLING_MEMBERSHIP_WITH_FEE() != null)) {
                    Assert.assertEquals("The DIRECT_BILLING_MEMBERSHIP_WITH_FEE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDIRECT_BILLING_MEMBERSHIP_WITH_FEE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDIRECT_BILLING_MEMBERSHIP_WITH_FEE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_MAINTAINED_PREPAYMENT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MAINTAINED_PREPAYMENT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MAINTAINED_PREPAYMENT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MAINTAINED_PREPAYMENT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MAINTAINED_PREPAYMENT() != null)) {
                    Assert.assertEquals("The SOCIETY_MAINTAINED_PREPAYMENT is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MAINTAINED_PREPAYMENT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MAINTAINED_PREPAYMENT());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_MAINTAINED_ACCOUNT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MAINTAINED_ACCOUNT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MAINTAINED_ACCOUNT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MAINTAINED_ACCOUNT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MAINTAINED_ACCOUNT() != null)) {
                    Assert.assertEquals("The SOCIETY_MAINTAINED_ACCOUNT is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_MAINTAINED_ACCOUNT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_MAINTAINED_ACCOUNT());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " AUTOMATIC_RENEWAL => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAUTOMATIC_RENEWAL() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAUTOMATIC_RENEWAL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAUTOMATIC_RENEWAL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAUTOMATIC_RENEWAL() != null)) {
                    Assert.assertEquals("The AUTOMATIC_RENEWAL is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAUTOMATIC_RENEWAL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAUTOMATIC_RENEWAL());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MAILING_LABELS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_LABELS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_LABELS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_LABELS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_LABELS() != null)) {
                    Assert.assertEquals("The MAILING_LABELS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_LABELS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_LABELS());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " TITLE_INCLUDED_IN_CK => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTITLE_INCLUDED_IN_CK() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTITLE_INCLUDED_IN_CK());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTITLE_INCLUDED_IN_CK() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTITLE_INCLUDED_IN_CK() != null)) {
                    Assert.assertEquals("The TITLE_INCLUDED_IN_CK is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTITLE_INCLUDED_IN_CK(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTITLE_INCLUDED_IN_CK());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOC_AGREED_TO_CK_CONTENT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_AGREED_TO_CK_CONTENT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_AGREED_TO_CK_CONTENT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_AGREED_TO_CK_CONTENT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_AGREED_TO_CK_CONTENT() != null)) {
                    Assert.assertEquals("The SOC_AGREED_TO_CK_CONTENT is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_AGREED_TO_CK_CONTENT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_AGREED_TO_CK_CONTENT());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOC_CK_CONTENT_AGREEMENT_TEXT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_CK_CONTENT_AGREEMENT_TEXT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_CK_CONTENT_AGREEMENT_TEXT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_CK_CONTENT_AGREEMENT_TEXT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_CK_CONTENT_AGREEMENT_TEXT() != null)) {
                    Assert.assertEquals("The SOC_CK_CONTENT_AGREEMENT_TEXT is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_CK_CONTENT_AGREEMENT_TEXT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_CK_CONTENT_AGREEMENT_TEXT());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MONTHS_TO_KEEP_TRANSFER_ONLINE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMONTHS_TO_KEEP_TRANSFER_ONLINE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMONTHS_TO_KEEP_TRANSFER_ONLINE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMONTHS_TO_KEEP_TRANSFER_ONLINE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMONTHS_TO_KEEP_TRANSFER_ONLINE() != null)) {
                    Assert.assertEquals("The MONTHS_TO_KEEP_TRANSFER_ONLINE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMONTHS_TO_KEEP_TRANSFER_ONLINE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMONTHS_TO_KEEP_TRANSFER_ONLINE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOC_CK_CONTENT_AGREEMENT_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_CK_CONTENT_AGREEMENT_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_CK_CONTENT_AGREEMENT_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_CK_CONTENT_AGREEMENT_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_CK_CONTENT_AGREEMENT_DATE() != null)) {
                    Assert.assertEquals("The SOC_CK_CONTENT_AGREEMENT_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOC_CK_CONTENT_AGREEMENT_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_CK_CONTENT_AGREEMENT_DATE().substring(0,10));
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Manifestation Info records from JMF MySQL of (.*)")
    public void getManifDetailRecordsFromJMF(String tableName) {
        Log.info("We get the Manifestation Info records from JMF MySql..");
        sql = String.format(jmObj.getManifDetailSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Manifestation Info records from DL of (.*)$")
    public void getManifDetailRecordsFromDL(String tableName) {
        Log.info("We get the Manifestation Info records from JMF DL..");
        sql = String.format(jmObj.getManifDetailSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Manifestation Info records in JMF MySQL and DL of (.*)$")
    public void compareJMManifDetailsDataSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Manifestion Electronic Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_MANIFESTATION_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_MANIFESTATION_ID));

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PRODUCT_MANIFESTATION_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_MANIFESTATION_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " EMBARGO_TIMES_INDICATOR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBARGO_TIMES_INDICATOR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBARGO_TIMES_INDICATOR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBARGO_TIMES_INDICATOR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBARGO_TIMES_INDICATOR() != null)) {
                    Assert.assertEquals("The EMBARGO_TIMES_INDICATOR is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBARGO_TIMES_INDICATOR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBARGO_TIMES_INDICATOR());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ELECTRONIC_RIGHTS_SECURED_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELECTRONIC_RIGHTS_SECURED_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELECTRONIC_RIGHTS_SECURED_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELECTRONIC_RIGHTS_SECURED_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELECTRONIC_RIGHTS_SECURED_TYPE() != null)) {
                    Assert.assertEquals("The ELECTRONIC_RIGHTS_SECURED_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELECTRONIC_RIGHTS_SECURED_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELECTRONIC_RIGHTS_SECURED_TYPE());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ONLINE_LAUNCH_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_LAUNCH_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_LAUNCH_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_LAUNCH_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_LAUNCH_DATE() != null)) {
                    Assert.assertEquals("The ONLINE_LAUNCH_DATE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_LAUNCH_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_LAUNCH_DATE());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ARTICLE_IN_PRESS_S5_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S5_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S5_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S5_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S5_IND() != null)) {
                    Assert.assertEquals("The ARTICLE_IN_PRESS_S5_IND is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S5_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S5_IND());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ARTICLE_IN_PRESS_S100_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S100_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S100_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S100_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S100_IND() != null)) {
                    Assert.assertEquals("The ARTICLE_IN_PRESS_S100_IND is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S100_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S100_IND());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ARTICLE_IN_PRESS_S250_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S250_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S250_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S250_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S250_IND() != null)) {
                    Assert.assertEquals("The ARTICLE_IN_PRESS_S250_IND is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_IN_PRESS_S250_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_IN_PRESS_S250_IND());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " EMBARGO_TIMES_NUMBER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBARGO_TIMES_NUMBER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBARGO_TIMES_NUMBER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBARGO_TIMES_NUMBER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBARGO_TIMES_NUMBER() != null)) {
                    Assert.assertEquals("The EMBARGO_TIMES_NUMBER is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBARGO_TIMES_NUMBER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBARGO_TIMES_NUMBER());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ONLINE_LAST_ISSUE_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_LAST_ISSUE_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_LAST_ISSUE_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_LAST_ISSUE_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_LAST_ISSUE_DATE() != null)) {
                    Assert.assertEquals("The ONLINE_LAST_ISSUE_DATE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_LAST_ISSUE_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_LAST_ISSUE_DATE().substring(0,10));
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Manifestation Print records from JMF MySQL of (.*)")
    public void getManifPrintRecordsFromJMF(String tableName) {
        Log.info("We get the Manifestation Print records from JMF MySql..");
        sql = String.format(jmObj.getManifPrintSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Manifestation Print records from DL of (.*)$")
    public void getManifPrintRecordsFromDL(String tableName) {
        Log.info("We get the Manifestation Print records from JMF DL..");
        sql = String.format(jmObj.getManifPrintSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Manifestation Print records in JMF MySQL and DL of (.*)$")
    public void compareManifPrintSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Manifestion Print Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_MANIFESTATION_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_MANIFESTATION_ID));

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PRODUCT_MANIFESTATION_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_MANIFESTATION_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID()+"is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " TRIM_SIZE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTRIM_SIZE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTRIM_SIZE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTRIM_SIZE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTRIM_SIZE() != null)) {
                    String TrimSizeSql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTRIM_SIZE();
                    if(TrimSizeSql.isEmpty()){
                        TrimSizeSql = null;
                    }
                    Assert.assertEquals("The TRIM_SIZE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            TrimSizeSql,dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTRIM_SIZE());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " BASE_PRINT_RUN_NUMBER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBASE_PRINT_RUN_NUMBER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBASE_PRINT_RUN_NUMBER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBASE_PRINT_RUN_NUMBER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBASE_PRINT_RUN_NUMBER() != null)) {
                    Assert.assertEquals("The BASE_PRINT_RUN_NUMBER is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBASE_PRINT_RUN_NUMBER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBASE_PRINT_RUN_NUMBER());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " COLOUR_FREQUENCY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_FREQUENCY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_FREQUENCY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_FREQUENCY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_FREQUENCY() != null)) {
                    Assert.assertEquals("The COLOUR_FREQUENCY is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_FREQUENCY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_FREQUENCY());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ARTWORK_SENSITIVITY_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTWORK_SENSITIVITY_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTWORK_SENSITIVITY_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTWORK_SENSITIVITY_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTWORK_SENSITIVITY_IND() != null)) {
                    Assert.assertEquals("The ARTWORK_SENSITIVITY_IND is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTWORK_SENSITIVITY_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTWORK_SENSITIVITY_IND());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " MAILING_BREAKDOWN_EUROPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_EUROPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_EUROPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_EUROPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_EUROPE() != null)) {
                    Assert.assertEquals("The MAILING_BREAKDOWN_EUROPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_EUROPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_EUROPE());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " MAILING_BREAKDOWN_USA => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_USA() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_USA());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_USA() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_USA() != null)) {
                    Assert.assertEquals("The MAILING_BREAKDOWN_USA is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_USA(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_USA());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " MAILING_BREAKDOWN_ROW => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_ROW() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_ROW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_ROW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_ROW() != null)) {
                    Assert.assertEquals("The MAILING_BREAKDOWN_ROW is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAILING_BREAKDOWN_ROW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAILING_BREAKDOWN_ROW());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ZERO_WAREHOUSING_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getZERO_WAREHOUSING_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getZERO_WAREHOUSING_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getZERO_WAREHOUSING_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getZERO_WAREHOUSING_IND() != null)) {
                    Assert.assertEquals("The ZERO_WAREHOUSING_IND is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getZERO_WAREHOUSING_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getZERO_WAREHOUSING_IND());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " BACK_STOCK_WAREHOUSE_LOCATION_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_STOCK_WAREHOUSE_LOCATION_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_STOCK_WAREHOUSE_LOCATION_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_STOCK_WAREHOUSE_LOCATION_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_STOCK_WAREHOUSE_LOCATION_TYPE() != null)) {
                    Assert.assertEquals("The BACK_STOCK_WAREHOUSE_LOCATION_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACK_STOCK_WAREHOUSE_LOCATION_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACK_STOCK_WAREHOUSE_LOCATION_TYPE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PRINTER_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINTER_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINTER_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINTER_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINTER_TYPE() != null)) {
                    Assert.assertEquals("The PRINTER_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINTER_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINTER_TYPE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PRINTER_LOCATION_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINTER_LOCATION_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINTER_LOCATION_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINTER_LOCATION_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINTER_LOCATION_TYPE() != null)) {
                    Assert.assertEquals("The PRINTER_LOCATION_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINTER_LOCATION_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINTER_LOCATION_TYPE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " INTERIOR_PAPER_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINTERIOR_PAPER_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINTERIOR_PAPER_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINTERIOR_PAPER_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINTERIOR_PAPER_TYPE() != null)) {
                    Assert.assertEquals("The INTERIOR_PAPER_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINTERIOR_PAPER_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINTERIOR_PAPER_TYPE());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " COVER_PAPER_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOVER_PAPER_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOVER_PAPER_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOVER_PAPER_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOVER_PAPER_TYPE() != null)) {
                    Assert.assertEquals("The COVER_PAPER_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOVER_PAPER_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOVER_PAPER_TYPE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " DISTRIBUTOR_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTOR_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTOR_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTOR_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTOR_CODE() != null)) {
                    Assert.assertEquals("The DISTRIBUTOR_CODE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTOR_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTOR_CODE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " DISTRIBUTOR_LOCATION_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTOR_LOCATION_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTOR_LOCATION_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTOR_LOCATION_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTOR_LOCATION_CODE() != null)) {
                    Assert.assertEquals("The DISTRIBUTOR_LOCATION_CODE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTOR_LOCATION_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTOR_LOCATION_CODE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PRINT_MODEL_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINT_MODEL_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINT_MODEL_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINT_MODEL_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINT_MODEL_CODE() != null)) {
                    Assert.assertEquals("The PRINT_MODEL_CODE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRINT_MODEL_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRINT_MODEL_CODE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " SEPARATE_PRINT_RUN_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSEPARATE_PRINT_RUN_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSEPARATE_PRINT_RUN_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSEPARATE_PRINT_RUN_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSEPARATE_PRINT_RUN_IND() != null)) {
                    Assert.assertEquals("The SEPARATE_PRINT_RUN_IND is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSEPARATE_PRINT_RUN_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSEPARATE_PRINT_RUN_IND());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " OFFPRINT_PRICING_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOFFPRINT_PRICING_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOFFPRINT_PRICING_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOFFPRINT_PRICING_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOFFPRINT_PRICING_CODE() != null)) {
                    Assert.assertEquals("The OFFPRINT_PRICING_CODE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOFFPRINT_PRICING_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOFFPRINT_PRICING_CODE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " OFFPRINT_COVER_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOFFPRINT_COVER_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOFFPRINT_COVER_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOFFPRINT_COVER_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOFFPRINT_COVER_IND() != null)) {
                    Assert.assertEquals("The OFFPRINT_COVER_IND is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOFFPRINT_COVER_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOFFPRINT_COVER_IND());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " FREE_ISSUES_QUANTITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFREE_ISSUES_QUANTITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_ISSUES_QUANTITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFREE_ISSUES_QUANTITY() != null ||
                        dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_ISSUES_QUANTITY() != null) {
                    Assert.assertEquals("The FREE_ISSUES_QUANTITY is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFREE_ISSUES_QUANTITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_ISSUES_QUANTITY());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " FREE_OFFPRINTS_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFREE_OFFPRINTS_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_OFFPRINTS_TYPE());

                String offerPrintType = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFREE_OFFPRINTS_TYPE();

                if (offerPrintType != null || dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_OFFPRINTS_TYPE() != null) {
                   if(offerPrintType.isEmpty()){
                       offerPrintType = null;
                   }
                    Assert.assertEquals("The FREE_OFFPRINTS_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            offerPrintType, dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_OFFPRINTS_TYPE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " FREE_PAID_COLOUR_OFFPRINTS_QUANTITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFREE_PAID_COLOUR_OFFPRINTS_QUANTITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_PAID_COLOUR_OFFPRINTS_QUANTITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFREE_PAID_COLOUR_OFFPRINTS_QUANTITY() != null ||
                        dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_PAID_COLOUR_OFFPRINTS_QUANTITY() != null) {
                    Assert.assertEquals("The FREE_PAID_COLOUR_OFFPRINTS_QUANTITY is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFREE_PAID_COLOUR_OFFPRINTS_QUANTITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFREE_PAID_COLOUR_OFFPRINTS_QUANTITY());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " COLOUR_PRINTING_CURRENCY_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_PRINTING_CURRENCY_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_PRINTING_CURRENCY_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_PRINTING_CURRENCY_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_PRINTING_CURRENCY_CODE() != null)) {
                    Assert.assertEquals("The COLOUR_PRINTING_CURRENCY_CODE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_PRINTING_CURRENCY_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_PRINTING_CURRENCY_CODE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " COLOUR_ARTWORK_EXCEPTIONS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_ARTWORK_EXCEPTIONS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_ARTWORK_EXCEPTIONS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_ARTWORK_EXCEPTIONS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_ARTWORK_EXCEPTIONS() != null)) {
                    Assert.assertEquals("The COLOUR_ARTWORK_EXCEPTIONS is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOLOUR_ARTWORK_EXCEPTIONS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOLOUR_ARTWORK_EXCEPTIONS());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " SOCIETY_OWNS_LABELS_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_OWNS_LABELS_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_OWNS_LABELS_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_OWNS_LABELS_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_OWNS_LABELS_IND() != null)) {
                    Assert.assertEquals("The SOCIETY_OWNS_LABELS_IND is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_OWNS_LABELS_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_OWNS_LABELS_IND());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " BINDING_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBINDING_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBINDING_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBINDING_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBINDING_TYPE() != null)) {
                    Assert.assertEquals("The BINDING_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBINDING_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBINDING_TYPE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " SPECIAL_BULK_ARRANGEMENTS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIAL_BULK_ARRANGEMENTS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIAL_BULK_ARRANGEMENTS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIAL_BULK_ARRANGEMENTS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIAL_BULK_ARRANGEMENTS() != null)) {
                    Assert.assertEquals("The SPECIAL_BULK_ARRANGEMENTS is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIAL_BULK_ARRANGEMENTS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIAL_BULK_ARRANGEMENTS());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " COST_FIRST_PRINTED_COLOUR_UNIT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOST_FIRST_PRINTED_COLOUR_UNIT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOST_FIRST_PRINTED_COLOUR_UNIT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOST_FIRST_PRINTED_COLOUR_UNIT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOST_FIRST_PRINTED_COLOUR_UNIT() != null)) {
                    Assert.assertEquals("The COST_FIRST_PRINTED_COLOUR_UNIT is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOST_FIRST_PRINTED_COLOUR_UNIT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOST_FIRST_PRINTED_COLOUR_UNIT());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " COST_NEXT_PRINTED_COLOUR_UNIT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOST_NEXT_PRINTED_COLOUR_UNIT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOST_NEXT_PRINTED_COLOUR_UNIT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOST_NEXT_PRINTED_COLOUR_UNIT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOST_NEXT_PRINTED_COLOUR_UNIT() != null)) {
                    Assert.assertEquals("The COST_NEXT_PRINTED_COLOUR_UNIT is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOST_NEXT_PRINTED_COLOUR_UNIT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOST_NEXT_PRINTED_COLOUR_UNIT());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

            }
        }
    }

    @When("^We get the JMF Party Product records from JMF MySQL of (.*)")
    public void getPartyPrdtRecordsFromJMF(String tableName) {
        Log.info("We get the Party Product records from JMF MySql..");
        sql = String.format(jmObj.getPartyProdSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Party Product records from DL of (.*)$")
    public void getPartyPrdtRecordsFromDL(String tableName) {
        Log.info("We get the Party Product records from JMF DL..");
        sql = String.format(jmObj.getPartyProdSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Party Product records in JMF MySQL and DL of (.*)$")
    public void comparePartyProdSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Party Product Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPARTY_IN_PRODUCT_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPARTY_IN_PRODUCT_ID));
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " PARTY_IN_PRODUCT_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPARTY_IN_PRODUCT_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPARTY_IN_PRODUCT_ID() != null)) {
                    Assert.assertEquals("The PARTY_IN_PRODUCT_ID is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPARTY_IN_PRODUCT_ID());
                }

                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " F_PRODUCT_WORK => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK() != null)) {
                    Assert.assertEquals("The F_PRODUCT_WORK is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK());
                }

                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " PARTY_ROLE_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_ROLE_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPARTY_ROLE_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_ROLE_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPARTY_ROLE_TYPE() != null)) {
                    Assert.assertEquals("The PARTY_ROLE_TYPE is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_ROLE_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPARTY_ROLE_TYPE());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " EMAIL_ADDRESS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMAIL_ADDRESS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMAIL_ADDRESS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMAIL_ADDRESS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMAIL_ADDRESS() != null)) {
                    Assert.assertEquals("The EMAIL_ADDRESS is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMAIL_ADDRESS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMAIL_ADDRESS());
                }

                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " FULL_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFULL_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFULL_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFULL_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFULL_NAME() != null)) {
                    Assert.assertEquals("The FULL_NAME is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFULL_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFULL_NAME());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " PHONE_NUMBER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPHONE_NUMBER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPHONE_NUMBER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPHONE_NUMBER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPHONE_NUMBER() != null)) {
                    Assert.assertEquals("The PHONE_NUMBER is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPHONE_NUMBER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPHONE_NUMBER());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " ADDRESS_LINE_1 => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDRESS_LINE_1() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_1());
                String AddressLine1Sql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDRESS_LINE_1();

                if (AddressLine1Sql != null || dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_1() != null) {
                    if(AddressLine1Sql.isEmpty()){
                        AddressLine1Sql = null;
                    }
                    Assert.assertEquals("The ADDRESS_LINE_1 is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            AddressLine1Sql,dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_1());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " ADDRESS_LINE_2 => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDRESS_LINE_2() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_2());
                String AddressLine2Sql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDRESS_LINE_2();
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDRESS_LINE_2() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_2() != null)) {
                    if(AddressLine2Sql.isEmpty()){
                        AddressLine2Sql = null;
                    }
                    Assert.assertEquals("The ADDRESS_LINE_2 is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            AddressLine2Sql,dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_2());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " ADDRESS_LINE_3 => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDRESS_LINE_3() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_3());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDRESS_LINE_3() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_3() != null)) {
                    String addressLine3Sql =dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDRESS_LINE_3();
                    if(addressLine3Sql.isEmpty()){
                        addressLine3Sql = null;
                    }

                    Assert.assertEquals("The ADDRESS_LINE_3 is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            addressLine3Sql,dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDRESS_LINE_3());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " CITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCITY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCITY() != null)) {
                    Assert.assertEquals("The CITY is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCITY());
                }

                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " COUNTRY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOUNTRY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOUNTRY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOUNTRY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOUNTRY() != null)) {
                    Assert.assertEquals("The COUNTRY is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOUNTRY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOUNTRY());
                }

                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " STATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTATE() != null)) {
                    Assert.assertEquals("The STATE is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTATE());
                }

                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " POST_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPOST_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPOST_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPOST_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPOST_CODE() != null)) {
                    Assert.assertEquals("The POST_CODE is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPOST_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPOST_CODE());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " ORGANISATION_1 => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getORGANISATION_1() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getORGANISATION_1());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getORGANISATION_1() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getORGANISATION_1() != null)) {
                    Assert.assertEquals("The ORGANISATION_1 is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getORGANISATION_1(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getORGANISATION_1());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " PMX_PARTY_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PARTY_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PARTY_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PARTY_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PARTY_ID() != null)) {
                    Assert.assertEquals("The PMX_PARTY_ID is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PARTY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PARTY_ID());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " PEOPLEHUB_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID() != null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
                Log.info("PARTY_IN_PRODUCT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() +
                        " EPH_PERSON_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PERSON_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PERSON_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PERSON_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PERSON_ID() != null)) {
                    Assert.assertEquals("The EPH_PERSON_ID is incorrect for PARTY_IN_PRODUCT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PERSON_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PERSON_ID());
                }


            }

        }

    }

    @When("^We get the JMF Product Availability records from JMF MySQL of (.*)")
    public void getPrdtAvailRecordsFromJMF(String tableName) {
        Log.info("We get the Product Avail records from JMF MySql..");
        sql = String.format(jmObj.getProdAvailSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Product Availability records from DL of (.*)$")
    public void getPrdtAvailRecordsFromDL(String tableName) {
        Log.info("We get the Product Avail records from JMF DL..");
        sql = String.format(jmObj.getProdAvailSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Product Availability records in JMF MySQL and DL of (.*)$")
    public void comparePrdtAvailJMFSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Avail Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_AVAILABILITY_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_AVAILABILITY_ID));
                Log.info("PRODUCT_AVAILABILITY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID() +
                        " PRODUCT_AVAILABILITY_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_AVAILABILITY_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPARTY_IN_PRODUCT_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_AVAILABILITY_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_AVAILABILITY_ID());
                }

                Log.info("PRODUCT_AVAILABILITY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID() +
                        " F_PRODUCT_MANIFESTATION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_MANIFESTATION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_MANIFESTATION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPARTY_IN_PRODUCT_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPARTY_IN_PRODUCT_ID() != null)) {
                    Assert.assertEquals("The F_PRODUCT_MANIFESTATION is incorrect for PRODUCT_AVAILABILITY_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_MANIFESTATION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_MANIFESTATION());
                }

                Log.info("PRODUCT_AVAILABILITY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID() +
                        " APPLICATION_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_CODE() != null)) {
                    Assert.assertEquals("The APPLICATION_CODE is incorrect for PRODUCT_AVAILABILITY_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_CODE());
                }

                Log.info("PRODUCT_AVAILABILITY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_AVAILABILITY_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_AVAILABILITY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

            }

        }
    }

    @When("^We get the JMF Product Chronicle records from JMF MySQL of (.*)")
    public void getPrdtChroRecordsFromJMF(String tableName) {
        Log.info("We get the Product Chronicle records from JMF MySql..");
        sql = String.format(jmObj.getPrdtChronicleSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Product Chronicle records from DL of (.*)$")
    public void getPrdtChroRecordsFromDL(String tableName) {
        Log.info("We get the Product Chronicle records from JMF DL..");
        sql = String.format(jmObj.getPrdtChronicleSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Product Chronicle records in JMF MySQL and DL of (.*)$")
    public void comparePrdtChronicleJMFSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Chronicle Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_CHRONICLE_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_CHRONICLE_ID));
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " PRODUCT_CHRONICLE_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_CHRONICLE_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_CHRONICLE_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_CHRONICLE_ID is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_CHRONICLE_ID());
                }

                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " COMPLETED_ON => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOMPLETED_ON() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOMPLETED_ON());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOMPLETED_ON() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOMPLETED_ON() != null)) {
                    Assert.assertEquals("The COMPLETED_ON is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOMPLETED_ON(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOMPLETED_ON());
                }

                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " DISTRIBUTION_LIST => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTION_LIST() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTION_LIST());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTION_LIST() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTION_LIST() != null)) {
                    Assert.assertEquals("The DISTRIBUTION_LIST is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISTRIBUTION_LIST(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISTRIBUTION_LIST());
                }

                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " RENAME_REQUIRED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENAME_REQUIRED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENAME_REQUIRED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENAME_REQUIRED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENAME_REQUIRED_IND() != null)) {
                    Assert.assertEquals("The RENAME_REQUIRED_IND is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRENAME_REQUIRED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENAME_REQUIRED_IND());
                }

                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " CHRONICLE_STATUS_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE() != null)) {
                    Assert.assertEquals("The CHRONICLE_STATUS_CODE is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " CHRONICLE_SCENARIO_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE() != null)) {
                    Assert.assertEquals("The CHRONICLE_SCENARIO_CODE is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " STARTED_BY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTARTED_BY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTARTED_BY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTARTED_BY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTARTED_BY() != null)) {
                    Assert.assertEquals("The STARTED_BY is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTARTED_BY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTARTED_BY());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " STARTED_ON => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTARTED_ON() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTARTED_ON());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTARTED_ON() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTARTED_ON() != null)) {
                    Assert.assertEquals("The STARTED_ON is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSTARTED_ON(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSTARTED_ON());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " UPDATED_BY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getUPDATED_BY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getUPDATED_BY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getUPDATED_BY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getUPDATED_BY() != null)) {
                    Assert.assertEquals("The UPDATED_BY is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getUPDATED_BY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getUPDATED_BY());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " UPDATED_ON => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getUPDATED_ON() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getUPDATED_ON());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getUPDATED_ON() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getUPDATED_ON() != null)) {
                    Assert.assertEquals("The UPDATED_ON is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getUPDATED_ON(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getUPDATED_ON());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " PROCESS_INSTANCE_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROCESS_INSTANCE_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROCESS_INSTANCE_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROCESS_INSTANCE_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROCESS_INSTANCE_ID() != null)) {
                    Assert.assertEquals("The PROCESS_INSTANCE_ID is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROCESS_INSTANCE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROCESS_INSTANCE_ID());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " REASON_FOR_CHANGE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREASON_FOR_CHANGE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREASON_FOR_CHANGE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREASON_FOR_CHANGE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREASON_FOR_CHANGE() != null)) {
                    Assert.assertEquals("The REASON_FOR_CHANGE is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREASON_FOR_CHANGE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREASON_FOR_CHANGE());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " CANCELLED_BY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCANCELLED_BY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCANCELLED_BY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCANCELLED_BY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCANCELLED_BY() != null)) {
                    Assert.assertEquals("The CANCELLED_BY is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCANCELLED_BY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCANCELLED_BY());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " CREATED_BY_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCREATED_BY_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCREATED_BY_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCREATED_BY_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCREATED_BY_NAME() != null)) {
                    Assert.assertEquals("The CREATED_BY_NAME is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCREATED_BY_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCREATED_BY_NAME());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " REJECTION_COMMENT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREJECTION_COMMENT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREJECTION_COMMENT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREJECTION_COMMENT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREJECTION_COMMENT() != null)) {
                    Assert.assertEquals("The REJECTION_COMMENT is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREJECTION_COMMENT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREJECTION_COMMENT());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " SUBMISSION_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBMISSION_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBMISSION_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBMISSION_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBMISSION_DATE() != null)) {
                    Assert.assertEquals("The SUBMISSION_DATE is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBMISSION_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBMISSION_DATE());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " CANCELLED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCANCELLED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCANCELLED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCANCELLED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCANCELLED_DATE() != null)) {
                    Assert.assertEquals("The CANCELLED_DATE is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCANCELLED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCANCELLED_DATE());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " REJECTION_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREJECTION_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREJECTION_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREJECTION_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREJECTION_DATE() != null)) {
                    Assert.assertEquals("The REJECTION_DATE is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREJECTION_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREJECTION_DATE());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " VERSION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getVERSION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getVERSION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getVERSION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getVERSION() != null)) {
                    Assert.assertEquals("The VERSION is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getVERSION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getVERSION());
                }
                Log.info("PRODUCT_CHRONICLE_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_CHRONICLE_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_CHRONICLE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Product Family records from JMF MySQL of (.*)")
    public void getPrdtFamilyRecordsFromJMF(String tableName) {
        Log.info("We get the Product Family records from JMF MySql..");
        sql = String.format(jmObj.getProdFamilySql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Product Family records from DL of (.*)$")
    public void getPrdtFamilyRecordsFromDL(String tableName) {
        Log.info("We get the Product Family records from JMF DL..");
        sql = String.format(jmObj.getProdFamilySql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Product Family records in JMF MySQL and DL of (.*)$")
    public void comparePrdtFamilyJMFSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Family Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_FAMILY_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_FAMILY_ID));
                Log.info("PRODUCT_FAMILY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID() +
                        " PRODUCT_FAMILY_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_FAMILY_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_FAMILY_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_FAMILY_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_FAMILY_ID());
                }

                Log.info("PRODUCT_FAMILY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID() +
                        " F_PRODUCT_CHRONICLE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_CHRONICLE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_CHRONICLE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_CHRONICLE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_CHRONICLE() != null)) {
                    Assert.assertEquals("The F_PRODUCT_CHRONICLE is incorrect for PRODUCT_FAMILY_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_CHRONICLE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_CHRONICLE());
                }

                Log.info("PRODUCT_FAMILY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID() +
                        " PRODUCT_FAMILY_TITLE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_TITLE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_FAMILY_TITLE());


                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_TITLE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_FAMILY_TITLE() != null)) {
                    String ProductFamilySql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_TITLE();
                    if(ProductFamilySql.isEmpty()){
                        ProductFamilySql = null;
                    }

                    Assert.assertEquals("The PRODUCT_FAMILY_TITLE is incorrect for PRODUCT_FAMILY_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID(),
                            ProductFamilySql, dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_FAMILY_TITLE());
                }
                Log.info("PRODUCT_FAMILY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID() +
                        " PRODUCT_JOURNEY_IDENTIFIER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_JOURNEY_IDENTIFIER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_JOURNEY_IDENTIFIER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_JOURNEY_IDENTIFIER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_JOURNEY_IDENTIFIER() != null)) {
                    Assert.assertEquals("The PRODUCT_JOURNEY_IDENTIFIER is incorrect for PRODUCT_FAMILY_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_JOURNEY_IDENTIFIER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_JOURNEY_IDENTIFIER());
                }

                Log.info("PRODUCT_FAMILY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID() +
                        " PMX_PRODUCT_FAMILY_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_FAMILY_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_FAMILY_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_FAMILY_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_FAMILY_ID() != null)) {
                    Assert.assertEquals("The PMX_PRODUCT_FAMILY_ID is incorrect for PRODUCT_FAMILY_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_FAMILY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_FAMILY_ID());
                }
                Log.info("PRODUCT_FAMILY_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_FAMILY_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_FAMILY_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Product Manifestation records from JMF MySQL of (.*)")
    public void getProdManifRecordsFromJMF(String tableName) {
        Log.info("We get the Product Manifestation records from JMF MySql..");
        sql = String.format(jmObj.getProdManifSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Product Manifestation records from DL of (.*)$")
    public void getProdManifRecordsFromDL(String tableName) {
        Log.info("We get the Product Manifestation records from JMF DL..");
        sql = String.format(jmObj.getProdManifSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Product Manifestation records in JMF MySQL and DL of (.*)$")
    public void compareProdManifJMFSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Manifestation Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_MANIFESTATION_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_MANIFESTATION_ID));
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PRODUCT_MANIFESTATION_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_MANIFESTATION_ID is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_ID());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " F_PRODUCT_WORK => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK() != null)) {
                    Assert.assertEquals("The F_PRODUCT_WORK is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PRODUCT_MANIFESTATION_TITLE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_TITLE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_TITLE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_TITLE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_TITLE() != null)) {
                    Assert.assertEquals("The PRODUCT_MANIFESTATION_TITLE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_TITLE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_MANIFESTATION_TITLE());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ISSN => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getISSN() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSN());
                String issnSql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getISSN();
                if (issnSql != null || dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSN() != null) {
                   if(issnSql.isEmpty()){
                       issnSql = null;
                   }
                    Assert.assertEquals("The ISSN is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            issnSql, dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSN());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " ELSEVIER_JOURNAL_NUMBER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELSEVIER_JOURNAL_NUMBER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELSEVIER_JOURNAL_NUMBER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELSEVIER_JOURNAL_NUMBER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELSEVIER_JOURNAL_NUMBER() != null)) {
                    Assert.assertEquals("The ELSEVIER_JOURNAL_NUMBER is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELSEVIER_JOURNAL_NUMBER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELSEVIER_JOURNAL_NUMBER());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " SUBSCRIPTION_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBSCRIPTION_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBSCRIPTION_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBSCRIPTION_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBSCRIPTION_TYPE() != null)) {
                    Assert.assertEquals("The SUBSCRIPTION_TYPE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBSCRIPTION_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBSCRIPTION_TYPE());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PRICE_CATEGORIES => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRICE_CATEGORIES() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRICE_CATEGORIES());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRICE_CATEGORIES() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRICE_CATEGORIES() != null)) {

                    String PriceCategoriesSql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRICE_CATEGORIES();
                    if(PriceCategoriesSql.isEmpty()){
                        PriceCategoriesSql = null;
                    }
                    Assert.assertEquals("The PRICE_CATEGORIES is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            PriceCategoriesSql,dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRICE_CATEGORIES());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " PMX_PRODUCT_MANIFESTATION_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_MANIFESTATION_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_MANIFESTATION_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_MANIFESTATION_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_MANIFESTATION_ID() != null)) {
                    Assert.assertEquals("The PMX_PRODUCT_MANIFESTATION_ID is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_MANIFESTATION_ID());
                }

                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
                Log.info("PRODUCT_MANIFESTATION_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_MANIFESTATION_ID() +
                        " EPH_MANIFESTATION_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_MANIFESTATION_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_MANIFESTATION_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_MANIFESTATION_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_MANIFESTATION_ID() != null)) {
                    Assert.assertEquals("The EPH_MANIFESTATION_ID is incorrect for PRODUCT_MANIFESTATION_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_MANIFESTATION_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_MANIFESTATION_ID());
                }
            }
        }
    }

    @When("^We get the JMF Product Subject records from JMF MySQL of (.*)")
    public void getProdSubjRecordsFromJMF(String tableName) {
        Log.info("We get the Product Subject records from JMF MySql..");
        sql = String.format(jmObj.getProdSubjSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Product Subject records from DL of (.*)$")
    public void getProdSubjRecordsFromDL(String tableName) {
        Log.info("We get the Product Subject records from JMF DL..");
        sql = String.format(jmObj.getProdSubjSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Product Subject records in JMF MySQL and DL of (.*)$")
    public void compareProdSubjJMFSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Subject Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_SUBJECT_AREA_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_SUBJECT_AREA_ID));
                Log.info("PRODUCT_SUBJECT_AREA_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() +
                        " PRODUCT_SUBJECT_AREA_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_SUBJECT_AREA_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_SUBJECT_AREA_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_SUBJECT_AREA_ID is incorrect for PRODUCT_SUBJECT_AREA_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_SUBJECT_AREA_ID());
                }
                Log.info("PRODUCT_SUBJECT_AREA_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() +
                        " F_PRODUCT_WORK => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK() != null)) {
                    Assert.assertEquals("The F_PRODUCT_WORK is incorrect for PRODUCT_SUBJECT_AREA_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_WORK(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_WORK());
                }

                Log.info("PRODUCT_SUBJECT_AREA_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() +
                        " SUBJECT_AREA_TYPE_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_TYPE_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_TYPE_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_TYPE_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_TYPE_CODE() != null)) {
                    Assert.assertEquals("The SUBJECT_AREA_TYPE_CODE is incorrect for PRODUCT_SUBJECT_AREA_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_TYPE_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_TYPE_CODE());
                }

                Log.info("PRODUCT_SUBJECT_AREA_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() +
                        " SUBJECT_AREA_PRIORITY_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_PRIORITY_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_PRIORITY_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_PRIORITY_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_PRIORITY_CODE() != null)) {
                    Assert.assertEquals("The SUBJECT_AREA_PRIORITY_CODE is incorrect for PRODUCT_SUBJECT_AREA_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_PRIORITY_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_PRIORITY_CODE());
                }

                Log.info("PRODUCT_SUBJECT_AREA_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() +
                        " SUBJECT_AREA_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_CODE() != null)) {
                    Assert.assertEquals("The SUBJECT_AREA_CODE is incorrect for PRODUCT_SUBJECT_AREA_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_CODE());
                }

                Log.info("PRODUCT_SUBJECT_AREA_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() +
                        " SUBJECT_AREA_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_NAME() != null)) {
                    Assert.assertEquals("The SUBJECT_AREA_NAME is incorrect for PRODUCT_SUBJECT_AREA_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBJECT_AREA_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBJECT_AREA_NAME());
                }

                Log.info("PRODUCT_SUBJECT_AREA_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_SUBJECT_AREA_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBJECT_AREA_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Review Comment records from JMF MySQL of (.*)")
    public void getReviewCommentRecordsFromJMF(String tableName) {
        Log.info("We get the Review Comments records from JMF MySql..");
        sql = String.format(jmObj.getReviewCommentsSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Review Comment records from DL of (.*)$")
    public void getReviewCommentRecordsFromDL(String tableName) {
        Log.info("We get the Review Comments records from JMF DL..");
        sql = String.format(jmObj.getReviewCommentsSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Review Comment records in JMF MySQL and DL of (.*)$")
    public void compareReviewCommentJMFSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Review Comments Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getREVIEW_COMMENT_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getREVIEW_COMMENT_ID));
                Log.info("REVIEW_COMMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() +
                        " REVIEW_COMMENT_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT_ID() != null)) {
                    Assert.assertEquals("The REVIEW_COMMENT_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT_ID());
                }

                Log.info("REVIEW_COMMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() +
                        " F_APPROVAL_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_APPROVAL_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_APPROVAL_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT_ID() != null)) {
                    Assert.assertEquals("The F_APPROVAL_ID is incorrect for REVIEW_COMMENT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_APPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_APPROVAL_ID());
                }
                Log.info("REVIEW_COMMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() +
                        " REVIEW_ATTRIBUTE_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_ATTRIBUTE_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_ATTRIBUTE_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_ATTRIBUTE_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_ATTRIBUTE_NAME() != null)) {
                    Assert.assertEquals("The REVIEW_ATTRIBUTE_NAME is incorrect for REVIEW_COMMENT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_ATTRIBUTE_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_ATTRIBUTE_NAME());
                }

                Log.info("REVIEW_COMMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() +
                        " REVIEW_COMMENT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT().replace("\n"," ").replace("\r"," ").replace("\0", "") +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT() != null)) {
                    Assert.assertEquals("The REVIEW_COMMENT is incorrect for REVIEW_COMMENT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT().replace("\n"," ").replace("\r"," ").replace("\0", ""),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT());
                }
                Log.info("REVIEW_COMMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() +
                        " REVIEW_COMMENT_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT_DATE() != null)) {
                    Assert.assertEquals("The REVIEW_COMMENT_DATE is incorrect for REVIEW_COMMENT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREVIEW_COMMENT_DATE());
                }
                Log.info("REVIEW_COMMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() +
                        " CREATED_ON => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCREATED_ON() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCREATED_ON());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCREATED_ON() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCREATED_ON() != null)) {
                    Assert.assertEquals("The CREATED_ON is incorrect for REVIEW_COMMENT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCREATED_ON(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCREATED_ON());
                }
                Log.info("REVIEW_COMMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for REVIEW_COMMENT_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREVIEW_COMMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

            }
        }
    }

    @When("^We get the JMF Product Work records from JMF MySQL of (.*)")
    public void getProdWorkRecordsFromJMF(String tableName) {
        Log.info("We get the Product Work records from JMF MySql..");
        sql = String.format(jmObj.getProdWorkSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Product Work records from DL of (.*)$")
    public void getProdWorkRecordsFromDL(String tableName) {
        Log.info("We get the Product Work records from JMF DL..");
        sql = String.format(jmObj.getProdWorkSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }


    @And("^Compare JMF Product Work records in JMF MySQL and DL of (.*)$")
    public void compareProdWorkJMFSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Work Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_WORK_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_WORK_ID));
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRODUCT_WORK_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_WORK_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " F_PRODUCT_FAMILY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY() != null)) {
                    Assert.assertEquals("The F_PRODUCT_FAMILY is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRODUCT_WORK_TITLE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_TITLE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_TITLE());

                String ProductWorkTitleSql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_TITLE();

                if (ProductWorkTitleSql != null || dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_TITLE() != null) {
                   if(ProductWorkTitleSql.isEmpty()){
                       ProductWorkTitleSql = null;
                   }else if(ProductWorkTitleSql.length()>0){
                       ProductWorkTitleSql =  ProductWorkTitleSql.replace("\n"," ").replace("\r"," ").replace("\0", "");
                    }
                    Assert.assertEquals("The PRODUCT_WORK_TITLE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            ProductWorkTitleSql,dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_TITLE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRODUCT_SUBTITLE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBTITLE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_SUBTITLE());

                String productSubtitleSql =  dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_SUBTITLE();
                if (productSubtitleSql !=null || dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_SUBTITLE() != null) {
                    if(productSubtitleSql.isEmpty()){
                        productSubtitleSql = null;
                    }
                    Assert.assertEquals("The PRODUCT_SUBTITLE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            productSubtitleSql, dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_SUBTITLE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRODUCT_WORK_TITLE_INFO => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_TITLE_INFO() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_TITLE_INFO());
                    String ProductWorkTitleInfoSql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_TITLE_INFO();
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_TITLE_INFO() != null ||
                        dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_TITLE_INFO() != null) {
                    if(ProductWorkTitleInfoSql.isEmpty()){
                        ProductWorkTitleInfoSql = null;
                    }else if(ProductWorkTitleInfoSql.length()>0 ){
                        ProductWorkTitleInfoSql =  ProductWorkTitleInfoSql.replace("\n"," ").replace("\r"," ").replace("\0", "");
                    }
                    Assert.assertEquals("The PRODUCT_WORK_TITLE_INFO is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            ProductWorkTitleInfoSql, dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_TITLE_INFO());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ISSN_L => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getISSN_L() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSN_L());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getISSN_L() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSN_L() != null)) {
                    Assert.assertEquals("The ISSN_L is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getISSN_L(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSN_L());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ELSEVIER_JOURNAL_NUMBER => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELSEVIER_JOURNAL_NUMBER() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELSEVIER_JOURNAL_NUMBER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELSEVIER_JOURNAL_NUMBER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELSEVIER_JOURNAL_NUMBER() != null)) {
                    Assert.assertEquals("The ELSEVIER_JOURNAL_NUMBER is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getELSEVIER_JOURNAL_NUMBER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getELSEVIER_JOURNAL_NUMBER());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " INTERNAL_ELSEVIER_DIVISION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINTERNAL_ELSEVIER_DIVISION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINTERNAL_ELSEVIER_DIVISION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINTERNAL_ELSEVIER_DIVISION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINTERNAL_ELSEVIER_DIVISION() != null)) {
                    Assert.assertEquals("The INTERNAL_ELSEVIER_DIVISION is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINTERNAL_ELSEVIER_DIVISION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINTERNAL_ELSEVIER_DIVISION());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MANIFESTATION_TYPES_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_TYPES_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_TYPES_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_TYPES_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_TYPES_CODE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_TYPES_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_TYPES_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_TYPES_CODE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MANIFESTATION_PERSONAL_MODEL_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_PERSONAL_MODEL_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_PERSONAL_MODEL_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_PERSONAL_MODEL_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_PERSONAL_MODEL_TYPE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_PERSONAL_MODEL_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_PERSONAL_MODEL_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_PERSONAL_MODEL_TYPE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " IMPRINT_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getIMPRINT_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getIMPRINT_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getIMPRINT_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getIMPRINT_NAME() != null)) {
                    Assert.assertEquals("The IMPRINT_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getIMPRINT_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getIMPRINT_NAME());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PTS_JOURNAL_INDICATOR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPTS_JOURNAL_INDICATOR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPTS_JOURNAL_INDICATOR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPTS_JOURNAL_INDICATOR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPTS_JOURNAL_INDICATOR() != null)) {
                    Assert.assertEquals("The PTS_JOURNAL_INDICATOR is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPTS_JOURNAL_INDICATOR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPTS_JOURNAL_INDICATOR());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " YEAR_OF_FIRST_ISSUE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getYEAR_OF_FIRST_ISSUE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getYEAR_OF_FIRST_ISSUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getYEAR_OF_FIRST_ISSUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getYEAR_OF_FIRST_ISSUE() != null)) {
                    Assert.assertEquals("The YEAR_OF_FIRST_ISSUE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getYEAR_OF_FIRST_ISSUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getYEAR_OF_FIRST_ISSUE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " YEAR_OF_LAST_ISSUE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getYEAR_OF_LAST_ISSUE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getYEAR_OF_LAST_ISSUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getYEAR_OF_LAST_ISSUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getYEAR_OF_LAST_ISSUE() != null)) {
                    Assert.assertEquals("The YEAR_OF_LAST_ISSUE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getYEAR_OF_LAST_ISSUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getYEAR_OF_LAST_ISSUE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " FIRST_VOLUME_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_VOLUME_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_VOLUME_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_VOLUME_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_VOLUME_NAME() != null)) {
                    Assert.assertEquals("The FIRST_VOLUME_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_VOLUME_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_VOLUME_NAME());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " FIRST_ISSUE_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_ISSUE_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_ISSUE_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_ISSUE_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_ISSUE_NAME() != null)) {
                    Assert.assertEquals("The FIRST_ISSUE_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_ISSUE_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_ISSUE_NAME());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " LAST_VOLUME_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAST_VOLUME_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAST_VOLUME_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAST_VOLUME_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAST_VOLUME_NAME() != null)) {
                    Assert.assertEquals("The LAST_VOLUME_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAST_VOLUME_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAST_VOLUME_NAME());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " LAST_ISSUE_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAST_ISSUE_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAST_ISSUE_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAST_ISSUE_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAST_ISSUE_NAME() != null)) {
                    Assert.assertEquals("The LAST_ISSUE_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAST_ISSUE_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAST_ISSUE_NAME());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " VOLUMES_PER_YEAR_QUANTITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getVOLUMES_PER_YEAR_QUANTITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getVOLUMES_PER_YEAR_QUANTITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getVOLUMES_PER_YEAR_QUANTITY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getVOLUMES_PER_YEAR_QUANTITY() != null)) {
                    Assert.assertEquals("The VOLUMES_PER_YEAR_QUANTITY is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getVOLUMES_PER_YEAR_QUANTITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getVOLUMES_PER_YEAR_QUANTITY());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ISSUES_PER_VOLUME_QUANTITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getISSUES_PER_VOLUME_QUANTITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSUES_PER_VOLUME_QUANTITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getISSUES_PER_VOLUME_QUANTITY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSUES_PER_VOLUME_QUANTITY() != null)) {
                    Assert.assertEquals("The ISSUES_PER_VOLUME_QUANTITY is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getISSUES_PER_VOLUME_QUANTITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getISSUES_PER_VOLUME_QUANTITY());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " FIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY() != null)) {
                    Assert.assertEquals("The FIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_YEAR_VOLUMES_PER_YEAR_QUANTITY());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " FIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY() != null)) {
                    Assert.assertEquals("The FIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFIRST_YEAR_ISSUES_PER_VOLUME_QUANTITY());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PERIODICAL_TIMING_DESC => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERIODICAL_TIMING_DESC() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERIODICAL_TIMING_DESC());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERIODICAL_TIMING_DESC() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERIODICAL_TIMING_DESC() != null)) {
                    Assert.assertEquals("The PERIODICAL_TIMING_DESC is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPERIODICAL_TIMING_DESC(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPERIODICAL_TIMING_DESC());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPEN_ACCESSTYPE_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESSTYPE_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESSTYPE_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESSTYPE_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESSTYPE_CODE() != null)) {
                    Assert.assertEquals("The OPEN_ACCESSTYPE_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESSTYPE_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESSTYPE_CODE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPEN_ACCESS_SPONSOR_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_SPONSOR_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_SPONSOR_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_SPONSOR_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_SPONSOR_NAME() != null)) {
                    Assert.assertEquals("The OPEN_ACCESS_SPONSOR_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_SPONSOR_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_SPONSOR_NAME());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPEN_ACCESS_FEE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_FEE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_FEE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_FEE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_FEE() != null)) {
                    Assert.assertEquals("The OPEN_ACCESS_FEE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_FEE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_FEE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPEN_ACCESS_CURRENCY_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_CURRENCY_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_CURRENCY_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_CURRENCY_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_CURRENCY_CODE() != null)) {
                    Assert.assertEquals("The OPEN_ACCESS_CURRENCY_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_CURRENCY_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_CURRENCY_CODE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPEN_ACCESS_DISCOUNT_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_DISCOUNT_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_DISCOUNT_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_DISCOUNT_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_DISCOUNT_IND() != null)) {
                    Assert.assertEquals("The OPEN_ACCESS_DISCOUNT_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_DISCOUNT_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_DISCOUNT_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPEN_ACCESS_DISCOUNT_PERIOD => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_DISCOUNT_PERIOD() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_DISCOUNT_PERIOD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_DISCOUNT_PERIOD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_DISCOUNT_PERIOD() != null)) {
                    Assert.assertEquals("The OPEN_ACCESS_DISCOUNT_PERIOD is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ACCESS_DISCOUNT_PERIOD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ACCESS_DISCOUNT_PERIOD());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OA_FIRST_YEAR_DISCOUNT_PRICE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOA_FIRST_YEAR_DISCOUNT_PRICE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOA_FIRST_YEAR_DISCOUNT_PRICE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOA_FIRST_YEAR_DISCOUNT_PRICE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOA_FIRST_YEAR_DISCOUNT_PRICE() != null)) {
                    Assert.assertEquals("The OA_FIRST_YEAR_DISCOUNT_PRICE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOA_FIRST_YEAR_DISCOUNT_PRICE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOA_FIRST_YEAR_DISCOUNT_PRICE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OA_SECOND_YEAR_DISCOUNT_PRICE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOA_SECOND_YEAR_DISCOUNT_PRICE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOA_SECOND_YEAR_DISCOUNT_PRICE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOA_SECOND_YEAR_DISCOUNT_PRICE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOA_SECOND_YEAR_DISCOUNT_PRICE() != null)) {
                    Assert.assertEquals("The OA_SECOND_YEAR_DISCOUNT_PRICE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOA_SECOND_YEAR_DISCOUNT_PRICE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOA_SECOND_YEAR_DISCOUNT_PRICE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DDP_ELIGIBLE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDDP_ELIGIBLE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDDP_ELIGIBLE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDDP_ELIGIBLE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDDP_ELIGIBLE_IND() != null)) {
                    Assert.assertEquals("The DDP_ELIGIBLE_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDDP_ELIGIBLE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDDP_ELIGIBLE_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " WEBSHOP_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getWEBSHOP_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getWEBSHOP_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getWEBSHOP_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getWEBSHOP_IND() != null)) {
                    Assert.assertEquals("The WEBSHOP_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getWEBSHOP_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getWEBSHOP_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MEDLINE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMEDLINE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMEDLINE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMEDLINE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMEDLINE_IND() != null)) {
                    Assert.assertEquals("The MEDLINE_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMEDLINE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMEDLINE_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " THOMSON_REUTERS_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTHOMSON_REUTERS_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTHOMSON_REUTERS_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTHOMSON_REUTERS_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTHOMSON_REUTERS_IND() != null)) {
                    Assert.assertEquals("The THOMSON_REUTERS_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTHOMSON_REUTERS_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTHOMSON_REUTERS_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SCOPUS_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSCOPUS_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSCOPUS_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSCOPUS_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSCOPUS_IND() != null)) {
                    Assert.assertEquals("The SCOPUS_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSCOPUS_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSCOPUS_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EMBASE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBASE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBASE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBASE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBASE_IND() != null)) {
                    Assert.assertEquals("The EMBASE_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEMBASE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEMBASE_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DOAJ_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOAJ_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOAJ_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOAJ_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOAJ_IND() != null)) {
                    Assert.assertEquals("The DOAJ_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOAJ_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOAJ_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ROAD_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getROAD_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getROAD_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getROAD_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getROAD_IND() != null)) {
                    Assert.assertEquals("The ROAD_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getROAD_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getROAD_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBMEDCENTRAL_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBMEDCENTRAL_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBMEDCENTRAL_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBMEDCENTRAL_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBMEDCENTRAL_IND() != null)) {
                    Assert.assertEquals("The PUBMEDCENTRAL_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBMEDCENTRAL_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBMEDCENTRAL_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MAIN_LANGUAGE_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAIN_LANGUAGE_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAIN_LANGUAGE_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAIN_LANGUAGE_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAIN_LANGUAGE_CODE() != null)) {
                    Assert.assertEquals("The MAIN_LANGUAGE_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAIN_LANGUAGE_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAIN_LANGUAGE_CODE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ENGLISH_LANGUAGE_PERCENTAGE_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getENGLISH_LANGUAGE_PERCENTAGE_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getENGLISH_LANGUAGE_PERCENTAGE_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getENGLISH_LANGUAGE_PERCENTAGE_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getENGLISH_LANGUAGE_PERCENTAGE_TYPE() != null)) {
                    Assert.assertEquals("The ENGLISH_LANGUAGE_PERCENTAGE_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getENGLISH_LANGUAGE_PERCENTAGE_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getENGLISH_LANGUAGE_PERCENTAGE_TYPE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " OPEN_ARCHIVE_PERIOD => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ARCHIVE_PERIOD() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ARCHIVE_PERIOD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ARCHIVE_PERIOD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ARCHIVE_PERIOD() != null)) {
                    Assert.assertEquals("The OPEN_ARCHIVE_PERIOD is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getOPEN_ARCHIVE_PERIOD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getOPEN_ARCHIVE_PERIOD());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DELAYED_OPEN_ARCHIVE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDELAYED_OPEN_ARCHIVE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDELAYED_OPEN_ARCHIVE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDELAYED_OPEN_ARCHIVE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDELAYED_OPEN_ARCHIVE_IND() != null)) {
                    Assert.assertEquals("The DELAYED_OPEN_ARCHIVE_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDELAYED_OPEN_ARCHIVE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDELAYED_OPEN_ARCHIVE_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " INCLUDE_IN_COLLECTIONS_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINCLUDE_IN_COLLECTIONS_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINCLUDE_IN_COLLECTIONS_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINCLUDE_IN_COLLECTIONS_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINCLUDE_IN_COLLECTIONS_IND() != null)) {
                    Assert.assertEquals("The INCLUDE_IN_COLLECTIONS_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINCLUDE_IN_COLLECTIONS_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINCLUDE_IN_COLLECTIONS_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " LAUNCH_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAUNCH_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAUNCH_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAUNCH_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAUNCH_DATE() != null)) {
                    Assert.assertEquals("The LAUNCH_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAUNCH_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAUNCH_DATE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_JAN => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JAN() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JAN());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JAN() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JAN() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_JAN is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JAN(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JAN());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_FEB => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_FEB() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_FEB());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_FEB() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_FEB() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_FEB is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_FEB(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_FEB());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_MAR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_MAR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_MAR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_MAR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_MAR() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_MAR is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_MAR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_MAR());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_APR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_APR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_APR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_APR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_APR() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_APR is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_APR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_APR());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_MAY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_MAY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_MAY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_MAY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_MAY() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_MAY is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_MAY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_MAY());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_JUN => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JUN() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JUN());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JUN() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JUN() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_JUN is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JUN(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JUN());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_JUL => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JUL() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JUL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JUL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JUL() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_JUL is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_JUL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_JUL());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_SEP => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_SEP() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_SEP());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_SEP() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_SEP() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_SEP is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_SEP(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_SEP());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_AUG => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_AUG() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_AUG());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_AUG() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_AUG() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_AUG is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_AUG(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_AUG());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_OCT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_OCT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_OCT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_OCT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_OCT() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_OCT is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_OCT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_OCT());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_NOV => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_NOV() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_NOV());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_NOV() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_NOV() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_NOV is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_NOV(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_NOV());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PUBLICATION_SCHEDULE_DEC => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_DEC() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_DEC());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_DEC() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_DEC() != null)) {
                    Assert.assertEquals("The PUBLICATION_SCHEDULE_DEC is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPUBLICATION_SCHEDULE_DEC(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPUBLICATION_SCHEDULE_DEC());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " JOURNAL_ACRONYM_ARGI => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_ARGI() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_ARGI());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_ARGI() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_ARGI() != null)) {
                    Assert.assertEquals("The JOURNAL_ACRONYM_ARGI is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_ARGI(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_ARGI());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MANIFESTATION_FORMATS_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_FORMATS_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_FORMATS_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_FORMATS_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_FORMATS_CODE() != null)) {
                    Assert.assertEquals("The MANIFESTATION_FORMATS_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANIFESTATION_FORMATS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANIFESTATION_FORMATS_CODE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " TAKEOVER_YEAR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTAKEOVER_YEAR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTAKEOVER_YEAR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTAKEOVER_YEAR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTAKEOVER_YEAR() != null)) {
                    Assert.assertEquals("The TAKEOVER_YEAR is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTAKEOVER_YEAR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTAKEOVER_YEAR());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DOI_PREFIX => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_PREFIX() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_PREFIX());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_PREFIX() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_PREFIX() != null)) {
                    Assert.assertEquals("The DOI_PREFIX is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_PREFIX(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_PREFIX());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DOI_RIGHT_ASSIGNED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_RIGHT_ASSIGNED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_RIGHT_ASSIGNED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_RIGHT_ASSIGNED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_RIGHT_ASSIGNED_IND() != null)) {
                    Assert.assertEquals("The DOI_RIGHT_ASSIGNED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_RIGHT_ASSIGNED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_RIGHT_ASSIGNED_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_OWNED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_OWNED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_OWNED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_OWNED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_OWNED_IND() != null)) {
                    Assert.assertEquals("The SOCIETY_OWNED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_OWNED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_OWNED_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " CHECKED_WITH_OA_TEAM_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHECKED_WITH_OA_TEAM_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHECKED_WITH_OA_TEAM_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHECKED_WITH_OA_TEAM_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHECKED_WITH_OA_TEAM_IND() != null)) {
                    Assert.assertEquals("The CHECKED_WITH_OA_TEAM_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHECKED_WITH_OA_TEAM_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHECKED_WITH_OA_TEAM_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " IMPRINT_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getIMPRINT_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getIMPRINT_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getIMPRINT_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getIMPRINT_CODE() != null)) {
                    Assert.assertEquals("The IMPRINT_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getIMPRINT_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getIMPRINT_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DISCONTINUE_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISCONTINUE_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISCONTINUE_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISCONTINUE_DATE() != null ||
                        dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISCONTINUE_DATE() != null) {
                    Assert.assertEquals("The DISCONTINUE_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDISCONTINUE_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDISCONTINUE_DATE().substring(0,10));
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PMX_PRODUCT_WORK_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_WORK_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_WORK_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_WORK_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_WORK_ID() != null)) {
                    Assert.assertEquals("The PMX_PRODUCT_WORK_ID is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PRODUCT_WORK_ID());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " REMOVED_FROM_CATALOGUE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMOVED_FROM_CATALOGUE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMOVED_FROM_CATALOGUE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMOVED_FROM_CATALOGUE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMOVED_FROM_CATALOGUE_IND() != null)) {
                    Assert.assertEquals("The REMOVED_FROM_CATALOGUE_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREMOVED_FROM_CATALOGUE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMOVED_FROM_CATALOGUE_IND());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " TRANSFER_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTRANSFER_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTRANSFER_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTRANSFER_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTRANSFER_DATE() != null)) {
                    Assert.assertEquals("The TRANSFER_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTRANSFER_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTRANSFER_DATE().substring(0,10));
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EPH_WORK_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_WORK_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_WORK_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_WORK_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_WORK_ID() != null)) {
                    Assert.assertEquals("The EPH_WORK_ID is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_WORK_ID());
                }

            }
        }
    }

    @When("^We get the JMF Approval Attachment records from JMF MySQL of (.*)")
    public void getAppAttachFromJMF(String tableName) {
        Log.info("We get the Application Attachment records from JMF MySql..");
        sql = String.format(jmObj.getAppAttachSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);
    }

    @Then("^We get the JMF Approval Attachment records from DL of (.*)")
    public void getAppAttachFromDL(String tableName) {
        Log.info("We get the Application Attachment records from JMF DL..");
        sql = String.format(jmObj.getAppAttachSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Approval Attachment records in JMF MySQL and DL of (.*)$")
    public void compareJMapprovalAttachDataSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the approval_attach records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getAPPROVAL_ATTACHMENT_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getAPPROVAL_ATTACHMENT_ID));

                Log.info("APPROVAL_ATTACHMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID() +
                        " APPROVAL_ATTACHMENT_ID => MysqL=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ATTACHMENT_ID());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ATTACHMENT_ID() != null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The APPROVAL_ATTACHMENT_ID =" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID()+" is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ATTACHMENT_ID());

                }

                Log.info("APPROVAL_ATTACHMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID() +
                        " F_APPROVAL => MysqL=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_APPROVAL() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_APPROVAL());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_APPROVAL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_APPROVAL() != null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The F_APPROVAL is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_APPROVAL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_APPROVAL());

                }

                Log.info("APPROVAL_ATTACHMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID() +
                        " ATTACHMENT_NAME => MysqL=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getATTACHMENT_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getATTACHMENT_NAME());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getATTACHMENT_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getATTACHMENT_NAME() != null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The ATTACHMENT_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getATTACHMENT_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getATTACHMENT_NAME());
                }

                Log.info("APPROVAL_ATTACHMENT_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID() +
                        " NOTIFIED_DATE => MysqL=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The ATTACHMENT_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ATTACHMENT_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

            }
        }
    }

    @When("^We get the JMF Production Information records from JMF MySQL of (.*)")
    public void getProdInfoRecordsFromJMF(String tableName) {
        Log.info("We get the Production Information records from JMF MySql..");
        sql = String.format(jmObj.getProdInfoSql("mysql", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Production Information records from DL of (.*)$")
    public void getProdInfoRecordsFromDL(String tableName) {
        Log.info("We get the Product Subject records from JMF DL..");
        sql = String.format(jmObj.getProdInfoSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Production Information records in JMF MySQL and DL of (.*)$")
    public void compareProdInfoJMFSQLtoDL(String tableName) {
        if (dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Production Information Details records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {
                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_WORK_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getPRODUCT_WORK_ID));

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRODUCT_WORK_ID => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID() != null)) {
                    Assert.assertEquals("The PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID()+ " is missing in DL",
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCT_WORK_ID());
                }
                Log.info("PRODUCT_SUBJECT_AREA_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " LE_MANS_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLE_MANS_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLE_MANS_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLE_MANS_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLE_MANS_IND() != null)) {
                    Assert.assertEquals("The LE_MANS_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLE_MANS_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLE_MANS_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_RELATIONSHIP_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_RELATIONSHIP_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_RELATIONSHIP_TYPE());


                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_RELATIONSHIP_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_RELATIONSHIP_TYPE() != null)) {
                    Assert.assertEquals("The SOCIETY_RELATIONSHIP_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_RELATIONSHIP_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_RELATIONSHIP_TYPE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_NAME() != null)) {
                    Assert.assertEquals("The SOCIETY_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_NAME());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ARTICLE_NUMBER_PER_YEAR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_NUMBER_PER_YEAR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_NUMBER_PER_YEAR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_NUMBER_PER_YEAR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_NUMBER_PER_YEAR() != null)) {
                    Assert.assertEquals("The ARTICLE_NUMBER_PER_YEAR is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getARTICLE_NUMBER_PER_YEAR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getARTICLE_NUMBER_PER_YEAR());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SUBMISSION_NUMBER_PER_YEAR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBMISSION_NUMBER_PER_YEAR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBMISSION_NUMBER_PER_YEAR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBMISSION_NUMBER_PER_YEAR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBMISSION_NUMBER_PER_YEAR() != null)) {
                    Assert.assertEquals("The SUBMISSION_NUMBER_PER_YEAR is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSUBMISSION_NUMBER_PER_YEAR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSUBMISSION_NUMBER_PER_YEAR());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_REQUESTED_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_REQUESTED_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_REQUESTED_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_REQUESTED_CODE() != null)) {
                    Assert.assertEquals("The EVISE_REQUESTED_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_REQUESTED_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_REQUESTED_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " JOURNAL_ACRONYM_EVISE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_EVISE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_EVISE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_EVISE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_EVISE() != null)) {
                    Assert.assertEquals("The JOURNAL_ACRONYM_EVISE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_EVISE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_EVISE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " JOURNAL_ACRONYM_PTS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_PTS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_PTS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_PTS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_PTS() != null)) {
                String JOURNALACRONYMPTS = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ACRONYM_PTS();
                if(JOURNALACRONYMPTS.isEmpty()){
                    JOURNALACRONYMPTS = null;
                }

                    Assert.assertEquals("The JOURNAL_ACRONYM_PTS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            JOURNALACRONYMPTS, dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ACRONYM_PTS());
                }
                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EVISE_SUPPORT_LEVEL => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_SUPPORT_LEVEL() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_SUPPORT_LEVEL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_SUPPORT_LEVEL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_SUPPORT_LEVEL() != null)) {
                    Assert.assertEquals("The EVISE_SUPPORT_LEVEL is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_SUPPORT_LEVEL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_SUPPORT_LEVEL());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EVISE_WORKFLOW_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_WORKFLOW_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_WORKFLOW_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_WORKFLOW_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_WORKFLOW_TYPE() != null)) {
                    Assert.assertEquals("The EVISE_WORKFLOW_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEVISE_WORKFLOW_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEVISE_WORKFLOW_TYPE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EDITOR_LOCATION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITOR_LOCATION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITOR_LOCATION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITOR_LOCATION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITOR_LOCATION() != null)) {
                    Assert.assertEquals("The EDITOR_LOCATION is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITOR_LOCATION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITOR_LOCATION());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ABP_USAGE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getABP_USAGE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getABP_USAGE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getABP_USAGE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getABP_USAGE_IND() != null)) {
                    Assert.assertEquals("The ABP_USAGE_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getABP_USAGE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getABP_USAGE_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NON_STANDARD_PRODUCTION_ASPECTS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_STANDARD_PRODUCTION_ASPECTS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_STANDARD_PRODUCTION_ASPECTS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_STANDARD_PRODUCTION_ASPECTS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_STANDARD_PRODUCTION_ASPECTS() != null)) {

                    String nonStdPrdtAspectsSql = dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNON_STANDARD_PRODUCTION_ASPECTS();
                    if(nonStdPrdtAspectsSql.isEmpty()){
                        nonStdPrdtAspectsSql = null;
                    }
                    Assert.assertEquals("The NON_STANDARD_PRODUCTION_ASPECTS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            nonStdPrdtAspectsSql,dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNON_STANDARD_PRODUCTION_ASPECTS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EDITORIAL_PRODUCTION_SITE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_SITE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_SITE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_SITE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_SITE() != null)) {
                    Assert.assertEquals("The EDITORIAL_PRODUCTION_SITE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_SITE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_SITE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PRODUCTION_SPECIFICATION_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCTION_SPECIFICATION_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCTION_SPECIFICATION_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCTION_SPECIFICATION_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCTION_SPECIFICATION_TYPE() != null)) {
                    Assert.assertEquals("The PRODUCTION_SPECIFICATION_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCTION_SPECIFICATION_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPRODUCTION_SPECIFICATION_TYPE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " TYPESET_MODEL_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESET_MODEL_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESET_MODEL_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESET_MODEL_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESET_MODEL_TYPE() != null)) {
                    Assert.assertEquals("The TYPESET_MODEL_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESET_MODEL_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESET_MODEL_TYPE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " REFERENCE_STYLE_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREFERENCE_STYLE_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREFERENCE_STYLE_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREFERENCE_STYLE_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREFERENCE_STYLE_TYPE() != null)) {
                    Assert.assertEquals("The REFERENCE_STYLE_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getREFERENCE_STYLE_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREFERENCE_STYLE_TYPE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BUDGETED_PAGE_NUMBER_PER_ISSUE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUDGETED_PAGE_NUMBER_PER_ISSUE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUDGETED_PAGE_NUMBER_PER_ISSUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUDGETED_PAGE_NUMBER_PER_ISSUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUDGETED_PAGE_NUMBER_PER_ISSUE() != null)) {
                    Assert.assertEquals("The BUDGETED_PAGE_NUMBER_PER_ISSUE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBUDGETED_PAGE_NUMBER_PER_ISSUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBUDGETED_PAGE_NUMBER_PER_ISSUE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " LATEX_SUBMISSION_PERCENTAGE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLATEX_SUBMISSION_PERCENTAGE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLATEX_SUBMISSION_PERCENTAGE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLATEX_SUBMISSION_PERCENTAGE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLATEX_SUBMISSION_PERCENTAGE() != null)) {
                    Assert.assertEquals("The LATEX_SUBMISSION_PERCENTAGE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLATEX_SUBMISSION_PERCENTAGE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLATEX_SUBMISSION_PERCENTAGE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " TYPESETTER_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESETTER_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESETTER_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESETTER_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESETTER_CODE() != null)) {
                    Assert.assertEquals("The TYPESETTER_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESETTER_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESETTER_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PAGE_REVIEW_CHARGES => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPAGE_REVIEW_CHARGES() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPAGE_REVIEW_CHARGES());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPAGE_REVIEW_CHARGES() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPAGE_REVIEW_CHARGES() != null)) {
                    Assert.assertEquals("The PAGE_REVIEW_CHARGES is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPAGE_REVIEW_CHARGES(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPAGE_REVIEW_CHARGES());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " COPY_EDITING_CODE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOPY_EDITING_CODE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOPY_EDITING_CODE());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOPY_EDITING_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOPY_EDITING_CODE() != null)) {
                    Assert.assertEquals("The COPY_EDITING_CODE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOPY_EDITING_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOPY_EDITING_CODE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " HISTORY_DATE_FORMAT => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getHISTORY_DATE_FORMAT() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getHISTORY_DATE_FORMAT());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getHISTORY_DATE_FORMAT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getHISTORY_DATE_FORMAT() != null)) {
                    Assert.assertEquals("The HISTORY_DATE_FORMAT is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getHISTORY_DATE_FORMAT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getHISTORY_DATE_FORMAT());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PROOF_SENT_TO_AUTHOR_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROOF_SENT_TO_AUTHOR_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROOF_SENT_TO_AUTHOR_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROOF_SENT_TO_AUTHOR_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROOF_SENT_TO_AUTHOR_IND() != null)) {
                    Assert.assertEquals("The PROOF_SENT_TO_AUTHOR_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROOF_SENT_TO_AUTHOR_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROOF_SENT_TO_AUTHOR_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PROOF_SENT_TO_EDITOR_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROOF_SENT_TO_AUTHOR_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROOF_SENT_TO_EDITOR_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROOF_SENT_TO_EDITOR_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROOF_SENT_TO_EDITOR_IND() != null)) {
                    Assert.assertEquals("The PROOF_SENT_TO_EDITOR_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROOF_SENT_TO_EDITOR_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROOF_SENT_TO_EDITOR_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EDITOR_EMAIL_ADDRESS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITOR_EMAIL_ADDRESS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITOR_EMAIL_ADDRESS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITOR_EMAIL_ADDRESS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITOR_EMAIL_ADDRESS() != null)) {
                    Assert.assertEquals("The EDITOR_EMAIL_ADDRESS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITOR_EMAIL_ADDRESS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITOR_EMAIL_ADDRESS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " E_SUITE_JOURNAL_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getE_SUITE_JOURNAL_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getE_SUITE_JOURNAL_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getE_SUITE_JOURNAL_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getE_SUITE_JOURNAL_IND() != null)) {
                    Assert.assertEquals("The E_SUITE_JOURNAL_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getE_SUITE_JOURNAL_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getE_SUITE_JOURNAL_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SPONSOR_ACCRESS_REQUIRED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPONSOR_ACCRESS_REQUIRED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPONSOR_ACCRESS_REQUIRED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPONSOR_ACCRESS_REQUIRED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPONSOR_ACCRESS_REQUIRED_IND() != null)) {
                    Assert.assertEquals("The SPONSOR_ACCRESS_REQUIRED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPONSOR_ACCRESS_REQUIRED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPONSOR_ACCRESS_REQUIRED_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ONLINE_PUBLICATION_DATE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_PUBLICATION_DATE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_PUBLICATION_DATE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_PUBLICATION_DATE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_PUBLICATION_DATE_IND() != null)) {
                    Assert.assertEquals("The ONLINE_PUBLICATION_DATE_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getONLINE_PUBLICATION_DATE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getONLINE_PUBLICATION_DATE_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " AUTHOR_FEEDBACK_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAUTHOR_FEEDBACK_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAUTHOR_FEEDBACK_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAUTHOR_FEEDBACK_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAUTHOR_FEEDBACK_IND() != null)) {
                    Assert.assertEquals("The AUTHOR_FEEDBACK_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAUTHOR_FEEDBACK_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAUTHOR_FEEDBACK_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SEND_COPYRIGHT_FORM_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSEND_COPYRIGHT_FORM_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSEND_COPYRIGHT_FORM_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSEND_COPYRIGHT_FORM_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSEND_COPYRIGHT_FORM_IND() != null)) {
                    Assert.assertEquals("The SEND_COPYRIGHT_FORM_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSEND_COPYRIGHT_FORM_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSEND_COPYRIGHT_FORM_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " RUNNING_ORDER_DETAILS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRUNNING_ORDER_DETAILS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRUNNING_ORDER_DETAILS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRUNNING_ORDER_DETAILS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRUNNING_ORDER_DETAILS() != null)) {
                    Assert.assertEquals("The RUNNING_ORDER_DETAILS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRUNNING_ORDER_DETAILS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRUNNING_ORDER_DETAILS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " FLEXIBILITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFLEXIBILITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFLEXIBILITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFLEXIBILITY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFLEXIBILITY() != null)) {
                    Assert.assertEquals("The FLEXIBILITY is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFLEXIBILITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFLEXIBILITY());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MAXIMUM_PAGE_DETAILS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAXIMUM_PAGE_DETAILS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAXIMUM_PAGE_DETAILS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAXIMUM_PAGE_DETAILS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAXIMUM_PAGE_DETAILS() != null)) {
                    Assert.assertEquals("The MAXIMUM_PAGE_DETAILS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMAXIMUM_PAGE_DETAILS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMAXIMUM_PAGE_DETAILS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SPECIFIC_LOGO_REQUIRED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIFIC_LOGO_REQUIRED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIFIC_LOGO_REQUIRED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIFIC_LOGO_REQUIRED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIFIC_LOGO_REQUIRED_IND() != null)) {
                    Assert.assertEquals("The SPECIFIC_LOGO_REQUIRED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSPECIFIC_LOGO_REQUIRED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSPECIFIC_LOGO_REQUIRED_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " ADDITIONAL_DELIVERIES_DETAILS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDITIONAL_DELIVERIES_DETAILS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDITIONAL_DELIVERIES_DETAILS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDITIONAL_DELIVERIES_DETAILS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDITIONAL_DELIVERIES_DETAILS() != null)) {
                    Assert.assertEquals("The ADDITIONAL_DELIVERIES_DETAILS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getADDITIONAL_DELIVERIES_DETAILS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getADDITIONAL_DELIVERIES_DETAILS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " MANDATORY_SUBMISSION_ITEM_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANDATORY_SUBMISSION_ITEM_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANDATORY_SUBMISSION_ITEM_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANDATORY_SUBMISSION_ITEM_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANDATORY_SUBMISSION_ITEM_IND() != null)) {
                    Assert.assertEquals("The MANDATORY_SUBMISSION_ITEM_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getMANDATORY_SUBMISSION_ITEM_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getMANDATORY_SUBMISSION_ITEM_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DOI_STATEMENT_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_STATEMENT_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_STATEMENT_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_STATEMENT_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_STATEMENT_IND() != null)) {
                    Assert.assertEquals("The DOI_STATEMENT_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDOI_STATEMENT_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDOI_STATEMENT_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " LANGUAGE_EDITING_PERFORMED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLANGUAGE_EDITING_PERFORMED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLANGUAGE_EDITING_PERFORMED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLANGUAGE_EDITING_PERFORMED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLANGUAGE_EDITING_PERFORMED_IND() != null)) {
                    Assert.assertEquals("The LANGUAGE_EDITING_PERFORMED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLANGUAGE_EDITING_PERFORMED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLANGUAGE_EDITING_PERFORMED_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " LANGUAGE_EDITING_STAGE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLANGUAGE_EDITING_STAGE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLANGUAGE_EDITING_STAGE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLANGUAGE_EDITING_STAGE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLANGUAGE_EDITING_STAGE() != null)) {
                    Assert.assertEquals("The LANGUAGE_EDITING_STAGE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLANGUAGE_EDITING_STAGE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLANGUAGE_EDITING_STAGE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DEDICATED_JOURNAL_URL_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDEDICATED_JOURNAL_URL_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDEDICATED_JOURNAL_URL_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDEDICATED_JOURNAL_URL_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDEDICATED_JOURNAL_URL_IND() != null)) {
                    Assert.assertEquals("The DEDICATED_JOURNAL_URL_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDEDICATED_JOURNAL_URL_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDEDICATED_JOURNAL_URL_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " DEDICATED_JOURNAL_URL => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDEDICATED_JOURNAL_URL() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDEDICATED_JOURNAL_URL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDEDICATED_JOURNAL_URL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDEDICATED_JOURNAL_URL() != null)) {
                    Assert.assertEquals("The DEDICATED_JOURNAL_URL is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getDEDICATED_JOURNAL_URL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getDEDICATED_JOURNAL_URL());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " COI_REQUIRED_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOI_REQUIRED_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOI_REQUIRED_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOI_REQUIRED_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOI_REQUIRED_IND() != null)) {
                    Assert.assertEquals("The COI_REQUIRED_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCOI_REQUIRED_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCOI_REQUIRED_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EDITORIAL_SYSTEM_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_SYSTEM_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_SYSTEM_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_SYSTEM_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_SYSTEM_NAME() != null)) {
                    Assert.assertEquals("The EDITORIAL_SYSTEM_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_SYSTEM_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_SYSTEM_NAME());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " TYPESETTER_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESETTER_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESETTER_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESETTER_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESETTER_NAME() != null)) {
                    Assert.assertEquals("The TYPESETTER_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getTYPESETTER_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getTYPESETTER_NAME());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EDITORIAL_TURNAROUND_TIME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_TURNAROUND_TIME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_TURNAROUND_TIME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_TURNAROUND_TIME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_TURNAROUND_TIME() != null)) {
                    Assert.assertEquals("The EDITORIAL_TURNAROUND_TIME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_TURNAROUND_TIME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_TURNAROUND_TIME());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PENDING_SUBMISSIONS_QUANTITY => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPENDING_SUBMISSIONS_QUANTITY() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPENDING_SUBMISSIONS_QUANTITY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPENDING_SUBMISSIONS_QUANTITY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPENDING_SUBMISSIONS_QUANTITY() != null)) {
                    Assert.assertEquals("The PENDING_SUBMISSIONS_QUANTITY is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPENDING_SUBMISSIONS_QUANTITY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPENDING_SUBMISSIONS_QUANTITY());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " CHECKED_WITH_SOCIETY_TEAM_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHECKED_WITH_SOCIETY_TEAM_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHECKED_WITH_SOCIETY_TEAM_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHECKED_WITH_SOCIETY_TEAM_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHECKED_WITH_SOCIETY_TEAM_IND() != null)) {
                    Assert.assertEquals("The CHECKED_WITH_SOCIETY_TEAM_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHECKED_WITH_SOCIETY_TEAM_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHECKED_WITH_SOCIETY_TEAM_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " LAUNCH_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAUNCH_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAUNCH_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAUNCH_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAUNCH_DATE() != null)) {
                    Assert.assertEquals("The LAUNCH_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getLAUNCH_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getLAUNCH_DATE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " PROPOSED_EVISE_ROLLOUT_PERIOD_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROPOSED_EVISE_ROLLOUT_PERIOD_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROPOSED_EVISE_ROLLOUT_PERIOD_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROPOSED_EVISE_ROLLOUT_PERIOD_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROPOSED_EVISE_ROLLOUT_PERIOD_DATE() != null)) {
                    Assert.assertEquals("The PROPOSED_EVISE_ROLLOUT_PERIOD_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPROPOSED_EVISE_ROLLOUT_PERIOD_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPROPOSED_EVISE_ROLLOUT_PERIOD_DATE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACKSTOCK_END_YEAR => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_END_YEAR() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_END_YEAR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_END_YEAR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_END_YEAR() != null)) {
                    Assert.assertEquals("The BACKSTOCK_END_YEAR is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_END_YEAR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_END_YEAR());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACKSTOCK_END_OPTION => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_END_OPTION() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_END_OPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_END_OPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_END_OPTION() != null)) {
                    Assert.assertEquals("The BACKSTOCK_END_OPTION is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_END_OPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_END_OPTION());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " JBS_SITE_IND => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJBS_SITE_IND() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJBS_SITE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJBS_SITE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJBS_SITE_IND() != null)) {
                    Assert.assertEquals("The JBS_SITE_IND is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJBS_SITE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJBS_SITE_IND());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EDITORIAL_PRODUCTION_EMAIL_ADDRESS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_EMAIL_ADDRESS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_EMAIL_ADDRESS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_EMAIL_ADDRESS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_EMAIL_ADDRESS() != null)) {
                    Assert.assertEquals("The EDITORIAL_PRODUCTION_EMAIL_ADDRESS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_EMAIL_ADDRESS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_EMAIL_ADDRESS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " EDITORIAL_PRODUCTION_SITE_NAME => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_SITE_NAME() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_SITE_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_SITE_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_SITE_NAME() != null)) {
                    Assert.assertEquals("The EDITORIAL_PRODUCTION_SITE_NAME is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEDITORIAL_PRODUCTION_SITE_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEDITORIAL_PRODUCTION_SITE_NAME());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " JOURNAL_HAS_ARTICLE_NOS => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_HAS_ARTICLE_NOS() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_HAS_ARTICLE_NOS());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_HAS_ARTICLE_NOS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_HAS_ARTICLE_NOS() != null)) {
                    Assert.assertEquals("The JOURNAL_HAS_ARTICLE_NOS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_HAS_ARTICLE_NOS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_HAS_ARTICLE_NOS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " JOURNAL_ARTICLE_NUMBER_TYPE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ARTICLE_NUMBER_TYPE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ARTICLE_NUMBER_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ARTICLE_NUMBER_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ARTICLE_NUMBER_TYPE() != null)) {
                    Assert.assertEquals("The JOURNAL_ARTICLE_NUMBER_TYPE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOURNAL_ARTICLE_NUMBER_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOURNAL_ARTICLE_NUMBER_TYPE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " BACKSTOCK_TREATMENT_NOTE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_TREATMENT_NOTE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_TREATMENT_NOTE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_TREATMENT_NOTE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_TREATMENT_NOTE() != null)) {
                    Assert.assertEquals("The BACKSTOCK_TREATMENT_NOTE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getBACKSTOCK_TREATMENT_NOTE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKSTOCK_TREATMENT_NOTE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " NOTIFIED_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE() != null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }


            }
        }
    }
}