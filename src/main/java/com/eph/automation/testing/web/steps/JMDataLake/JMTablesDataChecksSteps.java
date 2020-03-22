package com.eph.automation.testing.web.steps.JMDataLake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.JMDataLake.JMTablesDLObject;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMTablesDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import com.eph.automation.testing.models.contexts.DataQualityJMContext;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JMTablesDataChecksSteps {

    public DataQualityJMContext dataQualityJMContext;
    private static String sql;
    private static List<String> Ids;
   private JMTablesDataChecksSQL jmObj = new JMTablesDataChecksSQL();


    @Given("^We get (.*) random ids of (.*)")
    public void getRandomIds(String numberOfRecords, String tableName){
        Log.info("Get random record...");
        switch (tableName){
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
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }
    @When("^We get the JMF Allocation records from JMF MySQL of (.*)")
    public void getRecordsFromJMF(String tableName){
        Log.info("We get the Allocations Change records from JMF MySql..");
        sql = String.format(jmObj.getAllocationChangesSql("mysql",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Allocation records from DL of (.*)$")
    public void getRecordsFromDL(String tableName) {
        Log.info("We get the Allocations Change records from JMF DL..");
        sql = String.format(jmObj.getAllocationChangesSql("aws",tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Allocation records in JMF MySQL and DL of (.*)$")
    public void compareJMapplicationChangeDataSQLtoDL(String tableName) {
        if(dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the allocation_change records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getALLOCATION_CHANGE_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getALLOCATION_CHANGE_ID));

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " ALLOCATION_CHANGE_ID => MysqL="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The ALLOCATION_CHANGE_ID is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID());

                }
                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " ALLOCATION_TYPE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE()!= null)) {
                    Assert.assertEquals("The ALLOCATION_TYPE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE());
                }
                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_FILTER => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER()!= null)) {
                    Assert.assertEquals("The PMG_FILTER is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER());
                }
                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMC_FILTER => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER()+
                        " DL="+ dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER()!= null)) {
                    Assert.assertEquals("The PMC_FILTER is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER());
                }
                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PPC_FILTER_EMAIL => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL()!= null)) {
                    Assert.assertEquals("The PPC_FILTER_EMAIL is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL());
                }


                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PPC_FILTER_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME()!=null)) {
                    Assert.assertEquals("The PPC_FILTER_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMC_CHANGE_IND => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND()!=null)) {
                    Assert.assertEquals("The PMC_CHANGE_IND is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PPC_CHANGE_IND => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND()!=null)) {
                    Assert.assertEquals("The PPC_CHANGE_IND is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMX_PMGCODE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE()!=null)) {
                    Assert.assertEquals("The PMX_PMGCODE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE());
                }
                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMX_PMG_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE()!=null)) {
                    Assert.assertEquals("The PMX_PMG_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_NAME());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMX_PMG_F_BUSINESS_UNIT => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT()!=null)) {
                    Assert.assertEquals("The PMX_PMG_F_BUSINESS_UNIT is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_BUS_CTRL_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME()!=null)) {
                    Assert.assertEquals("The PMG_BUS_CTRL_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME());
                }


                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_BUS_CTRL_EMAIL => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL()!=null)) {
                    Assert.assertEquals("The PMG_BUS_CTRL_EMAIL is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_NAME_OLD => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD()!=null)) {
                    Assert.assertEquals("The PMG_PUBDIR_NAME_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD());
                }


                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_EMAIL_OLD => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD()!=null)) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD());
                }


                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_PEOPLE_HUB_ID_OLD => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD()!=null)) {
                    Assert.assertEquals("The PMG_PUBDIR_PEOPLE_HUB_ID_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD());
                }


                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_NAME_NEW => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW()!=null)) {
                    Assert.assertEquals("The PMG_PUBDIR_NAME_NEW is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_EMAIL_NEW => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW()!=null)) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_NEW is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());
                }


                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_PEOPLE_HUB_ID_NEW => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_NEW()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW()!=null)) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_NEW is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " NOTIFIED_DATE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE()!=null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

                Log.info("ALLOCATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " EPH_PMG_CODE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE()!=null)) {
                    Assert.assertEquals("The EPH_PMG_CODE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE());
                }



            }
        }
    }

    @Given("^We get (.*) random application key of (.*)")
    public void getApplicationkey(String numberOfRecords, String tableName){
        Log.info("Get random record...");
        sql = String.format(JMTablesDataChecksSQL.GET_APPLICATION_KEY, numberOfRecords);
        List<Map<?, ?>> randomAllocationIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
        Ids = randomAllocationIds.stream().map(m -> (String) m.get("APPLICATION_PROPERTY_KEY")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }
    @When("^We get the JMF Application properties records from JMF MySQL of (.*)")
    public void getAppPropertyFromJMF(String tableName){
        Log.info("We get the Application Property records from JMF MySql..");
        sql = String.format(jmObj.getAppPropertySql("mysql",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);
    }
    @Then("^We get the JMF Application properties records from DL of (.*)")
    public void getAppPropertyFromDL(String tableName){
        Log.info("We get the Application Property records from JMF DL..");
        sql = String.format(jmObj.getAppPropertySql("aws",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Application properties records in JMF MySQL and DL of (.*)$")
    public void compareJMappPropertyDataSQLtoDL(String tableName) {
        if(dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the application_property records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getAPPLICATION_PROPERTY_KEY)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getAPPLICATION_PROPERTY_KEY));

                Log.info("APP_PROP_KEY => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY()+
                        " APPLICATION_PROPERTY_KEY => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_KEY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_KEY()!=null)) {
                    Assert.assertEquals("The APPLICATION_PROPERTY_KEY is incorrect for app_prop_key=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_KEY());
                }

                Log.info("APP_PROP_KEY => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY()+
                        " APPLICATION_PROPERTY_VALUE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_VALUE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_VALUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_VALUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_VALUE()!=null)) {
                    Assert.assertEquals("The APPLICATION_PROPERTY_VALUE is incorrect for app_prop_key=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_VALUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_VALUE());
                }

                Log.info("APP_PROP_KEY => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " NOTIFIED_DATE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE()!=null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for app_prop_key=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

            }
        }
    }

    @When("^We get the JMF Approval Request records from JMF MySQL of (.*)")
    public void getAppRequestFromJMF(String tableName){
        Log.info("We get the Application Request records from JMF MySql..");
        sql = String.format(jmObj.getAppRequestSql("mysql",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);
    }
    @Then("^We get the JMF Approval Request records from DL of (.*)")
    public void getgetAppRequestFromDL(String tableName){
        Log.info("We get the Application Request records from JMF DL..");
        sql = String.format(jmObj.getAppRequestSql("aws",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Approval Request records in JMF MySQL and DL of (.*)$")
    public void compareJMappRequestDataSQLtoDL(String tableName) {
        if(dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the approval_request records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getAPPROVAL_ID)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getAPPROVAL_ID));

                Log.info("APPROVAL_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+
                        " APPROVAL_ID => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ID()!=null)) {
                    Assert.assertEquals("The APPROVAL_ID is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_ID());
                }

                Log.info("APPROVAL_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+
                        " F_PRODUCT_CHRONICLE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_CHRONICLE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_CHRONICLE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPLICATION_PROPERTY_VALUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPLICATION_PROPERTY_VALUE()!=null)) {
                    Assert.assertEquals("The F_PRODUCT_CHRONICLE is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_CHRONICLE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_CHRONICLE());
                }

                Log.info("APPROVAL_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+
                        " APPROVAL_TYPE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_TYPE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_TYPE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_TYPE()!=null)) {
                    Assert.assertEquals("The APPROVAL_TYPE is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_TYPE());
                }

                Log.info("APPROVAL_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+
                        " APPROVER_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVER_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVER_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVER_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVER_NAME()!=null)) {
                    Assert.assertEquals("The APPROVER_NAME is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVER_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVER_NAME());
                }

                Log.info("APPROVAL_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+
                        " APPROVAL_GIVEN_INDICATOR => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_GIVEN_INDICATOR()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_GIVEN_INDICATOR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_GIVEN_INDICATOR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_GIVEN_INDICATOR()!=null)) {
                    Assert.assertEquals("The APPROVAL_GIVEN_INDICATOR is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_GIVEN_INDICATOR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_GIVEN_INDICATOR());
                }

                Log.info("APPROVAL_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+
                        " APPROVAL_REQUEST_DATE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_REQUEST_DATE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_REQUEST_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_REQUEST_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_REQUEST_DATE()!=null)) {
                    Assert.assertEquals("The APPROVAL_REQUEST_DATE is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_REQUEST_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_REQUEST_DATE());
                }

                Log.info("APPROVAL_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID()+
                        " APPROVAL_RESPONSE_DATE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_RESPONSE_DATE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_RESPONSE_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_RESPONSE_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_RESPONSE_DATE()!=null)) {
                    Assert.assertEquals("The APPROVAL_RESPONSE_DATE is incorrect for APPROVAL_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getAPPROVAL_RESPONSE_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getAPPROVAL_RESPONSE_DATE());
                }

            }
        }
    }

    @When("^We get the JMF Chronicle Scenario records from JMF MySQL of (.*)")
    public void getChronicleRecFromJMF(String tableName){
        Log.info("We get the Chronicle Scenario records from JMF MySql..");
        sql = String.format(jmObj.getChronicleSceSql("mysql",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Chronicle Scenario records from DL of (.*)$")
    public void getChronicleRecFromDL(String tableName) {
        Log.info("We get the Chronicle Scenario records from JMF DL..");
        sql = String.format(jmObj.getChronicleSceSql("aws",tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Chronicle Scenario records in JMF MySQL and DL of (.*)$")
    public void compareJMChronSceDataSQLtoDL(String tableName) {
        if(dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the chronicle_scenario records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getCHRONICLE_SCENARIO_CODE)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getCHRONICLE_SCENARIO_CODE));

                Log.info("CHRO_SCE_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE()+
                        " CHRO_SCE_CODE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE()!=null)) {
                    Assert.assertEquals("The CHRO_SCE_CODE is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_CODE());
                }
                Log.info("CHRO_SCE_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE()+
                        " CHRONICLE_SCENARIO_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_NAME()!=null)) {
                    Assert.assertEquals("The CHRONICLE_SCENARIO_NAME is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_NAME());
                }

                Log.info("CHRO_SCE_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE()+
                        " CHRONICLE_SCENARIO_DESCRIPTION => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_DESCRIPTION()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_DESCRIPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_DESCRIPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_DESCRIPTION()!=null)) {
                    Assert.assertEquals("The CHRONICLE_SCENARIO_DESCRIPTION is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_DESCRIPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_DESCRIPTION());
                }

                Log.info("CHRO_SCE_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE()+
                        " CHRONICLE_SCENARIO_EVOLUTIONARY_IND => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND()!=null)) {
                    Assert.assertEquals("The CHRONICLE_SCENARIO_EVOLUTIONARY_IND is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_SCENARIO_EVOLUTIONARY_IND());
                }

                Log.info("CHRO_SCE_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE()+
                        " NOTIFIED_DATE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE()!=null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for CHRO_SCE_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_SCENARIO_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }


    @When("^We get the JMF Chronicle Status records from JMF MySQL of (.*)")
    public void getChronicleStatusRecFromJMF(String tableName){
        Log.info("We get the Chronicle Status records from JMF MySql..");
        sql = String.format(jmObj.getChronicleStatusSql("mysql",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Chronicle Status records from DL of (.*)$")
    public void getChronicleStatusRecFromDL(String tableName) {
        Log.info("We get the Chronicle Status records from JMF DL..");
        sql = String.format(jmObj.getChronicleStatusSql("aws",tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Chronicle Status records in JMF MySQL and DL of (.*)$")
    public void compareJMChronStatusDataSQLtoDL(String tableName) {
        if(dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the chronicle_status records in MySql and DATA LAKE ..");
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                dataQualityJMContext.tbJMDataObjectsFromMysql.sort(Comparator.comparing(JMTablesDLObject::getCHRONICLE_STATUS_CODE)); //sort data in the lists
                dataQualityJMContext.tbJMDataObjectsFromDL.sort(Comparator.comparing(JMTablesDLObject::getCHRONICLE_STATUS_CODE));

                Log.info("CHRO_STATUS_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE()+
                        " CHRO_STATUS_CODE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE()!=null)) {
                    Assert.assertEquals("The CHRO_STATUS_CODE is incorrect for CHRO_STATUS_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_CODE());
                }
                Log.info("CHRO_STATUS_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE()+
                        " CHRONICLE_STATUS_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_NAME() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_NAME()!=null)) {
                    Assert.assertEquals("The CHRONICLE_STATUS_NAME is incorrect for CHRO_STATUS_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_NAME());
                }

                Log.info("CHRO_STATUS_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE()+
                        " CHRONICLE_STATUS_DESCRIPTION => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_DESCRIPTION()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_DESCRIPTION());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_DESCRIPTION() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_DESCRIPTION()!=null)) {
                    Assert.assertEquals("The CHRONICLE_STATUS_DESCRIPTION is incorrect for CHRO_STATUS_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_DESCRIPTION(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCHRONICLE_STATUS_DESCRIPTION());
                }

                Log.info("CHRO_STATUS_CODE => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE()+
                        " NOTIFIED_DATE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE()!=null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for CHRO_STATUS_CODE=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getCHRONICLE_STATUS_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Family Resource records from JMF MySQL of (.*)")
    public void getFamilyResourceRecFromJMF(String tableName){
        Log.info("We get the Family Resources records from JMF MySql..");
        sql = String.format(jmObj.getFamilyResourceSql("mysql",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Family Resource records from DL of (.*)$")
    public void getFamilyResourceRecFromDL(String tableName) {
        Log.info("We get the Family Resources records from JMF DL..");
        sql = String.format(jmObj.getFamilyResourceSql("aws",tableName),Joiner.on("','").join(Ids));
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

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " FAMILY_RESOURCE_DETAILS_ID => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFAMILY_RESOURCE_DETAILS_ID());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFAMILY_RESOURCE_DETAILS_ID()!=null)) {
                    Assert.assertEquals("The FAMILY_RESOURCE_DETAILS_ID is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getFAMILY_RESOURCE_DETAILS_ID());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " RESOURCE_KEY => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_KEY()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_KEY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_KEY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_KEY()!=null)) {
                    Assert.assertEquals("The RESOURCE_KEY is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_KEY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_KEY());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " INITIAL_VALUE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINITIAL_VALUE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINITIAL_VALUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINITIAL_VALUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINITIAL_VALUE()!=null)) {
                    Assert.assertEquals("The INITIAL_VALUE is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getINITIAL_VALUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getINITIAL_VALUE());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " RESOURCE_CHANGE_INDICATOR => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_CHANGE_INDICATOR()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_CHANGE_INDICATOR());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_CHANGE_INDICATOR() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_CHANGE_INDICATOR()!=null)) {
                    Assert.assertEquals("The RESOURCE_CHANGE_INDICATOR is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getRESOURCE_CHANGE_INDICATOR(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRESOURCE_CHANGE_INDICATOR());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " NEW_VALUE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNEW_VALUE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNEW_VALUE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNEW_VALUE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNEW_VALUE()!=null)) {
                    Assert.assertEquals("The NEW_VALUE is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNEW_VALUE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNEW_VALUE());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " F_PRODUCT_FAMILY => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY()!=null)) {
                    Assert.assertEquals("The F_PRODUCT_FAMILY is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getF_PRODUCT_FAMILY(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getF_PRODUCT_FAMILY());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " PEOPLEHUB_ID_OLD => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_OLD()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_OLD() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_OLD()!=null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID_OLD is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_OLD());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " PEOPLEHUB_ID_NEW => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_NEW()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_NEW() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_NEW()!=null)) {
                    Assert.assertEquals("The PEOPLEHUB_ID_NEW is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPEOPLEHUB_ID_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPEOPLEHUB_ID_NEW());
                }

                Log.info("FAMILY_RESOURCE_DETAILS_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID()+
                        " NOTIFIED_DATE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE()!=null)) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for FAMILY_RESOURCE_DETAILS_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getFAMILY_RESOURCE_DETAILS_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }
            }
        }
    }

    @When("^We get the JMF Finanacial Info records from JMF MySQL of (.*)")
    public void getFinanceInfoRecFromJMF(String tableName){
        Log.info("We get the Financial Info records from JMF MySql..");
        sql = String.format(jmObj.getFinancialInfoSql("mysql",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Finanacial Info records from DL of (.*)$")
    public void getFinanceInfoRecFromDL(String tableName) {
        Log.info("We get the Financial Info records from JMF DL..");
        sql = String.format(jmObj.getFinancialInfoSql("aws",tableName),Joiner.on("','").join(Ids));
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
                    Assert.assertEquals("The PRODUCT_WORK_ID is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
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
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getREMINDER_DATE());
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
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getRENEWAL_DATE());
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
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getCLAIMS_HANDLING_END_DATE());
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
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getBACKISSUES_HANDLING_END_DATE());
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
    public void getLegalInfoRecordsFromJMF(String tableName){
        Log.info("We get the Legal Info records from JMF MySql..");
        sql = String.format(jmObj.getLegalInfoSql("mysql",tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Legal Info records from DL of (.*)$")
    public void getLegalInfoRecordsFromDL(String tableName) {
        Log.info("We get the Legal Info records from JMF DL..");
        sql = String.format(jmObj.getLegalInfoSql("aws",tableName),Joiner.on("','").join(Ids));
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
                    Assert.assertEquals("The PRODUCT_WORK_ID is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
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

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOINT_OWNERSHIP_DETAILS() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOINT_OWNERSHIP_DETAILS() != null)) {
                    Assert.assertEquals("The JOINT_OWNERSHIP_DETAILS is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getJOINT_OWNERSHIP_DETAILS(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getJOINT_OWNERSHIP_DETAILS());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_CONTRACT_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_DATE() != null)) {
                    Assert.assertEquals("The SOCIETY_CONTRACT_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_DATE());
                }

                Log.info("PRODUCT_WORK_ID => " + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID() +
                        " SOCIETY_CONTRACT_EXPIRY_DATE => Mysql=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE() +
                        " DL=" + dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE() != null ||
                        (dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE() != null)) {
                    Assert.assertEquals("The SOCIETY_CONTRACT_EXPIRY_DATE is incorrect for PRODUCT_WORK_ID=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPRODUCT_WORK_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOCIETY_CONTRACT_EXPIRY_DATE());
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
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getSOC_CK_CONTENT_AGREEMENT_DATE());
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