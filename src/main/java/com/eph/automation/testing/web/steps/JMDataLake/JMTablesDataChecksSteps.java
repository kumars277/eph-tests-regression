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




}
