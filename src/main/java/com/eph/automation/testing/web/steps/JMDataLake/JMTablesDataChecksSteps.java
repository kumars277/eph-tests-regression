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
//    private JMTablesDataChecksSQL jmObj = new JMTablesDataChecksSQL();


    @Given("^We get (.*) random allocation ids of (.*)")
    public void getRandomIds(String numberOfRecords, String tableName){
        Log.info("Get random record...");

        sql = String.format(JMTablesDataChecksSQL.GET_ALLOCATION_IDS, numberOfRecords);
        List<Map<?, ?>> randomAllocationIds = DBManager.getDBResultMap(sql, Constants.MYSQL_JM_URL);
        Ids = randomAllocationIds.stream().map(m -> (Integer) m.get("ALLOCATION_CHANGE_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }
    @When("^We get the JMF Allocation records from JMF MySQL of (.*)")
    public void getRecordsFromJMF(String tableName){
        Log.info("We get the Allocations Change records from JMF MySql..");
        sql = String.format(JMTablesDataChecksSQL.GET_DATA_ALLOCATION_CHANGE_JM, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromMysql = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.MYSQL_JM_URL);

    }

    @Then("^We get the JMF Allocation records from DL of (.*)$")
    public void getRecordsFromDL(String tableName) {
        Log.info("We get the Allocations Change records from JMF DL..");
        sql = String.format(JMTablesDataChecksSQL.GET_DATA_ALLOCATION_CHANGE_DL,Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityJMContext.tbJMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JMF Allocation records in JMF MySQL and DL of (.*)$")
    public void compareJMapplicationChangeDataSQLtoDL(String tableName) {
        if(dataQualityJMContext.tbJMDataObjectsFromMysql.isEmpty()){
            Log.info("No Data Found ....");
        }else{
            Log.info("Sorting the data to compare the application_change records in MySql and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityJMContext.tbJMDataObjectsFromMysql.size(); i++) {

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " APPLICATION_CHANGE_ID => MysqL="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID());
                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The APPLICATION_CHANGE_ID is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_CHANGE_ID());

                }
                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " ALLOCATION_TYPE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE().equals("null"))) {
                    Assert.assertEquals("The ALLOCATION_TYPE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_TYPE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getALLOCATION_TYPE());
                }
                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                            " PMG_FILTER => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER()+
                            " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER() != null ||
                            (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER().equals("null"))) {
                        Assert.assertEquals("The PMG_FILTER is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                                dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_FILTER(),
                                dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_FILTER());
                }
                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMC_FILTER => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER()+
                            " DL="+ dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER() != null ||
                            (! dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER().equals("null"))) {
                        Assert.assertEquals("The PMC_FILTER is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                                dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_FILTER(),
                                dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_FILTER());
                    }
                    Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                            " PPC_FILTER_EMAIL => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL()+
                            " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL());

                    if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL() != null ||
                            (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL().equals("null"))) {
                        Assert.assertEquals("The PPC_FILTER_EMAIL is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                                dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_EMAIL(),
                                dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_EMAIL());
                    }


                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PPC_FILTER_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME().equals("null"))) {
                    Assert.assertEquals("The PPC_FILTER_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_FILTER_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_FILTER_NAME());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMC_CHANGE_IND => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND().equals("null"))) {
                    Assert.assertEquals("The PMC_CHANGE_IND is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMC_CHANGE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMC_CHANGE_IND());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PPC_CHANGE_IND => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND().equals("null"))) {
                    Assert.assertEquals("The PPC_CHANGE_IND is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPPC_CHANGE_IND(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPPC_CHANGE_IND());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMX_PMGCODE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE().equals("null"))) {
                    Assert.assertEquals("The PMX_PMGCODE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE());
                }
                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMX_PMG_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMGCODE() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMGCODE().equals("null"))) {
                    Assert.assertEquals("The PMX_PMG_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_NAME());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMX_PMG_F_BUSINESS_UNIT => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT().equals("null"))) {
                    Assert.assertEquals("The PMX_PMG_F_BUSINESS_UNIT is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMX_PMG_F_BUSINESS_UNIT(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMX_PMG_F_BUSINESS_UNIT());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_BUS_CTRL_NAME => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME().equals("null"))) {
                    Assert.assertEquals("The PMG_BUS_CTRL_NAME is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_NAME(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_NAME());
                }


                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_BUS_CTRL_EMAIL => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL().equals("null"))) {
                    Assert.assertEquals("The PMG_BUS_CTRL_EMAIL is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_BUS_CTRL_EMAIL(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_BUS_CTRL_EMAIL());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_NAME_OLD => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD().equals("null"))) {
                    Assert.assertEquals("The PMG_PUBDIR_NAME_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_OLD());
                }


                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_EMAIL_OLD => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD().equals("null"))) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_OLD());
                }


                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_PEOPLE_HUB_ID_OLD => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD().equals("null"))) {
                    Assert.assertEquals("The PMG_PUBDIR_PEOPLE_HUB_ID_OLD is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_OLD());
                }


                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_NAME_NEW => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW().equals("null"))) {
                    Assert.assertEquals("The PMG_PUBDIR_NAME_NEW is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_NAME_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_NAME_NEW());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_EMAIL_NEW => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW().equals("null"))) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_NEW is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());
                }


                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " PMG_PUBDIR_PEOPLE_HUB_ID_NEW => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_NEW()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_PEOPLE_HUB_ID_NEW());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW().equals("null"))) {
                    Assert.assertEquals("The PMG_PUBDIR_EMAIL_NEW is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getPMG_PUBDIR_EMAIL_NEW(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getPMG_PUBDIR_EMAIL_NEW());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " NOTIFIED_DATE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE().equals("null"))) {
                    Assert.assertEquals("The NOTIFIED_DATE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getNOTIFIED_DATE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getNOTIFIED_DATE());
                }

                Log.info("APPLICATION_CHANGE_ID => "+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID()+
                        " EPH_PMG_CODE => Mysql="+dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE()+
                        " DL="+dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE());

                if (dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE() != null ||
                        (!dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE().equals("null"))) {
                    Assert.assertEquals("The EPH_PMG_CODE is incorrect for id=" + dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getALLOCATION_CHANGE_ID(),
                            dataQualityJMContext.tbJMDataObjectsFromMysql.get(i).getEPH_PMG_CODE(),
                            dataQualityJMContext.tbJMDataObjectsFromDL.get(i).getEPH_PMG_CODE());
                }



            }
        }
    }



}
