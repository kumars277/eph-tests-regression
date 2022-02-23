package com.eph.automation.testing.steps.dataLake.DLGdTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JMETL.JMContext;
import com.eph.automation.testing.models.dao.JMDataLake.JMETLObject;
import com.eph.automation.testing.models.dao.JMDataLake.JsonObjects.StitchManifestationObjectJson;
import com.eph.automation.testing.models.dao.JMDataLake.JsonObjects.StitchPersonJson;
import com.eph.automation.testing.models.dao.JMDataLake.JsonObjects.StitchSubIdentifierJson;
import com.eph.automation.testing.models.dao.JMDataLake.STITCHObject;
import com.eph.automation.testing.services.db.gdSQLDLSQL.gdTableDLSQL;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMtoDataLakeCountChecksSQL;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class gdTablesDataLake {
    private static String sql;
    private static List<String> Ids;
    private static String ExternalRef;

    private static String sqlGdPostgresCounts;
    private static String sqlgdDLCount;
    private static int gdTablePosgreSQLCount;
    private static int gdDLCount;


    @Given ("^We get the count (.*) from postgreSQL$")
    public void getTheCountfromSQL(String table){
       // Log.info("Getting the count from the PostgreSQL");
        switch (table) {
            case "gd_accountable_product":
                sqlGdPostgresCounts = gdTableDLSQL.GET_ACC_PROD_GD_SQL_COUNT;
                break;
            case "gd_event":
                 sqlGdPostgresCounts = gdTableDLSQL.GET_EVENT_GD_SQL_COUNT;
                break;
            case "gd_legal_owner":
                sqlGdPostgresCounts = gdTableDLSQL.GET_LEGAL_OWNER_GD_SQL_COUNT;
                break;
            case "gd_manifestation":
                sqlGdPostgresCounts = gdTableDLSQL.GET_MANIF_GD_SQL_COUNT;
                break;
            case "gd_manifestation_identifier":
                sqlGdPostgresCounts = gdTableDLSQL.GET_MANIF_ID_GD_SQL_COUNT;
                break;
            case "gd_person":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PERSON_GD_SQL_COUNT;
                break;
            case "gd_price":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PRICE_GD_SQL_COUNT;
                break;
            case "gd_product":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_GD_SQL_COUNT;
                break;
            case "gd_product_financial_attribs":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_FIN_ATTR_GD_SQL_COUNT;
                break;
            case "gd_product_hierarchy":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_HIER_GD_SQL_COUNT;
                break;
            case "gd_product_line":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_LINE_GD_SQL_COUNT;
                break;
            case "gd_product_person_role":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_PERSON_ROLE_GD_SQL_COUNT;
                break;
            case "gd_product_product_hchy_link":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_HCHY_LINK_GD_SQL_COUNT;
                break;
            case "gd_product_rel_package":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_REL_PKG_GD_SQL_COUNT;
                break;
            case "gd_product_relationship":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_REL_GD_SQL_COUNT;
                break;
            case "gd_subject_area":
                sqlGdPostgresCounts = gdTableDLSQL.GET_SUB_AREA_GD_SQL_COUNT;
                break;
            case "gd_work_access_model":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_ACCESS_GD_SQL_COUNT;
                break;
            case "gd_work_business_model":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_BUSINESS_GD_SQL_COUNT;
                break;
            case "gd_work_financial_attribs":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_FIN_ATTR_GD_SQL_COUNT;
                break;
            case "gd_work_hierarchy":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_HERCHY_GD_SQL_COUNT;
                break;
            case "gd_work_identifier":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_IDENTF_GD_SQL_COUNT;
                break;
            case "gd_work_legal_owner":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_LEGAL_OWN_GD_SQL_COUNT;
                break;
            case "gd_work_metric":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_METRIC_GD_SQL_COUNT;
                break;
            case "gd_work_person_role":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_PERS_ROLE_GD_SQL_COUNT;
                break;
            case "gd_work_rel_package":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_REL_PKG_GD_SQL_COUNT;
                break;
            case "gd_work_relationship":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_REL_GD_SQL_COUNT;
                break;
            case "gd_work_subject_area_link":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_SUBJ_AREA_LINK_GD_SQL_COUNT;
                break;
            case "gd_work_work_hchy_link":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_WORK_HCHY_GD_SQL_COUNT;
                break;
            case "gd_wwork":
                sqlGdPostgresCounts = gdTableDLSQL.GET_WORK_SQL_COUNT;
                break;
        }
       // Log.info(sqlGdPostgresCounts);
        List<Map<String, Object>> gdTableSqlCount = DBManager.getDBResultMap(sqlGdPostgresCounts, Constants.EPH_URL);
        gdTablePosgreSQLCount = ((Long) gdTableSqlCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Get the count of (.*) from the DataLake$")
    public void getCountGdTables(String SemarchyTable) {
       // Log.info("Getting the count from the DL");
        switch (SemarchyTable) {
            case "gd_accountable_product":
                sqlgdDLCount = gdTableDLSQL.GET_ACC_PROD_GD_DL_COUNT;
                break;
            case "gd_event":
                sqlgdDLCount = gdTableDLSQL.GET_EVENT_GD_DL_COUNT;
                break;
            case "gd_legal_owner":
                sqlgdDLCount = gdTableDLSQL.GET_LEGAL_OWNER_GD_DL_COUNT;
                break;
            case "gd_manifestation":
                sqlgdDLCount = gdTableDLSQL.GET_MANIF_GD_DL_COUNT;
                break;
            case "gd_manifestation_identifier":
                sqlgdDLCount = gdTableDLSQL.GET_MANIF_ID_GD_DL_COUNT;
                break;
            case "gd_person":
                sqlgdDLCount = gdTableDLSQL.GET_PERSON_GD_DL_COUNT;
                break;
            case "gd_price":
                sqlgdDLCount = gdTableDLSQL.GET_PRICE_GD_DL_COUNT;
                break;
            case "gd_product":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_GD_DL_COUNT;
                break;
            case "gd_product_financial_attribs":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_FIN_ATTR_GD_DL_COUNT;
                break;
            case "gd_product_hierarchy":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_HIER_GD_DL_COUNT;
                break;
            case "gd_product_line":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_LINE_GD_DL_COUNT;
                break;
            case "gd_product_person_role":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_PERSON_ROLE_GD_DL_COUNT;
                break;
            case "gd_product_product_hchy_link":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_HCHY_LINK_GD_DL_COUNT;
                break;
            case "gd_product_rel_package":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_REL_PKG_GD_DL_COUNT;
                break;
            case "gd_product_relationship":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_REL_GD_DL_COUNT;
                break;
            case "gd_subject_area":
                sqlgdDLCount = gdTableDLSQL.GET_SUB_AREA_GD_DL_COUNT;
                break;
            case "gd_work_access_model":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_ACCESS_GD_DL_COUNT;
                break;
            case "gd_work_business_model":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_BUSINESS_GD_DL_COUNT;
                break;
            case "gd_work_financial_attribs":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_FIN_ATTR_GD_DL_COUNT;
                break;
            case "gd_work_hierarchy":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_HERCHY_GD_DL_COUNT;
                break;
            case "gd_work_identifier":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_IDENTF_GD_DL_COUNT;
                break;
            case "gd_work_legal_owner":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_LEGAL_OWN_GD_DL_COUNT;
                break;
            case "gd_work_metric":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_METRIC_GD_DL_COUNT;
                break;
            case "gd_work_person_role":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_PERS_ROLE_GD_DL_COUNT;
                break;
            case "gd_work_rel_package":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_REL_PKG_GD_DL_COUNT;
                break;
            case "gd_work_relationship":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_REL_GD_DL_COUNT;
                break;
            case "gd_work_subject_area_link":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_SUBJ_AREA_LINK_GD_DL_COUNT;
                break;
            case "gd_work_work_hchy_link":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_WORK_HCHY_GD_DL_COUNT;
                break;
            case "gd_wwork":
                sqlgdDLCount = gdTableDLSQL.GET_WORK_DL_COUNT;
                break;

        }

        //Log.info(sqlgdDLCount);
        List<Map<String, Object>> gdTableDLCount = DBManager.getDBResultMap(sqlgdDLCount, Constants.AWS_URL);
        gdDLCount = ((Long) gdTableDLCount.get(0).get("Target_Count")).intValue();
    }

    @And("^we compare both the counts are identical for the table (.*)$")
    public void compareGdandDLCounts(String srctable){
        Log.info("The count for "+srctable+" in SQL => " + gdTablePosgreSQLCount + " and in Data_LAke  => " + gdDLCount);
        Assert.assertEquals("The counts are not equal for "+srctable+" in Posgres and DL ",gdDLCount, gdTablePosgreSQLCount);

    }


 }
