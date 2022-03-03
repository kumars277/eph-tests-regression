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
            case "gd_product_identifier":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_IDENTIF_GD_SQL_COUNT;
                break;
            case "gd_product_person_role":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_PERSON_ROLE_GD_SQL_COUNT;
                break;
             case "gd_product_rel_package":
                sqlGdPostgresCounts = gdTableDLSQL.GET_PROD_REL_PKG_GD_SQL_COUNT;
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
            case "gd_product_identifier":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_IDENTIF_GD_DL_COUNT;
                break;
            case "gd_product_person_role":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_PERSON_ROLE_GD_DL_COUNT;
                break;
            case "gd_product_rel_package":
                sqlgdDLCount = gdTableDLSQL.GET_PROD_REL_PKG_GD_DL_COUNT;
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

    @Given ("^Get (.*) random ids of (.*) from the postgreSQL$")
    public void getRandomGdTableIds(String numberOfRecords, String SourceTable) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (SourceTable) {
            case "gd_accountable_product":
                sql = String.format(gdTableDLSQL.GET_GD_ACC_PROD_IDS, numberOfRecords);
                List<Map<?, ?>> randomAccProdIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomAccProdIds.stream().map(m -> (String) m.get("external_reference")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_event":
                sql = String.format(gdTableDLSQL.GET_GD_EVENT_IDS, numberOfRecords);
                List<Map<?, ?>> randomEventIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomEventIds.stream().map(m -> (BigDecimal) m.get("event_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_LEGAL_OWNER_IDS, numberOfRecords);
                List<Map<?, ?>> randomLegalOwnerIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomLegalOwnerIds.stream().map(m -> (BigDecimal) m.get("legal_owner_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_manifestation":
                sql = String.format(gdTableDLSQL.GET_GD_MANIF_IDS, numberOfRecords);
                List<Map<?, ?>> randomManifIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManifIds.stream().map(m -> (String) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_manifestation_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_MANIF_IDENTIF_IDS, numberOfRecords);
                List<Map<?, ?>> randomManifIdentifierIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManifIdentifierIds.stream().map(m -> (BigDecimal) m.get("manif_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_person":
                sql = String.format(gdTableDLSQL.GET_GD_PERSON_IDS, numberOfRecords);
                List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomPersonIds.stream().map(m -> (BigDecimal) m.get("person_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_IDS, numberOfRecords);
                List<Map<?, ?>> randomPriceIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomPriceIds.stream().map(m -> (String) m.get("product_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_FIN_IDS, numberOfRecords);
                List<Map<?, ?>> randomProdFinIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProdFinIds.stream().map(m -> (BigDecimal) m.get("product_fin_attribs_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_IDENTIF_IDS, numberOfRecords);
                List<Map<?, ?>> randomProdIdentifIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProdIdentifIds.stream().map(m -> (BigDecimal) m.get("product_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_PERSON_ROLE_IDS, numberOfRecords);
                List<Map<?, ?>> randomProdPersRoleIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProdPersRoleIds.stream().map(m -> (BigDecimal) m.get("product_person_role_id")).map(String::valueOf).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

 }
