package com.eph.automation.testing.steps.dataLake.DLGdTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.GDTablesDLSQLContext;
import com.eph.automation.testing.models.dao.GDTableDLSQL.GDTableDLSQLObject;
import com.eph.automation.testing.services.db.gdSQLDLSQL.gdTableDLSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
//import sun.util.resources.cldr.ka.LocaleNames_ka;

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
    private static String sqlGdPresentationCounts;
    private static String sqlGdPresenationCounts;
    private static String sqlgdDLCount;
    private static int gdTablePosgreSQLCount;
    private static int gdTablePresentationCount;
    private static int gdTablePresentationSQLCOunt;
    private static int gdDLCount;


    @Given("^We get the count (.*) from postgreSQL$")
    public void getTheCountfromSQL(String table) {
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
            case "gd_x_lov_access_model":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_ACCESS_MODEL_COUNT;
                break;
            case "gd_x_lov_business_model":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_BUSINESS_MODEL_COUNT;
                break;
            case "gd_x_lov_currency":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_CURRENCY_COUNT;
                break;
            case "gd_x_lov_etax_product_code":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_ETAX_PROD_CODE_COUNT;
                break;
            case "gd_x_lov_event_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_EVENT_COUNT;
                break;
            case "gd_x_lov_gl_company":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_GL_COMPANY_COUNT;
                break;
            case "gd_x_lov_gl_prod_seg_parent":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_GL_PROD_SEG_PARENT_COUNT;
                break;
            case "gd_x_lov_gl_resp_centre":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_GL_RESP_CENTER_COUNT;
                break;
            case "gd_x_lov_identifier_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_IDENTIFIER_COUNT;
                break;
            case "gd_x_lov_imprint":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_IMPRINT_COUNT;
                break;
            case "gd_x_lov_language":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_LANGUAGE_COUNT;
                break;
            case "gd_x_lov_legal_ownership":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_LEGAL_OWNERSHIP_COUNT;
                break;
            case "gd_x_lov_manif_status":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_MANIF_STATUS_COUNT;
                break;
            case "gd_x_lov_manif_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_MANIF_TYPE_COUNT;
                break;
            case "gd_x_lov_metric_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_METRIC_TYPE_COUNT;
                break;
            case "gd_x_lov_origin":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_ORIGIN_COUNT;
                break;
            case "gd_x_lov_owner_description":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_OWNER_DESCRIP_COUNT;
                break;
            case "gd_x_lov_person_role":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_PERSON_ROLE_COUNT;
                break;
            case "gd_x_lov_pmc":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_PMC_COUNT;
                break;
            case "gd_x_lov_pmg":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_PMG_COUNT;
                break;
            case "gd_x_lov_product_status":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_PROD_STATUS_COUNT;
                break;
            case "gd_x_lov_product_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_PROD_TYPE_COUNT;
                break;
            case "gd_x_lov_relationship_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_RELATION_TYPE_COUNT;
                break;
            case "gd_x_lov_revenue_account":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_REVENUE_COUNT;
                break;
            case "gd_x_lov_revenue_model":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_REVENUE_MODEL_COUNT;
                break;
            case "gd_x_lov_subject_area_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_SUBJ_AREA_COUNT;
                break;
            case "gd_x_lov_subscription_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_SUB_COUNT;
                break;
            case "gd_x_lov_work_hchy_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_WORK_HCHY_COUNT;
                break;
            case "gd_x_lov_work_status":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_WORK_STATUS_COUNT;
                break;
            case "gd_x_lov_work_type":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_WORK_TYPE_COUNT;
                break;
            case "gd_x_lov_workflow_source":
                sqlGdPostgresCounts = gdTableDLSQL.GET_GD_LOV_WORKFOW_SOURCE_COUNT;
                break;
        }
        Log.info(sqlGdPostgresCounts);
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
            case "gd_x_lov_access_model":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_ACCESS_MODEL_COUNT_DL;
                break;
            case "gd_x_lov_business_model":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_BUSINESS_MODEL_COUNT_DL;
                break;
            case "gd_x_lov_currency":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_CURRENCY_COUNT_DL;
                break;
            case "gd_x_lov_etax_product_code":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_ETAX_PROD_CODE_COUNT_DL;
                break;
            case "gd_x_lov_event_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_EVENT_COUNT_DL;
                break;
            case "gd_x_lov_gl_company":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_GL_COMPANY_COUNT_DL;
                break;
            case "gd_x_lov_gl_prod_seg_parent":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_GL_PROD_SEG_PARENT_COUNT_DL;
                break;
            case "gd_x_lov_gl_resp_centre":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_GL_RESP_CENTER_COUNT_DL;
                break;
            case "gd_x_lov_identifier_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_IDENTIFIER_COUNT_DL;
                break;
            case "gd_x_lov_imprint":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_IMPRINT_COUNT_DL;
                break;
            case "gd_x_lov_language":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_LANGUAGE_COUNT_DL;
                break;
            case "gd_x_lov_legal_ownership":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_LEGAL_OWNERSHIP_COUNT_DL;
                break;
            case "gd_x_lov_manif_status":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_MANIF_STATUS_COUNT_DL;
                break;
            case "gd_x_lov_manif_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_MANIF_TYPE_COUNT_DL;
                break;
            case "gd_x_lov_metric_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_METRIC_TYPE_COUNT_DL;
                break;
            case "gd_x_lov_origin":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_ORIGIN_COUNT_DL;
                break;
            case "gd_x_lov_owner_description":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_OWNER_DESCRIP_COUNT_DL;
                break;
            case "gd_x_lov_person_role":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_PERSON_ROLE_COUNT_DL;
                break;
            case "gd_x_lov_pmc":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_PMC_COUNT_DL;
                break;
            case "gd_x_lov_pmg":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_PMG_COUNT_DL;
                break;
            case "gd_x_lov_product_status":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_PROD_STATUS_COUNT_DL;
                break;
            case "gd_x_lov_product_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_PROD_TYPE_COUNT_DL;
                break;
            case "gd_x_lov_relationship_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_RELATION_TYPE_COUNT_DL;
                break;
            case "gd_x_lov_revenue_account":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_REVNUE_COUNT_DL;
                break;
            case "gd_x_lov_revenue_model":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_REVNUE_MODEL_COUNT_DL;
                break;
            case "gd_x_lov_subject_area_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_SUBJ_AREA_COUNT_DL;
                break;
            case "gd_x_lov_subscription_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_SUB_COUNT_DL;
                break;
            case "gd_x_lov_work_hchy_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_WORK_HCHY_COUNT_DL;
                break;
            case "gd_x_lov_work_status":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_WORK_STATUS_COUNT_DL;
                break;
            case "gd_x_lov_work_type":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_WORK_TYPE_COUNT_DL;
                break;
            case "gd_x_lov_workflow_source":
                sqlgdDLCount = gdTableDLSQL.GET_GD_LOV_WORKFOW_SOURCE_COUNT_DL;
                break;

        }
        Log.info(sqlgdDLCount);
        List<Map<String, Object>> gdTableDLCount = DBManager.getDBResultMap(sqlgdDLCount, Constants.AWS_URL);
        gdDLCount = ((Long) gdTableDLCount.get(0).get("Target_Count")).intValue();
    }

    @And("^we compare both the counts are identical for the table (.*)$")
    public void compareGdandDLCounts(String srctable) {
        Log.info("The count for " + srctable + " in SQL => " + gdTablePosgreSQLCount + " and in Data_LAke  => " + gdDLCount);
        Assert.assertEquals("The counts are not equal for " + srctable + " in Posgres and DL ", gdDLCount, gdTablePosgreSQLCount);
    }

    @Given("^Get (.*) random ids of (.*) from the postgreSQL$")
    public void getRandomGdTableIds(String numberOfRecords, String SourceTable) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
            case "gd_product_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_REL_PKG_IDS, numberOfRecords);
                List<Map<?, ?>> randomProdRelPkgsIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProdRelPkgsIds.stream().map(m -> (BigDecimal) m.get("product_rel_pack_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_subject_area":
                sql = String.format(gdTableDLSQL.GET_GD_SUBJ_AREA_IDS, numberOfRecords);
                List<Map<?, ?>> randomSubjAreaIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomSubjAreaIds.stream().map(m -> (BigDecimal) m.get("subject_area_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_ACCESS_MODEL_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkAccessIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkAccessIds.stream().map(m -> (BigDecimal) m.get("work_access_model_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_BUSINESS_MODEL_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkBusinessIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkBusinessIds.stream().map(m -> (BigDecimal) m.get("work_business_model_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_FIN_ATTR_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkFinAttrIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkFinAttrIds.stream().map(m -> (BigDecimal) m.get("work_fin_attribs_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_IDENTIFIER_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkIdentifierIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkIdentifierIds.stream().map(m -> (BigDecimal) m.get("work_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_LEGAL_OWNER_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkLegalOwnerIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkLegalOwnerIds.stream().map(m -> (BigDecimal) m.get("work_legal_owner_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_PERSON_ROLE_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkpersonRoleIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkpersonRoleIds.stream().map(m -> (BigDecimal) m.get("work_person_role_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_REL_PKG_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkRelPkgIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkRelPkgIds.stream().map(m -> (BigDecimal) m.get("work_rel_pack_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_relationship":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_RELATIONS_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkRelationsIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkRelationsIds.stream().map(m -> (BigDecimal) m.get("work_relationship_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_subject_area_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_SUBJ_AREA_LINK_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkSubjAreaLinkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkSubjAreaLinkIds.stream().map(m -> (BigDecimal) m.get("work_subject_area_link_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_work_hchy_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HCHY_LINK_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkHchyLinkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkHchyLinkIds.stream().map(m -> (BigDecimal) m.get("wrk_wrk_hchy_link_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_wwork":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkIds.stream().map(m -> (String) m.get("work_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_hierarchy":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HIER_IDS, numberOfRecords);
                List<Map<?, ?>> randomWorkHieRachyIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkHieRachyIds.stream().map(m -> (BigDecimal) m.get("work_hierarchy_id")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Given("^Get (.*) random codes of (.*) from the Lov")
    public void getRandomLovCodes(String numberOfRecords, String SourceTable) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (SourceTable) {
            case "gd_x_lov_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_ACCESS_IDS, numberOfRecords);
                break;
            case "gd_x_lov_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_BUSINESS_IDS, numberOfRecords);
                break;
            case "gd_x_lov_currency":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_CURRENCY_IDS, numberOfRecords);
                break;
            case "gd_x_lov_etax_product_code":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_ETAX_IDS, numberOfRecords);
                break;
            case "gd_x_lov_event_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_EVENT_IDS, numberOfRecords);
                break;
            case "gd_x_lov_gl_company":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_GL_COMPANY_IDS, numberOfRecords);
                break;
            case "gd_x_lov_gl_prod_seg_parent":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_GL_PROD_SEG_PARENT_IDS, numberOfRecords);
                break;
            case "gd_x_lov_gl_resp_centre":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_GL_RESP_CENTER_IDS, numberOfRecords);
                break;
            case "gd_x_lov_identifier_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_IDENTIFIER_IDS, numberOfRecords);
                break;
            case "gd_x_lov_imprint":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_IMPRINT_IDS, numberOfRecords);
                break;
            case "gd_x_lov_language":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_LANGUAGE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_legal_ownership":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_LEGALOWNER_IDS, numberOfRecords);
                break;
            case "gd_x_lov_manif_status":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_MANIF_STATUS_IDS, numberOfRecords);
                break;
            case "gd_x_lov_manif_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_MANIF_TYPE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_metric_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_METRIC_IDS, numberOfRecords);
                break;
            case "gd_x_lov_origin":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_ORIGIN_IDS, numberOfRecords);
                break;
            case "gd_x_lov_owner_description":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_OWNER_DESCRIPTION_IDS, numberOfRecords);
                break;
            case "gd_x_lov_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_PERSON_ROLE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_pmc":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_PMC_ROLE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_pmg":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_PMG_ROLE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_product_status":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_PROD_STATUS_IDS, numberOfRecords);
                break;
            case "gd_x_lov_product_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_PROD_TYPE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_relationship_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_RELATION_TYPE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_revenue_account":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_REVNUE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_revenue_model":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_REVNUE_MODEL_IDS, numberOfRecords);
                break;
            case "gd_x_lov_subject_area_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_SUBJ_AREA_IDS, numberOfRecords);
                break;
            case "gd_x_lov_subscription_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_SUB_IDS, numberOfRecords);
                break;
            case "gd_x_lov_work_hchy_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_WORK_HCHY_IDS, numberOfRecords);
                break;
            case "gd_x_lov_work_status":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_WORK_STATUS_IDS, numberOfRecords);
                break;
            case "gd_x_lov_work_type":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_WORK_TYPE_IDS, numberOfRecords);
                break;
            case "gd_x_lov_workflow_source":
                sql = String.format(gdTableDLSQL.GET_GD_LOV_WORKFOW_SOURCE_IDS, numberOfRecords);
                break;
        }
        Log.info(sql);
        List<Map<?, ?>> randomCode = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        Ids = randomCode.stream().map(m -> (String) m.get("code")).map(String::valueOf).collect(Collectors.toList());
        Log.info(Ids.toString());
    }

    @When("^We get the records for (.*) from postgreSQL$")
    public void getRecordsGDTableSQL(String SemarchyTable) {
        Log.info("We get the records from Posgres..");
        switch (SemarchyTable) {
            case "gd_accountable_product":
                sql = String.format(gdTableDLSQL.GET_GD_ACCOUNTABLE_PRODUCT, Joiner.on("','").join(Ids));
                break;
            case "gd_event":
                sql = String.format(gdTableDLSQL.GET_GD_EVENT, Joiner.on("','").join(Ids));
                break;
            case "gd_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_LEGAL_OWNER, Joiner.on("','").join(Ids));
                break;
            case "gd_manifestation":
                sql = String.format(gdTableDLSQL.GET_GD_MANIFESTATION, Joiner.on("','").join(Ids));
                break;
            case "gd_manifestation_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_person":
                sql = String.format(gdTableDLSQL.GET_GD_PERSON, Joiner.on("','").join(Ids));
                break;
            case "gd_product":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT, Joiner.on("','").join(Ids));
                break;
            case "gd_product_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_FIN_ATTR, Joiner.on("','").join(Ids));
                break;
            case "gd_product_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_product_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_PERSON_ROLE, Joiner.on("','").join(Ids));
                break;
            case "gd_product_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_REL_PKG, Joiner.on("','").join(Ids));
                break;
            case "gd_subject_area":
                sql = String.format(gdTableDLSQL.GET_GD_SUBJECT_AREA, Joiner.on("','").join(Ids));
                break;
            case "gd_work_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_ACCESS_MODEL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_BUSINESS_MODEL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_FIN_ATTR, Joiner.on("','").join(Ids));
                break;
            case "gd_work_hierarchy":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HIRERACHY, Joiner.on("','").join(Ids));
                break;
            case "gd_work_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_work_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_LEGAL_OWNER, Joiner.on("','").join(Ids));
                break;
            case "gd_work_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_PERSON_ROLE, Joiner.on("','").join(Ids));
                break;
            case "gd_work_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_REL_PKG, Joiner.on("','").join(Ids));
                break;
            case "gd_work_relationship":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_RELATIONSHIP, Joiner.on("','").join(Ids));
                break;
            case "gd_work_subject_area_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_SUB_AREA_LINK, Joiner.on("','").join(Ids));
                break;
            case "gd_work_work_hchy_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HCHU_LINK, Joiner.on("','").join(Ids));
                break;
            case "gd_wwork":
                sql = String.format(gdTableDLSQL.GET_GD_WWORK, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ACCESS_MODEL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_BUSINESS_MODEL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_currency":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_CURRENCY, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_etax_product_code":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ETAX_PROD_CODE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_event_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_EVENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_company":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_COMPANY, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_prod_seg_parent":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_PROD_SEG_PARENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_resp_centre":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_RESP_CENTER, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_identifier_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_imprint":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_IMPRINT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_language":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_LANGUAGE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_legal_ownership":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_LEGAL_OWNERSHIP, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_manif_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_MANIF_STATUS, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_manif_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_MANIF_TYPE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_metric_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_METRIC_TYPE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_origin":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ORIGIN, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_owner_description":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_OWNER_DESCRIP, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PERSON_ROLE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_pmc":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PMC, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_pmg":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PMG_ROLE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_product_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PROD_STATUS, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_product_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PROD_TYPE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_relationship_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_RELATION_TYPE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_revenue_account":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_REVENUE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_revenue_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_REVENUE_MODEL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_subject_area_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_SUBJ_AREA, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_subscription_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_SUB, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_hchy_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_HCHY, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_STATUS, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_TYPE, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_workflow_source":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORKFOW_SOURCE, Joiner.on("','").join(Ids));
                break;

        }
        Log.info(sql);
        GDTablesDLSQLContext.recordsFromSql = DBManager.getDBResultAsBeanList(sql, GDTableDLSQLObject.class, Constants.EPH_URL);
    }

    @When("^Get the records for (.*) from the DataLake$")
    public void getRecordsGDTableDL(String SemarchyTable) {
        Log.info("We get the records from DL..");
        switch (SemarchyTable) {
            case "gd_accountable_product":
                sql = String.format(gdTableDLSQL.GET_GD_ACCOUNTABLE_PRODUCT_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_event":
                sql = String.format(gdTableDLSQL.GET_GD_EVENT_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_LEGAL_OWNER_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_manifestation":
                sql = String.format(gdTableDLSQL.GET_GD_MANIFESTATION_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_manifestation_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_MANIFESTATION_IDENTIFIER_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_person":
                sql = String.format(gdTableDLSQL.GET_GD_PERSON_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_product":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_product_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_FIN_ATTR_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_product_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_IDENTIFIER_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_product_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_PERSON_ROLE_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_product_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_REL_PKG_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_subject_area":
                sql = String.format(gdTableDLSQL.GET_GD_SUBJECT_AREA_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_ACCESS_MODEL_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_BUSINESS_MODEL_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_FIN_ATTR_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_hierarchy":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HIRERACHY_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_IDENTIFIER_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_LEGAL_OWNER_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_PERSON_ROLE_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_REL_PKG_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_relationship":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_RELATIONSHIP_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_subject_area_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_SUB_AREA_LINK_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_work_work_hchy_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HCHU_LINK_DL, Joiner.on(",").join(Ids));
                break;
            case "gd_wwork":
                sql = String.format(gdTableDLSQL.GET_GD_WWORK_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ACCESS_MODEL_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_BUSINESS_MODEL_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_currency":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_CURRENCY_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_etax_product_code":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ETAX_PROD_CODE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_event_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_EVENT_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_company":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_COMPANY_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_prod_seg_parent":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_PROD_SEG_PARENT_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_resp_centre":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_RESP_CENTER_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_identifier_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_IDENTIFIER_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_imprint":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_IMPRINT_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_language":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_LANGUAGE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_legal_ownership":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_LEGAL_OWNERSHIP_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_manif_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_MANIF_STATUS_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_manif_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_MANIF_TYPE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_metric_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_METRIC_TYPE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_origin":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ORIGIN_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_owner_description":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_OWNER_DESCRIP_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PERSON_ROLE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_pmc":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PMC_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_pmg":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PMG_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_product_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PROD_STATUS_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_product_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PROD_TYPE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_relationship_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_RELATION_TYPE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_revenue_account":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_REVENUE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_revenue_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_REVENUE_MODEL_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_subject_area_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_SUBJ_AREA_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_subscription_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_SUB_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_hchy_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_HCHY_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_STATUS_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_TYPE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_workflow_source":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORKFOW_SOURCE_DL, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        GDTablesDLSQLContext.recordsFromDL = DBManager.getDBResultAsBeanList(sql, GDTableDLSQLObject.class, Constants.AWS_URL);
    }

    @And("^Compare the records of (.*) from postgreSQL and DataLake$")
    public void compareSemarchyAndDL(String SourceTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.recordsFromSql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare Records in Semarchy and DL ..");
            for (int i = 0; i < GDTablesDLSQLContext.recordsFromSql.size(); i++) {
                switch (SourceTable) {
                    case "gd_accountable_product":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getaccountable_product_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getaccountable_product_id));
                        String[] accProd = {"getaccountable_product_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getgl_product_segment_code", "getgl_product_segment_name", "getf_gl_product_segment_parent", "getf_event"};
                        for (String strTemp : accProd) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("Acc_Prod_Id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getaccountable_product_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getaccountable_product_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("Acc_Prod_Id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getaccountable_product_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getaccountable_product_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_event":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getevent_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getevent_id));
                        String[] event = {"getevent_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getddate", "getttimestamp", "getdescription"};
                        for (String strTemp : event) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("Event_Id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getevent_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getevent_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("Event_Id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getevent_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getevent_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_legal_owner":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getlegal_owner_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getlegal_owner_id));
                        String[] legal_owner = {"getlegal_owner_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getname", "gets_name", "getf_legal_ownership"};
                        for (String strTemp : legal_owner) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("Legal_ownership_Id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getlegal_owner_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getlegal_owner_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("Legal_ownership_Id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getlegal_owner_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getlegal_owner_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_manifestation":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getmanifestation_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getmanifestation_id));
                        String[] manif = {"getmanifestation_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "gets_manifestation_id", "getexternal_reference", "getmanifestation_key_title", "gets_manifestation_key_title", "getinter_edition_flag",
                                "getfirst_pub_date", "getlast_pub_date", "gett_event_description", "getf_type", "getf_status", "getf_format_type", "getf_wwork", "getf_t_event_type", "getf_event", "getf_self_one"};
                        for (String strTemp : manif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getinter_edition_flag") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id(),
                                            flagVal1, flagVal2);


                                }
                            } else {
                                Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_manifestation_identifier":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getmanif_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getmanif_identifier_id));
                        String[] manifIdentif = {"getmanif_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getidentifier", "gets_identifier", "geteffective_start_date", "geteffective_end_date",
                                "getf_type", "getf_manifestation", "getf_event", "getf_type", "getlead_indicator"};
                        for (String strTemp : manifIdentif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getlead_indicator") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id(),
                                            flagVal1, flagVal2);

                                }


                            } else {
                                Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_person":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getperson_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getperson_id));
                        String[] person = {"getperson_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getgiven_name", "gets_given_name", "getfamily_name", "gets_family_name",
                                "getpeoplehub_id", "getemail", "gets_email","geteffective_start_date","geteffective_end_date"};
                        for (String strTemp : person) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("person_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getperson_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getperson_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("person_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getperson_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getperson_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_id));
                        String[] product = {"getproduct_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getname", "gets_name", "getshort_name", "gets_short_name",
                                "getseparately_sale_indicator", "gettrial_allowed_indicator", "getrestricted_sale_indicator", "getlaunch_date", "getcontent_from_date",
                                "getcontent_to_date", "getcontent_date_offset", "gett_summary_changed", "gett_event_description", "getf_type", "getf_status", "getf_status", "getf_accountable_product"
                                , "getf_tax_code", "getf_revenue_model", "getf_revenue_account", "getf_wwork", "getf_manifestation", "getf_t_event_type", "getf_event", "getf_self_one", "getf_self_two", "getf_self_three", "getf_self_four", "getf_self_five", "getf_self_six", "getf_self_seven"};
                        for (String strTemp : product) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prodId => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getseparately_sale_indicator" || method.getName() == "gettrial_allowed_indicator" || method.getName() == "getrestricted_sale_indicator" ||
                                    method.getName() == "gett_summary_changed") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("prodId => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id(),
                                            flagVal1, flagVal2);

                                }


                            } else {
                                Log.info("prodId => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_financial_attribs":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_fin_attribs_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_fin_attribs_id));
                        String[] finAttr = {"getproduct_fin_attribs_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getf_gl_company", "getf_gl_cost_resp_centre", "getf_gl_revenue_resp_centre", "getf_event", "getf_product", "geteffective_start_date", "geteffective_end_date"};
                        for (String strTemp : finAttr) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_fin_att_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_fin_attribs_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_fin_attribs_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_fin_att_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_fin_attribs_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_fin_attribs_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_identifier":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_identifier_id));
                        String[] prodIdentifId = {"getproduct_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getidentifier", "gets_identifier", "getf_event", "getf_product", "geteffective_start_date", "geteffective_end_date", "getf_type"};
                        for (String strTemp : prodIdentifId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_identifier_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_identifier_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_identifier_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_person_role":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_person_role_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_person_role_id));
                        String[] prodPersRoleId = {"getproduct_person_role_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getf_event", "getf_product", "geteffective_start_date", "geteffective_end_date", "getf_role", "getf_person"};
                        for (String strTemp : prodPersRoleId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_pers_role_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_person_role_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_person_role_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_pers_role_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_person_role_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_person_role_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_rel_package":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_rel_pack_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_rel_pack_id));
                        String[] prodRelPackID = {"getproduct_rel_pack_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getallocation", "getf_package_owner", "geteffective_start_date", "geteffective_end_date", "getf_component", "getf_relationship_type", "getf_event"};
                        for (String strTemp : prodRelPackID) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_rel_pack_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_rel_pack_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_rel_pack_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_rel_pack_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_rel_pack_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_rel_pack_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_subject_area":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getsubject_area_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getsubject_area_id));
                        String[] subjAreaId = {"getsubject_area_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getcode", "getname", "getf_type", "getf_parent_subject_area"};
                        for (String strTemp : subjAreaId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("subj_area_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getsubject_area_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getsubject_area_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("subj_area_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getsubject_area_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getsubject_area_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_access_model":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_access_model_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_access_model_id));
                        String[] workAccess = {"getwork_access_model_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getf_access_model", "getf_wwork"};
                        for (String strTemp : workAccess) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_access_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_access_model_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_access_model_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_access_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_access_model_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_access_model_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_business_model":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_business_model_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_business_model_id));
                        String[] workBusinessId = {"getwork_business_model_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getf_business_model", "getf_wwork"};
                        for (String strTemp : workBusinessId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_businees_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_business_model_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_business_model_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_businees_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_business_model_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_business_model_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_financial_attribs":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_fin_attribs_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_fin_attribs_id));
                        String[] workFinAttrId = {"getwork_fin_attribs_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_gl_company", "getf_gl_cost_resp_centre", "getf_gl_revenue_resp_centre", "getf_wwork", "getf_event"};
                        for (String strTemp : workFinAttrId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_fin_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_fin_attribs_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_fin_attribs_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_fin_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_fin_attribs_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_fin_attribs_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_hierarchy":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_hierarchy_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_hierarchy_id));
                        String[] workHirerchy = {"getwork_hierarchy_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "gethierarchy_level", "getcode", "getname", "getf_type", "getf_parent_work_hierarchy"};
                        for (String strTemp : workHirerchy) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_Hchy_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_hierarchy_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_hierarchy_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_Hchy_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_hierarchy_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_hierarchy_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_identifier":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_identifier_id));
                        String[] workIdentifier = {"getwork_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getidentifier", "gets_identifier", "getf_wwork", "getf_type", "getf_event"};
                        for (String strTemp : workIdentifier) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_Identif_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_identifier_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_Identif_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_legal_owner":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_legal_owner_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_legal_owner_id));
                        String[] workLegalOwner = {"getwork_legal_owner_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_wwork", "getf_legal_owner", "getf_ownership_description", "getf_event"};
                        for (String strTemp : workLegalOwner) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_legal_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_legal_owner_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_legal_owner_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_legal_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_legal_owner_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_legal_owner_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_person_role":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_person_role_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_person_role_id));
                        String[] workPersonRole = {"getwork_person_role_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_wwork", "getf_role", "getf_person", "getf_event"};
                        for (String strTemp : workPersonRole) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_pers_role_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_person_role_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_person_role_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_pers_role_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_person_role_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_person_role_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_rel_package":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_rel_pack_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_rel_pack_id));
                        String[] workRelPkg = {"getwork_rel_pack_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getallocation", "geteffective_start_date", "geteffective_end_date", "getf_package_owner", "getf_component", "getf_relationship_type", "getf_event"};
                        for (String strTemp : workRelPkg) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_rel_pkg_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_rel_pack_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_rel_pack_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_rel_pkg_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_rel_pack_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_rel_pack_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_relationship":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_relationship_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_relationship_id));
                        String[] workRel = {"getwork_relationship_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_child", "getf_parent", "getf_relationship_type", "getf_event"};
                        for (String strTemp : workRel) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_rel_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_relationship_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_relationship_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_rel_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_relationship_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_relationship_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_subject_area_link":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_subject_area_link_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_subject_area_link_id));
                        String[] workSubjAreaLink = {"getwork_subject_area_link_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getpprimary", "getf_subject_area", "getf_wwork"};
                        for (String strTemp : workSubjAreaLink) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_subj_area_link_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_subject_area_link_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_subject_area_link_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getpprimary") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("work_subj_area_link_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_subject_area_link_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_subject_area_link_id(),
                                            flagVal1, flagVal2);
                                }
                            } else {
                                Log.info("work_subj_area_link_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_subject_area_link_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_subject_area_link_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_work_hchy_link":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwrk_wrk_hchy_link_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwrk_wrk_hchy_link_id));
                        String[] workhchylink = {"getwrk_wrk_hchy_link_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_work_hierarchy", "getf_wwork", "getf_event"};
                        for (String strTemp : workhchylink) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("wrk_hchy_link_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwrk_wrk_hchy_link_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwrk_wrk_hchy_link_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("wrk_hchy_link_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwrk_wrk_hchy_link_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwrk_wrk_hchy_link_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_wwork":
                        GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getwork_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_id));
                        String[] work = {"getwork_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "gets_work_id", "getexternal_reference", "getwork_title", "gets_work_title", "getwork_sub_title",
                                "gets_work_sub_title", "getwork_short_title", "gets_work_short_title", "getelectro_rights_indicator", "getvolume_name", "getcopyright_year", "getedition_number", "getplanned_launch_date", "getactual_launch_date", "getplanned_discontinue_date",
                                "getactual_discontinue_date", "gett_summary_changed", "gett_event_description", "getf_type", "getf_status", "getf_accountable_product", "getf_pmc", "getf_family", "getf_imprint",
                                "getf_legal_ownership", "getf_subscription_type", "getf_llanguage", "getf_t_event_type", "getf_event", "getf_self_one", "getf_self_two", "getf_self_three", "getf_self_four", "getf_self_five", "getf_self_six", "getf_self_seven", "getf_self_eight", "getf_self_nine", "getf_self_ten"};
                        for (String strTemp : work) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "gett_summary_changed" || method.getName() == "getelectro_rights_indicator") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("work_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id(),
                                            flagVal1, flagVal2);
                                }
                            } else {
                                Log.info("work_id => " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getwork_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                }
            }
        }
    }
    @And("^we Compare the records of Lov table (.*) from postgreSQL and DataLake$")
    public void compareLovTables(String SourceTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.recordsFromSql.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare Records in Semarchy and DL ..");
            for (int i = 0; i < GDTablesDLSQLContext.recordsFromSql.size(); i++) {
                String[] accessModel;
                GDTablesDLSQLContext.recordsFromSql.sort(Comparator.comparing(GDTableDLSQLObject::getcode)); //sort data in the lists
                GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getcode));
                if (SourceTable.equalsIgnoreCase("gd_x_lov_legal_ownership")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getl_description", "getl_start_date", "getl_end_date", "getroll_up_ownership"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_identifier_type")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_at_work", "getvalid_at_manifestation", "getvalid_at_product", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_event_type")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getlevel_2_event", "getlevel_3_event", "getl_description", "getl_start_date", "getl_end_date"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_imprint")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_manif_status")||SourceTable.equalsIgnoreCase("gd_x_lov_work_status")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date","getroll_up_status"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_manif_type")||SourceTable.equalsIgnoreCase("gd_x_lov_work_type")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date", "getroll_up_type"};
                }else if (SourceTable.equalsIgnoreCase("gd_x_lov_product_status")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date", "getvalid_for_digital_package", "getroll_up_status"};
                }else if (SourceTable.equalsIgnoreCase("gd_x_lov_relationship_type")) {
                        accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getparent_description", "getchild_description", "getl_description", "getl_start_date", "getl_end_date"};
                } else {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getl_description", "getl_start_date", "getl_end_date"};
                }
                for (String strTemp : accessModel) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                    GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getl_start_date" || method.getName() == "getl_end_date") {
                        try {
                            String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                            String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                            Log.info("code => " + GDTablesDLSQLContext.recordsFromSql.get(i).getcode() +
                                    " " + strTemp + " => Semarchy=" + val1 +
                                    " " + strTemp + " DL=" + val2);
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getcode(),
                                        val1, val2);
                            }
                        } catch (NullPointerException e) {
                            Log.info("Null Values for " + method.getName());
                        }
                    } else if (method.getName() == "getvalid_at_work" || method.getName() == "getvalid_at_manifestation" ||
                            method.getName() == "getvalid_at_product" || method.getName() == "getvalid_for_books" ||
                            method.getName() == "getvalid_for_journals" || method.getName()=="getvalid_for_digital_package") {
                        if (method.invoke(objectToCompare1) != null) {
                            String flagVal1 = method.invoke(objectToCompare1).toString();
                            if (flagVal1.equalsIgnoreCase("f")) {
                                flagVal1 = "0";
                            } else {
                                flagVal1 = "1";
                            }
                            String flagVal2 = method.invoke(objectToCompare2).toString();
                            Log.info("code => " + GDTablesDLSQLContext.recordsFromSql.get(i).getcode() +
                                    " " + strTemp + " => Semarchy=" + flagVal1 +
                                    " " + strTemp + " DL=" + flagVal2);
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                            " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getcode(),
                                    flagVal1, flagVal2);
                        }
                    } else {
                        Log.info("code => " + GDTablesDLSQLContext.recordsFromSql.get(i).getcode() +
                                " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                            " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getcode(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                }
            }
        }
    }


    @Given("^We get the count (.*) from presentation")
    public void getTheCountfromPresentation(String table) {
        switch (table) {
            case "gd_accountable_product":
                sqlGdPresentationCounts = gdTableDLSQL.GET_ACC_PROD_GD_PRESENT_COUNT;
                break;
            case "gd_event":
                sqlGdPresentationCounts = gdTableDLSQL.GET_EVENT_GD_PRESENT_COUNT;
                break;
            case "gd_legal_owner":
                sqlGdPresentationCounts = gdTableDLSQL.GET_LEGAL_OWNER_GD_PRESENT_COUNT;
                break;
            case "gd_manifestation":
                sqlGdPresentationCounts = gdTableDLSQL.GET_MANIF_GD_PRESENT_COUNT;
                break;
            case "gd_manifestation_identifier":
                sqlGdPresentationCounts = gdTableDLSQL.GET_MANIF_ID_GD_PRESENT_COUNT;
                break;
            case "gd_person":
                sqlGdPresentationCounts = gdTableDLSQL.GET_PERSON_GD_PRESENT_COUNT;
                break;
            case "gd_price":
                sqlGdPresentationCounts = gdTableDLSQL.GET_PRICE_GD_PRESENT_COUNT;
                break;
            case "gd_product":
                sqlGdPresentationCounts = gdTableDLSQL.GET_PROD_GD_PRESENT_COUNT;
                break;
            case "gd_product_financial_attribs":
                sqlGdPresentationCounts = gdTableDLSQL.GET_PROD_FIN_ATTR_GD_PRESENT_COUNT;
                break;
            case "gd_product_identifier":
                sqlGdPresentationCounts = gdTableDLSQL.GET_PROD_IDENTIF_GD_PRESENT_COUNT;
                break;
            case "gd_product_person_role":
                sqlGdPresentationCounts = gdTableDLSQL.GET_PROD_PERSON_ROLE_GD_PRESENT_COUNT;
                break;
            case "gd_product_rel_package":
                sqlGdPresentationCounts = gdTableDLSQL.GET_PROD_REL_PKG_GD_PRESENT_COUNT;
                break;
            case "gd_subject_area":
                sqlGdPresentationCounts = gdTableDLSQL.GET_SUB_AREA_GD_PRESENT_COUNT;
                break;
            case "gd_work_access_model":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_ACCESS_GD_PRESENT_COUNT;
                break;
            case "gd_work_business_model":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_BUSINESS_GD_PRESENT_COUNT;
                break;
            case "gd_work_financial_attribs":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_FIN_ATTR_GD_PRESENT_COUNT;
                break;
            case "gd_work_hierarchy":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_HERCHY_GD_PRESENT_COUNT;
                break;
            case "gd_work_identifier":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_IDENTF_GD_PRESENT_COUNT;
                break;
            case "gd_work_legal_owner":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_LEGAL_OWN_GD_PRESENT_COUNT;
                break;
            case "gd_work_metric":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_METRIC_GD_PRESENT_COUNT;
                break;
            case "gd_work_person_role":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_PERS_ROLE_GD_PRESENT_COUNT;
                break;
            case "gd_work_rel_package":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_REL_PKG_GD_PRESENT_COUNT;
                break;
            case "gd_work_relationship":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_REL_GD_PRESENT_COUNT;
                break;
            case "gd_work_subject_area_link":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_SUBJ_AREA_LINK_GD_PRESENT_COUNT;
                break;
            case "gd_work_work_hchy_link":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_WORK_HCHY_GD_PRESENT_COUNT;
                break;
            case "gd_wwork":
                sqlGdPresentationCounts = gdTableDLSQL.GET_WORK_PRESENT_PRESENT_COUNT;
                break;
            case "gd_x_lov_access_model":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_ACCESS_MODEL_PRESENT_COUNT;
                break;
            case "gd_x_lov_business_model":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_BUSINESS_MODEL_PRESENT_COUNT;
                break;
            case "gd_x_lov_currency":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_CURRENCY_PRESENT_COUNT;
                break;
            case "gd_x_lov_etax_product_code":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_ETAX_PROD_CODE_PRESENT_COUNT;
                break;
            case "gd_x_lov_event_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_EVENT_PRESENT_COUNT;
                break;
            case "gd_x_lov_gl_company":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_GL_COMPANY_PRESENT_COUNT;
                break;
            case "gd_x_lov_gl_prod_seg_parent":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_GL_PROD_SEG_PARENT_PRESENT_COUNT;
                break;
            case "gd_x_lov_gl_resp_centre":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_GL_RESP_CENTER_PRESENT_COUNT;
                break;
            case "gd_x_lov_identifier_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_IDENTIFIER_PRESENT_COUNT;
                break;
            case "gd_x_lov_imprint":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_IMPRINT_PRESENT_COUNT;
                break;
            case "gd_x_lov_language":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_LANGUAGE_PRESENT_COUNT;
                break;
            case "gd_x_lov_legal_ownership":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_LEGAL_OWNERSHIP_PRESENT_COUNT;
                break;
            case "gd_x_lov_manif_status":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_MANIF_STATUS_PRESENT_COUNT;
                break;
            case "gd_x_lov_manif_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_MANIF_TYPE_PRESENT_COUNT;
                break;
            case "gd_x_lov_metric_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_METRIC_TYPE_PRESENT_COUNT;
                break;
            case "gd_x_lov_origin":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_ORIGIN_PRESENT_COUNT;
                break;
            case "gd_x_lov_owner_description":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_OWNER_DESCRIP_PRESENT_COUNT;
                break;
            case "gd_x_lov_person_role":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_PERSON_ROLE_PRESENT_COUNT;
                break;
            case "gd_x_lov_pmc":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_PMC_PRESENT_COUNT;
                break;
            case "gd_x_lov_pmg":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_PMG_PRESENT_COUNT;
                break;
            case "gd_x_lov_product_status":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_PROD_STATUS_PRESENT_COUNT;
                break;
            case "gd_x_lov_product_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_PROD_TYPE_PRESENT_COUNT;
                break;
            case "gd_x_lov_relationship_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_RELATION_TYPE_PRESENT_COUNT;
                break;
            case "gd_x_lov_revenue_account":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_REVENUE_PRESENT_COUNT;
                break;
            case "gd_x_lov_revenue_model":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_REVENUE_MODEL_PRESENT_COUNT;
                break;
            case "gd_x_lov_subject_area_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_SUBJ_AREA_PRESENT_COUNT;
                break;
            case "gd_x_lov_subscription_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_SUB_PRESENT_COUNT;
                break;
            case "gd_x_lov_work_hchy_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_WORK_HCHY_PRESENT_COUNT;
                break;
            case "gd_x_lov_work_status":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_WORK_STATUS_PRESENT_COUNT;
                break;
            case "gd_x_lov_work_type":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_WORK_TYPE_PRESENT_COUNT;
                break;
            case "gd_x_lov_workflow_source":
                sqlGdPresentationCounts = gdTableDLSQL.GET_GD_LOV_WORKFOW_SOURCE_PRESENT_COUNT;
                break;
        }
        Log.info(sqlGdPresentationCounts);
        List<Map<String, Object>> gdPresentationSqlCount = DBManager.getDBResultMap(sqlGdPresentationCounts, Constants.AWS_URL);
        gdTablePresentationCount = ((Long) gdPresentationSqlCount.get(0).get("Target_Count")).intValue();
    }


    @And("^we compare presentation and prod gd table count (.*)$")
    public void compareGdandPresentCounts(String srctable) {
        Log.info("The count for " + srctable + " in Presentation => " + gdTablePresentationCount + " and in Data_LAke  => " + gdDLCount);
        Assert.assertEquals("The counts are not equal for " + srctable + " in Presentation and DL ", gdDLCount, gdTablePresentationCount);
    }


    @Given("^Get (.*) random ids of (.*) for Datalake")
    public void getRandomIdsFromDL(String numberOfRecords, String SourceTable) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (SourceTable) {
            case "gd_accountable_product":
                sql = String.format(gdTableDLSQL.GET_GD_ACC_PROD_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomAccProdIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomAccProdIds.stream().map(m -> (String) m.get("external_reference")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_event":
                sql = String.format(gdTableDLSQL.GET_GD_EVENT_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomEventIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomEventIds.stream().map(m -> (BigDecimal) m.get("event_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_LEGAL_OWNER_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomLegalOwnerIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomLegalOwnerIds.stream().map(m -> (BigDecimal) m.get("legal_owner_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_manifestation":
                sql = String.format(gdTableDLSQL.GET_GD_MANIF_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomManifIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifIds.stream().map(m -> (String) m.get("manifestation_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_manifestation_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_MANIF_IDENTIF_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomManifIdentifierIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifIdentifierIds.stream().map(m -> (BigDecimal) m.get("manif_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_person":
                sql = String.format(gdTableDLSQL.GET_GD_PERSON_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomPersonIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPersonIds.stream().map(m -> (BigDecimal) m.get("person_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomPriceIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPriceIds.stream().map(m -> (String) m.get("product_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_FIN_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomProdFinIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomProdFinIds.stream().map(m -> (BigDecimal) m.get("product_fin_attribs_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_IDENTIF_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomProdIdentifIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomProdIdentifIds.stream().map(m -> (BigDecimal) m.get("product_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_PERSON_ROLE_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomProdPersRoleIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomProdPersRoleIds.stream().map(m -> (BigDecimal) m.get("product_person_role_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_REL_PKG_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomProdRelPkgsIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomProdRelPkgsIds.stream().map(m -> (BigDecimal) m.get("product_rel_pack_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_subject_area":
                sql = String.format(gdTableDLSQL.GET_GD_SUBJ_AREA_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomSubjAreaIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomSubjAreaIds.stream().map(m -> (BigDecimal) m.get("subject_area_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_ACCESS_MODEL_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkAccessIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkAccessIds.stream().map(m -> (BigDecimal) m.get("work_access_model_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_BUSINESS_MODEL_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkBusinessIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkBusinessIds.stream().map(m -> (BigDecimal) m.get("work_business_model_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_FIN_ATTR_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkFinAttrIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkFinAttrIds.stream().map(m -> (BigDecimal) m.get("work_fin_attribs_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_IDENTIFIER_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkIdentifierIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkIdentifierIds.stream().map(m -> (BigDecimal) m.get("work_identifier_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_LEGAL_OWNER_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkLegalOwnerIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkLegalOwnerIds.stream().map(m -> (BigDecimal) m.get("work_legal_owner_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_PERSON_ROLE_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkpersonRoleIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkpersonRoleIds.stream().map(m -> (BigDecimal) m.get("work_person_role_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_REL_PKG_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkRelPkgIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkRelPkgIds.stream().map(m -> (BigDecimal) m.get("work_rel_pack_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_relationship":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_RELATIONS_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkRelationsIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkRelationsIds.stream().map(m -> (BigDecimal) m.get("work_relationship_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_subject_area_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_SUBJ_AREA_LINK_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkSubjAreaLinkIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkSubjAreaLinkIds.stream().map(m -> (BigDecimal) m.get("work_subject_area_link_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_work_hchy_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HCHY_LINK_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkHchyLinkIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkHchyLinkIds.stream().map(m -> (BigDecimal) m.get("wrk_wrk_hchy_link_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_wwork":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkIds.stream().map(m -> (String) m.get("work_id")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_hierarchy":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HIER_IDS_DL, numberOfRecords);
                List<Map<?, ?>> randomWorkHieRachyIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkHieRachyIds.stream().map(m -> (BigDecimal) m.get("work_hierarchy_id")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }


    @When("^We get the records for (.*) for presentation$")
    public void getRecsPresentation(String SemarchyTable) {
        Log.info("We get the records from Posgres..");
        switch (SemarchyTable) {
            case "gd_accountable_product":
                sql = String.format(gdTableDLSQL.GET_GD_ACCOUNTABLE_PRODUCT_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_event":
                sql = String.format(gdTableDLSQL.GET_GD_EVENT_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_LEGAL_OWNER_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_manifestation":
                sql = String.format(gdTableDLSQL.GET_GD_MANIFESTATION_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_manifestation_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_MANIFESTATION_IDENTIFIER_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_person":
                sql = String.format(gdTableDLSQL.GET_GD_PERSON_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_product":
                sql = String.format(gdTableDLSQL.GET_GD_PRODUCT_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_product_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_FIN_ATTR_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_product_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_IDENTIFIER_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_product_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_PERSON_ROLE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_product_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_REL_PKG_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_subject_area":
                sql = String.format(gdTableDLSQL.GET_GD_SUBJECT_AREA_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_ACCESS_MODEL_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_BUSINESS_MODEL_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_FIN_ATTR_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_hierarchy":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HIRERACHY_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_IDENTIFIER_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_LEGAL_OWNER_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_PERSON_ROLE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_REL_PKG_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_relationship":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_RELATIONSHIP_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_subject_area_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_SUB_AREA_LINK_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_work_hchy_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HCHU_LINK_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_wwork":
                sql = String.format(gdTableDLSQL.GET_GD_WWORK_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ACCESS_MODEL_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_BUSINESS_MODEL_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_currency":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_CURRENCY_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_etax_product_code":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ETAX_PROD_CODE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_event_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_EVENT_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_company":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_COMPANY_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_prod_seg_parent":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_PROD_SEG_PARENT_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_gl_resp_centre":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_GL_RESP_CENTER_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_identifier_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_IDENTIFIER_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_imprint":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_IMPRINT_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_language":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_LANGUAGE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_legal_ownership":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_LEGAL_OWNERSHIP_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_manif_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_MANIF_STATUS_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_manif_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_MANIF_TYPE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_metric_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_METRIC_TYPE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_origin":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_ORIGIN_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_owner_description":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_OWNER_DESCRIP_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PERSON_ROLE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_pmc":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PMC_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_pmg":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PMG_ROLE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_product_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PROD_STATUS_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_product_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_PROD_TYPE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_relationship_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_RELATION_TYPE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_revenue_account":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_REVENUE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_revenue_model":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_REVENUE_MODEL_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_subject_area_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_SUBJ_AREA_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_subscription_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_SUB_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_hchy_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_HCHY_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_status":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_STATUS_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_work_type":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORK_TYPE_PRESENT, Joiner.on("','").join(Ids));
                break;
            case "gd_x_lov_workflow_source":
                sql = String.format(gdTableDLSQL.GET_GD_GD_LOV_WORKFOW_SOURCE_PRESENT, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        GDTablesDLSQLContext.recordsFromPresentation = DBManager.getDBResultAsBeanList(sql, GDTableDLSQLObject.class, Constants.AWS_URL);
    }

    @And("^we Compare the records of Lov table (.*) for presentation and prodDb$")
    public void compareLovPresentAndProdTables(String SourceTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.recordsFromPresentation.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare Records in Semarchy and DL ..");
            for (int i = 0; i < GDTablesDLSQLContext.recordsFromPresentation.size(); i++) {
                String[] accessModel;
                GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getcode)); //sort data in the lists
                GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getcode));
                if (SourceTable.equalsIgnoreCase("gd_x_lov_legal_ownership")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getl_description", "getl_start_date", "getl_end_date", "getroll_up_ownership"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_identifier_type")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_at_work", "getvalid_at_manifestation", "getvalid_at_product", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_event_type")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getlevel_2_event", "getlevel_3_event", "getl_description", "getl_start_date", "getl_end_date"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_imprint")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_manif_status")||SourceTable.equalsIgnoreCase("gd_x_lov_work_status")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date","getroll_up_status"};
                } else if (SourceTable.equalsIgnoreCase("gd_x_lov_manif_type")||SourceTable.equalsIgnoreCase("gd_x_lov_work_type")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date", "getroll_up_type"};
                }else if (SourceTable.equalsIgnoreCase("gd_x_lov_product_status")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getvalid_for_books", "getvalid_for_journals", "getl_description", "getl_start_date", "getl_end_date", "getvalid_for_digital_package", "getroll_up_status"};
                }else if (SourceTable.equalsIgnoreCase("gd_x_lov_relationship_type")) {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getparent_description", "getchild_description", "getl_description", "getl_start_date", "getl_end_date"};
                } else {
                    accessModel = new String[]{"getcode", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                            , "getb_updator", "getl_description", "getl_start_date", "getl_end_date"};
                }
                for (String strTemp : accessModel) {
                    java.lang.reflect.Method method;
                    java.lang.reflect.Method method2;
                    GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                    GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                    method = objectToCompare1.getClass().getMethod(strTemp);
                    method2 = objectToCompare2.getClass().getMethod(strTemp);

                    if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getl_start_date" || method.getName() == "getl_end_date") {
                        try {
                            String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                            String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                            Log.info("code => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getcode() +
                                    " " + strTemp + " => Semarchy=" + val1 +
                                    " " + strTemp + " DL=" + val2);
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getcode(),
                                        val1, val2);
                            }
                        } catch (NullPointerException e) {
                            Log.info("Null Values for " + method.getName());
                        }
                    } else if (method.getName() == "getvalid_at_work" || method.getName() == "getvalid_at_manifestation" ||
                            method.getName() == "getvalid_at_product" || method.getName() == "getvalid_for_books" ||
                            method.getName() == "getvalid_for_journals" || method.getName()=="getvalid_for_digital_package") {
                        if (method.invoke(objectToCompare1) != null) {
                            String flagVal1 = method.invoke(objectToCompare1).toString();
                            if (flagVal1.equalsIgnoreCase("f")) {
                                flagVal1 = "0";
                            } else {
                                flagVal1 = "1";
                            }
                            String flagVal2 = method.invoke(objectToCompare2).toString();
                            Log.info("code => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getcode() +
                                    " " + strTemp + " => Semarchy=" + flagVal1 +
                                    " " + strTemp + " DL=" + flagVal2);
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                            " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getcode(),
                                    flagVal1, flagVal2);
                        }
                    } else {
                        Log.info("code => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getcode() +
                                " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                            " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getcode(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                }
            }
        }
    }


    @And("^Compare the records of (.*) for presentation and prodDb$")
    public void comparePresentAndDL(String SourceTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (GDTablesDLSQLContext.recordsFromPresentation.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare Records in Semarchy and DL ..");
            for (int i = 0; i < GDTablesDLSQLContext.recordsFromPresentation.size(); i++) {
                switch (SourceTable) {
                    case "gd_accountable_product":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getaccountable_product_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getaccountable_product_id));
                        String[] accProd = {"getaccountable_product_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getgl_product_segment_code", "getgl_product_segment_name", "getf_gl_product_segment_parent", "getf_event"};
                        for (String strTemp : accProd) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("Acc_Prod_Id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getaccountable_product_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getaccountable_product_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("Acc_Prod_Id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getaccountable_product_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getaccountable_product_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_event":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getevent_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getevent_id));
                        String[] event = {"getevent_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getddate", "getttimestamp", "getdescription"};
                        for (String strTemp : event) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("Event_Id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getevent_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getevent_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("Event_Id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getevent_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getevent_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_legal_owner":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getlegal_owner_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getlegal_owner_id));
                        String[] legal_owner = {"getlegal_owner_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getname", "gets_name", "getf_legal_ownership"};
                        for (String strTemp : legal_owner) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("Legal_ownership_Id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getlegal_owner_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getlegal_owner_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("Legal_ownership_Id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getlegal_owner_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getlegal_owner_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_manifestation":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getmanifestation_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getmanifestation_id));
                        String[] manif = {"getmanifestation_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "gets_manifestation_id", "getexternal_reference", "getmanifestation_key_title", "gets_manifestation_key_title", "getinter_edition_flag",
                                "getfirst_pub_date", "getlast_pub_date", "gett_event_description", "getf_type", "getf_status", "getf_format_type", "getf_wwork", "getf_t_event_type", "getf_event", "getf_self_one"};
                        for (String strTemp : manif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanifestation_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanifestation_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getinter_edition_flag") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanifestation_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanifestation_id(),
                                            flagVal1, flagVal2);


                                }
                            } else {
                                Log.info("manifestation_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanifestation_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanifestation_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_manifestation_identifier":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getmanif_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getmanif_identifier_id));
                        String[] manifIdentif = {"getmanif_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getidentifier", "gets_identifier", "geteffective_start_date", "geteffective_end_date",
                                "getf_type", "getf_manifestation", "getf_event", "getf_type", "getlead_indicator"};
                        for (String strTemp : manifIdentif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanif_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanif_identifier_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getlead_indicator") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanifestation_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanif_identifier_id(),
                                            flagVal1, flagVal2);

                                }


                            } else {
                                Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanif_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getmanif_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_person":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getperson_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getperson_id));
                        String[] person = {"getperson_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getgiven_name", "gets_given_name", "getfamily_name", "gets_family_name",
                                "getpeoplehub_id", "getemail", "gets_email"};
                        for (String strTemp : person) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("person_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getperson_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getperson_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("person_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getperson_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getperson_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_id));
                        String[] product = {"getproduct_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getname", "gets_name", "getshort_name", "gets_short_name",
                                "getseparately_sale_indicator", "gettrial_allowed_indicator", "getrestricted_sale_indicator", "getlaunch_date", "getcontent_from_date",
                                "getcontent_to_date", "getcontent_date_offset", "gett_summary_changed", "gett_event_description", "getf_type", "getf_status", "getf_status", "getf_accountable_product"
                                , "getf_tax_code", "getf_revenue_model", "getf_revenue_account", "getf_wwork", "getf_manifestation", "getf_t_event_type", "getf_event", "getf_self_one", "getf_self_two", "getf_self_three", "getf_self_four", "getf_self_five", "getf_self_six", "getf_self_seven"};
                        for (String strTemp : product) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prodId => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getseparately_sale_indicator" || method.getName() == "gettrial_allowed_indicator" || method.getName() == "getrestricted_sale_indicator" ||
                                    method.getName() == "gett_summary_changed") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("prodId => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_id(),
                                            flagVal1, flagVal2);

                                }


                            } else {
                                Log.info("prodId => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_financial_attribs":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_fin_attribs_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_fin_attribs_id));
                        String[] finAttr = {"getproduct_fin_attribs_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getf_gl_company", "getf_gl_cost_resp_centre", "getf_gl_revenue_resp_centre", "getf_event", "getf_product", "geteffective_start_date", "geteffective_end_date"};
                        for (String strTemp : finAttr) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_fin_att_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_fin_attribs_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_fin_attribs_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_fin_att_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_fin_attribs_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_fin_attribs_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_identifier":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_identifier_id));
                        String[] prodIdentifId = {"getproduct_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getidentifier", "gets_identifier", "getf_event", "getf_product", "geteffective_start_date", "geteffective_end_date", "getf_type"};
                        for (String strTemp : prodIdentifId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_identifier_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_identifier_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_identifier_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_person_role":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_person_role_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_person_role_id));
                        String[] prodPersRoleId = {"getproduct_person_role_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getf_event", "getf_product", "geteffective_start_date", "geteffective_end_date", "getf_role", "getf_person"};
                        for (String strTemp : prodPersRoleId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_pers_role_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_person_role_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_person_role_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_pers_role_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_person_role_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_person_role_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_product_rel_package":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_rel_pack_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getproduct_rel_pack_id));
                        String[] prodRelPackID = {"getproduct_rel_pack_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getallocation", "getf_package_owner", "geteffective_start_date", "geteffective_end_date", "getf_component", "getf_relationship_type", "getf_event"};
                        for (String strTemp : prodRelPackID) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("prod_rel_pack_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_rel_pack_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_rel_pack_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("prod_rel_pack_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_rel_pack_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getproduct_rel_pack_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_subject_area":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getsubject_area_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getsubject_area_id));
                        String[] subjAreaId = {"getsubject_area_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getcode", "getname", "getf_type", "getf_parent_subject_area"};
                        for (String strTemp : subjAreaId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("subj_area_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getsubject_area_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getsubject_area_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("subj_area_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getsubject_area_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getsubject_area_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_access_model":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_access_model_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_access_model_id));
                        String[] workAccess = {"getwork_access_model_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getf_access_model", "getf_wwork"};
                        for (String strTemp : workAccess) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_access_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_access_model_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_access_model_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_access_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_access_model_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_access_model_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_business_model":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_business_model_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_business_model_id));
                        String[] workBusinessId = {"getwork_business_model_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getf_business_model", "getf_wwork"};
                        for (String strTemp : workBusinessId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_businees_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_business_model_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_business_model_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_businees_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_business_model_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_business_model_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_financial_attribs":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_fin_attribs_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_fin_attribs_id));
                        String[] workFinAttrId = {"getwork_fin_attribs_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_gl_company", "getf_gl_cost_resp_centre", "getf_gl_revenue_resp_centre", "getf_wwork", "getf_event"};
                        for (String strTemp : workFinAttrId) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_fin_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_fin_attribs_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_fin_attribs_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_fin_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_fin_attribs_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_fin_attribs_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_hierarchy":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_hierarchy_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_hierarchy_id));
                        String[] workHirerchy = {"getwork_hierarchy_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "gethierarchy_level", "getcode", "getname", "getf_type", "getf_parent_work_hierarchy"};
                        for (String strTemp : workHirerchy) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_Hchy_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_hierarchy_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_hierarchy_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_Hchy_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_hierarchy_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_hierarchy_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_identifier":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_identifier_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_identifier_id));
                        String[] workIdentifier = {"getwork_identifier_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getidentifier", "gets_identifier", "getf_wwork", "getf_type", "getf_event"};
                        for (String strTemp : workIdentifier) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_Identif_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_identifier_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_Identif_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_identifier_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_identifier_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_legal_owner":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_legal_owner_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_legal_owner_id));
                        String[] workLegalOwner = {"getwork_legal_owner_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_wwork", "getf_legal_owner", "getf_ownership_description", "getf_event"};
                        for (String strTemp : workLegalOwner) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_legal_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_legal_owner_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_legal_owner_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_legal_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_legal_owner_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_legal_owner_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_person_role":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_person_role_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_person_role_id));
                        String[] workPersonRole = {"getwork_person_role_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_wwork", "getf_role", "getf_person", "getf_event"};
                        for (String strTemp : workPersonRole) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_pers_role_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_person_role_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_person_role_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_pers_role_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_person_role_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_person_role_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_rel_package":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_rel_pack_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_rel_pack_id));
                        String[] workRelPkg = {"getwork_rel_pack_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "getallocation", "geteffective_start_date", "geteffective_end_date", "getf_package_owner", "getf_component", "getf_relationship_type", "getf_event"};
                        for (String strTemp : workRelPkg) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_rel_pkg_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_rel_pack_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_rel_pack_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_rel_pkg_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_rel_pack_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_rel_pack_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_relationship":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_relationship_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_relationship_id));
                        String[] workRel = {"getwork_relationship_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_child", "getf_parent", "getf_relationship_type", "getf_event"};
                        for (String strTemp : workRel) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_rel_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_relationship_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_relationship_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("work_rel_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_relationship_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_relationship_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_subject_area_link":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_subject_area_link_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_subject_area_link_id));
                        String[] workSubjAreaLink = {"getwork_subject_area_link_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getpprimary", "getf_subject_area", "getf_wwork"};
                        for (String strTemp : workSubjAreaLink) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_subj_area_link_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_subject_area_link_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_subject_area_link_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "getpprimary") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("work_subj_area_link_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_subject_area_link_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_subject_area_link_id(),
                                            flagVal1, flagVal2);
                                }
                            } else {
                                Log.info("work_subj_area_link_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_subject_area_link_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_subject_area_link_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_work_work_hchy_link":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwrk_wrk_hchy_link_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwrk_wrk_hchy_link_id));
                        String[] workhchylink = {"getwrk_wrk_hchy_link_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getexternal_reference", "geteffective_start_date", "geteffective_end_date", "getf_work_hierarchy", "getf_wwork", "getf_event"};
                        for (String strTemp : workhchylink) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("wrk_hchy_link_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwrk_wrk_hchy_link_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwrk_wrk_hchy_link_id(),
                                            val1, val2);
                                }
                            } else {
                                Log.info("wrk_hchy_link_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwrk_wrk_hchy_link_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwrk_wrk_hchy_link_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                    case "gd_wwork":
                        GDTablesDLSQLContext.recordsFromPresentation.sort(Comparator.comparing(GDTableDLSQLObject::getwork_id)); //sort data in the lists
                        GDTablesDLSQLContext.recordsFromDL.sort(Comparator.comparing(GDTableDLSQLObject::getwork_id));
                        String[] work = {"getwork_id", "getb_classname", "getb_batchid", "getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "gets_work_id", "getexternal_reference", "getwork_title", "gets_work_title", "getwork_sub_title",
                                "gets_work_sub_title", "getwork_short_title", "gets_work_short_title", "getelectro_rights_indicator", "getvolume_name", "getcopyright_year", "getedition_number", "getplanned_launch_date", "getactual_launch_date", "getplanned_discontinue_date",
                                "getactual_discontinue_date", "gett_summary_changed", "gett_event_description", "getf_type", "getf_status", "getf_accountable_product", "getf_pmc", "getf_family", "getf_imprint",
                                "getf_legal_ownership", "getf_subscription_type", "getf_llanguage", "getf_t_event_type", "getf_event", "getf_self_one", "getf_self_two", "getf_self_three", "getf_self_four", "getf_self_five", "getf_self_six", "getf_self_seven", "getf_self_eight", "getf_self_nine", "getf_self_ten"};
                        for (String strTemp : work) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromPresentation.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate" || method.getName() == "getb_upddate" || method.getName() == "getttimestamp") {
                                String val1 = method.invoke(objectToCompare1).toString().substring(0, 19);
                                String val2 = method.invoke(objectToCompare2).toString().substring(0, 19);
                                Log.info("work_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_id() +
                                        " " + strTemp + " => Semarchy=" + val1 +
                                        " " + strTemp + " DL=" + val2);
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_id(),
                                            val1, val2);
                                }
                            } else if (method.getName() == "gett_summary_changed" || method.getName() == "getelectro_rights_indicator") {
                                if (method.invoke(objectToCompare1) != null) {
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if (flagVal1.equalsIgnoreCase("f")) {
                                        flagVal1 = "0";
                                    } else {
                                        flagVal1 = "1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("work_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_id(),
                                            flagVal1, flagVal2);
                                }
                            } else {
                                Log.info("work_id => " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_id() +
                                        " " + strTemp + " => Semarchy=" + method.invoke(objectToCompare1) +
                                        " " + strTemp + " DL=" + method2.invoke(objectToCompare2));
                                if (method.invoke(objectToCompare1) != null ||
                                        (method2.invoke(objectToCompare2) != null)) {
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromPresentation.get(i).getwork_id(),
                                            method.invoke(objectToCompare1),
                                            method2.invoke(objectToCompare2));
                                }
                            }
                        }
                        break;
                }
            }
        }
    }
}
