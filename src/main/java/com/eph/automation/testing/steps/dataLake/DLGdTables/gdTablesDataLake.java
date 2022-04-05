package com.eph.automation.testing.steps.dataLake.DLGdTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.GDTablesDLSQLContext;
import com.eph.automation.testing.models.dao.GDTableDLSQL.GDTableDLSQLObject;
import com.eph.automation.testing.services.db.gdSQLDLSQL.gdTableDLSQL;
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
                Ids = randomWorkHieRachyIds.stream().map(m -> (String) m.get("work_hierarchy_id")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
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
                sql = String.format(gdTableDLSQL.GET_GD_PROD_FIN_ATTR_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_product_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_IDENTIFIER_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_product_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_PERSON_ROLE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_product_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_PROD_REL_PKG_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_subject_area":
                sql = String.format(gdTableDLSQL.GET_GD_SUBJECT_AREA_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_access_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_ACCESS_MODEL_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_business_model":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_BUSINESS_MODEL_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_financial_attribs":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_FIN_ATTR_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_hierarchy":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HIRERACHY_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_identifier":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_IDENTIFIER_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_legal_owner":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_LEGAL_OWNER_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_person_role":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_PERSON_ROLE_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_rel_package":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_REL_PKG_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_relationship":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_RELATIONSHIP_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_subject_area_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_SUB_AREA_LINK_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_work_hchy_link":
                sql = String.format(gdTableDLSQL.GET_GD_WORK_HCHU_LINK_DL, Joiner.on("','").join(Ids));
                break;
            case "gd_wwork":
                sql = String.format(gdTableDLSQL.GET_GD_WWORK_DL, Joiner.on("','").join(Ids));
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
                    if (method.getName() == "getb_credate"||method.getName() == "getb_upddate"||method.getName()=="getttimestamp") {
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
                        String[] event = {"getevent_id","getb_classname","getb_batchid","getb_credate", "getb_upddate", "getb_creator"
                                , "getb_updator", "getddate", "getttimestamp", "getdescription"};
                        for (String strTemp : event) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate"||method.getName() == "getb_upddate"||method.getName()=="getttimestamp") {
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
                        String[] legal_owner = {"getlegal_owner_id","getb_classname","getb_batchid","getb_credate", "getb_upddate","getb_creator"
                                , "getb_updator","getexternal_reference", "getname", "gets_name", "getf_legal_ownership"};
                        for (String strTemp : legal_owner) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            if (method.getName() == "getb_credate"||method.getName() == "getb_upddate"||method.getName()=="getttimestamp") {
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
                        String[] manif = {"getmanifestation_id","getb_classname","getb_batchid","getb_credate", "getb_upddate","getb_creator"
                                , "getb_updator","gets_manifestation_id", "getexternal_reference", "getmanifestation_key_title", "gets_manifestation_key_title","getinter_edition_flag",
                                "getfirst_pub_date","getlast_pub_date","gett_event_description","getf_type","getf_status","getf_format_type","getf_wwork","getf_t_event_type","getf_event","getf_self_one"};
                        for (String strTemp : manif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate"||method.getName() == "getb_upddate"||method.getName()=="getttimestamp") {
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
                            }
                            else if(method.getName() =="getinter_edition_flag"){
                                if(method.invoke(objectToCompare1)!=null){
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if(flagVal1.equalsIgnoreCase("f")){
                                        flagVal1="0";
                                    }else{
                                        flagVal1="1";
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
                        String[] manifIdentif = {"getmanif_identifier_id","getb_classname","getb_batchid","getb_credate", "getb_upddate","getb_creator"
                                , "getb_updator","getexternal_reference", "getidentifier", "gets_identifier", "geteffective_start_date","geteffective_end_date",
                                "getf_type","getf_manifestation","getf_event","getf_type","getlead_indicator"};
                        for (String strTemp : manifIdentif) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate"||method.getName() == "getb_upddate"||method.getName()=="getttimestamp") {
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
                            }
                            else if(method.getName() =="getlead_indicator"){
                                if(method.invoke(objectToCompare1)!=null){
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if(flagVal1.equalsIgnoreCase("f")){
                                        flagVal1="0";
                                    }else{
                                        flagVal1="1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("manifes_identifier => " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanifestation_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getmanif_identifier_id(),
                                            flagVal1, flagVal2);

                                }


                            }else {
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
                        String[] person = {"getperson_id","getb_classname","getb_batchid","getb_credate", "getb_upddate","getb_creator"
                                , "getb_updator","getexternal_reference", "getgiven_name", "gets_given_name", "getfamily_name","gets_family_name",
                                "getpeoplehub_id","getemail","gets_email"};
                        for (String strTemp : person) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate"||method.getName() == "getb_upddate"||method.getName()=="getttimestamp") {
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
                            }else {
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
                        String[] product = {"getproduct_id","getb_classname","getb_batchid","getb_credate", "getb_upddate","getb_creator"
                                , "getb_updator","getexternal_reference", "getname", "gets_name", "getshort_name","gets_short_name",
                                "getseparately_sale_indicator","gettrial_allowed_indicator","getrestricted_sale_indicator","getlaunch_date","getcontent_from_date",
                        "getcontent_to_date","getcontent_date_offset","gett_summary_changed","gett_event_description","getf_type","getf_status","getf_status","getf_accountable_product"
                        ,"getf_tax_code","getf_revenue_model","getf_revenue_account","getf_wwork","getf_manifestation","getf_t_event_type","getf_event","getf_self_one","getf_self_two","getf_self_three","getf_self_four","getf_self_five","getf_self_six","getf_self_seven"};
                        for (String strTemp : product) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            GDTableDLSQLObject objectToCompare1 = GDTablesDLSQLContext.recordsFromSql.get(i);
                            GDTableDLSQLObject objectToCompare2 = GDTablesDLSQLContext.recordsFromDL.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            if (method.getName() == "getb_credate"||method.getName() == "getb_upddate"||method.getName()=="getttimestamp") {
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
                            }
                            else if(method.getName() =="getseparately_sale_indicator"||method.getName() =="gettrial_allowed_indicator" ||method.getName() =="getrestricted_sale_indicator"||
                                    method.getName() =="gett_summary_changed"){
                                if(method.invoke(objectToCompare1)!=null){
                                    String flagVal1 = method.invoke(objectToCompare1).toString();
                                    if(flagVal1.equalsIgnoreCase("f")){
                                        flagVal1="0";
                                    }else{
                                        flagVal1="1";
                                    }
                                    String flagVal2 = method.invoke(objectToCompare2).toString();
                                    Log.info("prodId => " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id() +
                                            " " + strTemp + " => Semarchy=" + flagVal1 +
                                            " " + strTemp + " DL=" + flagVal2);
                                    Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) +
                                                    " is missing/not found in " + SourceTable + " for: " + GDTablesDLSQLContext.recordsFromSql.get(i).getproduct_id(),
                                            flagVal1, flagVal2);

                                }


                            }else {
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
                }
            }
        }
    }
}
