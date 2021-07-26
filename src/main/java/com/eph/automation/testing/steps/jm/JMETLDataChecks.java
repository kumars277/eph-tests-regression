package com.eph.automation.testing.steps.jm;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.JMETL.JMContext;
import com.eph.automation.testing.models.dao.JMDataLake.JMETLObject;
import com.eph.automation.testing.models.dao.JMDataLake.JsonObjects.*;
import com.eph.automation.testing.models.dao.JMDataLake.STITCHObject;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.*;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMETLDataChecksSQL;
import com.eph.automation.testing.services.db.JMDataLakeSQL.JMtoDataLakeCountChecksSQL;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

public class JMETLDataChecks {
    private static String sql;
    private static List<String> Ids;
    private static String ExternalRef;

    private static String dlAllCoreSQLViewCount;
    private static int DLAllCoreViewCount;
    private static String gdSQLViewCount;
    private static int gdCount;


    private static String sqlDL;
    private static String sqlJM;
    private static int JMCount;
    private static int DLJMCount;

    @Given("^We get the (.*) random JMDB ids of (.*)$")
    public void getRandomJMDBIds(String numberOfRecords, String JMDBtable) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random IDs...");
        switch (JMDBtable) {
                case "work_business_model":
                    sql = String.format(JMETLDataChecksSQL.GET_WORK_BUSINESS_MODEL_IDs, numberOfRecords);
                    List<Map<?, ?>> randomWorkBusinessIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                    Ids = randomWorkBusinessIds.stream().map(m -> (Integer) m.get("WORK_BUSINESS_MODEL_ID")).map(String::valueOf).collect(Collectors.toList());
                    break;
                case "work_access_model":
                    sql = String.format(JMETLDataChecksSQL.GET_WORK_ACCESS_MODEL_IDs, numberOfRecords);
                    List<Map<?, ?>> randomWorkAccessIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                    Ids = randomWorkAccessIds.stream().map(m -> (Integer) m.get("WORK_ACCESS_MODEL_ID")).map(String::valueOf).collect(Collectors.toList());
                    break;
            case "work_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_PRODUCT_GROUP_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkProductGroupIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkProductGroupIds.stream().map(m -> (Integer) m.get("WORK_PRODUCTGROUP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "product_group":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_GROUP_IDs, numberOfRecords);
                List<Map<?, ?>> randomProductGroupIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomProductGroupIds.stream().map(m -> (Integer) m.get("PRODUCTGROUP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "pricing_option":
                sql = String.format(JMETLDataChecksSQL.GET_PRICING_OPTION_IDs, numberOfRecords);
                List<Map<?, ?>> randomPricingOptionIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPricingOptionIds.stream().map(m -> (Integer) m.get("PRICING_OPTION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "bm_pg_options":
                sql = String.format(JMETLDataChecksSQL.GET_BM_PG_OPTIONS_IDs, numberOfRecords);
                List<Map<?, ?>> randomBMPGOptionsIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomBMPGOptionsIds.stream().map(m -> (Integer) m.get("BM_PG_OPTIONS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the JM DB records from (.*)$")
    public void getJMMDBRecords(String JMDBtable) {
        Log.info("We get the records from jm DB..");
        switch (JMDBtable) {
            case "work_business_model":
                sql = String.format(JMETLDataChecksSQL.GET_JMDB_WORK_BUSINESS_MODEL, Joiner.on(",").join(Ids));
                break;
            case "work_access_model":
                sql = String.format(JMETLDataChecksSQL.GET_JMDB_WORK_ACCESS_MODEL, Joiner.on(",").join(Ids));
                break;
            case "work_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_JMDB_WORK_PRODUCT_GROUP, Joiner.on(",").join(Ids));
                break;
            case "product_group":
                sql = String.format(JMETLDataChecksSQL.GET_JMDB_PRODUCT_GROUP, Joiner.on(",").join(Ids));
                break;
            case "pricing_option":
                sql = String.format(JMETLDataChecksSQL.GET_JMDB_PRICING_OPTION, Joiner.on(",").join(Ids));
                break;
            case "bm_pg_options":
                sql = String.format(JMETLDataChecksSQL.GET_JMDB_BM_PG_OPTIONS, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.MYSQL_JM_URL);
        System.out.println(JMContext.JMObjectsFromDL.size());
    }

    @Then("^We get the JMInbound records from (.*)$")
    public void getJMInboundRecords(String InboundTable) {
        Log.info("We get the Current records..");
        switch (InboundTable) {
            case "jmf_work_business_model":
                sql = String.format(JMETLDataChecksSQL.GET_JMINBOUND_WORK_BUSINESS_MODEL, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_access_model":
                sql = String.format(JMETLDataChecksSQL.GET_JMINBOUND_WORK_ACCESS_MODEL, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_JMINBOUND_WORK_PRODUCT_GROUP, Joiner.on(",").join(Ids));
                break;
            case "jmf_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_JMINBOUND_PRODUCT_GROUP, Joiner.on(",").join(Ids));
                break;
            case "jmf_pricing_option":
                sql = String.format(JMETLDataChecksSQL.GET_JMINBOUND_PRICING_OPTION, Joiner.on(",").join(Ids));
                break;
            case "jmf_bm_pg_options":
                sql = String.format(JMETLDataChecksSQL.GET_JMINBOUND_BM_PG_OPTIONS, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMTransformObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JM records in between JMDB and Inbound of (.*)$")
    public void compareJMDBtoJMInbound(String JMDBtable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JMContext.JMObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the jm records in DB and Inbound ..");
            for (int i = 0; i < JMContext.JMObjectsFromDL.size(); i++) {
                switch (JMDBtable) {
                    case "work_business_model":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_BUSINESS_MODEL_ID)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_BUSINESS_MODEL_ID));

                        String[] JMF_WorkBusinessColumnName = {"getWORK_BUSINESS_MODEL_ID","getF_PRODUCT_WORK","getBUSINESS_MODEL_CODE","getBUSINESS_MODEL_DESCRIPTION"};
                        String[] JMFInbound_WorkColumnName = {"getWORK_BUSINESS_MODEL_ID","getf_work","getBUSINESS_MODEL_CODE","getBUSINESS_MODEL_DESCRIPTION"};
                        int j=0;
                        for (String strTemp : JMF_WorkBusinessColumnName) {


                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(JMFInbound_WorkColumnName[j]);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("WORK_BUSINESS_MODEL_ID => " + JMContext.JMObjectsFromDL.get(i).getWORK_BUSINESS_MODEL_ID() +
                                    " " + JMFInbound_WorkColumnName[j] + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            j++;
                        }
                        break;
                    case "work_access_model":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_ACCESS_MODEL_ID)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_ACCESS_MODEL_ID));

                        String[] JMF_WorkAccessColumnName = {"getWORK_ACCESS_MODEL_ID","getF_PRODUCT_WORK","getACCESS_MODEL_CODE","getACCESS_MODEL_DESCRIPTION"};
                        String[] JMFInbound_WorkAccessColumnName = {"getWORK_ACCESS_MODEL_ID","getf_work","getACCESS_MODEL_CODE","getACCESS_MODEL_DESCRIPTION"};
                        int a=0;
                        for (String strTemp : JMF_WorkAccessColumnName) {


                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(JMFInbound_WorkAccessColumnName[a]);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("WORK_ACCESS_MODEL_ID => " + JMContext.JMObjectsFromDL.get(i).getWORK_ACCESS_MODEL_ID() +
                                    " " + JMFInbound_WorkAccessColumnName[a] + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            a++;
                        }
                        break;
                    case "work_product_group":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_PRODUCT_GROUP_ID)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_productgroup_id));

                        String[] JMFInbound_WorkProductGroupColumnName = {"getWORK_PRODUCT_GROUP_ID","getF_PRODUCT_WORK","getF_PRODUCT_GROUP"};
                        String[] JMF_WorkProductGroupColumnName = {"getwork_productgroup_id","getf_work","getf_productgroup"};
                        int b=0;
                        for (String strTemp : JMF_WorkProductGroupColumnName) {


                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(JMFInbound_WorkProductGroupColumnName[b]);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("WORK_PRODUCT_GROUP_ID => " + JMContext.JMObjectsFromDL.get(i).getWORK_PRODUCT_GROUP_ID() +
                                    " " + JMFInbound_WorkProductGroupColumnName[b] + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            b++;
                        }
                        break;
                    case "product_group":
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getproductgroup_id)); //sort data in the lists
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getPRODUCT_GROUP_ID));

                        String[] JMFInbound_ProductGroupColumnName = {"getproductgroup_id","getproductgroup_code","getproductgroup_description"};
                        String[] JMF_ProductGroupColumnName = {"getPRODUCT_GROUP_ID","getPRODUCT_GROUP_CODE","getPRODUCT_GROUP_DESCRIPTION"};
                        int c=0;
                        for (String strTemp : JMF_ProductGroupColumnName) {


                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMTransformObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(JMFInbound_ProductGroupColumnName[c]);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("PRODUCT_GROUP_ID => " + JMContext.JMObjectsFromDL.get(i).getPRODUCT_GROUP_ID() +
                                    " " + JMFInbound_ProductGroupColumnName[c] + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            c++;
                        }
                        break;
                    case "pricing_option":
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getpricing_option_id)); //sort data in the lists
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getpricing_option_id));

                        String[] JMF_PricingOptionColumnName = {"getpricing_option_id","getpricing_option_code","getpricing_option_description"};
                        for (String strTemp : JMF_PricingOptionColumnName) {


                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMTransformObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("pricing_option_id => " + JMContext.JMObjectsFromDL.get(i).getpricing_option_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "bm_pg_options":
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getbm_pg_options_id)); //sort data in the lists
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getbm_pg_options_id));

                        String[] JMF_BM_PG_OptionsColumnName = {"getbm_pg_options_id","getbusiness_model_code","getmanifestation_formats_code","getownership_brand_type","getjbs_site_ind","getoptions"};
                        for (String strTemp : JMF_BM_PG_OptionsColumnName) {


                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMTransformObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("bm_pg_options_id => " + JMContext.JMObjectsFromDL.get(i).getbm_pg_options_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }

    @Given("^We get the (.*) random JM ids of (.*)$")
    public void getRandomJMIds(String numberOfRecords, String Currenttable) {
//  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (Currenttable) {
            case "jmf_work":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkIds.stream().map(m -> (Integer) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_MANIFESTATION_IDs, numberOfRecords);
                List<Map<?, ?>> randomManifestationIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifestationIds.stream().map(m -> (Integer) m.get("MANIFESTATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_PERSON_ROLE_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkPersonRolesIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkPersonRolesIds.stream().map(m -> (Integer) m.get("WORK_PERSON_ROLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_SUBJECT_AREA_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkSubjectAreaIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkSubjectAreaIds.stream().map(m -> (Integer) m.get("WORK_SUBJECT_AREA_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_chronicle":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_CHRONICLE_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkChronicleIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkChronicleIds.stream().map(m -> (Integer) m.get("WORK_CHRONICLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_pmg_pubdir_allocation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PMG_PUBDIR_ALLOCATION_IDs, numberOfRecords);
                List<Map<?, ?>> randomPmgPubdirAllocationIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPmgPubdirAllocationIds.stream().map(m -> (Integer) m.get("PMG_PUBDIR_ALLOCATION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_approval_attachment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_ATTACHMENT_IDs, numberOfRecords);
                List<Map<?, ?>> randomApprovalAttachmentIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApprovalAttachmentIds.stream().map(m -> (Integer) m.get("APPROVAL_ATTACHMENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_review_comment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_REVIEW_COMMENT_IDs, numberOfRecords);
                List<Map<?, ?>> randomReviewCommentIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomReviewCommentIds.stream().map(m -> (Integer) m.get("REVIEW_COMMENT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_approval_request":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_REQUEST_IDs, numberOfRecords);
                List<Map<?, ?>> randomApprovalRequestIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApprovalRequestIds.stream().map(m -> (Integer) m.get("APPROVAL_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_application_properties":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPLICATION_PROPERTIES_IDs, numberOfRecords);
                List<Map<?, ?>> randomApplicationPropertiesIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomApplicationPropertiesIds.stream().map(m -> (String) m.get("APPLICATION_PROPERTY_KEY")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_chronicle_scenario":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_CHRONICLE_SCENARIO_IDs, numberOfRecords);
                List<Map<?, ?>> randomChronicleScenarioIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomChronicleScenarioIds.stream().map(m -> (String) m.get("CHRONICLE_SCENARIO_CODE")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_ownership":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_OWNERSHIP_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkOwnershipIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkOwnershipIds.stream().map(m -> (Integer) m.get("WORK_OWNERSHIP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_business_model":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_BUSINESS_MODEL_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkBusinessModelIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkBusinessModelIds.stream().map(m -> (Integer) m.get("WORK_BUSINESS_MODEL_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_access_model":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_ACCESS_MODEL_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkAccessModelIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkAccessModelIds.stream().map(m -> (Integer) m.get("WORK_ACCESS_MODEL_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_work_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_PRODUCT_GROUP_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkProductGroupIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkProductGroupIds.stream().map(m -> (Integer) m.get("WORK_PRODUCTGROUP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PRODUCT_GROUP_IDs, numberOfRecords);
                List<Map<?, ?>> randomProductGroupIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomProductGroupIds.stream().map(m -> (Integer) m.get("PRODUCTGROUP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_pricing_option":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PRICING_OPTION_IDs, numberOfRecords);
                List<Map<?, ?>> randomPricingOptionIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPricingOptionIds.stream().map(m -> (Integer) m.get("PRICING_OPTION_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "jmf_bm_pg_options":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_BM_PG_OPTIONS_IDs, numberOfRecords);
                List<Map<?, ?>> randomBMPGOptionIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomBMPGOptionIds.stream().map(m -> (Integer) m.get("BM_PG_OPTIONS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the JM Transformed Inbound records from (.*)$")
    public void getRecordsJMInbound(String Currenttable) {
        Log.info("We get the records from jm Access..");
        switch (Currenttable) {
            case "jmf_work":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK, Joiner.on(",").join(Ids));
                break;
            case "jmf_manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_MANIFESTATION, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_PERSON_ROLE, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_SUBJECT_AREA, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_chronicle":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_CHRONICLE, Joiner.on(",").join(Ids));
                break;
            case "jmf_pmg_pubdir_allocation":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PMG_PUBDIR_ALLOCATION, Joiner.on(",").join(Ids));
                break;
            case "jmf_approval_attachment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_ATTACHMENT, Joiner.on(",").join(Ids));
                break;
            case "jmf_review_comment":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_REVIEW_COMMENT, Joiner.on(",").join(Ids));
                break;
            case "jmf_approval_request":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPROVAL_REQUEST, Joiner.on(",").join(Ids));
                break;
            case "jmf_application_properties":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_APPLICATION_PROPERTIES, Joiner.on("','").join(Ids));
                break;
            case "jmf_chronicle_scenario":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_CHRONICLE_SCENARIO, Joiner.on("','").join(Ids));
                break;
            case "jmf_work_ownership":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_OWNERSHIP, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_business_model":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_BUSINESS_MODEL, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_access_model":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_ACCESS_MODEL, Joiner.on(",").join(Ids));
                break;
            case "jmf_work_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_WORK_PRODUCTGROUP, Joiner.on(",").join(Ids));
                break;
            case "jmf_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PRODUCTGROUP, Joiner.on(",").join(Ids));
                break;
            case "jmf_pricing_option":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_PRICING_OPTION, Joiner.on(",").join(Ids));
                break;
            case "jmf_bm_pg_options":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_BM_PG_OPTIONS, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);

    }

    @Then("^We get the JM Staging Query records from (.*)$")
    public void getRecordsJMStagingQUERY(String CurrenttableQuery) {
        Log.info("We get the Current records..");
        switch (CurrenttableQuery) {
            case "work":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_QUERY, Joiner.on(",").join(Ids));
                break;
            case "manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_PERSON_ROLE_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_SUBJECT_AREA_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_chronicle":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_CHRONICLE_QUERY, Joiner.on(",").join(Ids));
                break;
            case "pmg_pubdir_allocation":
                sql = String.format(JMETLDataChecksSQL.GET_PMG_PUBDIR_ALLOCATION_QUERY, Joiner.on(",").join(Ids));
                break;
            case "approval_attachment":
                sql = String.format(JMETLDataChecksSQL.GET_APPROVAL_ATTACHMENT_QUERY, Joiner.on(",").join(Ids));
                break;
            case "review_comment":
                sql = String.format(JMETLDataChecksSQL.GET_REVIEW_COMMENT_QUERY, Joiner.on(",").join(Ids));
                break;
            case "approval_request":
                sql = String.format(JMETLDataChecksSQL.GET_APPROVAL_REQUEST_QUERY, Joiner.on(",").join(Ids));
                break;
            case "application_properties":
                sql = String.format(JMETLDataChecksSQL.GET_APPLICATION_PROPERTIES_QUERY, Joiner.on("','").join(Ids));
                break;
            case "chronicle_scenario":
                sql = String.format(JMETLDataChecksSQL.GET_CHRONICLE_SCENARIO_QUERY, Joiner.on("','").join(Ids));
                break;
            case "work_ownership":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_OWNERSHIP_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_business_model":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_BUSINESS_MODEL_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_access_model":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_ACCESS_MODEL_QUERY, Joiner.on(",").join(Ids));
                break;
            case "work_product_group":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_PRODUCTGROUP_QUERY, Joiner.on(",").join(Ids));
                break;
            case "product_group":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCTGROUP_QUERY, Joiner.on(",").join(Ids));
                break;
            case "pricing_option":
                sql = String.format(JMETLDataChecksSQL.GET_PRICING_OPTION, Joiner.on(",").join(Ids));
                break;
            case "bm_pg_options":
                sql = String.format(JMETLDataChecksSQL.GET_JMF_BM_PG_OPTIONS_QUERY, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMTransformObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JM records in Transformed Inbound and Current of (.*)$")
    public void compareJMTransformInboundtoCurrent(String Currenttable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JMContext.JMObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the jm records in Transformed Inbound and Current ..");
            for (int i = 0; i < JMContext.JMObjectsFromDL.size(); i++) {
                switch (Currenttable) {
                    case "jmf_work":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_id));

                        String[] JMF_WorkColumnName = {"getwork_id", "getwork_chronicle_id", "getelsevier_journal_number", "getwork_journey_identifier", "getwork_title", "getwork_subtitle", "getwork_title_info", "getissn_l", "getjournal_acronym_pts", "getjournal_acronym_evise", "getjournal_acronym_argi", "geteph_work_id", "getfamily_id", "getfamily_title", "getpmx_product_family_id", "getpmc_family_resource_details_id", "getpmc_old", "getpmc_new", "getpu_family_resource_details_id", "getpu_email_old", "getpu_email_new", "getpu_peoplehubid_old", "getpu_peoplehubid_new", "getinternal_elsevier_division", "getmanifestation_types_code", "getmanifestation_personal_model_type", "getimprint_name", "getpts_journal_indicator", "getyear_of_first_issue", "getyear_of_last_issue", "getfirst_volume_name", "getfirst_issue_name", "getlast_volume_name", "getlast_issue_name", "getvolumes_per_year_quantity", "getissues_per_volume_quantity", "getfirst_year_volumes_per_year_quantity", "getfirst_year_issues_per_volume_quantity", "getperiodical_timing_desc", "getopen_accesstype_code", "getopen_access_sponsor_name", "getopen_access_fee", "getopen_access_fee_short_art", "getopen_access_currency_code", "getopen_access_discount_ind", "getopen_access_discount_period", "getoa_first_year_discount_price_percentage", "getoa_second_year_discount_price_percentage", "getoa_first_year_discount_price", "getoa_second_year_discount_price", "getddp_eligible_ind", "getwebshop_ind", "getmedline_ind", "getthomson_reuters_ind", "getscopus_ind", "getembase_ind", "getdoaj_ind", "getroad_ind", "getpubmedcentral_ind", "getmain_language_code", "getenglish_language_percentage_type", "getopen_archive_period", "getdelayed_open_archive_ind", "getinclude_in_collections_ind", "getlaunch_date", "getpublication_schedule_jan", "getpublication_schedule_feb", "getpublication_schedule_mar", "getpublication_schedule_apr", "getpublication_schedule_may", "getpublication_schedule_jun", "getpublication_schedule_jul", "getpublication_schedule_aug", "getpublication_schedule_sep", "getpublication_schedule_oct", "getpublication_schedule_nov", "getpublication_schedule_dec", "getmanifestation_formats_code", "gettakeover_year", "getdoi_prefix", "getdoi_right_assigned_ind", "getsociety_owned_ind", "getchecked_with_oa_team_ind", "getimprint_code", "getdiscontinue_date", "getpmx_product_work_id", "getremoved_from_catalogue_ind", "gettransfer_date", "getle_mans_ind", "getsociety_relationship_type", "getarticle_number_per_year", "getsubmission_number_per_year", "getevise_requested_code", "getevise_support_level", "getevise_workflow_type", "geteditor_location", "getabp_usage_ind", "getnon_standard_production_aspects", "geteditorial_production_site", "getproduction_specification_type", "gettypeset_model_type", "getreference_style_type", "getbudgeted_page_number_per_issue", "getlatex_submission_percentage", "gettypesetter_code", "getpage_review_charges", "getcopy_editing_code", "gethistory_date_format", "getproof_sent_to_author_ind", "getproof_sent_to_editor_ind", "geteditor_email_address", "gete_suite_journal_ind", "getsponsor_accress_required_ind", "getonline_publication_date_ind", "getauthor_feedback_ind", "getsend_copyright_form_ind", "getrunning_order_details", "getflexibility", "getmaximum_page_details", "getspecific_logo_required_ind", "getadditional_deliveries_details", "getmandatory_submission_item_ind", "getdoi_statement_ind", "getlanguage_editing_performed_ind", "getlanguage_editing_stage", "getdedicated_journal_url_ind", "getdedicated_journal_url", "getcoi_required_ind", "geteditorial_system_name", "gettypesetter_name", "geteditorial_turnaround_time", "getpending_submissions_quantity", "getchecked_with_society_team_ind", "getproduction_launch_date", "getproposed_evise_rollout_period_date", "getbackstock_end_year", "getbackstock_end_option", "getjbs_site_ind", "geteditorial_production_email_address", "geteditorial_production_site_name", "getjournal_has_article_nos", "getjournal_article_number_type", "getbackstock_treatment_note", "getjoint_venture_indicator", "getjoint_venture_party_name", "getbusiness_controller_name", "getbusiness_unit_code", "getpmg_code", "getpmc_code", "getopco_r12_id", "getopco_r12_code", "getcontract_opco_r12_code", "getresponsibility_centre_code", "gethfm_hierarchy_position_code", "getcity_opco_r12_code", "getcountry_opco_r12_code", "getopco_r12_name", "getopco_r12_legal_name", "getrefund_option", "getreminder_date", "getreminder_option", "getrenewal_option", "getrenewal_date", "getbusiness_controller_email_address", "getclaims_handling_option", "getclaims_handling_end_date", "getbackissues_handling_option", "getbackissues_handling_end_date", "getownership_brand_type", "getsociety_contract_date", "getsociety_contract_expiry_date", "getjournal_copyright_owner", "getstandard_copyright_statement_ind", "getnon_standard_copyright_statement", "getopen_access_article_copyright_type", "getarticle_disclaimer", "getpublishing_right_type", "getprint_rights_secured_ind", "getexclusive_rights_secured_ind", "getno_exclusive_print_rights_reason", "getelectronic_print_rights_secured_ind", "getno_exclusive_electronic_rights_reason", "getrights_restrictions_ind", "getrights_restrictions_list", "getbackfile_consent_ind", "getbackfile_consent_info", "getback_rights_type", "getback_rights_start_volume_and_issue", "getnon_exclusive_electronic_rights_ind", "getpost_termination_electronic_rights_type", "getno_post_termination_electronic_rights_reason", "getpermission_type", "getpermission_handling_email_address", "getsociety_membership_held_type", "getspecial_arrangements", "getdespatch_method", "getcontract_seen_by_els_attorney_ind", "getcontract_sent_ind", "getdirect_billing_membership", "getdirect_billing_membership_with_fee", "getsociety_maintained_prepayment", "getsociety_maintained_account", "getautomatic_renewal", "getmailing_labels", "gettitle_included_in_ck", "getsoc_agreed_to_ck_content", "getsoc_ck_content_agreement_text", "getmonths_to_keep_transfer_online", "getsoc_ck_content_agreement_date", "getnotified_date"};

                        for (String strTemp : JMF_WorkColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("WORK_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_manifestation":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getmanifestation_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getmanifestation_id));

                        String[] JMF_ManifestationColumnName = {"getmanifestation_id", "getf_work", "getmanifestation_title", "getmanifestation_type", "getissn", "getelsevier_journal_number", "getsubscription_type", "getprice_categories", "getpmx_product_manifestation_id", "geteph_manifestation_id", "getembargo_times_indicator", "getelectronic_rights_secured_type", "getonline_launch_date", "getarticle_in_press_s5_ind", "getarticle_in_press_s100_ind", "getarticle_in_press_s250_ind", "getembargo_times_number", "getonline_last_issue_date", "gettrim_size", "getbase_print_run_number", "getcolour_frequency", "getartwork_sensitivity_ind", "getmailing_breakdown_europe", "getmailing_breakdown_usa", "getmailing_breakdown_row", "getzero_warehousing_ind", "getback_stock_warehouse_location_type", "getprinter_type", "getprinter_location_type", "getinterior_paper_type", "getcover_paper_type", "getdistributor_code", "getdistributor_location_code", "getprint_model_code", "getseparate_print_run_ind", "getoffprint_pricing_code", "getoffprint_cover_ind", "getfree_issues_quantity", "getfree_offprints_type", "getfree_paid_colour_offprints_quantity", "getcolour_printing_currency_code", "getcolour_artwork_exceptions", "getsociety_owns_labels_ind", "getbinding_type", "getspecial_bulk_arrangements", "getcost_first_printed_colour_unit", "getcost_next_printed_colour_unit", "getapplication_code", "getnotified_date"};

                        for (String strTemp : JMF_ManifestationColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Manifestation_ID => " + JMContext.JMObjectsFromDL.get(i).getmanifestation_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_person_role":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_person_role_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_person_role_id));

                        String[] JMF_WorkPersonRoleColumnName = {"getwork_person_role_id", "getf_work", "getparty_role_type", "getemail_address", "getfull_name", "getphone_number", "getaddress_line_1", "getaddress_line_2", "getaddress_line_3", "getcity", "getcountry", "getstate", "getpost_code", "getorganisation_1", "getpmx_party_id", "getpeoplehub_id", "geteph_person_id", "getnotified_date"};

                        for (String strTemp : JMF_WorkPersonRoleColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Person_Role_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_person_role_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_subject_area":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_subject_area_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_subject_area_id));

                        String[] JMF_WorkSubjectAreaColumnName = {"getwork_subject_area_id", "getf_work", "getsubject_area_type_code", "getsubject_area_priority_code", "getsubject_area_code", "getsubject_area_name", "getnotified_date"};

                        for (String strTemp : JMF_WorkSubjectAreaColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Subject_Area_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_subject_area_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_chronicle":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_chronicle_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_chronicle_id));

                        String[] JMF_WorkChronicleColumnName = {"getwork_chronicle_id", "getcompleted_on", "getdistribution_list", "getrename_required_ind", "getjournal_renamed_before_launch", "getchronicle_status_code", "getchronicle_scenario_code", "getstarted_by", "getstarted_on", "getupdated_by", "getupdated_on", "getprocess_instance_id", "getreason_for_change", "getcancelled_by", "getcreated_by_name", "getrejection_comment", "getsubmission_date", "getcancelled_date", "getrejection_date", "getversion", "getnotified_date"};

                        for (String strTemp : JMF_WorkChronicleColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Chronicle_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_chronicle_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_pmg_pubdir_allocation":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getpmg_pubdir_allocation_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getpmg_pubdir_allocation_id));

                        String[] JMF_PmgPubDirAllocationColumnName = {"getpmg_pubdir_allocation_id", "getallocation_type", "getpmg_filter", "getpmc_filter", "getpu_filter_email", "getpu_filter_name", "getpmc_change_ind", "getpu_change_ind", "getpmx_pmgcode", "getpmx_pmg_name", "getpmx_pmg_f_business_unit", "getpmg_bus_ctrl_name", "getpmg_bus_ctrl_email", "getpmg_pubdir_name_old", "getpmg_pubdir_email_old", "getpmg_pubdir_peoplehub_id_old", "getpmg_pubdir_name_new", "getpmg_pubdir_email_new", "getpmg_pubdir_peoplehub_id_new", "getnotified_date", "geteph_pmg_code"};

                        for (String strTemp : JMF_PmgPubDirAllocationColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("PMG_Pubdir_Allocation_ID => " + JMContext.JMObjectsFromDL.get(i).getpmg_pubdir_allocation_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_approval_attachment":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapproval_attachment_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapproval_attachment_id));

                        String[] JMF_ApprovalAttachmentColumnName = {"getapproval_attachment_id", "getf_approval", "getattachment_name", "getattachment", "getnotified_date"};

                        for (String strTemp : JMF_ApprovalAttachmentColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Approval_Attachment_ID => " + JMContext.JMObjectsFromDL.get(i).getapproval_attachment_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_review_comment":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getreview_comment_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getreview_comment_id));

                        String[] JMF_ReviewCommentColumnName = {"getreview_comment_id", "getf_approval_id", "getreview_attribute_name", "getreview_comment", "getreview_comment_date", "getcreated_on", "getnotified_date"};

                        for (String strTemp : JMF_ReviewCommentColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Review_Comment_ID => " + JMContext.JMObjectsFromDL.get(i).getreview_comment_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_approval_request":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapproval_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapproval_id));

                        String[] JMF_ApprovalRequestColumnName = {"getapproval_id", "getf_work_chronicle", "getapproval_type", "getapprover_name", "getapproval_given_indicator", "getapproval_request_date", "getapproval_response_date", "getnotified_date"};

                        for (String strTemp : JMF_ApprovalRequestColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Approval_ID => " + JMContext.JMObjectsFromDL.get(i).getapproval_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_application_properties":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapplication_property_key)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getapplication_property_key));

                        String[] JMF_ApplicationPropertiesColumnName = {"getapplication_property_key", "getapplication_property_value", "getnotified_date"};

                        for (String strTemp : JMF_ApplicationPropertiesColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Application_Property_key => " + JMContext.JMObjectsFromDL.get(i).getapplication_property_key() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_chronicle_scenario":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getchronicle_scenario_code)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getchronicle_scenario_code));

                        String[] JMF_ChronicleScenarioColumnName = {"getchronicle_scenario_code", "getchronicle_scenario_name", "getchronicle_scenario_description", "getchronicle_scenario_evolutionary_ind", "getnotified_date"};

                        for (String strTemp : JMF_ChronicleScenarioColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Chronicle_Scenario_Code => " + JMContext.JMObjectsFromDL.get(i).getchronicle_scenario_code() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_ownership":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_ownership_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_ownership_id));

                        String[] JMF_WorkOwnershipColumnName = {"getwork_ownership_id", "getf_work", "getnotified_date", "getf_legal_owner", "getlegal_owner_name", "getlegal_owner_type", "getjournal_ownership_type"};

                        for (String strTemp : JMF_WorkOwnershipColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Ownership_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_ownership_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_business_model":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_BUSINESS_MODEL_ID)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_BUSINESS_MODEL_ID));

                        String[] JMF_WorkBusinessModelColumnName = {"getWORK_BUSINESS_MODEL_ID","getf_work","getBUSINESS_MODEL_CODE","getBUSINESS_MODEL_DESCRIPTION"};

                        for (String strTemp : JMF_WorkBusinessModelColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Business_Model_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_business_model_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_access_model":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_ACCESS_MODEL_ID)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getWORK_ACCESS_MODEL_ID));

                        String[] JMF_WorkAccessModelColumnName = {"getWORK_ACCESS_MODEL_ID","getf_work","getACCESS_MODEL_CODE","getACCESS_MODEL_DESCRIPTION"};

                        for (String strTemp : JMF_WorkAccessModelColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_Access_Model_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_access_model_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_work_product_group":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_productgroup_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_productgroup_id));

                        String[] JMF_WorkGroupModelColumnName = {"getwork_productgroup_id"};

                        for (String strTemp : JMF_WorkGroupModelColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("Work_GroupProduct_ID => " + JMContext.JMObjectsFromDL.get(i).getwork_productgroup_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_product_group":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getproductgroup_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getproductgroup_id));

                        String[] JMF_GroupModelColumnName = {"getproductgroup_id"};

                        for (String strTemp : JMF_GroupModelColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("GroupProduct_ID => " + JMContext.JMObjectsFromDL.get(i).getproductgroup_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_pricing_option":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getpricing_option_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getpricing_option_id));

                        String[] JMF_PricingOptionColumnName = {"getpricing_option_id","getpricing_option_code","getpricing_option_description","getpricing_option_help"};

                        for (String strTemp : JMF_PricingOptionColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("PricingOption_ID => " + JMContext.JMObjectsFromDL.get(i).getpricing_option_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "jmf_bm_pg_options":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getbm_pg_options_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getbm_pg_options_id));

                        String[] JMF_BMPGOptionsColumnName = {"getbm_pg_options_id","getbusiness_model_code","getmanifestation_formats_code","getf_productgroup","getownership_brand_type","getjbs_site_ind","geteffective_to_date"};

                        for (String strTemp : JMF_BMPGOptionsColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("PricingOption_ID => " + JMContext.JMObjectsFromDL.get(i).getbm_pg_options_id() +
                                    " " + strTemp + " => Inbound=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Current=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }

    //  ETL Data Comparisons
    @Given("^We get the (.*) random JM ETL ids of (.*)$")
    public void getRandomJMETLIds(String numberOfRecords, String Currenttable) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (Currenttable) {
            case "etl_manifestation_updates1_v":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_UPDATES1_IDs, numberOfRecords);
                List<Map<?, ?>> randomManifestationUpdates1Ids = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifestationUpdates1Ids.stream().map(m -> (Integer) m.get("W0_CHRONICLE_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "sd_subject_areas_v":
                sql = String.format(JMETLDataChecksSQL.GET_SD_SUBJECT_AREAS_IDs, numberOfRecords);
                List<Map<?, ?>> randomSDSubjectAreasIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomSDSubjectAreasIds.stream().map(m -> (BigDecimal) m.get("SA_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "etl_work_legal_owner_dq_V":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_LEGAL_OWNER_DQ_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkLegalOwnerDQIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkLegalOwnerDQIds.stream().map(m -> (String) m.get("WORK_EXTERNAL_REF")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "etl_work_identifier_dq_v":
            case "etl_work_person_role_dq_v":
            case "etl_manifestation_identifier_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_JM_SOURCE_REF_NEW_IDs, Currenttable, numberOfRecords);
                List<Map<?, ?>> randomWorkIdentifierDQIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkIdentifierDQIds.stream().map(m -> (String) m.get("JM_SOURCE_REF_NEW")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "etl_work_business_model_dq_v":
            case "etl_work_access_model_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_JM_EXTERNAL_REFERENCE_IDs, Currenttable, numberOfRecords);
                List<Map<?, ?>> randomExternalRefsIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomExternalRefsIds.stream().map(m -> (String) m.get("EXTERNAL_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
                break;
            default:
                sql = String.format(JMETLDataChecksSQL.GET_JM_SOURCE_REFERENCE_IDs, Currenttable, numberOfRecords);
                List<Map<?, ?>> randomAccountableProductDQIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomAccountableProductDQIds.stream().map(m -> (String) m.get("JM_SOURCE_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the JM Transformed ETL Query records from (.*)$")
    public void getRecordsJMTransformedETL(String ETLtable) {
        Log.info("We get the records from jm ETL..");
        switch (ETLtable) {
            case "etl_accountable_product_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_ACCOUNTABLE_PRODUCT_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_wwork_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WWORK_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_work_identifier_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_IDENTIFIER_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_work_subject_area_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_SUBJECT_AREA_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_PERSON_ROLE_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_updates1_v":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_UPDATES1_QUERY, Joiner.on(",").join(Ids));
                break;
            case "etl_manifestation_identifier_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_IDENTIFIER_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_product_part1_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_PART1_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_product_inserts_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_INSERTS_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_product_updates_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_UPDATES_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_product_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_product_person_role_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_PERSON_ROLE_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "sd_subject_areas_v":
                sql = String.format(JMETLDataChecksSQL.GET_SD_SUBJECT_AREAS_QUERY, Joiner.on(",").join(Ids));
                break;
            case "etl_manifestation_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_work_legal_owner_dq_V":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_LEGAL_OWNER_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_work_business_model_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_BUSINESS_MODEL_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
            case "etl_work_access_model_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_ACCESS_MODEL_DQ_QUERY, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMTransformObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
        System.out.println(JMContext.JMTransformObjectsFromDL.size());
    }

    @Then("^We get the JM ETL records from (.*)$")
    public void getRecordsJMETL(String ETLtable) {
        Log.info("We get the ETL records..");
        switch (ETLtable) {
            case "etl_accountable_product_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_ACCOUNTABLE_PRODUCT_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_wwork_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WWORK_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_work_identifier_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_IDENTIFIER_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_PERSON_ROLE_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_work_subject_area_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_SUBJECT_AREA_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_updates1_v":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_UPDATES1, Joiner.on(",").join(Ids));
                break;
            case "etl_manifestation_identifier_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "etl_product_part1_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_PART1, Joiner.on("','").join(Ids));
                break;
            case "etl_product_inserts_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_INSERTS, Joiner.on("','").join(Ids));
                break;
            case "etl_product_updates_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_UPDATES, Joiner.on("','").join(Ids));
                break;
            case "etl_product_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_product_person_role_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_PRODUCT_PERSON_ROLE_DQ, Joiner.on("','").join(Ids));
                break;
            case "sd_subject_areas_v":
                sql = String.format(JMETLDataChecksSQL.GET_SD_SUBJECT_AREAS, Joiner.on(",").join(Ids));
                break;
            case "etl_manifestation_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_MANIFESTATION_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_work_legal_owner_dq_V":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_LEGAL_OWNER_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_work_business_model_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_BUSINESS_MODEL_DQ, Joiner.on("','").join(Ids));
                break;
            case "etl_work_access_model_dq_v":
                sql = String.format(JMETLDataChecksSQL.GET_WORK_ACCESS_MODEL_DQ, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare JM records in Transformed ETL and ETL of (.*)$")
    public void compareJMTransformETLtoETL(String ETLtable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JMContext.JMObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the jm records in Transformed Inbound and Current ..");
            for (int i = 0; i < JMContext.JMObjectsFromDL.size(); i++) {
                switch (ETLtable) {
                    case "etl_accountable_product_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));
                        int j=0;
                        String[] JMF_AccProdDQColumnName = {"getscenario_name", "getupsert", "getjm_source_reference", "getacc_prod_id", "gethfm_hierarchy_position_code", "getwork_title", "getdq_err"};
                        String[] JMF_AccProdColumn = {"getscenario_name", "getupsert", "getjm_source_reference", "getacc_prod_id", "gethfm_hierarchy_position_code", "getwork_title", "getdq_err"};
                        for (String strTemp : JMF_AccProdDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(JMF_AccProdColumn[j]);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            j++;
                        }
                        break;
                    case "etl_wwork_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));

                        String[] JMF_WworkDQColumnName = {"getscenario_name","getscenario_code","getupsert","getjm_source_reference","geteph_work_id","getwork_title","getwork_subtitle","getplanned_launch_date","getplanned_termination_date","getf_type","getf_status","getf_accountable_product","getpmc_old","getpmc_new","getf_oa_type","getf_imprint","getopco","getsubscription_type","getresp_centre"};

                        for (String strTemp : JMF_WworkDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_identifier_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_ref_new)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_ref_new));

                        String[] JMF_WorkIdentifierDQColumnName = {"getscenario_name", "getupsert", "getjm_source_ref_new", "geteph_work_id", "getwork_source_reference", "getf_type", "getidentifier", "geteffective_start_date", "getdq_err"};

                        for (String strTemp : JMF_WorkIdentifierDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_ref_new() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_subject_area_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));

                        String[] JMF_WorkSubjectAreaDQColumnName = {"getscenario", "getupsert", "getjm_source_reference", "geteph_work_id", "getwork_source_reference", "getf_type", "getidentifier", "geteffective_start_date", "getdq_err"};

                        for (String strTemp : JMF_WorkSubjectAreaDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_updates1_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getw0_chronicle_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getw0_chronicle_id));

                        String[] JMF_ManifestationUpdatesColumnName = {"getscenario", "getupsert", "getw0_chronicle_id", "getw0_journal_number", "getw0_journey_identifier", "getm0_manifestation_id", "getm0_manifestation_type", "getm0_journal_number", "getw0_issn_l", "getm0_manifestation_title", "getm0_issn", "getw0_eph_work_id", "getm0_eph_manifestation_id"};

                        for (String strTemp : JMF_ManifestationUpdatesColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("W0_CHRONICLE_ID => " + JMContext.JMObjectsFromDL.get(i).getw0_chronicle_id() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_identifier_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_ref_new)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_ref_new));

                        String[] JMF_ManifestationIdentifierDQColumnName = {"getscenario_name","getscenario_code","getupsert","getjm_source_ref_old","getjm_source_ref_new","geteph_work_id","geteph_manifestation_id","getmanifestation_source_reference","getf_type","getidentifier_old","getidentifier_new","geteffective_start_date","getdq_err"};

                        for (String strTemp : JMF_ManifestationIdentifierDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_ref_new() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_part1_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));

                        String[] JMF_ProductPart1ColumnName = {"getscenario_name","getscenario_code","getupsert","getjm_source_reference","geteph_work_id","geteph_manifestation_id","geteph_product_id","getbase_title","getw0_journal_number","getm0_journal_number","getw0_chronicle_id","getw0_journey_identifier","getm0_manifestation_type","getseparately_saleable_ind","gettrial_allowed_ind","getlaunch_date","gettax_code","getopen_access_journal_type","getsubscription","getbulk_sales","getback_files","getopen_access","getreprints","getauthor_charges","getone_off_access","getpackages","getavailability_status","getwork_status","getwork_title","getwork_type","getinternal_email_new","getinternal_email_old","getemployee_number_new","getemployee_number_old","getdq_err"};

                        for (String strTemp : JMF_ProductPart1ColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_inserts_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));

                        String[] JMF_ProductInsertsColumnName = {"getscenario", "getupsert", "getjm_source_reference", "geteph_work_id", "geteph_manifestation_id", "geteph_product_id", "getname", "getseparately_saleable_ind", "gettrial_allowed_ind", "getlaunch_date", "getf_type", "getf_status", "getf_revenue_model", "gettax_code", "getwork_type", "getdq_err"};

                        for (String strTemp : JMF_ProductInsertsColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_updates_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));

                        String[] JMF_ProductUpdatesColumnName = {"getscenario", "getupsert", "getjm_source_reference", "geteph_work_id", "geteph_manifestation_id", "geteph_product_id", "getname", "getlaunch_date", "getf_type", "getf_status", "getf_revenue_model", "gettax_code", "getwork_type", "getult_work_ref", "getdq_err"};

                        for (String strTemp : JMF_ProductUpdatesColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));

                        String[] JMF_ProductDQColumnName = {"getscenario_name","getscenario_code","getupsert","getjm_source_reference","geteph_work_id","geteph_manifestation_id","geteph_product_id","getname","getseparately_saleable_ind","gettrial_allowed_ind","getlaunch_date","getf_type","getf_status","getf_revenue_model","gettax_code","getwork_type","getmanifestation_ref","getult_work_ref","getdq_err"};

                        for (String strTemp : JMF_ProductDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_person_role_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));

                        String[] JMF_ProductPersonRoleDQColumnName = {"getscenario", "getupsert", "getjm_source_reference", "geteph_work_id", "geteph_manifestation_id", "geteph_product_id", "getf_type", "getpo_internal_email", "getpo_employee_number", "getpo_role", "getstart_date", "getend_date", "getdq_err"};

                        for (String strTemp : JMF_ProductPersonRoleDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("JM_SOURCE_REFERENCE => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "sd_subject_areas_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getsa_id)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getsa_id));

                        String[] JMF_SDSubjectAreasColumnName = {"getsa_id", "getsa_code", "getsa_name", "getsa_level", "getsa_parent_id", "getstart_date", "getend_date"};

                        for (String strTemp : JMF_SDSubjectAreasColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("SA_ID => " + JMContext.JMObjectsFromDL.get(i).getsa_id() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_reference));

                        String[] JMF_ManifestationDQColumnName = {"getscenario_name", "getupsert", "getjm_source_reference", "geteph_work_id", "geteph_manifestation_id", "getmanifestaton_key_title", "getonline_launch_date", "getf_type", "getf_status", "geteffective_start_date", "getdq_err", "getnotified_date", "getwork_source_reference"};

                        for (String strTemp : JMF_ManifestationDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("jm_source_reference => " + JMContext.JMObjectsFromDL.get(i).getjm_source_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_legal_owner_dq_V":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_external_ref)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_external_ref));

                        String[] JMF_WorkLegalOwnerDQColumnName = {"getwork_external_ref", "getlegalowner_external_ref", "getf_ownership_description", "getwork_legalowner_external_ref", "geteph_work_id", "getlegal_owner_id"};

                        for (String strTemp : JMF_WorkLegalOwnerDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("work_external_ref => " + JMContext.JMObjectsFromDL.get(i).getwork_external_ref() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_person_role_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_ref_new)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getjm_source_ref_new));

                        String[] JMF_WorkPersonRoleDQColumnName = {"getscenario_name","getscenario_code","getupsert","getjm_source_ref_new","getjm_source_ref_old","geteph_work_id","getwork_source_reference","getf_person","getemployee_number_new","getemployee_number_old","getelsevier_journal_number","getf_role","getinternal_email_new","getstart_date","getend_date","getdq_err"};

                        for (String strTemp : JMF_WorkPersonRoleDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("jm Source Ref New => " + JMContext.JMObjectsFromDL.get(i).getjm_source_ref_new() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_business_model_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] JMF_WorkBusinessModelDQColumnName = {"getscenario_name","getscenario_code","getexternal_work_ref","getexternal_reference","geteph_work_id","getaccess_model_code","getaccess_model_description"};

                        for (String strTemp : JMF_WorkBusinessModelDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("jm Source Ref New => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_work_access_model_dq_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] JMF_WorkAccessModelDQColumnName = {"getscenario_name","getscenario_code","getexternal_work_ref","getexternal_reference","geteph_work_id","getaccess_model_code","getaccess_model_description"};

                        for (String strTemp : JMF_WorkAccessModelDQColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);


                            Log.info("jm Source Ref New => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => ETL=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " TransformedETL=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }

    //Comparing Core records between Semarchy and Stiching
    @Given("^We get the (.*) random Stitching ids of (.*)$")
    public void getRandomStitchingIds(String numberOfRecords, String SemarchyTable) {
       //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (SemarchyTable) {
            case "gd_wwork":
            case "gd_work_identifier":
            case "gd_work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_STITCHING_WORK_CORE_EPR_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomWorkIds.stream().map(m -> (String) m.get("EPR_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_product":
            case "gd_person":
            case "gd_accountable_product":
                sql = String.format(JMETLDataChecksSQL.GET_STITCHING_PRODUCT_CORE_EPR_IDs, numberOfRecords);
                List<Map<?, ?>> randomProductIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductIds.stream().map(m -> (String) m.get("EPR_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_work_relationship":
                sql = String.format(JMETLDataChecksSQL.GET_GD_WORK_RELATIONSHIP_PARENT_IDs, numberOfRecords);
                List<Map<?, ?>> randomParentIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomParentIds.stream().map(m -> (String) m.get("F_PARENT")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_STITCHING_SubjectArea_EPR_IDs, numberOfRecords);
                List<Map<?, ?>> subjectAreaIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = subjectAreaIds.stream().map(m -> (String) m.get("EPR_ID")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "gd_manifestation":
            case "gd_manifestation_identifier":
                sql = String.format(JMETLDataChecksSQL.GET_GD_MANIFESTATION_IDs, numberOfRecords);
                List<Map<?, ?>> randomManifestationIdentifierIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomManifestationIdentifierIds.stream().map(m -> (String) m.get("F_WWORK")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    public void getStitchingRecords(String SemarchytableName, String workId) {
        Log.info("We get the records from Stitching..");
        switch (SemarchytableName) {
            case "gd_wwork":
            case "gd_work_identifier":
            case "gd_manifestation":
            case "gd_manifestation_identifier":
            case "gd_work_person_role":
            case "gd_work_relationship":
                sql = String.format(JMETLDataChecksSQL.GET_STITCHING_WORK_CORE, workId);
                break;
            case "gd_product":
            case "gd_person":
            case "gd_subject_area":
            case "gd_accountable_product":
                sql = String.format(JMETLDataChecksSQL.GET_STITCHING_PRODUCT_CORE, workId);
                break;
        }
        Log.info(sql);
        List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        if (SemarchytableName.equals("gd_person") || SemarchytableName.equals("gd_manifestation") || SemarchytableName.equals("gd_manifestation_identifier")) {
            JMContext.StitchingManifestationObject = new Gson().fromJson(jsonValue.get(0).get("json"), StitchManifestationObjectJson.class);
        } else {
            JMContext.StitchingWorkCoreObject = new Gson().fromJson(jsonValue.get(0).get("json"), STITCHObject.class);
        }

    }

    public void getSemarchyRecords(String Semarchytable, String workId, String ExternalRef) {
        Log.info("We get the records from Stitching..");
        switch (Semarchytable) {
            case "gd_wwork":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_WWORK, workId);
                break;
            case "gd_work_identifier":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_WORK_IDENTIFIER, workId, ExternalRef);
                break;
            case "gd_product":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_PRODUCT, workId);
                break;
            case "gd_work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_WORK_PERSON_ROLE, workId, ExternalRef);
                break;
            case "gd_work_relationship":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_WORK_RELATIONSHIP, workId);
                break;
            case "gd_person":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_PERSON, ExternalRef);
                break;
            case "gd_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_SUBJECT_Area, ExternalRef);
                break;
            case "gd_manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_MANIFESTATION, workId, ExternalRef);
                break;
            case "gd_manifestation_identifier":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_MANIFESTATION_IDENTIFIER, ExternalRef);
                break;
            case "gd_accountable_product":
                sql = String.format(JMETLDataChecksSQL.GET_SEMARCHY_GD_ACCOUNTABLE_PRODUCT, ExternalRef);
                break;
        }
        Log.info(sql);
        JMContext.JMObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.EPH_URL);
    }

    @And("^Compare Core records from (.*) with Work or Product stitching db$")
    public void compareSemarchytoStitching(String SemarchyTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        switch (SemarchyTable) {
            case "gd_work_identifier":
                for (int i = 0; i < Ids.size(); i++) {
                    String workId = Ids.get(i);
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    StitchSubIdentifierJson[] ExternalRefTemp = JMContext.StitchingWorkCoreObject.getworkCore().getidentifiers().clone();
                    ExternalRef = ExternalRefTemp[0].getidentifierType().getcode();
                    getSemarchyRecords(SemarchytableName, workId, ExternalRef);

                    Log.info("Semarchy -> F_wwork => " + JMContext.JMObjectsFromDL.get(0).getf_wwork() +
                            " Stitching_JSON -> EPR => " + JMContext.StitchingWorkCoreObject.getid());
                    if (JMContext.JMObjectsFromDL.get(0).getf_wwork() != null ||
                            (JMContext.StitchingWorkCoreObject.getid() != null)) {
                        Assert.assertEquals("The EPR => " + JMContext.JMObjectsFromDL.get(0).getf_wwork() + " is missing/not found in Work_Stitching table",
                                JMContext.JMObjectsFromDL.get(0).getf_wwork(),
                                JMContext.StitchingWorkCoreObject.getid());
                    }

                    Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_wwork() +
                            " Semarchy -> F_Type => " + JMContext.JMObjectsFromDL.get(0).getf_type() +
                            " Stitch_JSON -> Identifier_Code => " + ExternalRefTemp[0].getidentifierType().getcode());
                    if (JMContext.JMObjectsFromDL.get(0).getf_type() != null ||
                            (ExternalRefTemp[0].getidentifierType().getcode() != null)) {
                        Assert.assertEquals("The EPR => " + JMContext.JMObjectsFromDL.get(0).getf_type() + " is missing/not found in Work_Stitching table",
                                JMContext.JMObjectsFromDL.get(0).getf_type(),
                                ExternalRefTemp[0].getidentifierType().getcode());
                    }
                }
                break;
            case "gd_wwork":
                for (int i = 0; i < Ids.size(); i++) {
                    String workId = Ids.get(i);
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    getSemarchyRecords(SemarchytableName, workId, ExternalRef);
                    Log.info("Semarchy -> WORK_ID => " + JMContext.JMObjectsFromDL.get(0).getwork_id() +
                            " Stitching_JSON -> EPR => " + JMContext.StitchingWorkCoreObject.getid());
                    if (JMContext.JMObjectsFromDL.get(0).getwork_id() != null ||
                            (JMContext.StitchingWorkCoreObject.getid() != null)) {
                        Assert.assertEquals("The EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id() + " is missing/not found in Work_Stitching table",
                                JMContext.JMObjectsFromDL.get(0).getwork_id(),
                                JMContext.StitchingWorkCoreObject.getid());
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getwork_title() == null) {
                        Log.info("Work Title not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id() +
                                " Semarchy -> Work title => " + JMContext.JMObjectsFromDL.get(0).getwork_title() +
                                " Stitch_JSON -> title => " + JMContext.StitchingWorkCoreObject.getworkCore().gettitle());
                        if (JMContext.StitchingWorkCoreObject.getworkCore().gettitle() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getwork_title() != null)) {
                            Assert.assertEquals("The Work_Title is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id(),
                                    JMContext.StitchingWorkCoreObject.getworkCore().gettitle(),
                                    JMContext.JMObjectsFromDL.get(0).getwork_title());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getwork_title() == null) {
                        Log.info("copyrightYear not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id() +
                                " Semarchy -> copyrightYear => " + JMContext.JMObjectsFromDL.get(0).getcopyright_year() +
                                " Stitch_JSON -> copyrightYear => " + JMContext.StitchingWorkCoreObject.getworkCore().getcopyrightYear());
                        if (JMContext.StitchingWorkCoreObject.getworkCore().getcopyrightYear() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getcopyright_year() != null)) {
                            Assert.assertEquals("The copyrightYear is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id(),
                                    JMContext.StitchingWorkCoreObject.getworkCore().getcopyrightYear(),
                                    JMContext.JMObjectsFromDL.get(0).getcopyright_year());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getwork_title() == null) {
                        Log.info("f_type not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id() +
                                " Semarchy -> f_type => " + JMContext.JMObjectsFromDL.get(0).getf_type() +
                                " Stitch_JSON -> type code=> " + JMContext.StitchingWorkCoreObject.getworkCore().gettype().getcode());
                        if (JMContext.StitchingWorkCoreObject.getworkCore().gettype().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_type() != null)) {
                            Assert.assertEquals("The f_type is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_type(),
                                    JMContext.StitchingWorkCoreObject.getworkCore().gettype().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_type());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getwork_title() == null) {
                        Log.info("f_status not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id() +
                                " Semarchy -> f_status => " + JMContext.JMObjectsFromDL.get(0).getf_status() +
                                " Stitch_JSON -> status code => " + JMContext.StitchingWorkCoreObject.getworkCore().getstatus().getcode());
                        if (JMContext.StitchingWorkCoreObject.getworkCore().getstatus().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_status() != null)) {
                            Assert.assertEquals("The f_status is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_status(),
                                    JMContext.StitchingWorkCoreObject.getworkCore().getstatus().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_status());
                        }
                    }
                    if (JMContext.JMObjectsFromDL.get(0).getwork_title() == null) {
                        Log.info("f_pmc not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id() +
                                " Semarchy -> f_pmc => " + JMContext.JMObjectsFromDL.get(0).getf_pmc() +
                                " Stitch_JSON -> pmc code => " + JMContext.StitchingWorkCoreObject.getworkCore().getpmc().getcode());
                        if (JMContext.StitchingWorkCoreObject.getworkCore().getpmc().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_pmc() != null)) {
                            Assert.assertEquals("The f_pmc is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_pmc(),
                                    JMContext.StitchingWorkCoreObject.getworkCore().getpmc().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_pmc());
                        }
                    }
                    if (JMContext.JMObjectsFromDL.get(0).getwork_title() == null) {
                        Log.info("f_imprint not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getwork_id() +
                                " Semarchy -> f_imprint => " + JMContext.JMObjectsFromDL.get(0).getf_imprint() +
                                " Stitch_JSON -> imprint code=> " + JMContext.StitchingWorkCoreObject.getworkCore().getimprint().getcode());
                        if (JMContext.StitchingWorkCoreObject.getworkCore().getimprint().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_imprint() != null)) {
                            Assert.assertEquals("The f_imprint is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_imprint(),
                                    JMContext.StitchingWorkCoreObject.getworkCore().getimprint().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_imprint());
                        }
                    }
                }
                break;
            case "gd_product":
                for (int i = 0; i < Ids.size(); i++) {
                    String workId = Ids.get(i);
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    getSemarchyRecords(SemarchytableName, workId, ExternalRef);
                    Log.info("Semarchy -> Product_ID => " + JMContext.JMObjectsFromDL.get(0).getproduct_id() +
                            " Stitching_JSON -> EPR => " + JMContext.StitchingWorkCoreObject.getid());
                    if (JMContext.JMObjectsFromDL.get(0).getproduct_id() != null ||
                            (JMContext.StitchingWorkCoreObject.getid() != null)) {
                        Assert.assertEquals("The EPR => " + JMContext.JMObjectsFromDL.get(0).getproduct_id() + " is missing/not found in Work_Stitching table",
                                JMContext.JMObjectsFromDL.get(0).getproduct_id(),
                                JMContext.StitchingWorkCoreObject.getid());
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getname() == null) {
                        Log.info("Name not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getproduct_id() +
                                " Semarchy -> Name => " + JMContext.JMObjectsFromDL.get(0).getname() +
                                " Stitch_JSON -> Name => " + JMContext.StitchingWorkCoreObject.getproductCore().getname());
                        if (JMContext.StitchingWorkCoreObject.getproductCore().getname() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getname() != null)) {
                            Assert.assertEquals("The Name is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getname(),
                                    JMContext.StitchingWorkCoreObject.getproductCore().getname(),
                                    JMContext.JMObjectsFromDL.get(0).getname());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getshortName() == null) {
                        Log.info("shortName not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getproduct_id() +
                                " Semarchy -> shortName => " + JMContext.JMObjectsFromDL.get(0).getshortName() +
                                " Stitch_JSON -> shortName => " + JMContext.StitchingWorkCoreObject.getproductCore().getshortName());
                        if (JMContext.StitchingWorkCoreObject.getproductCore().getshortName() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getshortName() != null)) {
                            Assert.assertEquals("The shortName is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getshortName(),
                                    JMContext.StitchingWorkCoreObject.getproductCore().getshortName(),
                                    JMContext.JMObjectsFromDL.get(0).getshortName());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getf_type() == null) {
                        Log.info("F_type not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_type() +
                                " Semarchy -> F_type => " + JMContext.JMObjectsFromDL.get(0).getf_type() +
                                " Stitch_JSON -> type Code => " + JMContext.StitchingWorkCoreObject.getproductCore().gettype().getcode());
                        if (JMContext.StitchingWorkCoreObject.getproductCore().gettype().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_type() != null)) {
                            Assert.assertEquals("The F_type is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_type(),
                                    JMContext.StitchingWorkCoreObject.getproductCore().gettype().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_type());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getf_status() == null) {
                        Log.info("F_Status not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_status() +
                                " Semarchy -> F_Status => " + JMContext.JMObjectsFromDL.get(0).getf_status() +
                                " Stitch_JSON -> status Code => " + JMContext.StitchingWorkCoreObject.getproductCore().getstatus().getcode());
                        if (JMContext.StitchingWorkCoreObject.getproductCore().getstatus().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_status() != null)) {
                            Assert.assertEquals("The F_Status is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_status(),
                                    JMContext.StitchingWorkCoreObject.getproductCore().getstatus().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_status());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getf_tax_code() == null) {
                        Log.info("F_tax_code not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_tax_code() +
                                " Semarchy -> F_tax_code => " + JMContext.JMObjectsFromDL.get(0).getf_tax_code() +
                                " Stitch_JSON -> etaxProductCode Code => " + JMContext.StitchingWorkCoreObject.getproductCore().getetaxProductCode().getcode());
                        if (JMContext.StitchingWorkCoreObject.getproductCore().getetaxProductCode().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_tax_code() != null)) {
                            Assert.assertEquals("The F_Status is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_tax_code(),
                                    JMContext.StitchingWorkCoreObject.getproductCore().getetaxProductCode().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_tax_code());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getf_revenue_model() == null) {
                        Log.info("f_revenue_model not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_revenue_model() +
                                " Semarchy -> f_revenue_model => " + JMContext.JMObjectsFromDL.get(0).getf_revenue_model() +
                                " Stitch_JSON -> revenueModel Code => " + JMContext.StitchingWorkCoreObject.getproductCore().getrevenueModel().getcode());
                        if (JMContext.StitchingWorkCoreObject.getproductCore().getrevenueModel().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_revenue_model() != null)) {
                            Assert.assertEquals("The F_Status is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_revenue_model(),
                                    JMContext.StitchingWorkCoreObject.getproductCore().getrevenueModel().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_revenue_model());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getf_revenue_account() == null) {
                        Log.info("f_revenue_model not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_revenue_account() +
                                " Semarchy -> f_revenue_model => " + JMContext.JMObjectsFromDL.get(0).getf_revenue_account() +
                                " Stitch_JSON -> revenueModel Code => " + JMContext.StitchingWorkCoreObject.getproductCore().getrevenueAccount().getcode());
                        if (JMContext.StitchingWorkCoreObject.getproductCore().getrevenueAccount().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_revenue_account() != null)) {
                            Assert.assertEquals("The F_Status is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_revenue_account(),
                                    JMContext.StitchingWorkCoreObject.getproductCore().getrevenueAccount().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_revenue_account());
                        }
                    }

                }
                break;
            case "gd_work_person_role":
                for (int i = 0; i < Ids.size(); i++) {
                    int j = 0;
                    String workId = Ids.get(i);
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    StitchPersonJson[] TempData = JMContext.StitchingWorkCoreObject.getworkCore().getworkPersons().clone();
                    ExternalRef = TempData[j].getrole().getcode();
                    getSemarchyRecords(SemarchytableName, workId, ExternalRef);
                    Log.info("Semarchy -> F_WWORK => " + JMContext.JMObjectsFromDL.get(0).getf_wwork() +
                            " Stitching_JSON -> EPR => " + JMContext.StitchingWorkCoreObject.getid());
                    if (JMContext.JMObjectsFromDL.get(0).getf_wwork() != null ||
                            (JMContext.StitchingWorkCoreObject.getid() != null)) {
                        Assert.assertEquals("The EPR => " + JMContext.JMObjectsFromDL.get(0).getf_wwork() + " is missing/not found in Work_Stitching table",
                                JMContext.JMObjectsFromDL.get(0).getf_wwork(),
                                JMContext.StitchingWorkCoreObject.getid());
                    }

                    if (j < TempData.length) {
                        while (!JMContext.JMObjectsFromDL.get(0).getf_person().equals(TempData[j].getperson().getid())) {
                            j++;
                        }
                    }
                    if (JMContext.JMObjectsFromDL.get(0).getf_person() == null) {
                        Log.info("Person ID not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_person() +
                                " Semarchy -> Person ID => " + JMContext.JMObjectsFromDL.get(0).getf_person() +
                                " Stitch_JSON -> Person ID => " + TempData[j].getperson().getid());
                        if (TempData[j].getperson().getid() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_person() != null)) {
                            Assert.assertEquals("The Person ID is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_person(),
                                    TempData[j].getperson().getid(),
                                    JMContext.JMObjectsFromDL.get(0).getf_person());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getf_role() == null) {
                        Log.info("Role Code not available");
                    } else {
                        Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_role() +
                                " Semarchy -> Role Code => " + JMContext.JMObjectsFromDL.get(0).getf_role() +
                                " Stitch_JSON -> Role Code => " + TempData[0].getrole().getcode());
                        if (TempData[0].getrole().getcode() != null ||
                                (JMContext.JMObjectsFromDL.get(0).getf_role() != null)) {
                            Assert.assertEquals("The Role Code is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_role(),
                                    TempData[0].getrole().getcode(),
                                    JMContext.JMObjectsFromDL.get(0).getf_role());
                        }
                    }

                }
                break;
            case "gd_work_relationship":
                for (int i = 0; i < Ids.size(); i++) {
                    int j = 0;
                    String workId = Ids.get(j);
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    try {
                        StitchPersonJson[] TempData2 = JMContext.StitchingWorkCoreObject.getworkCore().getworkRelationships().getworkParent().clone();
                        ExternalRef = TempData2[j].getid();
                        getSemarchyRecords(SemarchytableName, workId, ExternalRef);
                        StitchPersonJson[] TempData = JMContext.StitchingWorkCoreObject.getworkCore().getworkRelationships().getworkChild().clone();
                        Log.info("Semarchy -> F_parent => " + JMContext.JMObjectsFromDL.get(0).getf_parent() +
                                " Stitching_JSON -> EPR => " + JMContext.StitchingWorkCoreObject.getid());
                        if (JMContext.JMObjectsFromDL.get(0).getf_parent() != null ||
                                (JMContext.StitchingWorkCoreObject.getid() != null)) {
                            Assert.assertEquals("The EPR => " + JMContext.JMObjectsFromDL.get(0).getf_parent() + " is missing/not found in Work_Stitching table",
                                    JMContext.JMObjectsFromDL.get(0).getf_parent(),
                                    JMContext.StitchingWorkCoreObject.getid());
                        }

                        if (j < TempData.length) {
                            while (!JMContext.JMObjectsFromDL.get(0).getf_child().equals(TempData[j].getid())) {
                                j++;
                            }
                        }
                        if (JMContext.JMObjectsFromDL.get(0).getf_child() == null) {
                            Log.info("f_CHILD not available");
                        } else {
                            Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getf_child() +
                                    " Semarchy -> f_CHILD ID => " + JMContext.JMObjectsFromDL.get(0).getf_child() +
                                    " Stitch_JSON -> f_CHILD ID => " + TempData[j].getid());
                            if (TempData[j].getid() != null ||
                                    (JMContext.JMObjectsFromDL.get(0).getf_child() != null)) {
                                Assert.assertEquals("The f_CHILD ID is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getf_role(),
                                        TempData[j].getid(),
                                        JMContext.JMObjectsFromDL.get(0).getf_child());
                            }
                        }
                    }catch(Exception e){Log.info("No data for this record");}
                    }
                break;
            case "gd_person":
                for (int i = 0; i < Ids.size(); i++) {
                    String workId = Ids.get(i);
                    int j = 0;
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    StitchPersonJson[] TempData = JMContext.StitchingManifestationObject.getmanifestation().getwork().getworkCore().getworkPersons().clone();
                    ExternalRef = TempData[j].getperson().getid();
                    getSemarchyRecords(SemarchytableName, workId, ExternalRef);

                    if (JMContext.JMObjectsFromDL.get(0).getperson_id() == null) {
                        Log.info("PERSON_ID not available");
                    } else {
                        Log.info("Semarchy -> Person Id => " + JMContext.JMObjectsFromDL.get(0).getperson_id() +
                                " Stitching_JSON -> Person Id => " + TempData[j].getperson().getid());
                        if (JMContext.JMObjectsFromDL.get(0).getperson_id() != null ||
                                (TempData[j].getperson().getid() != null)) {
                            Assert.assertEquals("The Person_ID => " + JMContext.JMObjectsFromDL.get(0).getperson_id() + " is missing/not found in Work_Stitching table",
                                    JMContext.JMObjectsFromDL.get(0).getperson_id(),
                                    TempData[j].getperson().getid());
                        }

                        if (JMContext.JMObjectsFromDL.get(0).getgiven_name() == null) {
                            Log.info("Given_Name not available");
                        } else {
                            Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getperson_id() +
                                    " Semarchy -> Given_Name => " + JMContext.JMObjectsFromDL.get(0).getgiven_name() +
                                    " Stitch_JSON -> Given_Name => " + TempData[j].getperson().getfirstName());
                            if (TempData[j].getid() != null ||
                                    (JMContext.JMObjectsFromDL.get(0).getgiven_name() != null)) {
                                Assert.assertEquals("The Given_Name is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getperson_id(),
                                        TempData[j].getperson().getfirstName(),
                                        JMContext.JMObjectsFromDL.get(0).getgiven_name());
                            }
                        }

                        if (JMContext.JMObjectsFromDL.get(0).getfamily_name() == null) {
                            Log.info("Family_Name not available");
                        } else {
                            Log.info("EPR => " + JMContext.JMObjectsFromDL.get(0).getperson_id() +
                                    " Semarchy -> Family_Name => " + JMContext.JMObjectsFromDL.get(0).getfamily_name() +
                                    " Stitch_JSON -> Family_Name => " + TempData[j].getperson().getlastName());
                            if (TempData[j].getid() != null ||
                                    (JMContext.JMObjectsFromDL.get(0).getfamily_name() != null)) {
                                Assert.assertEquals("The Given_Name is incorrect for EPR => " + JMContext.JMObjectsFromDL.get(0).getperson_id(),
                                        TempData[j].getperson().getlastName(),
                                        JMContext.JMObjectsFromDL.get(0).getfamily_name());
                            }
                        }
                    }
                }
                break;
            case "gd_subject_area":
                for (int i = 0; i < Ids.size(); i++) {
                    int j = 0;
                    String workId = Ids.get(i);
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    try {
                        StitchPersonJson[] TempData = JMContext.StitchingWorkCoreObject.getmanifestation().getwork().getworkCore().getworkSubjectAreas().clone();
                        ExternalRef = TempData[j].getsubjectArea().getcode();
                        getSemarchyRecords(SemarchytableName, workId, ExternalRef);
                        Log.info("Semarchy -> SubjectArea Code => " + JMContext.JMObjectsFromDL.get(0).getcode() +
                                " Stitching_JSON -> SubjectArea Code => " + TempData[j].getsubjectArea().getcode());
                        if (JMContext.JMObjectsFromDL.get(0).getcode() != null ||
                                (TempData[j].getsubjectArea().getcode() != null)) {
                            Assert.assertEquals("The SubjectAreaId => " + JMContext.JMObjectsFromDL.get(0).getsubject_area_id() + " is missing/not found in Work_Stitching table",
                                    JMContext.JMObjectsFromDL.get(0).getcode(),
                                    TempData[j].getsubjectArea().getcode());
                        }

                        if (JMContext.JMObjectsFromDL.get(0).getname() == null) {
                            Log.info("Name not available");
                        } else {
                            Log.info("SubjectArea ID => " + JMContext.JMObjectsFromDL.get(0).getsubject_area_id() +
                                    " Semarchy -> Name => " + JMContext.JMObjectsFromDL.get(0).getname() +
                                    " Stitch_JSON -> Name => " + TempData[j].getsubjectArea().getcode());
                            if (TempData[j].getsubjectArea().getcode() != null ||
                                    (JMContext.JMObjectsFromDL.get(0).getname() != null)) {
                                Assert.assertEquals("The Name is incorrect for EPR => " + JMContext.StitchingWorkCoreObject.getid(),
                                        TempData[j].getsubjectArea().getname(),
                                        JMContext.JMObjectsFromDL.get(0).getname());
                            }
                        }

                        if (JMContext.JMObjectsFromDL.get(0).getf_type() == null) {
                            Log.info("Name not available");
                        } else {
                            Log.info("SubjectArea ID => " + JMContext.JMObjectsFromDL.get(0).getsubject_area_id() +
                                    " Semarchy -> Name => " + JMContext.JMObjectsFromDL.get(0).getf_type() +
                                    " Stitch_JSON -> Name => " + TempData[j].getsubjectArea().gettype().getcode());
                            if (TempData[j].getsubjectArea().gettype().getcode() != null ||
                                    (JMContext.JMObjectsFromDL.get(0).getf_type() != null)) {
                                Assert.assertEquals("The Name is incorrect for EPR => " + JMContext.StitchingWorkCoreObject.getid(),
                                        TempData[j].getsubjectArea().gettype().getcode(),
                                        JMContext.JMObjectsFromDL.get(0).getf_type());
                            }
                        }
                    } catch (Exception e) {
                        Log.info("Stitch Record " + JMContext.StitchingWorkCoreObject.getid() + " Has no Subject Area");
                    }
                }
                break;
            case "gd_manifestation":
                for (int i = 0; i < Ids.size(); i++) {
                    String workId = Ids.get(i);
                    int j = 0;
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    STITCHObject[] TempData = JMContext.StitchingManifestationObject.getmanifestations().clone();
                    ExternalRef = TempData[j].getmanifestationCore().gettype().getcode();
                    getSemarchyRecords(SemarchytableName, workId, ExternalRef);

                    if (JMContext.JMObjectsFromDL.get(0).getf_type() == null) {
                        Log.info("F_TYPE not available");
                    } else {
                        Log.info("Semarchy -> F_type => " + JMContext.JMObjectsFromDL.get(0).getf_type() +
                                " Stitching_JSON -> F_type => " + TempData[j].getmanifestationCore().gettype().getcode());
                        if (JMContext.JMObjectsFromDL.get(0).getf_type() != null ||
                                (TempData[j].getmanifestationCore().gettype().getcode() != null)) {
                            Assert.assertEquals("The ManifestationID => " + JMContext.JMObjectsFromDL.get(0).getmanifestation_id() + " is missing/not found in Work_Stitching table",
                                    JMContext.JMObjectsFromDL.get(0).getf_type(),
                                    TempData[j].getmanifestationCore().gettype().getcode());
                        }
                    }

                    if (JMContext.JMObjectsFromDL.get(0).getf_status() == null) {
                        Log.info("F_STATUS not available");
                    } else {
                        Log.info("Semarchy -> F_STATUS => " + JMContext.JMObjectsFromDL.get(0).getf_status() +
                                " Stitching_JSON -> F_STATUS => " + TempData[j].getmanifestationCore().getstatus().getcode());
                        if (JMContext.JMObjectsFromDL.get(0).getf_status() != null ||
                                (TempData[j].getmanifestationCore().getstatus().getcode() != null)) {
                            Assert.assertEquals("The ManifestationID => " + JMContext.JMObjectsFromDL.get(0).getmanifestation_id() + " is missing/not found in Work_Stitching table",
                                    JMContext.JMObjectsFromDL.get(0).getf_status(),
                                    TempData[j].getmanifestationCore().getstatus().getcode());
                        }
                    }
                }
                break;
            case "gd_manifestation_identifier":
                for (int i = 0; i < Ids.size(); i++) {
                    String workId = Ids.get(i);
                    int j = 0;
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    STITCHObject[] TempData = JMContext.StitchingManifestationObject.getmanifestations().clone();
                    ExternalRef = TempData[j].getid();
                    getSemarchyRecords(SemarchytableName, workId, ExternalRef);

                    if (JMContext.JMObjectsFromDL.size() == 0) {
                        Log.info("Item not in scope");
                    } else {
                        if (JMContext.JMObjectsFromDL.get(0).getf_manifestation() == null) {
                            Log.info("F_MANIFESTATION not available");
                        } else {
                            Log.info("Semarchy -> F_MANIFESTATION => " + JMContext.JMObjectsFromDL.get(0).getf_manifestation() +
                                    " Stitching_JSON -> F_MANIFESTATION=> " + TempData[j].getid());
                            if (JMContext.JMObjectsFromDL.get(0).getf_manifestation() != null ||
                                    (TempData[j].getid() != null)) {
                                Assert.assertEquals("The ManifestationID => " + JMContext.JMObjectsFromDL.get(0).getmanifestation_identifier_id() + " is missing/not found in Work_Stitching table",
                                        JMContext.JMObjectsFromDL.get(0).getf_manifestation(),
                                        TempData[j].getid());
                            }
                        }

                        StitchSubIdentifierJson[] Tempdata2 = TempData[j].getmanifestationCore().getidentifiers().clone();
                        if (JMContext.JMObjectsFromDL.get(0).getf_manifestation() == null) {
                            Log.info("F_TYPE not available");
                        } else {
                            Log.info("Semarchy -> F_TYPE => " + JMContext.JMObjectsFromDL.get(0).getf_type() +
                                    " Stitching_JSON -> F_TYPE=> " + Tempdata2[j].getidentifierType().getcode());
                            if (JMContext.JMObjectsFromDL.get(0).getf_type() != null ||
                                    (Tempdata2[j].getidentifierType().getcode() != null)) {
                                Assert.assertEquals("The ManifestationID => " + JMContext.JMObjectsFromDL.get(0).getmanifestation_identifier_id() + " is missing/not found in Work_Stitching table",
                                        JMContext.JMObjectsFromDL.get(0).getf_type(),
                                        Tempdata2[j].getidentifierType().getcode());
                            }
                        }

                        if (Tempdata2[j].getidentifier() == null) {
                            Log.info("IDENTIFIER not available");
                        } else {
                            Log.info("Semarchy -> IDENTIFIER => " + JMContext.JMObjectsFromDL.get(0).getidentifier() +
                                    " Stitching_JSON -> IDENTIFIER=> " + Tempdata2[j].getidentifier());
                            if (JMContext.JMObjectsFromDL.get(0).getidentifier() != null ||
                                    (Tempdata2[j].getidentifier() != null)) {
                                Assert.assertEquals("The ManifestationID => " + JMContext.JMObjectsFromDL.get(0).getmanifestation_identifier_id() + " is missing/not found in Work_Stitching table",
                                        JMContext.JMObjectsFromDL.get(0).getidentifier(),
                                        Tempdata2[j].getidentifier());
                            }
                        }
                    }
                }
                    break;
            case "gd_accountable_product":
                for (int i = 0; i < Ids.size(); i++) {
                    int j = 0;
                    String workId = Ids.get(i);
                    String SemarchytableName = SemarchyTable;
                    getStitchingRecords(SemarchytableName, workId);
                    try{
                    ExternalRef = JMContext.StitchingWorkCoreObject.getmanifestation().getwork().getworkCore().getaccountableProduct().getglProductSegmentCode();
                    getSemarchyRecords(SemarchytableName, workId, ExternalRef);

                    if (JMContext.JMObjectsFromDL.get(0).getgl_product_segment_code() == null) {
                        Log.info("Segment Code not available");
                    } else {
                        Log.info("Semarchy -> SEGMENT CODE => " + JMContext.JMObjectsFromDL.get(0).getgl_product_segment_code() +
                                " Stitching_JSON -> SEGMENT CODE => " + JMContext.StitchingWorkCoreObject.getmanifestation().getwork().getworkCore().getaccountableProduct().getglProductSegmentCode());
                        if (JMContext.JMObjectsFromDL.get(0).getgl_product_segment_code() != null ||
                                (JMContext.StitchingWorkCoreObject.getmanifestation().getwork().getworkCore().getaccountableProduct().getglProductSegmentCode() != null)) {
                            Assert.assertEquals("The SubjectAreaId => " + JMContext.JMObjectsFromDL.get(0).getaccountable_product_id() + " is missing/not found in Work_Stitching table",
                                    JMContext.JMObjectsFromDL.get(0).getgl_product_segment_code(),
                                    JMContext.StitchingWorkCoreObject.getmanifestation().getwork().getworkCore().getaccountableProduct().getglProductSegmentCode());
                        }

                        if (JMContext.JMObjectsFromDL.get(0).getgl_product_segment_name() == null) {
                            Log.info("Segment Name not available");
                        } else {
                            Log.info("Accountable Product ID => " + JMContext.JMObjectsFromDL.get(0).getaccountable_product_id() +
                                    " Semarchy -> Segment Name => " + JMContext.JMObjectsFromDL.get(0).getgl_product_segment_name() +
                                    " Stitch_JSON -> Segment Name => " + JMContext.StitchingWorkCoreObject.getmanifestation().getwork().getworkCore().getaccountableProduct().getglProductSegmentName());
                            if (JMContext.StitchingWorkCoreObject.getmanifestation().getwork().getworkCore().getaccountableProduct().getglProductSegmentName() != null ||
                                    (JMContext.JMObjectsFromDL.get(0).getgl_product_segment_name() != null)) {
                                Assert.assertEquals("The Segment Name is incorrect for Accountable Product => " + JMContext.JMObjectsFromDL.get(0).getaccountable_product_id(),
                                        JMContext.StitchingWorkCoreObject.getmanifestation().getwork().getworkCore().getaccountableProduct().getglProductSegmentName(),
                                        JMContext.JMObjectsFromDL.get(0).getgl_product_segment_name());
                            }
                        }
                    }
                    }catch(Exception e){Log.info("No Accountable Product data for this record");}
                }
                break;
                }
        }


    @Given ("^We get the count of all core views (.*)$")
    public void getCountfromAllCore(String table){
        switch (table) {
            case "all_manifestation_identifiers_v":
                Log.info("Getting Dl all_manifestation_identifiers_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_MANIFESTATION_IDENTIFIERS_COUNT;
                break;
            case "all_work_identifier_v":
                Log.info("Getting Dl all_work_identifier_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_WORK_IDENTIFIER_COUNT;
                break;
            case "all_product_v":
                Log.info("Getting Dl all_product_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_PRODUCT_COUNT;
                break;
            case "all_work_subject_areas_v":
                Log.info("Getting Dl all_work_subject_areas_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_SUBJECT_AREA_COUNT;
                break;
            case "all_accountable_product_v":
                Log.info("Getting Dl all_accountable_product_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_SUBJECT_AREA_COUNT;
                break;
            case "all_manifestation_v":
                Log.info("Getting Dl all_manifestation_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_MANIFESTATION_COUNT;
                break;
            case "all_person_v":
                Log.info("Getting Dl all_person_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_PERSON_COUNT;
                break;
            case "all_work_person_role_v":
                Log.info("Getting Dl all_work_person_role_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_WORK_PERSON_ROLE_COUNT;
                break;
            case "all_work_relationship_v":
                Log.info("Getting Dl all_work_relationship_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_WORK_RELATIONSHIP_COUNT;
                break;
            case "all_work_v":
                Log.info("Getting Dl all_work_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_WORK_COUNT;
                break;
            case "all_work_access_model_v":
                Log.info("Getting Dl all_work_access_model_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_WORK_ACCESS_MODEL_COUNT;
                break;
            case "all_work_business_model_v":
                Log.info("Getting Dl all_work_business_model_v Count...");
                dlAllCoreSQLViewCount = JMETLDataChecksSQL.GET_ALL_WORK_BUSINESS_MODEL_COUNT;
                break;

        }
        Log.info(dlAllCoreSQLViewCount);
        List<Map<String, Object>> dlAllCoreViewTableCount = DBManager.getDBResultMap(dlAllCoreSQLViewCount, Constants.AWS_URL);
        DLAllCoreViewCount = ((Long) dlAllCoreViewTableCount.get(0).get("Source_Count")).intValue();
    }

    @Then("^Get the count of Gd tables (.*)$")
    public void getCountGdTables(String SemarchyTable) {
        switch (SemarchyTable) {
            case "gd_manifestation_identifier":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_MANIFESTATION_IDENTIFIER_COUNT;
                break;
            case "gd_manifestation":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_MANIFESTATION_COUNT;
                break;
            case "gd_person":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_PERSON_COUNT;
                break;
            case "gd_product":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_PRODUCT_COUNT;
                break;
            case "gd_work_identifier":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_WORK_IDENTIFIER_COUNT;
                break;
            case "gd_work_person_role":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_WORK_PERSON_ROLE_COUNT;
                break;
            case "gd_work_relationship":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_WORK_RELATIONSHIP_COUNT;
                break;
            case "gd_subject_area":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_SUBJECT_AREA_COUNT;
                break;
            case "gd_wwork":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_WWORK_COUNT;
                break;
            case "gd_accountable_product":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_ACCOUNTABLE_PRODUCT_COUNT;
                break;
            case "gd_work_access_model":
                gdSQLViewCount = JMETLDataChecksSQL.GET_GD_WORK_ACCESS_MODEL_COUNT;
                break;
            case "gd_work_business_model":
                gdSQLViewCount =JMETLDataChecksSQL.GET_GD_WORK_BUSINESS_MODEL_COUNT;
                break;
        }
        Log.info(gdSQLViewCount);
        List<Map<String, Object>> gdTableCount = DBManager.getDBResultMap(gdSQLViewCount, Constants.EPH_URL);
        gdCount = ((Long) gdTableCount.get(0).get("Target_Count")).intValue();
    }

    @And("^we compare count of All core Views (.*) and gd tables (.*) are identical$")
    public void compareGdandCoreCounts(String srctable, String trgtTable){
        Log.info("The count for All core view "+srctable+" => " + DLAllCoreViewCount + " and in "+trgtTable+"  => " + gdCount);
        Assert.assertEquals("The counts are not equal when compared with "+srctable+" and "+trgtTable,DLAllCoreViewCount, gdCount);
    }


    @Given("^We get the (.*) random JM Staging ids of (.*)$")
    public void getRandomSemarchySourceIds(String numberOfRecords, String SourceTable) {
     numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (SourceTable) {
            case "all_manifestation_identifiers_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_MANIFESTATION_IDENTIFIERS_IDs,SourceTable, numberOfRecords);
                List<Map<?, ?>> randomManifestationIdentifierIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomManifestationIdentifierIds.stream().map(m -> (String) m.get("IDENTIFIER")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "all_work_identifier_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK_IDENTIFIER_IDs, numberOfRecords);
                List<Map<?, ?>> randomWorkIdentifierIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkIdentifierIds.stream().map(m -> (String) m.get("IDENTIFIER")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "all_product_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_PRODUCT_IDs, SourceTable, numberOfRecords);
                List<Map<?, ?>> randomproductIdentifierIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomproductIdentifierIds.stream().map(m -> (String) m.get("EXTERNAL_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "all_work_subject_areas_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_SUBJECT_AREA_IDs, numberOfRecords);
                List<Map<?, ?>> randomSubjectAreaIdentifierIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomSubjectAreaIdentifierIds.stream().map(m -> (String) m.get("EXTERNAL_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "all_accountable_product_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK_SOURCE_REFERENCE_IDs, SourceTable, numberOfRecords);
                List<Map<?, ?>> randomWorkSourceIdentifierIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkSourceIdentifierIds.stream().map(m -> (String) m.get("EXTERNAL_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
                break;
            default:
                sql = String.format(JMETLDataChecksSQL.GET_ALL_External_Reference_IDs, SourceTable, numberOfRecords);
                List<Map<?, ?>> randomExternalReferenceIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomExternalReferenceIds.stream().map(m -> (String) m.get("EXTERNAL_REFERENCE")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the JM Staging records from (.*)$")
    public void getRecordsSemarchySource(String SourceTable) {
        Log.info("We get the records from jm Access..");
        switch (SourceTable) {
            case "all_manifestation_identifiers_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_MANIFESTATION, Joiner.on("','").join(Ids));
                break;
            case "all_person_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_PERSON, Joiner.on("','").join(Ids));
                break;
            case "all_product_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_PRODUCT, Joiner.on("','").join(Ids));
                break;
            case "all_work_identifier_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "all_work_person_role_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK_PERSON_ROLE, Joiner.on("','").join(Ids));
                break;
            case "all_work_relationship_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK_RELATIONSHIP, Joiner.on("','").join(Ids));
                break;
            case "all_work_subject_areas_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK_SUBJECT_AREAS, Joiner.on("','").join(Ids));
                break;
            case "all_work_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK, Joiner.on("','").join(Ids));
                break;
            case "all_accountable_product_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_ACCOUNTABLE_PRODUCT, Joiner.on("','").join(Ids));
                break;
            case "all_work_access_model_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK_ACCESS_MODEL, Joiner.on("','").join(Ids));
                break;
            case "all_work_business_model_v":
                sql = String.format(JMETLDataChecksSQL.GET_ALL_WORK_BUSINESS_MODEL, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.AWS_URL);
    }

    @Then("^We get the JM Semarchy records from (.*)$")
    public void getRecordsSemarchyRecords(String SemarchyTable) {
        Log.info("We get the Current records..");
        switch (SemarchyTable) {
            case "gd_manifestation_identifier":
                sql = String.format(JMETLDataChecksSQL.GET_GD_MANIFESTATION_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_manifestation":
                sql = String.format(JMETLDataChecksSQL.GET_GD_MANIFESTATION, Joiner.on("','").join(Ids));
                break;
            case "gd_person":
                sql = String.format(JMETLDataChecksSQL.GET_GD_PERSON, Joiner.on("','").join(Ids));
                break;
            case "gd_product":
                sql = String.format(JMETLDataChecksSQL.GET_GD_PRODUCT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_identifier":
                sql = String.format(JMETLDataChecksSQL.GET_GD_WORK_IDENTIFIER, Joiner.on("','").join(Ids));
                break;
            case "gd_work_person_role":
                sql = String.format(JMETLDataChecksSQL.GET_GD_WORK_PERSON_ROLE, Joiner.on("','").join(Ids));
                break;
            case "gd_work_relationship":
                sql = String.format(JMETLDataChecksSQL.GET_GD_WORK_RELATIONSHIP, Joiner.on("','").join(Ids));
                break;
            case "gd_subject_area":
                sql = String.format(JMETLDataChecksSQL.GET_GD_SUBJECT_AREA, Joiner.on("','").join(Ids));
                break;
            case "gd_wwork":
                sql = String.format(JMETLDataChecksSQL.GET_GD_WWORK, Joiner.on("','").join(Ids));
                break;
            case "gd_accountable_product":
                sql = String.format(JMETLDataChecksSQL.GET_GD_ACCOUNTABLE_PRODUCT, Joiner.on("','").join(Ids));
                break;
            case "gd_work_access_model":
                sql = String.format(JMETLDataChecksSQL.GET_GD_WORK_ACCESS_MODEL, Joiner.on("','").join(Ids));
                break;
            case "gd_work_business_model":
                sql = String.format(JMETLDataChecksSQL.GET_GD_WORK_BUSINESS_MODEL, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        JMContext.JMTransformObjectsFromDL = DBManager.getDBResultAsBeanList(sql, JMETLObject.class, Constants.EPH_URL);
    }

    @And("^Compare JM records in Staging and Semarchy of (.*)$")
    public void compareSemarchySourcetoSemarchy(String SourceTable) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (JMContext.JMObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare Records in Core and Semarchy ..");
            for (int i = 0; i < JMContext.JMObjectsFromDL.size(); i++) {
                System.out.println(i);
                switch (SourceTable) {
                    case "all_manifestation_identifiers_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getidentifier)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getidentifier));

                        String[] Manifestation_IdentifierColumnName = {"getidentifier", "getf_type"};

                        for (String strTemp : Manifestation_IdentifierColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("Identifier => " + JMContext.JMObjectsFromDL.get(i).getidentifier() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in GD Tables",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "all_manifestation_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] ManifestationColumnName = {"getexternal_reference","getf_type","getf_status","getmanifestation_key_title"};
                        for (String strTemp : ManifestationColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);



                            Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "all_person_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] PersonColumnName = {"getexternal_reference","getgiven_name","getfamily_name"};

                        for (String strTemp : PersonColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);



                            Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "all_product_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] ProductColumnName = {"getexternal_reference","getname","getshort_name","getseparately_sale_indicatore","gettrial_allowed_indicator","getrestricted_sale_indicator","getf_type","getf_status","getf_tax_code","getf_revenue_model"};
                        int p=0;
                        for (String strTemp : ProductColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                               Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                       " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                       " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                               if (method.invoke(objectToCompare1) != null ||
                                       (method2.invoke(objectToCompare2) != null)) {
                                   Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                           method.invoke(objectToCompare1),
                                           method2.invoke(objectToCompare2));
                               }
                               p++;
                           }
                        break;
                    case "all_work_identifier_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getidentifier)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getidentifier));

                        String[] WorkIdentifierColumnName = {"getidentifier","getf_type"};

                        for (String strTemp : WorkIdentifierColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("Identifier => " + JMContext.JMObjectsFromDL.get(i).getidentifier() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "all_work_person_role_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] WorkPersonRoleColumnName = {"getexternal_reference","getf_role"};

                        for (String strTemp : WorkPersonRoleColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "all_work_relationship_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] WorkRelationshipColumnName = {"getexternal_reference"};
                        for (String strTemp : WorkRelationshipColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "all_work_subject_areas_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] WorkSubjectAreaColumnName = {"getexternal_reference","getf_role"};

                        for (String strTemp : WorkSubjectAreaColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "all_work_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] WorkColumnName = {"getexternal_reference","getwork_title","getwork_sub_title","getelectro_rights_indicator","getf_legal_ownership"};
                        String[] WorkSemarchySourceColumnName = {"getexternal_reference","getwork_title","getwork_subtitle","getelectro_rights_indicator","getf_legal_ownership"};
                        int j =0;
                        for (String strTemp : WorkColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(WorkSemarchySourceColumnName[j]);

                            Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + WorkSemarchySourceColumnName[j] + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            j++;
                        }
                        break;
                    case "all_accountable_product_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getwork_source_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getgl_product_segment_code));

                        String[] AccountableProductColumnName = {"getwork_source_reference","getgl_product_segment_code","getgl_product_segment_name","getf_gl_product_segment_parent"};
                        String[] AccountableProduct2ColumnName = {"getgl_product_segment_code","getgl_product_segment_code","getgl_product_segment_name","getf_gl_product_segment_parent"};
                        int ap=0;
                        for (String strTemp : AccountableProductColumnName) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(AccountableProduct2ColumnName[ap]);

                            Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + strTemp + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            ap++;
                        }
                        break;
                    case "all_work_access_model_v":
                        JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                        JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                        String[] WorkAccessModelColumnName = {"getexternal_reference","getaccess_model_code"};
                        String[] WorkAccessModelSemarchySourceColumnName = {"getexternal_reference_core","getf_access_model"};
                        int w =0;
                        for (String strTemp : WorkAccessModelColumnName) {

                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                            JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(WorkAccessModelSemarchySourceColumnName[w]);

                            Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                    " " + WorkAccessModelSemarchySourceColumnName[w] + " => Core=" + method.invoke(objectToCompare1) +
                                    " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                            w++;
                        }
                        break;
                    case "all_work_business_model_v":
                    JMContext.JMObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference)); //sort data in the lists
                    JMContext.JMTransformObjectsFromDL.sort(Comparator.comparing(JMETLObject::getexternal_reference));

                    String[] WorkBusinessModelColumnName = {"getexternal_reference","getbusiness_model_code"};
                    String[] WorkBusinessModelSemarchySourceColumnName = {"getexternal_reference","getf_business_model"};
                    int a =0;
                    for (String strTemp : WorkBusinessModelColumnName) {

                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        JMETLObject objectToCompare1 = JMContext.JMObjectsFromDL.get(i);
                        JMETLObject objectToCompare2 = JMContext.JMTransformObjectsFromDL.get(i);

                        method = objectToCompare1.getClass().getMethod(WorkBusinessModelSemarchySourceColumnName[a]);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("External_Reference => " + JMContext.JMObjectsFromDL.get(i).getexternal_reference() +
                                " " + WorkBusinessModelSemarchySourceColumnName[a] + " => Core=" + method.invoke(objectToCompare1) +
                                " " + strTemp + " Semarchy=" + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Data Lake",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                        a++;
                    }
                    break;
                }
            }
        }
    }

    @Given("^We know the number of JM (.*) data in inbound")
    public void getJMMYSQLCount(String tableName) {
        switch (tableName){
            case "jmf_allocation_change":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_allocation_change_Count;
                break;
            case "jmf_application_properties":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_application_properties_Count;
                break;
            case "jmf_approval_attachment":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_approval_attachment_Count;
                break;
            case "jmf_approval_request":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_approval_request_Count;
                break;
            case "jmf_chronicle_scenario":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_chronicle_scenario_Count;
                break;
            case "jmf_chronicle_status":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_chronicle_status_Count;
                break;
            case "jmf_family_resource_details":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_family_resource_details_Count;
                break;
            case "jmf_financial_information":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_financial_information_Count;
                break;
            case "jmf_legal_information":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_legal_information_Count;
                break;
            case "jmf_manifestation_electronic_details":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_manifestation_electronic_details_Count;
                break;
            case "jmf_manifestation_print_details":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_manifestation_print_details_Count;
                break;
            case "jmf_party_in_product":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_party_in_product_Count;
                break;
            case "jmf_product_availability":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_availability_Count;
                break;
            case "jmf_product_chronicle":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_chronicle_Count;
                break;
            case "jmf_product_family":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_family_Count;
                break;
            case "jmf_product_manifestation":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_manifestation_Count;
                break;
            case "jmf_product_subject_area":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_subject_area_Count;
                break;
            case "jmf_product_work":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_product_work_Count;
                break;
            case "jmf_production_information":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_production_information_Count;
                break;
            case "jmf_review_comment":
                sqlJM = JMtoDataLakeCountChecksSQL.jmf_review_comment_Count;
                break;
        }
        Log.info(sqlJM);
        List<Map<String, Object>> JMTableCount = DBManager.getDBResultMap(sqlJM, Constants.MYSQL_JM_URL);
        JMCount = ((Long) JMTableCount.get(0).get("count")).intValue();
        Log.info(tableName + " in JM Count: " + JMCount);
    }

    @When("^The JM (.*) data is in the current$")
    public void getJMDLLCount(String tableName) {
        switch (tableName){
           case "jmf_allocation_change":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_allocation_change_Count;
                break;
           case "jmf_application_properties":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_application_properties_Count;
                break;
            case "jmf_approval_attachment":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_approval_attachment_Count;
                break;
            case "jmf_approval_request":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_approval_request_Count;
                break;
            case "jmf_chronicle_scenario":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_chronicle_scenario_Count;
                break;
            case "jmf_chronicle_status":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_chronicle_status_Count;
                break;
           case "jmf_family_resource_details":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_family_resource_details_Count;
                break;
            case "jmf_financial_information":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_financial_information_Count;
                break;
            case "jmf_legal_information":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_legal_information_Count;
                break;
            case "jmf_manifestation_electronic_details":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_manifestation_electronic_details_Count;
                break;
            case "jmf_manifestation_print_details":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_manifestation_print_details_Count;
                break;
            case "jmf_party_in_product":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_party_in_product_Count;
                break;
            case "jmf_product_availability":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_availability_Count;
                break;
            case "jmf_product_chronicle":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_chronicle_Count;
                break;
            case "jmf_product_family":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_family_Count;
                break;
            case "jmf_product_manifestation":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_manifestation_Count;
                break;
            case "jmf_product_subject_area":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_subject_area_Count;
                break;
            case "jmf_product_work":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_product_work_Count;
                break;
            case "jmf_production_information":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_production_information_Count;
                break;
            case "jmf_review_comment":
                sqlDL = JMtoDataLakeCountChecksSQL.DL_jmf_review_comment_Count;
                break;

        }
        Log.info(sqlDL);
        List<Map<String, Object>> DPPCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLJMCount = ((Long) DPPCountDL.get(0).get("count")).intValue();
        Log.info(tableName + " data in DL: " + DLJMCount);
    }

    @Then("^The JM count for (.*) table between inbound and current are identical$")
    public void JMcompareMYSQLtoDL(String tableName) {
        Assert.assertEquals("The counts between " + tableName + " is not equal", JMCount, DLJMCount);
        Log.info("The count for " + tableName + " table between MYSQL: " + JMCount + " and DL: " + DLJMCount + " is equal");
    }
}