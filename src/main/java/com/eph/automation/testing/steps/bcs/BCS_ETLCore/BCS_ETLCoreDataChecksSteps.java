package com.eph.automation.testing.steps.bcs.BCS_ETLCore;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSETL_CoreAccessDLContext;
import com.eph.automation.testing.models.dao.BCS.BCS_ETLCoreDLAccessObject;
import com.eph.automation.testing.services.db.BCS_ETLCoreSQL.BCS_ETLCoreDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class BCS_ETLCoreDataChecksSteps {

    public BCSETL_CoreAccessDLContext dataQualityBCSContext;
    private static String sql;
    private static List<String> Ids;
    private BCS_ETLCoreDataChecksSQL bcsCoreObj = new BCS_ETLCoreDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^Get the (.*) of BCS Core data from Inbound Tables (.*)$")
    public void getRandomIdsFromInound(String numberOfRecords, String tableName) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Inbound Tables....");
        List<Map<?, ?>> randomIds;
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_ACCPROD_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_IDENT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PRODUCT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WRK_PERS_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WRK_RELT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_IDENT_KEY_INBOUND, numberOfRecords);
                break;
            case "etl_person_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_INBOUND, numberOfRecords);
                break;
            case "all_manifestation_statuses_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_STATUS_KEY_INBOUND, numberOfRecords);
                break;
            case "all_manifestation_pubdates_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_PUBDATES_KEY_INBOUND, numberOfRecords);
                break;
       }
        randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }


    @When("^Get the Data from the Inbound Tables (.*)$")
    public void getIngestRecords(String tableName) {
        Log.info("We get the BCS Ingest records...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_person_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELT_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_statuses_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ALL_MANIF_STATUS_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_pubdates_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ALL_MANIF_PUBDATES_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PROD_INBOUND_DATA, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recordsFromInboundData = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^Data from the BCS Core Current Tables to compare Inbound Check (.*)$")
    public void getDataforInboundCheck(String tableName){
        Log.info("We get the records from Current BCS Core table for Inbound Check...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_CURR_DATA, Joiner.on("','").join(Ids));
                break;
           case "etl_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_CURR_REC, Joiner.on("','").join(Ids));
                break;

            case "etl_work_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_person_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_CURR_DATA, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_statuses_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_STATUSES_DATA, Joiner.on("','").join(Ids));
                break;
            case "all_manifestation_pubdates_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_PUBDATES_DATA, Joiner.on("','").join(Ids));
                break;

        }
        dataQualityBCSContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @And("^Compare data of BCS Inbound and BCS Core (.*) tables are identical$")
    public void compareIngestandCurrent(String tableName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recordsFromInboundData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Ingest and current...");
            for (int i = 0; i < dataQualityBCSContext.recordsFromInboundData.size(); i++) {
                switch (tableName) {
                    case "etl_accountable_product_current_v":
                        Log.info("comparing inbound and etl_accountable_product_current_v records...");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] all_accountable_prod_Col = {"getSOURCEREF","getACCOUNTABLEPRODUCT","getACCOUNTABLENAME","getACCOUNTABLEPARENT","getDQ_ERR"};
                        for (String strTemp : all_accountable_prod_Col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Acc_Prod_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "all_manifestation_statuses_v":
                        Log.info("all_manifestation_statuses_v records.... ");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getSOURCEREF)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getSOURCEREF));

                        String[] all_manifestation_statuses_v_col = {"getSOURCEREF","getREFKEYPRODPRIORITY","getDELIVERYSTATUSPRODPRIORITY","getDELTASTATUSPRODPRIORITY","getREFKEYMANIFPRIORITY","getDELIVERYSTATUSMANIFPRIORITY","getDELTASTATUSMANIFPRIORITY"};
                        for (String strTemp : all_manifestation_statuses_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("SOURCEREF => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_statuses = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_statuses for SOURCEREF:"+dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                    break;
                    case "all_manifestation_pubdates_v":
                        Log.info("all_manifestation_pubdates_v records ");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getSOURCEREF)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getSOURCEREF));

                        String[] all_manifestation_pubdates_v_col = {"getSOURCEREF","getWORKMASTERPROJECTNO","getMINACTUALPUBDATE","getMINPLANNEDPUBDATE"};
                        for (String strTemp : all_manifestation_pubdates_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("SOURCEREF => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_pubdates = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_statuses for SOURCEREF:"+dataQualityBCSContext.recordsFromInboundData.get(i).getSOURCEREF(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_person_current_v":
                        Log.info("comparing inbound and etl_person_current_v records...");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_person_current_v_col = {"getUKEY","getSOURCEREF","getFIRSTNAME","getFAMILYNAME","getPEOPLEHUBID","getEMAIL","getDQ_ERR"};
                        for (String strTemp : etl_person_current_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Person_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_manifestation_identifier_current_v":
                        Log.info("comparing inbound and etl_manifestation_identifier_current_v Records:");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_manifestation_identifier_current_v_col = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE"};
                        for (String strTemp : etl_manifestation_identifier_current_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_Ident_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_identifier_current_v":
                        Log.info("comparing inbound and etl_work_identifier Records:");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_work_identifier_current_v_col = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE"};
                        for (String strTemp : etl_work_identifier_current_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Work_Ident_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_relationship_current_v":
                        Log.info("comparing inbound and etl_work_relationship Records:");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_work_relationship_current_v_col = {"getUKEY","getPARENTREF","getCHILDREF","getRELATIONTYPEREF","getDQ_ERR"};
                        for (String strTemp : etl_work_relationship_current_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Work_Relation_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Relation_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                    break;

                    case "etl_work_person_role_current_v":
                        Log.info("comparing inbound and etl_work_person_role_current_v Records:");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_work_person_role_current_v_col = {"getUKEY","getWORKSOURCEREF","getPERSONSOURCEREF","getROLETYPE","getSEQUENCE","getDEDUPLICATOR","getDQ_ERR"};
                        for (String strTemp : etl_work_person_role_current_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " PERS_ROLE_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "etl_work_current_v":
                        Log.info("comparing inbound and etl_work_current_v Records:");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_work_current_v_col = {"getUKEY","getSOURCEREF","getTITLE","getSUBTITLE","getVOLUMENO","getCOPYRIGHTYEAR","getEDITIONNO","getPMC","getWORKTYPE","getSTATUSCODE","getIMPRINTCODE","getTEOPCO","getOPCO"
                                ,"getRESPCENTRE","getLANGUAGECODE","getELECTRORIGHTSINDICATOR","getFOAJOURNALTYPE","getFSOCIETYOWNERSHIP","getSUBSCRIPTIONTYPE"};
                        for (String strTemp : etl_work_current_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " WORK_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_manifestation_current_v":
                        Log.info("comparing inbound and etl_manifestation_current_v Records:");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_manif_current_v_col = {"getUKEY","getSOURCEREF","getTITLE","getINTEREDITIONFLAG","getFIRSTPUBLISHEDDATE","getBINDING","getMANIFESTATIONTYPE","getSTATUS","getWORKID","getLASTPUBDATE","getDQ_ERR"};
                        for (String strTemp : etl_manif_current_v_col) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Inbound = " + method.invoke(objectToCompare1) +
                                    " Manif_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "etl_product_current_v":
                        Log.info("comparing current and etl_product_current_v Records:");
                        dataQualityBCSContext.recordsFromInboundData.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                        dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                        String[] etl_transform_history_product_part = {"getUKEY","getSOURCEREF","getBINDINGCODE","getNAME","getSHORTTITLE","getLAUNCHDATE",
                                "getTAXCODE","getSTATUS","getMANIFESTATIONREF","getWORKSOURCE","getWORKTYPE","getSEPRATELYSALEINDICATOR","getTRIALALLOWEDINDICATOR","getFWORKSOURCEREF",
                                "getPRODUCTTYPE","getREVENUEMODEL","getDQ_ERR"};
                        for (String strTemp : etl_transform_history_product_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromInboundData.get(i);
                            BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recordsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("UKEY => " +  dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY() +
                                    " " + strTemp + " => Prod_Curr = " + method.invoke(objectToCompare1) +
                                    " Prod_Curr = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Curr for UKEY:"+dataQualityBCSContext.recordsFromInboundData.get(i).getUKEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Given("^Get the (.*) of BCS Core data from Current Tables (.*)$")
    public void getRandomIdsFromCurrent(String numberOfRecords, String tableName) {
         numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Ids for BCS Core Current Tables....");

        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_ACCPROD_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_MANIF_IDENT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PRODUCT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_PERS_ROLE_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_RELATION_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_WORK_IDENT_KEY_CURRENT, numberOfRecords);
                break;
            case "etl_person_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_RANDOM_PERSON_KEY_CURRENT, numberOfRecords);
                break;
        }
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomIds.stream().map(m -> (String) m.get("u_key")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());

    }

    @Then("^Get the Data from the BCS Core Current Tables (.*)$")
    public void getRecFromCurrent(String tableName){
        Log.info("We get the records from Current BCS Core table...");
        switch (tableName) {
            case "etl_accountable_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_product_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_relationship_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_work_identifier_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_CURR_REC, Joiner.on("','").join(Ids));
                break;
            case "etl_person_current_v":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_CURR_REC, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recordsFromCurrent = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);
    }

    @Then("^We Get the records from transform BCS Current History (.*)$")
   public void getRecFromCurrentHistory(String tableName){
       Log.info("We get the records from Current_History BCS Core table...");
       switch (tableName) {
           case "etl_transform_history_accountable_product_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_manifestation_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_manifestation_identifier_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_product_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_work_person_role_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_work_relationship_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_work_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_work_identifier_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
               break;
           case "etl_transform_history_person_part":
               sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_CURR_HIST_DATA, Joiner.on("','").join(Ids));
           break;
       }
       dataQualityBCSContext.recFromCurrentHist = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
       Log.info(sql);

   }

    @And("^Compare the records of BCS Core current and BCS Current_History (.*)$")
    public void compareCurrentandCurrHist(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Current and current hist tables...");
        }
        for (int i = 0; i < dataQualityBCSContext.recordsFromCurrent.size(); i++) {
            switch (targetTableName) {
                case "etl_transform_history_accountable_product_part":
                    Log.info("comparing current and etl_transform_history_accountable_product_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_accountable_product_part = {"getUKEY","getSOURCEREF","getACCOUNTABLEPRODUCT","getACCOUNTABLENAME","getACCOUNTABLEPARENT","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_accountable_product_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                                " " + strTemp + " => Acc_Prod_Curr = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_Curr_hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_Curr_hist for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_manifestation_part":
                    Log.info("comparing current and etl_transform_history_manifestation_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_manifestation_part = {"getUKEY","getSOURCEREF","getTITLE","getINTEREDITIONFLAG","getFIRSTPUBLISHEDDATE","getBINDING","getMANIFESTATIONTYPE","getSTATUS"
                            ,"getWORKID","getLASTPUBDATE","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_manifestation_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                                " " + strTemp + " => Manif_Curr = " + method.invoke(objectToCompare1) +
                                " Manif_Curr_hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Curr_hist UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_manifestation_identifier_part":
                    Log.info("comparing current and etl_transform_history_manifestation_identifier_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_manifestation_identifier_part = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE"};
                    for (String strTemp : etl_transform_history_manifestation_identifier_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => Manif_Ident_Curr = " + method.invoke(objectToCompare1) +
                                " Manif_Ident_Curr_hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_Curr_hist UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_product_part":
                    Log.info("comparing current and etl_transform_history_product_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_product_part = {"getUKEY","getSOURCEREF","getBINDINGCODE","getNAME","getSHORTTITLE","getLAUNCHDATE",
                    "getTAXCODE","getSTATUS","getMANIFESTATIONREF","getWORKSOURCE","getWORKTYPE","getSEPRATELYSALEINDICATOR","getTRIALALLOWEDINDICATOR","getFWORKSOURCEREF",
                    "getPRODUCTTYPE","getREVENUEMODEL","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_product_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => Prod_Curr = " + method.invoke(objectToCompare1) +
                                " Prod_Curr_hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_Curr_hist UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_transform_history_work_person_role_part":
                      Log.info("comparing current and etl_transform_history_work_person_role_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_person_role_part = {"getUKEY","getWORKSOURCEREF","getPERSONSOURCEREF","getROLETYPE","getSEQUENCE","getDEDUPLICATOR","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_work_person_role_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => PERS_ROLE_Curr = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_Curr_hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_Curr_hist for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_relationship_part":
                    Log.info("comparing current and etl_transform_history_work_relationship_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_relationship_part = {"getUKEY","getPARENTREF","getCHILDREF","getRELATIONTYPEREF","getLASTUDATEDDATE","getDQ_ERR"};
                    for (String strTemp : etl_transform_history_work_relationship_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => WORK_RELAT_Curr = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_Curr_hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_Curr_hist for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_work_part":
                    Log.info("comparing current and etl_transform_history_work_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_work_current_v_col = {"getUKEY","getSOURCEREF","getTITLE","getSUBTITLE","getVOLUMENO","getCOPYRIGHTYEAR","getEDITIONNO","getPMC","getWORKTYPE","getSTATUSCODE","getIMPRINTCODE","getTEOPCO","getOPCO"
                            ,"getRESPCENTRE","getLANGUAGECODE","getELECTRORIGHTSINDICATOR","getFOAJOURNALTYPE","getFSOCIETYOWNERSHIP","getSUBSCRIPTIONTYPE"};
                    for (String strTemp : etl_work_current_v_col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => WORK_Curr = " + method.invoke(objectToCompare1) +
                                " WORK_Curr_hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_Curr_hist for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_transform_history_work_identifier_part":
                    Log.info("comparing current and etl_transform_history_work_identifier_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_transform_history_work_identifier_part = {"getUKEY","getSOURCEREF","getIDENTIFIER","getIDENTIFIERTYPE"};
                    for (String strTemp : etl_transform_history_work_identifier_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " +  dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => Work_Ident_Curr = " + method.invoke(objectToCompare1) +
                                " Work_Ident_Curr_hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Curr_hist for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_transform_history_person_part":
                    Log.info("comparing current and etl_transform_history_person_part records ...");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromCurrentHist.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    String[] etl_person_current_v_col = {"getUKEY", "getSOURCEREF", "getFIRSTNAME", "getFAMILYNAME", "getPEOPLEHUBID", "getEMAIL", "getDQ_ERR"};
                    for (String strTemp : etl_person_current_v_col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromCurrentHist.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => Person_Curr = " + method.invoke(objectToCompare1) +
                                " Person_Curr_Hist = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Curr_Hist for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
            }

        }
    }



//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Then("^We Get the records from transform BCS Transform_File (.*)$")
    public void getRecFromTransformFile(String tableName){
        Log.info("We get the records from Transform_File BCS Core table...");
        switch (tableName) {
            case "etl_accountable_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_ACCPROD_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_manifestation_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_MANIF_IDENT_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_product_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PRODUCT_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_person_role_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_PERS_ROLE_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_relationship_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_RELATION_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_work_identifier_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_WORK_IDENT_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
            case "etl_person_transform_file_history_part":
                sql = String.format(BCS_ETLCoreDataChecksSQL.GET_PERSON_REC_TRANS_FILE_DATA, Joiner.on("','").join(Ids));
                break;
        }
        dataQualityBCSContext.recFromTransformFile = DBManager.getDBResultAsBeanList(sql, BCS_ETLCoreDLAccessObject.class, Constants.AWS_URL);
        Log.info(sql);

    }

    @And("^Compare the records of BCS Core current and BCS Transform_File (.*)$")
    public void compareCurrandTransFile(String targetTableName)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (dataQualityBCSContext.recordsFromCurrent.isEmpty()) {
            Log.info("No Data Found in the Current Tables ....");
        } else {
            Log.info("Sorting the Ids to compare the records between Current and transform_file tables...");
        }
        for (int i = 0; i < dataQualityBCSContext.recordsFromCurrent.size(); i++) {
            switch (targetTableName) {
                case "etl_accountable_product_transform_file_history_part":
                    Log.info("compare current and etl_accountable_product_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_accountable_product_transform_file_history_part = {"getUKEY", "getSOURCEREF", "getACCOUNTABLEPRODUCT", "getACCOUNTABLENAME", "getACCOUNTABLEPARENT", "getDQ_ERR"};
                    for (String strTemp : etl_accountable_product_transform_file_history_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                                " " + strTemp + " => Acc_Prod_Curr = " + method.invoke(objectToCompare1) +
                                " Acc_Prod_trans_file = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Acc_Prod_trans_file",
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_manifestation_transform_file_history_part":
                    Log.info("compare current and etl_manifestation_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_manifestation_transform_file_history_part = {"getUKEY", "getSOURCEREF", "getTITLE", "getINTEREDITIONFLAG", "getFIRSTPUBLISHEDDATE", "getBINDING", "getMANIFESTATIONTYPE", "getSTATUS"
                            , "getWORKID", "getLASTPUBDATE", "getDQ_ERR"};
                    for (String strTemp : etl_manifestation_transform_file_history_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getSOURCEREF() +
                                " " + strTemp + " => Manif_Curr = " + method.invoke(objectToCompare1) +
                                " Manif_trans_file = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_trans_file for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_manifestation_identifier_transform_file_history_part":
                    Log.info("compare current and etl_manifestation_identifier_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_manifestation_identifier_transform_file_history_part = {"getUKEY", "getSOURCEREF", "getIDENTIFIER", "getIDENTIFIERTYPE"};
                    for (String strTemp : etl_manifestation_identifier_transform_file_history_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => Manif_Ident_Curr = " + method.invoke(objectToCompare1) +
                                " Manif_Ident_trans_file = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Manif_Ident_trans_file for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_product_transform_file_history_part":
                    Log.info("compare current and etl_product_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_product_transform_file_history_part = {"getUKEY", "getSOURCEREF", "getBINDINGCODE", "getNAME", "getSHORTTITLE", "getLAUNCHDATE",
                            "getTAXCODE", "getSTATUS", "getMANIFESTATIONREF", "getWORKSOURCE", "getWORKTYPE", "getSEPRATELYSALEINDICATOR", "getTRIALALLOWEDINDICATOR", "getFWORKSOURCEREF",
                            "getPRODUCTTYPE", "getREVENUEMODEL", "getDQ_ERR"};
                    for (String strTemp : etl_product_transform_file_history_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => Prod_Curr = " + method.invoke(objectToCompare1) +
                                " Prod_trans_file = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Prod_trans_file for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_work_person_role_transform_file_history_part":
                    Log.info("compare current and etl_work_person_role_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_work_person_role_transform_file_history_part = {"getUKEY", "getWORKSOURCEREF", "getPERSONSOURCEREF", "getROLETYPE", "getSEQUENCE", "getDEDUPLICATOR", "getDQ_ERR"};
                    for (String strTemp : etl_work_person_role_transform_file_history_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => PERS_ROLE_Curr = " + method.invoke(objectToCompare1) +
                                " PERS_ROLE_trans_file = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in PERS_ROLE_trans_file for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_work_relationship_transform_file_history_part":
                    Log.info("compare current and etl_work_relationship_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_work_relationship_transform_file_history_part = {"getUKEY", "getPARENTREF", "getCHILDREF", "getRELATIONTYPEREF", "getLASTUDATEDDATE", "getDQ_ERR"};
                    for (String strTemp : etl_work_relationship_transform_file_history_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => WORK_RELAT_Curr = " + method.invoke(objectToCompare1) +
                                " WORK_RELAT_trans_file = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_RELAT_trans_file for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;

                case "etl_work_transform_file_history_part":
                    Log.info("etl_work_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));

                    String[] etl_work_transform_file_history_part = {"getUKEY", "getSOURCEREF", "getTITLE", "getSUBTITLE", "getVOLUMENO", "getCOPYRIGHTYEAR", "getEDITIONNO", "getPMC", "getWORKTYPE", "getSTATUSCODE", "getIMPRINTCODE", "getTEOPCO", "getOPCO"
                            , "getRESPCENTRE", "getLANGUAGECODE", "getELECTRORIGHTSINDICATOR", "getFOAJOURNALTYPE", "getFSOCIETYOWNERSHIP", "getSUBSCRIPTIONTYPE"};
                    for (String strTemp : etl_work_transform_file_history_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => WORK_Curr = " + method.invoke(objectToCompare1) +
                                " WORK_trans_file = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in WORK_trans_file for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "etl_work_identifier_transform_file_history_part":
                    Log.info("etl_work_identifier_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    String[] etl_work_identifier_transform_file_history_part = {"getUKEY", "getSOURCEREF", "getIDENTIFIER", "getIDENTIFIERTYPE"};
                    for (String strTemp : etl_work_identifier_transform_file_history_part) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => Work_Ident_Curr = " + method.invoke(objectToCompare1) +
                                " Work_Ident_Curr_file = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Work_Ident_Curr_file for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "etl_person_transform_file_history_part":
                    Log.info("etl_person_transform_file_history_part Records:");
                    dataQualityBCSContext.recordsFromCurrent.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY)); //sort primarykey data in the lists
                    dataQualityBCSContext.recFromTransformFile.sort(Comparator.comparing(BCS_ETLCoreDLAccessObject::getUKEY));
                    String[] etl_person_current_tranf_file_col = {"getUKEY", "getSOURCEREF", "getFIRSTNAME", "getFAMILYNAME", "getPEOPLEHUBID", "getEMAIL", "getDQ_ERR"};
                    for (String strTemp : etl_person_current_tranf_file_col) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        BCS_ETLCoreDLAccessObject objectToCompare1 = dataQualityBCSContext.recordsFromCurrent.get(i);
                        BCS_ETLCoreDLAccessObject objectToCompare2 = dataQualityBCSContext.recFromTransformFile.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("UKEY => " + dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY() +
                                " " + strTemp + " => Person_Curr = " + method.invoke(objectToCompare1) +
                                " Person_Curr_File = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in Person_Curr_File for UKEY:"+dataQualityBCSContext.recordsFromCurrent.get(i).getUKEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                }
            }

        }
    }
