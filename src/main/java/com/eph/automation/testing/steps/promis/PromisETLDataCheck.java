package com.eph.automation.testing.steps.promis;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.PromisETL.PromisContext;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesCurrentObject;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesETLObject;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesInboundObject;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.PromisETLDataCheckSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import net.minidev.json.parser.ParseException;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PromisETLDataCheck {
    public PromisContext PromisDataContext;
    private static String sql;
    private static List<String> Ids;
    private static String promisDuplicateLatestSQLCount;
    private static int promisDuplicateLatestCount;

    @Given("^We get the (.*) random Promis ids of (.*)$")
    public void getRandomPromisIds(String numberOfRecords, String Currenttablename) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (Currenttablename) {
            case "promis_prm_londes_2_html_current":
                sql = String.format(PromisETLDataCheckSQL.GET_PRMLONDEST_IDS, Currenttablename, Currenttablename, numberOfRecords);
                List<Map<?, ?>> randomLondestIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomLondestIds.stream().map(m -> (Integer) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "promis_prmclscodt_current":
                sql = String.format(PromisETLDataCheckSQL.GET_PRMCLSCODT_IDS, Currenttablename, Currenttablename, numberOfRecords);
                List<Map<?, ?>> randomClscodtIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomClscodtIds.stream().map(m -> (String) m.get("CLS_COD")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "promis_prmpubrelt_current":
                sql = String.format(PromisETLDataCheckSQL.GET_PRMPUBRELT_IDS, Currenttablename, Currenttablename, numberOfRecords);
                List<Map<?, ?>> randomPubPubIdtIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPubPubIdtIds.stream().map(m -> (Integer) m.get("PUB_PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "promis_prmpmccodt_current":
                sql = String.format(PromisETLDataCheckSQL.GET_MKT_IDT_IDS, Currenttablename, Currenttablename, numberOfRecords);
                List<Map<?, ?>> randomMkt_IdtIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomMkt_IdtIds.stream().map(m -> (String) m.get("MKT_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;
            default:
                sql = String.format(PromisETLDataCheckSQL.GET_PRMAUTPUBT_IDS, Currenttablename, Currenttablename, numberOfRecords);
                List<Map<?, ?>> randomPubIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomPubIds.stream().map(m -> (Integer) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the Promis Inbound records from (.*)$")
    public void getRecordsInbound(String Inboundtablename) throws ParseException {
        Log.info("We get the records from Inbound..");
        switch (Inboundtablename) {
            case "promis_prmautpubt_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmautpubt_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmclscodt_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmclscodt_part, Inboundtablename, Inboundtablename, Joiner.on("','").join(Ids));
                break;
            case "promis_prmclst_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmclst_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prm_londes_2_html_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmlondest_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpricest_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpricest_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpubinft_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpubinft_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpubrelt_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpubrelt_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids), Inboundtablename);
                break;
            case "promis_prmpmccodt_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpmccodt, Inboundtablename, Inboundtablename, Joiner.on("','").join(Ids));
                break;
            case "promis_prmincpmct_part":
                sql = String.format(PromisETLDataCheckSQL.GET_promis_prmincpmct, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        PromisDataContext.tbPRMDataObjectsFromInbound = DBManager.getDBResultAsBeanList(sql, PRMTablesInboundObject.class, Constants.AWS_URL);
        //System.out.println(PromisDataContext.tbPRMDataObjectsFromDL.size());
    }

    @Then("^We get the Promis Current records from (.*)$")
    public void getRecordsAutPubtDL(String Currenttablename) throws ParseException {
        Log.info("We get the Current records..");
        switch (Currenttablename) {
            case "promis_prmautpubt_current":
                sql = String.format(PromisETLDataCheckSQL.GETPRMPUBT, Currenttablename, Currenttablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmclscodt_current":
                sql = String.format(PromisETLDataCheckSQL.GETPRMCLSCODT, Currenttablename, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_prmclst_current":
                sql = String.format(PromisETLDataCheckSQL.GETPRMCLST, Currenttablename, Currenttablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prm_londes_2_html_current":
                sql = String.format(PromisETLDataCheckSQL.GETPRMLONDEST, Currenttablename, Currenttablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpricest_current":
                sql = String.format(PromisETLDataCheckSQL.GETPRMPRICEST, Currenttablename, Currenttablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpubinft_current":
                sql = String.format(PromisETLDataCheckSQL.GETPRMPUBINFT, Currenttablename, Currenttablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpubrelt_current":
                sql = String.format(PromisETLDataCheckSQL.GETPRMPUBRELT, Currenttablename, Currenttablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmincpmct_current":
                sql = String.format(PromisETLDataCheckSQL.GET_promis_prmincpmct, Currenttablename, Currenttablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpmccodt_current":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpmccodt, Currenttablename, Currenttablename, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisDataContext.tbPRMDataObjectsFromCurrent = DBManager.getDBResultAsBeanList(sql, PRMTablesCurrentObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records in Inbound and Current of (.*)$")
    public void comparePromDataInboundtoCurrent(String Inboundtablename) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (PromisDataContext.tbPRMDataObjectsFromInbound.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromInbound.size(); i++) {
                switch (Inboundtablename) {
                    case "promis_prmautpubt_part":
                        Log.info("comparing current and promis_prmautpubt_part Records:");
                     //   String pubIdt = (PRMTablesCurrentObject::getPUB_IDT.Substring(0, input.IndexOf("/") + 1);
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_IDT)); //sort primarykey data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getAUT_EDT_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getAUT_EDT_IDT));

                        String[] etl_promis_prmautpubt_part = {"getPUB_IDT", "getAUT_EDT_IDT", "getAUT_EDT_TYP", "getTYP_DES", "getSEQ_NUM", "getAUT_EDT_PRE", "getAUT_EDT_INI", "getAUT_EDT_NAM", "getAUT_EDT_SORT", "getAUT_EDT_SUF", "getAFF_TXT", "getFTN", "getAUT_EDT_JCI", "getBIO_IMAGE"};
                        for (String strTemp : etl_promis_prmautpubt_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT() +
                                    " " + strTemp + " => promis_prmautpubt_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmautpubt_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmautpubt_current for PUB_IDT:" + PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "promis_prmclscodt_part":
                        Log.info("Sorting the data to compare the PRMCLSCODT records between inbound and current ..");
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getCLS_COD)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getCLS_COD));

                        String[] etl_promis_prmclscodt_part = {"getCLS_COD","getCLS_DES"};
                        for (String strTemp : etl_promis_prmclscodt_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("CLS_COD => " +  PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getCLS_COD() +
                                    " " + strTemp + " => promis_prmclscodt_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmclscodt_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmclscodt_current for CLS_COD:"+PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getCLS_COD(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "promis_prmclst_part":
                        Log.info("Sorting the data to compare the PRMCLST records between inbound and current ..");
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getCLS_COD)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getCLS_COD));

                        String[] etl_promis_prmclst_part = {"getPUB_IDT","getCLS_COD"};
                        for (String strTemp : etl_promis_prmclst_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("PUB_IDT => " +  PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT() +
                                    " " + strTemp + " => promis_prmclst_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmclst_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmclst_current for PUB_IDT:"+PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "promis_prmlondest_part":
                        Log.info("Sorting the data to compare the PRMCLST records between inbound and current ..");
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_VOL_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_VOL_IDT));
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getVOL_PRT_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getVOL_PRT_IDT));

                        String[] etl_promis_prmlondest_part = {"getPUB_IDT","getVOL_PRT_IDT","getPUB_VOL_IDT"};
                        for (String strTemp : etl_promis_prmlondest_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("PUB_IDT => " +  PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT() +
                                    " " + strTemp + " => promis_prmlondest_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmlondest_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmlondest_part for PUB_IDT:"+PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;

                    case "promis_prmpricest_part":
                        Log.info("Sorting the data to compare the PRMPRICEST records in Inbound and DATA LAKE ..");
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));

                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_VOL_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_VOL_IDT));

                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getVOL_PRT_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getVOL_PRT_IDT));

                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getEDN_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getEDN_IDT));

                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getEDN_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getEDN_IDT));

                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPRC_TYP)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPRC_TYP));

                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPRC_GEO)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPRC_GEO));

                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getIPT_COD)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getIPT_COD));

                        String[] etl_promis_prmpricest_part = {"getPUB_IDT","getPUB_VOL_IDT","getVOL_PRT_IDT","getEDN_IDT","getPRC_TYP","getPRC_GEO","getIPT_COD","getPRC_DAT","getSTD_CUR_COD","getSTD_PRC","getPRC_PRE","getADD_PRC","getEXP_DAT"};
                        for (String strTemp : etl_promis_prmpricest_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("PUB_IDT => " +  PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT() +
                                    " " + strTemp + " => promis_prmpricest_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmpricest_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmpricest_part for PUB_IDT:"+PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "promis_prmpubinft_part":
                        Log.info("Sorting the data to compare the PRMPUBINFT records between inbound and current ..");
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));

                        String[] etl_promis_prmpubinft_part = {"getPUB_IDT","getSTA_DAA","getFUL_TIT","getPUB_TYP","getOWN_IDT","getPBL_ABR_NAM","getLOC","getSTA_PRM","getLNG_COD","getPUB_IMP_IDT","getPUB_BGN_YEA","getLCO_NUM","getIMP_DAT","getCDA","getCRE_IDT","getDEP_IDT","getLST_USR_IDT","getLST_UPD_DAT","getABR_TIT","getFUL_TIT_SRT","getSUB_TIT_1","getSUB_TIT_2","getSUB_TIT_3","getPRG_SUB_TIT","getAUT_EDT_NAM","getSLO_TXT","getNTB",
                                "getFTN","getFOR_TIT","getRPN_INF","getADV_COD","getRVW_COD","getSUP_APP","getPHY_SZE","getFS_IDT","getWTK_NUM","getWTK_CLS","getBWK","getAEI","getREV_EDN_TI","getJNL_IDT","getLST_UPD_DATE","getCDA_DATE","getAQS_COD","getMKT_COD","getAUD","getSHT_DES","getSHT_CTT_DES","getSLO_EXP_DAT","getIF_NO",
                                "getIF_YEAR","getIF_COMMENT","getAUT_BENEFITS","getSOURCE","getARG_COD","getIF_5","getIF_RANKING","getIF_CAT","getOA_TYPE"};
                        for (String strTemp : etl_promis_prmpubinft_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("PUB_IDT => " +  PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT() +
                                    " " + strTemp + " => promis_prmpubinft_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmpubinft_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmpubinft_part for PUB_IDT:"+PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;

                    case "promis_prmpubrelt_part":
                        Log.info("Sorting the data to compare the PRMPUBRELT records between inbound and current ..");
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getREL_NO)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getREL_NO));

                        String[] etl_promis_prmpubrelt_part = {"getPUB_IDT","getREL_NO","getREL_SRT","getREL_IDT","getREL_TITLE","getFOOTNOTE","getFRONT_TEXT","getEND_TEXT","getRTP_RTP_COD","getREL_END_DATE","getREL_START_DATE"};

                        for (String strTemp : etl_promis_prmpubrelt_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("PUB_IDT => " +  PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT() +
                                    " " + strTemp + " => promis_prmpubrelt_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmpubrelt_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmpubrelt_part for PUB_IDT:"+PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;
                    case "promis_prmincpmct_part":
                        Log.info("Sorting the data to compare the PRMINCPMCT records between inbound and current ..");
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));

                        String[] etl_promis_prmincpmct_part = {"getPUB_IDT","getMKT_SUB_IDT"};

                        for (String strTemp : etl_promis_prmincpmct_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("PUB_IDT => " +  PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT() +
                                    " " + strTemp + " => promis_prmincpmct_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmincpmct_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmincpmct_part for PUB_IDT:"+PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getPUB_IDT(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;

                    case "promis_prmpmccodt_part":
                        Log.info("Sorting the data to compare the PRMPMCCODT records between inbound and current ..");
                        PromisDataContext.tbPRMDataObjectsFromInbound.sort(Comparator.comparing(PRMTablesInboundObject::getMKT_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getMKT_IDT));

                        String[] etl_promis_prmpmccodt_part = {"getMKT_IDT","getMKT_DES","getDIV_IDT"};

                        for (String strTemp : etl_promis_prmpmccodt_part) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesInboundObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromInbound.get(i);
                            PRMTablesCurrentObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromCurrent.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("MKT_IDT => " +  PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getMKT_IDT() +
                                    " " + strTemp + " => promis_prmpmccodt_part = " + method.invoke(objectToCompare1) +
                                    " promis_prmpmccodt_current = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_prmpmccodt_part for PUB_IDT:"+PromisDataContext.tbPRMDataObjectsFromInbound.get(i).getMKT_IDT(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }
            }
        }
    }

    @Given("^We get the (.*) random Promis DeltaQuery ids of (.*)$")
    public void getRandomPromisDeltaIds(String numberOfRecords, String Deltatablename) {
     //  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        sql = String.format(PromisETLDataCheckSQL.GET_UKEY_IDS, Deltatablename, numberOfRecords);
        List<Map<?, ?>> randomDeltaIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomDeltaIds.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get Promis Delta Query records from (.*)$")
    public void getDeltaQueryRecords(String DeltaQueryTable) throws ParseException {
        Log.info("We get the records from DeltaQuery..");
        switch (DeltaQueryTable) {
            case "promis_transform_file_history_subject_areas_part":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS_DeltaQUERY, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_pricing_part":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING_DeltaQUERY, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_person_roles_part":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES_DeltaQUERY, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_works_part":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS_DeltaQUERY, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_metrics_part":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS_DeltaQUERY, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_urls_part":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS_DeltaQUERY, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_work_rels_part":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS_DeltaQUERY, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, DeltaQueryTable, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisContext.tbPRMDataObjectsFromDeltaQuery = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
        System.out.println(PromisDataContext.tbPRMDataObjectsFromDeltaQuery.size());
    }

    @Then("^We get the Promis Delta records from (.*)$")
    public void getRecordsInDelta(String Deltatablename) throws ParseException {
        Log.info("We get the PreviousHistory records..");
        sql = String.format(PromisETLDataCheckSQL.GET_DELTA, Deltatablename, Joiner.on("','").join(Ids));
        Log.info(sql);
        PromisDataContext.tbPRMDataObjectsFromDelta = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records for delta query and delta tables of (.*)$")
    public void comparePromDataDeltaQuerytoDelta(String Deltatablename) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDeltaQuery.size(); i++) {
            switch (Deltatablename) {
                case "promis_delta_current_subject_areas":
                    Log.info("Sorting the data to compare the Subject Area records for Delta..");
                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_delta_current_subject_areas = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRIORITY","getSUBJECT_AREA_CODE","getSUBJECT_AREA_NAME","getSUBJECT_TYPE_CODE","getSUBJECT_TYPE_NAME"};

                    for (String strTemp : etl_promis_delta_current_subject_areas) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;
                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromDelta.get(i);
                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);
                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " " + strTemp + " => _promis_subject_areas = " + method.invoke(objectToCompare1) +
                                " _promis_delta_current_subject_areas = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in _promis_delta_current_subject_areas for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "promis_delta_current_pricing":
                    Log.info("Sorting the data to compare the Pricing records between Delta and Delta_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_delta_current_pricing = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRODUCT_TYPE","getCURRENCY","getPRICE","getSTART_DATE","getEND_DATE","getREGION","getQUANTITY","getCUSTOMER_CATEGORY"};

                    for (String strTemp : etl_promis_delta_current_pricing) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " " + strTemp + " => _promis_pricing = " + method.invoke(objectToCompare1) +
                                " _promis_delta_current_pricing = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_delta_current_pricing for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;


                case "promis_delta_current_person_roles":
                    Log.info("Sorting the data to compare the Person Roles records between Delta and Delta_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_delta_current_person_roles = {"getU_KEY","getPUB_IDT","getEPR_ID","getSEQUENCE_NUMBER","getROLE_DESCRIPTION","getGROUP_NUMBER","getINITIALS","getLAST_NAME","getTITLE","getHONOURS","getAFFILIATION","getIMAGE_URL","getFOOTNOTE_TXT","getNOTES_TXT","getWORK_TYPE"};

                    for (String strTemp : etl_promis_delta_current_person_roles) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_person_roles = " + method.invoke(objectToCompare1) +
                                " promis_delta_current_person_roles = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_delta_current_person_roles for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "promis_delta_current_works":
                    Log.info("Sorting the data to compare the Works records between History Excluding and Delta_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_delta_current_works = {"getU_KEY","getPUB_IDT","getEPR_ID","getJOURNAL_AIMS_SCOPE","getELSEVIER_COM_IND","getPRIMARY_AUTHOR","getPREVIOUS_TITLE","getWORK_TYPE"};

                    for (String strTemp : etl_promis_delta_current_works) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_works = " + method.invoke(objectToCompare1) +
                                " promis_delta_current_works = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_delta_current_works for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "promis_delta_current_metrics":
                    Log.info("Sorting the data to compare the Metrics records between Delta and DeltaQuery ..");
                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_delta_current_metrics = {"getU_KEY","getPUB_IDT","getEPR_ID","getMETRIC_CODE","getMETRIC_NAME","getMETRIC","getMETRIC_YEAR","getMETRIC_URL","getWORK_TYPE"};

                    for (String strTemp : etl_promis_delta_current_metrics) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_metrics = " + method.invoke(objectToCompare1) +
                                " promis_delta_current_metrics = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_delta_current_metrics for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "promis_delta_current_urls":
                    Log.info("Sorting the data to compare the Urls records between Delta and DeltaQuery ..");
                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_delta_current_urls = {"getU_KEY","getPUB_IDT","getEPR_ID","getURL_CODE","getURL_NAME","getURL","getURL_TITLE","getWORK_TYPE"};

                    for (String strTemp : etl_promis_delta_current_urls) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_urls = " + method.invoke(objectToCompare1) +
                                " promis_delta_current_urls = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_delta_current_urls for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "promis_delta_current_work_rels":
                    Log.info("Sorting the data to compare the Work_Rels records between Delta and Delta_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_delta_current_work_rels = {"getU_KEY","getCHILD_PUB_IDT","getPARENT_EPR_ID","getCHILD_EPR_ID","getCHILD_TITLE","getCHILD_RELATED_TYPE_CODE","getCHILD_RELATED_TYPE_NAME","getCHILD_RELATED_STATUS_CODE","getCHILD_RELATED_TYPE_ROLL_UP","getCHILD_RELATED_STATUS_NAME","getCHILD_RELATED_STATUS_ROLL_UP","getRELATIONSHIP_TYPE_CODE","getRELATIONSHIP_TYPE_NAME"};

                    for (String strTemp : etl_promis_delta_current_work_rels) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromDelta.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_work_rels = " + method.invoke(objectToCompare1) +
                                " promis_delta_current_work_rels = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_delta_current_work_rels for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }

            }
        }
    }


    @Given("^We get the (.*) random Promis History Excluding Query ids of (.*)$")
    public void getRandomPromisHistExclIds(String numberOfRecords, String HistExcltablename) {
    //   numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        sql = String.format(PromisETLDataCheckSQL.GET_UKEY_IDS, HistExcltablename, numberOfRecords);
        List<Map<?, ?>> randomHistExclIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomHistExclIds.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get Promis History Excluding Query records from (.*)$")
    public void getHistExclQueryRecords(String tablename) throws ParseException {
        Log.info("We get the records from History Excluding Query..");
        switch (tablename) {
            case "subject_areas":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS_HistExclQUERY, Joiner.on("','").join(Ids));
                break;
            case "pricing":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING_HistExclQUERY, Joiner.on("','").join(Ids));
                break;
            case "person_roles":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES_HistExclQUERY, Joiner.on("','").join(Ids));
                break;
            case "works":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS_HistExclQUERY, Joiner.on("','").join(Ids));
                break;
            case "metrics":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS_HistExclQUERY, Joiner.on("','").join(Ids));
                break;
            case "urls":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS_HistExclQUERY, Joiner.on("','").join(Ids));
                break;
            case "work_rels":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS_HistExclQUERY, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisContext.tbPRMDataObjectsFromHistoryExcludingQuery = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
        System.out.println(PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.size());
    }

    @Then("^We get the Promis History Excluding records from (.*)$")
    public void getRecordsInHistExcltablename(String HistExcltablename) throws ParseException {
        Log.info("We get the History Excluding records..");
        switch (HistExcltablename) {
            case "promis_transform_history_subject_areas_delta_v":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_pricing_delta_v":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_person_roles_delta_v":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_works_delta_v":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_metrics_delta_v":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_urls_delta_v":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_work_rels_delta_v":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records for History Excluding query and History Excluding tables of (.*)$")
    public void comparePromDataHistExclQuerytoHistExcl(String tablename) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.size(); i++) {
                switch (tablename) {
                    case "subject_areas":
                        Log.info("Sorting the data to compare the Subject Area records for History Excluding..");

                        PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));
                        String[] etl_promis_subject_areas_exclude = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRIORITY","getSUBJECT_AREA_CODE","getSUBJECT_AREA_NAME","getSUBJECT_TYPE_CODE","getSUBJECT_TYPE_NAME"};
                        for (String strTemp : etl_promis_subject_areas_exclude) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i);
                            PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                    " " + strTemp + " => promis_subject_areas = " + method.invoke(objectToCompare1) +
                                    " promis_subject_areas_exclude = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_subject_areas_exclude for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                break;
                case "pricing":
                    Log.info("Sorting the data to compare the Pricing records between History Excluding and HistoryExcluding_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_pricing_excl = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRODUCT_TYPE","getCURRENCY","getPRICE","getSTART_DATE","getEND_DATE","getREGION","getQUANTITY","getCUSTOMER_CATEGORY"};

                    for (String strTemp : etl_promis_pricing_excl) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_pricing = " + method.invoke(objectToCompare1) +
                                " promis_pricing_excl = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_pricing_excl for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "person_roles":
                    Log.info("Sorting the data to compare the Person Roles records between History Excluding and HistoryExcluding_Query ..");
                        PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));
                    String[] promis_person_roles_excl = {"getU_KEY","getPUB_IDT","getEPR_ID","getSEQUENCE_NUMBER","getROLE_DESCRIPTION","getGROUP_NUMBER","getINITIALS","getLAST_NAME","getTITLE","getHONOURS","getAFFILIATION","getIMAGE_URL","getFOOTNOTE_TXT","getNOTES_TXT","getWORK_TYPE"};

                    for (String strTemp : promis_person_roles_excl) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_person_roles = " + method.invoke(objectToCompare1) +
                                " promis_person_roles_excl = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_delta_current_person_roles for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "works":
                    Log.info("Sorting the data to compare the Works records between History Excluding and HistoryExcluding_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_works_excl = {"getU_KEY","getPUB_IDT","getEPR_ID","getJOURNAL_AIMS_SCOPE","getELSEVIER_COM_IND","getPRIMARY_AUTHOR","getPREVIOUS_TITLE","getWORK_TYPE"};

                    for (String strTemp : etl_promis_works_excl) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_works = " + method.invoke(objectToCompare1) +
                                " promis_works_excl = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_works_excl for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "metrics":
                    Log.info("Sorting the data to compare the Metrics records between History Excluding and HistoryExcluding_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_metrics_excl = {"getU_KEY","getPUB_IDT","getEPR_ID","getMETRIC_CODE","getMETRIC_NAME","getMETRIC","getMETRIC_YEAR","getMETRIC_URL","getWORK_TYPE"};

                    for (String strTemp : etl_promis_metrics_excl) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_metrics = " + method.invoke(objectToCompare1) +
                                " promis_metrics_excl = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_metrics_excl for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "urls":
                    Log.info("Sorting the data to compare the Urls records between History Excluding and HistoryExcluding_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_urls_excl = {"getU_KEY","getPUB_IDT","getEPR_ID","getURL_CODE","getURL_NAME","getURL","getURL_TITLE","getWORK_TYPE"};

                    for (String strTemp : etl_promis_urls_excl) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_urls = " + method.invoke(objectToCompare1) +
                                " promis_urls_excl = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_urls_excl for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "work_rels":
                    Log.info("Sorting the data to compare the Work_Rels records between History Excluding and HistoryExcluding_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));
                    String[] etl_promis_work_rels_excl = {"getU_KEY","getCHILD_PUB_IDT","getPARENT_EPR_ID","getCHILD_EPR_ID","getCHILD_TITLE","getCHILD_RELATED_TYPE_CODE","getCHILD_RELATED_TYPE_NAME","getCHILD_RELATED_STATUS_CODE","getCHILD_RELATED_TYPE_ROLL_UP","getCHILD_RELATED_STATUS_NAME","getCHILD_RELATED_STATUS_ROLL_UP","getRELATIONSHIP_TYPE_CODE","getRELATIONSHIP_TYPE_NAME"};

                    for (String strTemp : etl_promis_work_rels_excl) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_work_rels = " + method.invoke(objectToCompare1) +
                                " promis_work_rels_excl = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_work_rels_excl for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                }
             }
        }
    }


    @Given("^We get the (.*) random Promis Latest Query ids of (.*)$")
    public void getRandomPromisLatestIds(String numberOfRecords, String Latesttablename) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        sql = String.format(PromisETLDataCheckSQL.GET_UKEY_IDS, Latesttablename, numberOfRecords);
        List<Map<?, ?>> randomLatestIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomLatestIds.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get Promis Latest Query records from (.*)$")
    public void getLatestQueryRecords(String tablename) throws ParseException {
        Log.info("We get the records from Latest Query..");
        switch (tablename) {
            case "subject_areas":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS_LatestQUERY, Joiner.on("','").join(Ids));
                break;
            case "pricing":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING_LatestQUERY, Joiner.on("','").join(Ids));
                break;
            case "person_roles":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES_LatestQUERY, Joiner.on("','").join(Ids));
                break;
            case "works":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS_LatestQUERY, Joiner.on("','").join(Ids));
                break;
            case "metrics":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS_LatestQUERY, Joiner.on("','").join(Ids));
                break;
            case "urls":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS_LatestQUERY, Joiner.on("','").join(Ids));
                break;
            case "work_rels":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS_LatestQUERY, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisContext.tbPRMDataObjectsFromLatestQuery = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
        System.out.println(PromisDataContext.tbPRMDataObjectsFromLatestQuery.size());
    }

    @Then("^We get the Promis Latest records from (.*)$")
    public void getRecordsInLatesttablename(String Latesttablename) throws ParseException {
        Log.info("We get the History Excluding records..");
        switch (Latesttablename) {
            case "promis_transform_latest_subject_areas":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS, Latesttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_latest_pricing":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING, Latesttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_latest_person_roles":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES, Latesttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_latest_works":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS, Latesttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_latest_metrics":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS, Latesttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_latest_urls":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS, Latesttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_latest_work_rels":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS, Latesttablename, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisDataContext.tbPRMDataObjectsFromLatest = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records for Latest query and Latest tables of (.*)$")
    public void comparePromDataLatestQuerytoLatest(String tablename) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromLatest.size(); i++) {
            switch (tablename) {
                case "subject_areas":
                    Log.info("Sorting the data to compare the Subject Area records for Latest..");

                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_subject_areas_latest = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRIORITY","getSUBJECT_AREA_CODE","getSUBJECT_AREA_NAME","getSUBJECT_TYPE_CODE","getSUBJECT_TYPE_NAME"};
                    for (String strTemp : etl_promis_subject_areas_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;
                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromLatest.get(i);
                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);
                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_subject_areas = " + method.invoke(objectToCompare1) +
                                " promis_subject_areas_latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_subject_areas_latest for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "pricing":
                    Log.info("Sorting the data to compare the Pricing records between Latest and Latest_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));


                    String[] etl_promis_pricing_latest = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRODUCT_TYPE","getCURRENCY","getPRICE","getSTART_DATE","getEND_DATE","getREGION","getQUANTITY","getCUSTOMER_CATEGORY"};

                    for (String strTemp : etl_promis_pricing_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " " + strTemp + " => _promis_pricing = " + method.invoke(objectToCompare1) +
                                " promis_pricing_latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_pricing_latest for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "person_roles":
                    Log.info("Sorting the data to compare the Person Roles records between Latest and Latest_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] promis_person_roles_latest = {"getU_KEY","getPUB_IDT","getEPR_ID","getSEQUENCE_NUMBER","getROLE_DESCRIPTION","getGROUP_NUMBER","getINITIALS","getLAST_NAME","getTITLE","getHONOURS","getAFFILIATION","getIMAGE_URL","getFOOTNOTE_TXT","getNOTES_TXT","getWORK_TYPE"};

                    for (String strTemp : promis_person_roles_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_person_roles = " + method.invoke(objectToCompare1) +
                                " promis_person_roles_latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_person_roles_latest for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "works":
                    Log.info("Sorting the data to compare the Works records between Latest and Latest_Query ..");

                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_works_latest = {"getU_KEY","getPUB_IDT","getEPR_ID","getJOURNAL_AIMS_SCOPE","getELSEVIER_COM_IND","getPRIMARY_AUTHOR","getPREVIOUS_TITLE","getWORK_TYPE"};
                    for (String strTemp : etl_promis_works_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_works = " + method.invoke(objectToCompare1) +
                                " promis_works_latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_works_latest for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "metrics":
                    Log.info("Sorting the data to compare the Metrics records between Latest and Latest_Query ..");

                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_metrics_latest = {"getU_KEY","getPUB_IDT","getEPR_ID","getMETRIC_CODE","getMETRIC_NAME","getMETRIC","getMETRIC_YEAR","getMETRIC_URL","getWORK_TYPE"};

                    for (String strTemp : etl_promis_metrics_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_metrics = " + method.invoke(objectToCompare1) +
                                " promis_metrics_latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_metrics_excl for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                 case "urls":
                    Log.info("Sorting the data to compare the Urls records between Latest and Latest_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_urls_latest = {"getU_KEY","getPUB_IDT","getEPR_ID","getURL_CODE","getURL_NAME","getURL","getURL_TITLE","getWORK_TYPE"};

                    for (String strTemp : etl_promis_urls_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_urls = " + method.invoke(objectToCompare1) +
                                " promis_urls_latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_urls_latest for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "work_rels":
                    Log.info("Sorting the data to compare the Work_Rels records between Latest and Latest_Query ..");
                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_work_rels_latest = {"getU_KEY","getCHILD_PUB_IDT","getPARENT_EPR_ID","getCHILD_EPR_ID","getCHILD_TITLE","getCHILD_RELATED_TYPE_CODE","getCHILD_RELATED_TYPE_NAME","getCHILD_RELATED_STATUS_CODE","getCHILD_RELATED_TYPE_ROLL_UP","getCHILD_RELATED_STATUS_NAME","getCHILD_RELATED_STATUS_ROLL_UP","getRELATIONSHIP_TYPE_CODE","getRELATIONSHIP_TYPE_NAME"};

                    for (String strTemp : etl_promis_work_rels_latest) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromLatest.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " " + strTemp + " => promis_work_rels = " + method.invoke(objectToCompare1) +
                                " promis_work_rels_latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_work_rels_latest for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
            }
            }
        }
    }

    @Given("^We get the (.*) random Promis transform mapping ids of (.*)$")
    public void getRandomPromisTransformMappingIds(String numberOfRecords, String Currenttablename) {
      numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        sql = String.format(PromisETLDataCheckSQL.GET_UKEY_IDS, Currenttablename, numberOfRecords);
        List<Map<?, ?>> randomTransformMappingIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomTransformMappingIds.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get Promis transform mapping records from (.*)$")
    public void getTransformMappingRecords(String tablename) throws ParseException {
        Log.info("We get the records from Transform Mapping Query..");
        switch (tablename) {
            case "subject_areas":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS_TransformMapping, Joiner.on("','").join(Ids));
                break;
            case "pricing":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING_TransformMapping, Joiner.on("','").join(Ids));
                break;
            case "person_roles":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES_TransformMapping, Joiner.on("','").join(Ids));
                break;
            case "works":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS_TransformMapping, Joiner.on("','").join(Ids));
                break;
            case "metrics":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS_TransformMapping, Joiner.on("','").join(Ids));
                break;
            case "urls":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS_TransformMapping, Joiner.on("','").join(Ids));
                break;
            case "work_rels":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS_TransformMapping, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisContext.tbPRMDataObjectsFromTransformMapping = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
//        System.out.println(PromisDataContext.tbPRMDataObjectsFromTransformMapping.size());
    }

    @Then("^We get the Promis Transform mapping current records from (.*)$")
    public void getRecordsInCurrenttablename(String Currenttablename) throws ParseException {
        Log.info("We get the Current records..");
        switch (Currenttablename) {
            case "promis_transform_current_subject_areas":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_current_pricing":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_current_person_roles":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_current_works":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_current_metrics":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_current_urls":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_current_work_rels":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS, Currenttablename, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records for transform mapping and current of (.*)$")
    public void comparePromDataTransformMappingtoCurrent(String tablename)throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.size(); i++) {
            switch (tablename) {
                case "subject_areas":
                    Log.info("Sorting the data to compare the Subject Area records for TransformMappingCurrent..");
                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_subject_areas_mapping = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRIORITY","getSUBJECT_AREA_CODE","getSUBJECT_AREA_NAME","getSUBJECT_TYPE_CODE","getSUBJECT_TYPE_NAME"};
                    for (String strTemp : etl_promis_subject_areas_mapping) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;
                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);
                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);
                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " " + strTemp + " => promis_subject_areas = " + method.invoke(objectToCompare1) +
                                " promis_subject_areas_mapping = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_subject_areas_mapping for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "pricing":
                    Log.info("Sorting the data to compare the Pricing records between TransformMappingCurrent and TransformMapping ..");
                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_pricing_mapping = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRODUCT_TYPE","getCURRENCY","getPRICE","getSTART_DATE","getEND_DATE","getREGION","getQUANTITY","getCUSTOMER_CATEGORY"};

                    for (String strTemp : etl_promis_pricing_mapping) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " " + strTemp + " => _promis_pricing = " + method.invoke(objectToCompare1) +
                                " promis_pricing_mapping = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_pricing_mapping for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "person_roles":
                    Log.info("Sorting the data to compare the Person Roles records between TransformMappingCurrent and TransformMapping ..");
                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] promis_person_roles_mapping = {"getU_KEY","getPUB_IDT","getEPR_ID","getSEQUENCE_NUMBER","getROLE_DESCRIPTION","getGROUP_NUMBER","getINITIALS","getLAST_NAME","getTITLE","getHONOURS","getAFFILIATION","getIMAGE_URL","getFOOTNOTE_TXT","getNOTES_TXT","getWORK_TYPE"};

                    for (String strTemp : promis_person_roles_mapping) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " " + strTemp + " => promis_person_roles = " + method.invoke(objectToCompare1) +
                                " promis_person_roles_mapping = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_person_roles_latest for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "works":
                    Log.info("Sorting the data to compare the Works records between TransformMappingCurrent and TransformMapping ..");

                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_works_mapping = {"getU_KEY","getPUB_IDT","getEPR_ID","getJOURNAL_AIMS_SCOPE","getELSEVIER_COM_IND","getPRIMARY_AUTHOR","getPREVIOUS_TITLE","getWORK_TYPE"};
                    for (String strTemp : etl_promis_works_mapping) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " " + strTemp + " => promis_works = " + method.invoke(objectToCompare1) +
                                " promis_works_mapping = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_works_mapping for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "metrics":
                    Log.info("Sorting the data to compare the Metrics records between TransformMappingCurrent and TransformMapping ..");

                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_metrics_mapping = {"getU_KEY","getPUB_IDT","getEPR_ID","getMETRIC_CODE","getMETRIC_NAME","getMETRIC","getMETRIC_YEAR","getMETRIC_URL","getWORK_TYPE"};

                    for (String strTemp : etl_promis_metrics_mapping) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " " + strTemp + " => promis_metrics = " + method.invoke(objectToCompare1) +
                                " promis_metrics_mapping = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_metrics_mapping for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                case "urls":
                    Log.info("Sorting the data to compare the Urls records between TransformMappingCurrent and TransformMapping ..");
                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_urls_mapping = {"getU_KEY","getPUB_IDT","getEPR_ID","getURL_CODE","getURL_NAME","getURL","getURL_TITLE","getWORK_TYPE"};

                    for (String strTemp : etl_promis_urls_mapping) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " " + strTemp + " => promis_urls = " + method.invoke(objectToCompare1) +
                                " promis_urls_mapping = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_urls_mapping for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }

                    break;
                case "work_rels":
                    Log.info("Sorting the data to compare the Work_Rels records between TransformMappingCurrent and TransformMapping ..");

                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                    String[] etl_promis_work_rels_mapping = {"getU_KEY","getCHILD_PUB_IDT","getPARENT_EPR_ID","getCHILD_EPR_ID","getCHILD_TITLE","getCHILD_RELATED_TYPE_CODE","getCHILD_RELATED_TYPE_NAME","getCHILD_RELATED_STATUS_CODE","getCHILD_RELATED_TYPE_ROLL_UP","getCHILD_RELATED_STATUS_NAME","getCHILD_RELATED_STATUS_ROLL_UP","getRELATIONSHIP_TYPE_CODE","getRELATIONSHIP_TYPE_NAME"};

                    for (String strTemp : etl_promis_work_rels_mapping) {
                        java.lang.reflect.Method method;
                        java.lang.reflect.Method method2;

                        PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i);
                        PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);

                        method = objectToCompare1.getClass().getMethod(strTemp);
                        method2 = objectToCompare2.getClass().getMethod(strTemp);

                        Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " " + strTemp + " => promis_work_rels = " + method.invoke(objectToCompare1) +
                                " promis_work_rels_latest = " + method2.invoke(objectToCompare2));
                        if (method.invoke(objectToCompare1) != null ||
                                (method2.invoke(objectToCompare2) != null)) {
                            Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_work_rels_mapping for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    method.invoke(objectToCompare1),
                                    method2.invoke(objectToCompare2));
                        }
                    }
                    break;
                    }
            }
        }
    }

    @Given("^We get the (.*) random Promis Latest ids of (.*)$")
    public void getRandomJMDBIds(String numberOfRecords, String latesttablename) {
//  numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random IDs...");
        switch (latesttablename) {
            case "promis_transform_latest_pricing":
                sql = String.format(PromisETLDataCheckSQL.GET_LATEST_PRICING_IDs, numberOfRecords);
                List<Map<?, ?>> randomLatestPricingIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomLatestPricingIds.stream().map(m -> (Integer) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "promis_transform_latest_works":
                sql = String.format(PromisETLDataCheckSQL.GET_LATEST_WORKS_IDs, numberOfRecords);
                List<Map<?, ?>> randomLatestWorksIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomLatestWorksIds.stream().map(m -> (Integer) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;
        }
        Log.info(sql);
        Log.info(Ids.toString());
    }




    @Given("^We get the (.*) random Promis current ids of (.*)$")
    public void getRandomPromisCurrId(String numberOfRecords, String promisCurrent) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        sql = String.format(PromisETLDataCheckSQL.GET_UKEY_IDS, promisCurrent, numberOfRecords);
        List<Map<?, ?>> randomCurrIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomCurrIds.stream().map(m -> (String) m.get("U_KEY")).map(String::valueOf).collect(Collectors.toList());
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the Promis transform file history records from (.*)$")
    public void gettransformFileRecords(String transformFileTable) throws ParseException {
        Log.info("We get the records from transform_file..");
        switch (transformFileTable) {
            case "promis_transform_file_history_subject_areas_part":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS_TRANSFORM_FILE, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_pricing_part":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING_TRANSFORM_FILE, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_person_roles_part":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES_TRANSFORM_FILE, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_works_part":
                    sql = String.format(PromisETLDataCheckSQL.GET_WORKS_TRANSFORM_FILE, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_metrics_part":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS_TRANSFORM_FILE, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_urls_part":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS_TRANSFORM_FILE, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_file_history_work_rels_part":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS_TRANSFORM_FILE, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisContext.tbPRMDataObjectsFromtransformFile = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
        //System.out.println(PromisDataContext.tbPRMDataObjectsFromtransformFile.size());
    }

    @And("^Compare Promis records for current and transform file of (.*)$")
    public void comparePromDataCurrtoTransformFile(String transformFileHist) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.size(); i++) {
                switch (transformFileHist) {
                    case "promis_transform_file_history_subject_areas_part":
                        Log.info("Sorting the data to compare the Subject Area records for transform File..");
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromtransformFile.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        String[] etl_promis_transform_file_subject_areas = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRIORITY","getSUBJECT_AREA_CODE","getSUBJECT_AREA_NAME","getSUBJECT_TYPE_CODE","getSUBJECT_TYPE_NAME"};

                        for (String strTemp : etl_promis_transform_file_subject_areas) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;
                            PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);
                            PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromtransformFile.get(i);
                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);
                            Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY() +
                                    " " + strTemp + " => promis_subject_areas = " + method.invoke(objectToCompare1) +
                                    " promis_transform_file_subject_areas = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_transform_file_subject_areas for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "promis_transform_file_history_pricing_part":
                        Log.info("Sorting the data to compare the Pricing records between current and transform_file ..");
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromtransformFile.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        String[] etl_promis_pricing_transform_file = {"getU_KEY","getPUB_IDT","getEPR_ID","getPRODUCT_TYPE","getCURRENCY","getPRICE","getSTART_DATE","getEND_DATE","getREGION","getQUANTITY","getCUSTOMER_CATEGORY"};

                        for (String strTemp : etl_promis_pricing_transform_file) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);
                            PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromtransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY() +
                                    " " + strTemp + " => promis_pricing = " + method.invoke(objectToCompare1) +
                                    " promis_pricing_transform_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_pricing_transform_file for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;

                    case "promis_transform_file_history_person_roles_part":
                        Log.info("Sorting the data to compare the Person Roles records between current and transform_file ..");
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromtransformFile.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        String[] etl_promis_person_roles_transform_file = {"getU_KEY","getPUB_IDT","getEPR_ID","getSEQUENCE_NUMBER","getROLE_DESCRIPTION","getGROUP_NUMBER","getINITIALS","getLAST_NAME","getTITLE","getHONOURS","getAFFILIATION","getIMAGE_URL","getFOOTNOTE_TXT","getNOTES_TXT","getWORK_TYPE"};

                        for (String strTemp : etl_promis_person_roles_transform_file) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);
                            PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromtransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY() +
                                    " " + strTemp + " => promis_person_roles = " + method.invoke(objectToCompare1) +
                                    " promis_person_roles_transform_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_person_roles_transform_file for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;
                    case "promis_transform_file_history_works_part":
                        Log.info("Sorting the data to compare the Works records between current and transform file ..");
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromtransformFile.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        String[] etl_promis_works_transform_file = {"getU_KEY","getPUB_IDT","getEPR_ID","getJOURNAL_AIMS_SCOPE","getELSEVIER_COM_IND","getPRIMARY_AUTHOR","getPREVIOUS_TITLE","getWORK_TYPE"};

                        for (String strTemp : etl_promis_works_transform_file) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);
                            PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromtransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY() +
                                    " " + strTemp + " => promis_works = " + method.invoke(objectToCompare1) +
                                    " promis_works_transform_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_works_transform_file for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }

                        break;
                    case "promis_transform_file_history_metrics_part":
                        Log.info("Sorting the data to compare the Metrics records between current and transform file ..");
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromtransformFile.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        String[] etl_promis_metrics_transform_file = {"getU_KEY","getPUB_IDT","getEPR_ID","getMETRIC_CODE","getMETRIC_NAME","getMETRIC","getMETRIC_YEAR","getMETRIC_URL","getWORK_TYPE"};

                        for (String strTemp : etl_promis_metrics_transform_file) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);
                            PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromtransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY() +
                                    " " + strTemp + " => promis_metrics = " + method.invoke(objectToCompare1) +
                                    " promis_metrics_transform_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_metrics_transform_file for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "promis_transform_file_history_urls_part":
                        Log.info("Sorting the data to compare the Urls records between current and transfom_file ..");
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromtransformFile.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        String[] etl_promis_urls_transform_file = {"getU_KEY","getPUB_IDT","getEPR_ID","getURL_CODE","getURL_NAME","getURL","getURL_TITLE","getWORK_TYPE"};

                        for (String strTemp : etl_promis_urls_transform_file) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);
                            PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromtransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY() +
                                    " " + strTemp + " => promis_urls = " + method.invoke(objectToCompare1) +
                                    " promis_urls_transform_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_urls_transform_file for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                    case "promis_transform_file_history_work_rels_part":
                        Log.info("Sorting the data to compare the Work_Rels records between current and transform_file ..");
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromtransformFile.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        String[] etl_promis_work_rels_transform_file = {"getU_KEY","getCHILD_PUB_IDT","getPARENT_EPR_ID","getCHILD_EPR_ID","getCHILD_TITLE","getCHILD_RELATED_TYPE_CODE","getCHILD_RELATED_TYPE_NAME","getCHILD_RELATED_STATUS_CODE","getCHILD_RELATED_TYPE_ROLL_UP","getCHILD_RELATED_STATUS_NAME","getCHILD_RELATED_STATUS_ROLL_UP","getRELATIONSHIP_TYPE_CODE","getRELATIONSHIP_TYPE_NAME"};

                        for (String strTemp : etl_promis_work_rels_transform_file) {
                            java.lang.reflect.Method method;
                            java.lang.reflect.Method method2;

                            PRMTablesETLObject objectToCompare1 = PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i);
                            PRMTablesETLObject objectToCompare2 = PromisDataContext.tbPRMDataObjectsFromtransformFile.get(i);

                            method = objectToCompare1.getClass().getMethod(strTemp);
                            method2 = objectToCompare2.getClass().getMethod(strTemp);

                            Log.info("U_KEY => " +  PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY() +
                                    " " + strTemp + " => promis_work_rels = " + method.invoke(objectToCompare1) +
                                    " promis_work_rels_transform_file = " + method2.invoke(objectToCompare2));
                            if (method.invoke(objectToCompare1) != null ||
                                    (method2.invoke(objectToCompare2) != null)) {
                                Assert.assertEquals("The " + strTemp + " is =" + method.invoke(objectToCompare1) + " is missing/not found in promis_work_rels_transform_file for U_KEY:"+PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getU_KEY(),
                                        method.invoke(objectToCompare1),
                                        method2.invoke(objectToCompare2));
                            }
                        }
                        break;
                }

            }
        }
    }

    @Given("^we Get the Duplicate count in the PROMIS (.*) table$")
    public static void getDuplicateCounts(String tableName){
        switch (tableName){
            case "promis_transform_latest_subject_areas":
                promisDuplicateLatestSQLCount = PromisETLDataCheckSQL.GET_PROMIS_DUPLICATES_COUNT_SUBJ_AREA;
                break;
            case "promis_transform_latest_pricing":
                promisDuplicateLatestSQLCount = PromisETLDataCheckSQL.GET_PROMIS_DUPLICATE_COUNT_PRICING;
                break;
            case "promis_transform_latest_person_roles":
                promisDuplicateLatestSQLCount = PromisETLDataCheckSQL.GET_PROMIS_DUPLICATE_COUNT_PERSON_ROLE;
                break;
            case "promis_transform_latest_metrics":
                promisDuplicateLatestSQLCount = PromisETLDataCheckSQL.GET_PROMIS_DUPLICATE_COUNT_METRICS;
                break;
            case "promis_transform_latest_urls":
                promisDuplicateLatestSQLCount = PromisETLDataCheckSQL.GET_PROMIS_DUPLICATE_COUNT_URL;
                break;
            case "promis_transform_latest_work_rels":
                    promisDuplicateLatestSQLCount = PromisETLDataCheckSQL.GET_PROMIS_DUPLICATE_COUNT_WORK_RELS;
                break;

            default:
                Log.info("no tables found");
        }
        Log.info(promisDuplicateLatestSQLCount);
        List<Map<String, Object>> promisDupLatestTableCount = DBManager.getDBResultMap(promisDuplicateLatestSQLCount, Constants.AWS_URL);
        promisDuplicateLatestCount = ((Long) promisDupLatestTableCount.get(0).get("Duplicate_Count")).intValue();
    }

    @Then("^Check the count should be Zero (.*)$")
    public void checkDupCountZero(String tableName){
        Log.info("The Duplicate count for "+tableName+" => " + promisDuplicateLatestCount);
        Assert.assertEquals("There are Duplicate Count for unique value in "+tableName,0,promisDuplicateLatestCount);

    }


}
