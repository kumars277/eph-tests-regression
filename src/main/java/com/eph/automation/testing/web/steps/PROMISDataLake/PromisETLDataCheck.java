package com.eph.automation.testing.web.steps.PROMISDataLake;

import com.eph.automation.testing.configuration.*;
import com.eph.automation.testing.helper.*;
import com.eph.automation.testing.models.contexts.PromisETL.PromisContext;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesCurrentObject;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesDLObject;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesETLObject;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.PromisETLDataCheckSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.*;
import net.minidev.json.parser.ParseException;
import org.apache.poi.ss.formula.functions.IDStarAlgorithm;
import org.junit.Assert;


import java.util.*;
import java.util.stream.*;

public class PromisETLDataCheck {
    public PromisContext PromisDataContext;
    public PromisContext PromisETLDataContext;
    private static String sql;
    private static List<String> Ids;

    @Given("^We get the (.*) random Promis ids of (.*)$")
    public void getRandomPromisIds(String numberOfRecords, String Currenttablename) {
        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Getting random records...");
        switch (Currenttablename) {
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
            case "promis_prmlondest_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmlondest_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpricest_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpricest_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpubinft_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpubinft_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpubrelt_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpubrelt_part, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
            case "promis_prmpmccodt_part":
                sql = String.format(PromisETLDataCheckSQL.GET_Promis_prmpmccodt, Inboundtablename, Inboundtablename, Joiner.on("','").join(Ids));
                break;
            case "promis_prmincpmct_part":
                sql = String.format(PromisETLDataCheckSQL.GET_promis_prmincpmct, Inboundtablename, Inboundtablename, Joiner.on(",").join(Ids));
                break;
        }
        Log.info(sql);
        PromisContext.tbPRMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.AWS_URL);
        System.out.println(PromisDataContext.tbPRMDataObjectsFromDL.size());
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
            case "promis_prmlondest_current":
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
/*
    @And("^Compare Promis records in Inbound and Current of (.*)$")
    public void comparePromDataInboundtoCurrent(String Inboundtablename) {
        if (PromisDataContext.tbPRMDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            switch (Inboundtablename) {
                case "promis_prmautpubt_part":
                    Log.info("Sorting the data to compare the PRMAUTPUBT records in Inbound and Current ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));
                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getAUT_EDT_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getAUT_EDT_IDT));


                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());

                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_IDT());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_IDT() != null)) {
                            Assert.assertEquals("The AUT_EDT_IDT is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_IDT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_TYP => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_TYP() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_TYP());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_TYP() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_TYP() != null)) {
                            Assert.assertEquals("The AUT_EDT_TYP is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_TYP(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_TYP());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " TYP_DES => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getTYP_DES() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getTYP_DES());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getTYP_DES() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getTYP_DES() != null)) {
                            Assert.assertEquals("The TYP_DES is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getTYP_DES(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getTYP_DES());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SEQ_NUM => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSEQ_NUM() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSEQ_NUM());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSEQ_NUM() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSEQ_NUM() != null)) {
                            Assert.assertEquals("The SEQ_NUM is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSEQ_NUM(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSEQ_NUM());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_PRE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PRE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_PRE());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PRE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_PRE() != null)) {
                            Assert.assertEquals("The AUT_EDT_PRE is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PRE(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_PRE());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_INI => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_INI() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_INI());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_INI() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_INI() != null)) {
                            Assert.assertEquals("The AUT_EDT_INI is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_INI(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_INI());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_NAM => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_NAM());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_NAM() != null)) {
                            Assert.assertEquals("The AUT_EDT_NAM is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_NAM());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_SORT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SORT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_SORT());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SORT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_SORT() != null)) {
                            Assert.assertEquals("The AUT_EDT_SORT is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SORT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_SORT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_SUF => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SUF() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_SUF());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SUF() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_SUF() != null)) {
                            Assert.assertEquals("The AUT_EDT_SUF is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SUF(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_SUF());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AFF_TXT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAFF_TXT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAFF_TXT());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAFF_TXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAFF_TXT() != null)) {
                            Assert.assertEquals("The AFF_TXT is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAFF_TXT().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAFF_TXT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " FTN => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFTN() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFTN());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFTN() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFTN() != null)) {
                            Assert.assertEquals("The FTN is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFTN().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFTN());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_JCI => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_JCI() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_JCI());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_JCI() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_JCI() != null)) {
                            Assert.assertEquals("The AUT_EDT_JCI is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_JCI(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_JCI());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " BIO_IMAGE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getBIO_IMAGE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getBIO_IMAGE());

                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getBIO_IMAGE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getBIO_IMAGE() != null)) {
                            Assert.assertEquals("The BIO_IMAGE is incorrect for PUB_IDT=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " and AUT_EDT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getBIO_IMAGE(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getBIO_IMAGE());
                        }
                    }
                    break;
                case "promis_prmclscodt_part":
                    Log.info("Sorting the data to compare the PRMCLSCODT records between inbound and current ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getCLS_COD)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getCLS_COD));

                        Log.info("CLS_COD => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() +
                                " CLS_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_COD() != null)) {
                            Assert.assertEquals("The CLS_COD =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_COD());
                        }

                        Log.info("CLS_COD => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() +
                                " CLS_DES => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_DES() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_DES());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_DES() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_DES() != null)) {
                            Assert.assertEquals("The CLS_DES is incorrect for CLS_COD=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_DES(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_DES());
                        }
                    }
                    break;
                case "promis_prmclst_part":
                    Log.info("Sorting the data to compare the PRMCLST records in Inbound and DATA LAKE ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getCLS_COD)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getCLS_COD));

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " CLS_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_COD() != null)) {
                            Assert.assertEquals("The CLS_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCLS_COD());

                        }
                    }
                    break;
                case "promis_prmlondest_part":
                    Log.info("Sorting the data to compare the PRMLONDEST records in Inbound and DATA LAKE ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_VOL_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_VOL_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getVOL_PRT_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getVOL_PRT_IDT));
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_VOL_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_VOL_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_VOL_IDT() != null)) {
                            Assert.assertEquals("The PUB_VOL_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_VOL_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " VOL_PRT_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getVOL_PRT_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getVOL_PRT_IDT() != null)) {
                            Assert.assertEquals("The VOL_PRT_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getVOL_PRT_IDT());
                        }
                    }
                    break;
                case "promis_prmpricest_part":
                    Log.info("Sorting the data to compare the PRMPRICEST records in Inbound and DATA LAKE ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_VOL_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_VOL_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getVOL_PRT_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getVOL_PRT_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getEDN_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getEDN_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getEDN_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getEDN_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPRC_TYP)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPRC_TYP));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPRC_GEO)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPRC_GEO));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getIPT_COD)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getIPT_COD));


                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_VOL_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_VOL_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_VOL_IDT() != null)) {
                            Assert.assertEquals("The PUB_VOL_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_VOL_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " VOL_PRT_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getVOL_PRT_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getVOL_PRT_IDT() != null)) {
                            Assert.assertEquals("The VOL_PRT_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getVOL_PRT_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " EDN_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEDN_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEDN_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEDN_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEDN_IDT() != null)) {
                            Assert.assertEquals("The EDN_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEDN_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEDN_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PRC_TYP => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_TYP() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_TYP());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_TYP() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_TYP() != null)) {
                            Assert.assertEquals("The PRC_TYP is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_TYP(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_TYP());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PRC_GEO => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_GEO() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_GEO());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_GEO() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_GEO() != null)) {
                            Assert.assertEquals("The PRC_GEO is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_GEO(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_GEO());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " IPT_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIPT_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIPT_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIPT_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIPT_COD() != null)) {
                            Assert.assertEquals("The IPT_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIPT_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIPT_COD());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PRC_DAT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_DAT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_DAT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_DAT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_DAT() != null)) {
                            Assert.assertEquals("The PRC_DAT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_DAT().substring(0, 10),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_DAT().substring(0, 10));
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " STD_CUR_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTD_CUR_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTD_CUR_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTD_CUR_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTD_CUR_COD() != null)) {
                            Assert.assertEquals("The STD_CUR_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTD_CUR_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTD_CUR_COD());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " STD_PRC => Inbound=" + String.valueOf(Math.round(Double.parseDouble(PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTD_PRC()))) +
                                " Current=" + String.valueOf(Math.round(Double.parseDouble(PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTD_PRC()))));
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTD_CUR_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTD_CUR_COD() != null)) {
                            Assert.assertEquals("The STD_PRC is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    String.valueOf(Math.round(Double.parseDouble(PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTD_PRC()))),
                                    String.valueOf(Math.round(Double.parseDouble(PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTD_PRC()))));
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PRC_PRE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_PRE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_PRE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_PRE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_PRE() != null)) {
                            Assert.assertEquals("The PRC_PRE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRC_PRE().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRC_PRE());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " ADD_PRC => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getADD_PRC() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getADD_PRC());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getADD_PRC() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getADD_PRC() != null)) {
                            Assert.assertEquals("The ADD_PRC is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getADD_PRC().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getADD_PRC());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " EXP_DAT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEXP_DAT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEXP_DAT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEXP_DAT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEXP_DAT() != null)) {
                            Assert.assertEquals("The EXP_DAT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEXP_DAT().substring(0, 10),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEXP_DAT().substring(0, 10));
                        }

                    }
                    break;
                case "promis_prmpubinft_part":
                    Log.info("Sorting the data to compare the PRMPUBINFT records in Inbound and DATA LAKE ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " STA_DAA => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTA_DAA() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTA_DAA());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTA_DAA() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTA_DAA() != null)) {
                            Assert.assertEquals("The STA_DAA is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTA_DAA(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTA_DAA());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " FUL_TIT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFUL_TIT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFUL_TIT() != null)) {
                            Assert.assertEquals("The FUL_TIT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFUL_TIT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_TYP => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_TYP() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_TYP());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_TYP() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_TYP() != null)) {
                            Assert.assertEquals("The PUB_TYP is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_TYP(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_TYP());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " OWN_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getOWN_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getOWN_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getOWN_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getOWN_IDT() != null)) {
                            Assert.assertEquals("The OWN_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getOWN_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getOWN_IDT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PBL_ABR_NAM => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPBL_ABR_NAM() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPBL_ABR_NAM());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPBL_ABR_NAM() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPBL_ABR_NAM() != null)) {
                            Assert.assertEquals("The PBL_ABR_NAM is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPBL_ABR_NAM(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPBL_ABR_NAM());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " LOC => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLOC() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLOC());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLOC() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLOC() != null)) {
                            Assert.assertEquals("The LOC is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLOC(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLOC());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " STA_PRM => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTA_PRM() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTA_PRM());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTA_PRM() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTA_PRM() != null)) {
                            Assert.assertEquals("The STA_PRM is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSTA_PRM(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSTA_PRM());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " LNG_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLNG_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLNG_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLNG_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLNG_COD() != null)) {
                            Assert.assertEquals("The LNG_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLNG_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLNG_COD());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_IMP_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IMP_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IMP_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IMP_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IMP_IDT() != null)) {
                            Assert.assertEquals("The PUB_IMP_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IMP_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_IMP_IDT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PUB_BGN_YEA => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_BGN_YEA() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_BGN_YEA());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_BGN_YEA() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_BGN_YEA() != null)) {
                            Assert.assertEquals("The PUB_BGN_YEA is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_BGN_YEA(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_BGN_YEA());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " LCO_NUM => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLCO_NUM() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLCO_NUM());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLCO_NUM() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLCO_NUM() != null)) {
                            Assert.assertEquals("The LCO_NUM is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLCO_NUM(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLCO_NUM());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " IMP_DAT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIMP_DAT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIMP_DAT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIMP_DAT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIMP_DAT() != null)) {
                            Assert.assertEquals("The IMP_DAT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIMP_DAT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIMP_DAT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " CDA => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCDA() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCDA());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCDA() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCDA() != null)) {
                            Assert.assertEquals("The CDA is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCDA(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCDA());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " CRE_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCRE_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCRE_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCRE_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCRE_IDT() != null)) {
                            Assert.assertEquals("The CRE_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCRE_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCRE_IDT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " DEP_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getDEP_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getDEP_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getDEP_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getDEP_IDT() != null)) {
                            Assert.assertEquals("The DEP_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getDEP_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getDEP_IDT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " LST_USR_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_USR_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_USR_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_USR_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_USR_IDT() != null)) {
                            Assert.assertEquals("The LST_USR_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_USR_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_USR_IDT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " LST_UPD_DAT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DAT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_UPD_DAT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DAT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_UPD_DAT() != null)) {
                            Assert.assertEquals("The LST_UPD_DAT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DAT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_UPD_DAT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " ABR_TIT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getABR_TIT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getABR_TIT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getABR_TIT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getABR_TIT() != null)) {
                            Assert.assertEquals("The ABR_TIT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getABR_TIT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getABR_TIT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " FUL_TIT_SRT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT_SRT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFUL_TIT_SRT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT_SRT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFUL_TIT_SRT() != null)) {
                            Assert.assertEquals("The FUL_TIT_SRT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT_SRT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFUL_TIT_SRT());
                        }


                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SUB_TIT_1 => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_1() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_1());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_1() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_1() != null)) {
                            Assert.assertEquals("The SUB_TIT_1 is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_1().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_1());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SUB_TIT_2 => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_2() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_2());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_2() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_2() != null)) {
                            Assert.assertEquals("The SUB_TIT_2 is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_2().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_2());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SUB_TIT_3 => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_3() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_3());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_3() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_3() != null)) {
                            Assert.assertEquals("The SUB_TIT_3 is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_3().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUB_TIT_3());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PRG_SUB_TIT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRG_SUB_TIT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRG_SUB_TIT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRG_SUB_TIT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRG_SUB_TIT() != null)) {
                            Assert.assertEquals("The PRG_SUB_TIT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPRG_SUB_TIT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPRG_SUB_TIT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_EDT_NAM => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_NAM());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_NAM() != null)) {
                            Assert.assertEquals("The AUT_EDT_NAM is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_EDT_NAM());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SLO_TXT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSLO_TXT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSLO_TXT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSLO_TXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSLO_TXT() != null)) {
                            Assert.assertEquals("The SLO_TXT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSLO_TXT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSLO_TXT());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " NTB => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getNTB() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getNTB());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getNTB() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getNTB() != null)) {
                            Assert.assertEquals("The NTB is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getNTB(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getNTB());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " FTN => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFTN() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFTN());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFTN() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFTN() != null)) {
                            Assert.assertEquals("The FTN is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFTN().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFTN());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " FOR_TIT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFOR_TIT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFOR_TIT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFOR_TIT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFOR_TIT() != null)) {
                            Assert.assertEquals("The FOR_TIT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFOR_TIT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFOR_TIT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " RPN_INF => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRPN_INF() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRPN_INF());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRPN_INF() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRPN_INF() != null)) {
                            Assert.assertEquals("The RPN_INF is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRPN_INF(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRPN_INF());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " ADV_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getADV_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getADV_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getADV_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getADV_COD() != null)) {
                            Assert.assertEquals("The ADV_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getADV_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getADV_COD());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " RVW_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRVW_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRVW_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRVW_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRVW_COD() != null)) {
                            Assert.assertEquals("The RVW_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRVW_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRVW_COD());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SUP_APP => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUP_APP() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRVW_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUP_APP() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUP_APP() != null)) {
                            Assert.assertEquals("The SUP_APP is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSUP_APP(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSUP_APP());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " PHY_SZE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPHY_SZE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPHY_SZE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPHY_SZE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPHY_SZE() != null)) {
                            Assert.assertEquals("The PHY_SZE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPHY_SZE(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPHY_SZE());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " FS_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFS_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFS_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFS_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFS_IDT() != null)) {
                            Assert.assertEquals("The FS_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFS_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFS_IDT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " WTK_NUM => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getWTK_NUM() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getWTK_NUM());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getWTK_NUM() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getWTK_NUM() != null)) {
                            Assert.assertEquals("The WTK_NUM is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getWTK_NUM(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getWTK_NUM());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " WTK_CLS => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getWTK_CLS() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getWTK_CLS());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getWTK_CLS() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getWTK_CLS() != null)) {
                            Assert.assertEquals("The WTK_CLS is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getWTK_CLS(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getWTK_CLS());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " BWK => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getBWK() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getBWK());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getBWK() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getBWK() != null)) {
                            Assert.assertEquals("The BWK is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getBWK(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getBWK());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AEI => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAEI() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAEI());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAEI() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAEI() != null)) {
                            Assert.assertEquals("The AEI is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAEI(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAEI());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " REV_EDN_TI => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREV_EDN_TI() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREV_EDN_TI());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREV_EDN_TI() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREV_EDN_TI() != null)) {
                            Assert.assertEquals("The REV_EDN_TI is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREV_EDN_TI(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREV_EDN_TI());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " JNL_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getJNL_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getJNL_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getJNL_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getJNL_IDT() != null)) {
                            Assert.assertEquals("The JNL_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getJNL_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getJNL_IDT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " LST_UPD_DATE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DATE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_UPD_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_UPD_DATE() != null)) {
                            Assert.assertEquals("The LST_UPD_DATE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DATE().substring(0, 10),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getLST_UPD_DATE().substring(0, 10));
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " CDA_DATE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCDA_DATE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCDA_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCDA_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCDA_DATE() != null)) {
                            Assert.assertEquals("The CDA_DATE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getCDA_DATE().substring(0, 10),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getCDA_DATE().substring(0, 10));
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AQS_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAQS_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAQS_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAQS_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAQS_COD() != null)) {
                            Assert.assertEquals("The AQS_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAQS_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAQS_COD());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " MKT_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_COD() != null)) {
                            Assert.assertEquals("The MKT_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_COD());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUD() != null)) {
                            Assert.assertEquals("The AUD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUD().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUD());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SHT_DES => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSHT_DES() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSHT_DES());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSHT_DES() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSHT_DES() != null)) {
                            Assert.assertEquals("The SHT_DES is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSHT_DES().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSHT_DES());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SHT_CTT_DES => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSHT_CTT_DES() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSHT_CTT_DES());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSHT_CTT_DES() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSHT_CTT_DES() != null)) {
                            Assert.assertEquals("The SHT_CTT_DES is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSHT_CTT_DES().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSHT_CTT_DES());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SLO_EXP_DAT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSLO_EXP_DAT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSLO_EXP_DAT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSLO_EXP_DAT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSLO_EXP_DAT() != null)) {
                            Assert.assertEquals("The SLO_EXP_DAT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSLO_EXP_DAT().substring(0, 10),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSLO_EXP_DAT().substring(0, 10));
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " IF_NO => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_NO() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_NO());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_NO() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_NO() != null)) {
                            Assert.assertEquals("The IF_NO is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_NO(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_NO());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " IF_YEAR => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_YEAR() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_YEAR());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_YEAR() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_YEAR() != null)) {
                            Assert.assertEquals("The IF_YEAR is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_YEAR(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_YEAR());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " IF_COMMENT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_COMMENT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_COMMENT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_COMMENT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_COMMENT() != null)) {
                            Assert.assertEquals("The IF_COMMENT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_COMMENT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_COMMENT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " AUT_BENEFITS => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_BENEFITS() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_BENEFITS());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_BENEFITS() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_BENEFITS() != null)) {
                            Assert.assertEquals("The AUT_BENEFITS is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getAUT_BENEFITS().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getAUT_BENEFITS());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " SOURCE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSOURCE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSOURCE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSOURCE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSOURCE() != null)) {
                            Assert.assertEquals("The SOURCE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getSOURCE(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getSOURCE());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " ARG_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getARG_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getARG_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getARG_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getARG_COD() != null)) {
                            Assert.assertEquals("The ARG_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getARG_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getARG_COD());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " IF_5 => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_5() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_5());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_5() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_5() != null)) {
                            Assert.assertEquals("The IF_5 is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_5(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_5());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " IF_RANKING => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_RANKING() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_RANKING());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_RANKING() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_RANKING() != null)) {
                            Assert.assertEquals("The IF_RANKING is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_RANKING(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_RANKING());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " IF_CAT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_CAT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_CAT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_CAT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_CAT() != null)) {
                            Assert.assertEquals("The IF_CAT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getIF_CAT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getIF_CAT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " OA_TYPE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getOA_TYPE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getOA_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getOA_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getOA_TYPE() != null)) {
                            Assert.assertEquals("The OA_TYPE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getOA_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getOA_TYPE());
                        }

                    }
                    break;
                case "promis_prmpubrelt_part":
                    Log.info("Sorting the data to compare the PRMPUBRELT records in Inbound and DATA LAKE ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_PUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_PUB_IDT));

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getREL_NO)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getREL_NO));


                        Log.info("PUB_PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " PUB_PUB_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_PUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_PUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getPUB_PUB_IDT());

                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " REL_NO => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_NO() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_NO());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_NO() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_NO() != null)) {
                            Assert.assertEquals("The REL_NO is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_NO(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_NO());
                        }

                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " REL_SRT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_NO() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_SRT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_SRT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_SRT() != null)) {
                            Assert.assertEquals("The REL_SRT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_SRT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_SRT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " REL_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_IDT() != null)) {
                            Assert.assertEquals("The REL_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_IDT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " REL_TITLE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_TITLE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_TITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_TITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_TITLE() != null)) {
                            Assert.assertEquals("The REL_TITLE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_TITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_TITLE());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " FOOTNOTE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFOOTNOTE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFOOTNOTE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFOOTNOTE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFOOTNOTE() != null)) {
                            Assert.assertEquals("The FOOTNOTE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFOOTNOTE(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFOOTNOTE());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " FRONT_TEXT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFRONT_TEXT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFRONT_TEXT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFRONT_TEXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFRONT_TEXT() != null)) {
                            Assert.assertEquals("The FRONT_TEXT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getFRONT_TEXT().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getFRONT_TEXT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " END_TEXT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEND_TEXT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEND_TEXT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEND_TEXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEND_TEXT() != null)) {
                            Assert.assertEquals("The END_TEXT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getEND_TEXT().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getEND_TEXT());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " RTP_RTP_COD => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRTP_RTP_COD() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRTP_RTP_COD());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRTP_RTP_COD() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRTP_RTP_COD() != null)) {
                            Assert.assertEquals("The RTP_RTP_COD is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getRTP_RTP_COD(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getRTP_RTP_COD());
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " REL_END_DATE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_END_DATE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_END_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_END_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_END_DATE() != null)) {
                            Assert.assertEquals("The REL_END_DATE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_END_DATE().substring(0, 10),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_END_DATE().substring(0, 10));
                        }
                        Log.info("PUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() +
                                " REL_START_DATE => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_START_DATE() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_START_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_START_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_START_DATE() != null)) {
                            Assert.assertEquals("The REL_START_DATE is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getREL_START_DATE().substring(0, 10),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getREL_START_DATE().substring(0, 10));
                        }
                    }
                    break;
                case "promis_prmincpmct_part":
                    Log.info("Sorting the data to compare the PRMINCPMCT records in Inbound and Current ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getPUB_IDT));

                        Log.info("getPUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " getMKT_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_IDT() != null)) {
                            Assert.assertEquals("The MKT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_IDT());

                        }

                        Log.info("getPUB_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() +
                                " MKT_SUB_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_SUB_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_SUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_SUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_SUB_IDT() != null)) {
                            Assert.assertEquals("The MKT_SUB_IDT is incorrect for PUB_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_SUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_SUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_SUB_IDT());
                        }
                    }
                    break;
                case "promis_prmpmccodt_part":
                    Log.info("Sorting the data to compare the PRMPMCCODT records in Inbound and Current ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDL.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getMKT_IDT)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromCurrent.sort(Comparator.comparing(PRMTablesCurrentObject::getMKT_IDT));

                        Log.info("getMKT_IDT => " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_IDT() +
                                " getMKT_DES => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_DES() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_DES());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_DES() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_DES() != null)) {
                            Assert.assertEquals("The MKT_DES =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_DES() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_DES(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getMKT_DES());

                        }

                        Log.info("getMKT_IDT=> " + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_IDT() +
                                " DIV_IDT => Inbound=" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getDIV_IDT() +
                                " Current=" + PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getDIV_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDL.get(i).getDIV_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getDIV_IDT() != null)) {
                            Assert.assertEquals("The DIV_IDT is incorrect for MKT_IDT =" + PromisDataContext.tbPRMDataObjectsFromDL.get(i).getMKT_SUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDL.get(i).getDIV_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromCurrent.get(i).getDIV_IDT());
                        }
                    }
                    break;
            }
        }
    }
*/
    @Given("^We get the (.*) random Promis DeltaQuery ids of (.*)$")
    public void getRandomPromisDeltaIds(String numberOfRecords, String Deltatablename) {
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
        System.out.println(PromisETLDataContext.tbPRMDataObjectsFromDeltaQuery.size());
    }

    @Then("^We get the Promis Delta records from (.*)$")
    public void getRecordsInDelta(String Deltatablename) throws ParseException {
        Log.info("We get the PreviousHistory records..");
        sql = String.format(PromisETLDataCheckSQL.GET_DELTA, Deltatablename, Joiner.on("','").join(Ids));
        Log.info(sql);
        PromisETLDataContext.tbPRMDataObjectsFromDelta = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records for delta query and delta tables of (.*)$")
    public void comparePromDataDeltaQuerytoDelta(String Deltatablename) {
        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            switch (Deltatablename) {
                case "promis_delta_current_subject_areas":
                    Log.info("Sorting the data to compare the Subject Area records for Delta..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDelta.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));


                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " PUB_IDT => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() +
                                " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " EPR_ID => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() +
                                " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " Priority => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRIORITY() +
                                " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRIORITY());

                        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRIORITY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRIORITY() != null)) {
                            Assert.assertEquals("The priority is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRIORITY(),
                                    PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRIORITY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " SUBJECT_AREA_CODE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_AREA_CODE() +
                                " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_AREA_CODE());

                        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_AREA_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_AREA_CODE() != null)) {
                            Assert.assertEquals("The SUBJECT_AREA_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_AREA_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_AREA_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " SUBJECT_AREA_NAME => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_AREA_NAME() +
                                " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_AREA_NAME());

                        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_AREA_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_AREA_NAME() != null)) {
                            Assert.assertEquals("The SUBJECT_AREA_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_AREA_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_AREA_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " SUBJECT_TYPE_CODE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_TYPE_CODE() +
                                " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_TYPE_CODE());

                        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_TYPE_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_TYPE_CODE() != null)) {
                            Assert.assertEquals("The SUBJECT_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_TYPE_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_TYPE_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                " SUBJECT_TYPE_NAME => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_TYPE_NAME() +
                                " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_TYPE_NAME());

                        if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_TYPE_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_TYPE_NAME() != null)) {
                            Assert.assertEquals("The SUBJECT_TYPE_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSUBJECT_TYPE_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSUBJECT_TYPE_NAME());
                        }
                    }
                    break;
                case "promis_delta_current_pricing":
                            Log.info("Sorting the data to compare the Pricing records between Delta and Delta_Query ..");
                            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDeltaQuery.size(); i++) {

                                PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                                PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PUB_IDT => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT() != null)) {
                                    Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());

                                }
                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " EPR_ID => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());

                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID() != null)) {
                                    Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " Product_type => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRODUCT_TYPE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRODUCT_TYPE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRODUCT_TYPE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRODUCT_TYPE() != null)) {
                                    Assert.assertEquals("The Product_Type for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRODUCT_TYPE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRODUCT_TYPE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CURRENCY => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCURRENCY() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCURRENCY());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCURRENCY() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCURRENCY() != null)) {
                                    Assert.assertEquals("The CURRENCY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCURRENCY(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCURRENCY());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PRICE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRICE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRICE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRICE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRICE() != null)) {
                                    Assert.assertEquals("The PRICE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRICE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRICE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " START_DATE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSTART_DATE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSTART_DATE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSTART_DATE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSTART_DATE() != null)) {
                                    Assert.assertEquals("The START_DATE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSTART_DATE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSTART_DATE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " END_DATE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEND_DATE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEND_DATE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEND_DATE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEND_DATE() != null)) {
                                    Assert.assertEquals("The END_DATE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEND_DATE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEND_DATE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " REGION => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getREGION() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getREGION());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getREGION() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getREGION() != null)) {
                                    Assert.assertEquals("The REGION is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getREGION(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getREGION());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " QUANTITY => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getQUANTITY() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getQUANTITY());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getQUANTITY() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getQUANTITY() != null)) {
                                    Assert.assertEquals("The QUANTITY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getQUANTITY(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getQUANTITY());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CUSTOMER_CATEGORY => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCUSTOMER_CATEGORY() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCUSTOMER_CATEGORY());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCUSTOMER_CATEGORY() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCUSTOMER_CATEGORY() != null)) {
                                    Assert.assertEquals("The CUSTOMER_CATEGORY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCUSTOMER_CATEGORY(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCUSTOMER_CATEGORY());
                                }
                            }
                            break;
                        case "promis_delta_current_person_roles":
                            Log.info("Sorting the data to compare the Person Roles records between Delta and Delta_Query ..");
                            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDeltaQuery.size(); i++) {

                                PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                                PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PUB_IDT => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT() != null)) {
                                    Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());

                                }
                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " EPR_ID => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());

                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID() != null)) {
                                    Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " ROLE_DESCRIPTION => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getROLE_DESCRIPTION() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getROLE_DESCRIPTION());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getROLE_DESCRIPTION() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getROLE_DESCRIPTION() != null)) {
                                    Assert.assertEquals("The ROLE_DESCRIPTION for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getROLE_DESCRIPTION(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getROLE_DESCRIPTION());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " SEQUENCE_NUMBER => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSEQUENCE_NUMBER() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSEQUENCE_NUMBER());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSEQUENCE_NUMBER() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSEQUENCE_NUMBER() != null)) {
                                    Assert.assertEquals("The SEQUENCE_NUMBER is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getSEQUENCE_NUMBER(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getSEQUENCE_NUMBER());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " GROUP_NUMBER => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getGROUP_NUMBER() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getGROUP_NUMBER());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getGROUP_NUMBER() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getGROUP_NUMBER() != null)) {
                                    Assert.assertEquals("The GROUP_NUMBER is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getGROUP_NUMBER(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getGROUP_NUMBER());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " INITIALS => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getINITIALS() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getINITIALS());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getINITIALS() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getINITIALS() != null)) {
                                    Assert.assertEquals("The INITIALS is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getINITIALS(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getINITIALS());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " LAST_NAME => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getLAST_NAME() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getLAST_NAME());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getLAST_NAME() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getLAST_NAME() != null)) {
                                    Assert.assertEquals("The LAST_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getLAST_NAME(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getLAST_NAME());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " TITLE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getTITLE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getTITLE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getTITLE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getTITLE() != null)) {
                                    Assert.assertEquals("The TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getTITLE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getTITLE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " HONOURS => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getHONOURS() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getHONOURS());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getHONOURS() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getHONOURS() != null)) {
                                    Assert.assertEquals("The HONOURS is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getHONOURS(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getHONOURS());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " AFFILIATION => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getAFFILIATION() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getAFFILIATION());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getAFFILIATION() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getAFFILIATION() != null)) {
                                    Assert.assertEquals("The AFFILIATION is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getAFFILIATION(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getAFFILIATION());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " IMAGE_URL => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getIMAGE_URL() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getIMAGE_URL());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getIMAGE_URL() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getIMAGE_URL() != null)) {
                                    Assert.assertEquals("The IMAGE_URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getIMAGE_URL(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getIMAGE_URL());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " FOOTNOTE_TXT => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getFOOTNOTE_TXT() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getFOOTNOTE_TXT());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getFOOTNOTE_TXT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getFOOTNOTE_TXT() != null)) {
                                    Assert.assertEquals("The FOOTNOTE_TXT is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getFOOTNOTE_TXT(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getFOOTNOTE_TXT());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " NOTES_TXT => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getNOTES_TXT() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getNOTES_TXT());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getNOTES_TXT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getNOTES_TXT() != null)) {
                                    Assert.assertEquals("The NOTES_TXT is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getNOTES_TXT(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getNOTES_TXT());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " WORK_TYPE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE() != null)) {
                                    Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                }
                            }
                            break;
                        case "promis_delta_current_works":
                            Log.info("Sorting the data to compare the Works records between History Excluding and Delta_Query ..");
                            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDeltaQuery.size(); i++) {

                                PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                                PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PUB_IDT => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT() != null)) {
                                    Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());

                                }
                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " EPR_ID => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());

                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID() != null)) {
                                    Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " JOURNAL_AIMS_SCOPE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getJOURNAL_AIMS_SCOPE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getJOURNAL_AIMS_SCOPE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getJOURNAL_AIMS_SCOPE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getJOURNAL_AIMS_SCOPE() != null)) {
                                    Assert.assertEquals("The JOURNAL_AIMS_SCOPE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getJOURNAL_AIMS_SCOPE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getJOURNAL_AIMS_SCOPE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " ELSEVIER_COM_IND => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getELSEVIER_COM_IND() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getELSEVIER_COM_IND());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getELSEVIER_COM_IND() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getELSEVIER_COM_IND() != null)) {
                                    Assert.assertEquals("The ELSEVIER_COM_IND is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getELSEVIER_COM_IND(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getELSEVIER_COM_IND());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PRIMARY_AUTHOR => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRIMARY_AUTHOR() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRIMARY_AUTHOR());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRIMARY_AUTHOR() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRIMARY_AUTHOR() != null)) {
                                    Assert.assertEquals("The PRIMARY_AUTHOR is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPRIMARY_AUTHOR(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPRIMARY_AUTHOR());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PREVIOUS_TITLE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPREVIOUS_TITLE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPREVIOUS_TITLE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPREVIOUS_TITLE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPREVIOUS_TITLE() != null)) {
                                    Assert.assertEquals("The PREVIOUS_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPREVIOUS_TITLE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPREVIOUS_TITLE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " WORK_TYPE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE() != null)) {
                                    Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " WORK_TYPE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE() != null)) {
                                    Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                }
                            }
                            break;
                        case "promis_delta_current_metrics":
                            Log.info("Sorting the data to compare the Metrics records between Delta and DeltaQuery ..");
                            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDeltaQuery.size(); i++) {

                                PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                                PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PUB_IDT => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT() != null)) {
                                    Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());

                                }
                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " EPR_ID => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());

                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID() != null)) {
                                    Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " METRIC_CODE => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_CODE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_CODE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_CODE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_CODE() != null)) {
                                    Assert.assertEquals("The METRIC_CODE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_CODE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_CODE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " METRIC_NAME => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_NAME() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_NAME());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_NAME() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_NAME() != null)) {
                                    Assert.assertEquals("The METRIC_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_NAME(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_NAME());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " METRIC => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC() != null)) {
                                    Assert.assertEquals("The METRIC is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " METRIC_YEAR => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_YEAR() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_YEAR());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_YEAR() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_YEAR() != null)) {
                                    Assert.assertEquals("The METRIC_YEAR is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_YEAR(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_YEAR());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " METRIC_URL => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_URL() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_URL());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_URL() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_URL() != null)) {
                                    Assert.assertEquals("The METRIC_URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getMETRIC_URL(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getMETRIC_URL());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " WORK_TYPE => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE() != null)) {
                                    Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                }
                            }
                            break;
                        case "promis_delta_current_urls":
                            Log.info("Sorting the data to compare the Urls records between Delta and DeltaQuery ..");
                            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDeltaQuery.size(); i++) {

                                PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                                PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PUB_IDT => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT() != null)) {
                                    Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPUB_IDT(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPUB_IDT());

                                }
                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " EPR_ID => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());

                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID() != null)) {
                                    Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getEPR_ID(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getEPR_ID());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " URL_CODE => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_CODE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_CODE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_CODE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_CODE() != null)) {
                                    Assert.assertEquals("The URL_CODE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_CODE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_CODE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " URL_NAME => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_NAME() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_NAME());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_NAME() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_NAME() != null)) {
                                    Assert.assertEquals("The URL_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_NAME(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_NAME());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " URL => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL() != null)) {
                                    Assert.assertEquals("The URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " URL_TITLE => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_TITLE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_TITLE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_TITLE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_TITLE() != null)) {
                                    Assert.assertEquals("The URL_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getURL_TITLE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getURL_TITLE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " WORK_TYPE => DeltaQuery=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE() != null)) {
                                    Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getWORK_TYPE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getWORK_TYPE());
                                }
                            }
                            break;
                        case "promis_delta_current_work_rels":
                            Log.info("Sorting the data to compare the Work_Rels records between Delta and Delta_Query ..");
                            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromDeltaQuery.size(); i++) {

                                PromisDataContext.tbPRMDataObjectsFromDeltaQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                                PromisDataContext.tbPRMDataObjectsFromDelta.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_PUB_IDT => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_PUB_IDT() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_PUB_IDT());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_PUB_IDT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_PUB_IDT() != null)) {
                                    Assert.assertEquals("The CHILD_PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_PUB_IDT() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_PUB_IDT(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_PUB_IDT());

                                }
                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " PARENT_EPR_ID => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPARENT_EPR_ID() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPARENT_EPR_ID());

                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPARENT_EPR_ID() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPARENT_EPR_ID() != null)) {
                                    Assert.assertEquals("The PARENT_EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getPARENT_EPR_ID(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getPARENT_EPR_ID());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_EPR_ID => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_EPR_ID() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_EPR_ID());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_EPR_ID() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_EPR_ID() != null)) {
                                    Assert.assertEquals("The CHILD_EPR_ID for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_EPR_ID(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_EPR_ID());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_TITLE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_TITLE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_TITLE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_TITLE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_TITLE() != null)) {
                                    Assert.assertEquals("The CHILD_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_TITLE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_TITLE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_RELATED_TYPE_CODE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_TYPE_CODE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_TYPE_CODE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_TYPE_CODE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_TYPE_CODE() != null)) {
                                    Assert.assertEquals("The CHILD_RELATED_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_TYPE_CODE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_TYPE_CODE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_RELATED => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED() != null)) {
                                    Assert.assertEquals("The CHILD_RELATED is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_RELATED_STATUS_CODE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_CODE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_CODE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_CODE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_CODE() != null)) {
                                    Assert.assertEquals("The CHILD_RELATED_STATUS_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_CODE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_CODE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_RELATED_TYPE_ROLL_UP => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_TYPE_ROLL_UP());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_TYPE_ROLL_UP() != null)) {
                                    Assert.assertEquals("The CHILD_RELATED_TYPE_ROLL_UP is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_TYPE_ROLL_UP());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_RELATED_STATUS_NAME => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_NAME() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_NAME());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_NAME() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_NAME() != null)) {
                                    Assert.assertEquals("The CHILD_RELATED_STATUS_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_NAME(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_NAME());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " CHILD_RELATED_STATUS_ROLL_UP => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_ROLL_UP());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_ROLL_UP() != null)) {
                                    Assert.assertEquals("The CHILD_RELATED_STATUS_ROLL_UP is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getCHILD_RELATED_STATUS_ROLL_UP());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " RELATIONSHIP_TYPE_CODE => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getRELATIONSHIP_TYPE_CODE() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getRELATIONSHIP_TYPE_CODE());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getRELATIONSHIP_TYPE_CODE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getRELATIONSHIP_TYPE_CODE() != null)) {
                                    Assert.assertEquals("The RELATIONSHIP_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getRELATIONSHIP_TYPE_CODE(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getRELATIONSHIP_TYPE_CODE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY() +
                                        " RELATIONSHIP_TYPE_NAME => Delta_Query=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getRELATIONSHIP_TYPE_NAME() +
                                        " Delta=" + PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getRELATIONSHIP_TYPE_NAME());
                                if (PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getRELATIONSHIP_TYPE_NAME() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getRELATIONSHIP_TYPE_NAME() != null)) {
                                    Assert.assertEquals("The RELATIONSHIP_TYPE_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromDeltaQuery.get(i).getRELATIONSHIP_TYPE_NAME(),
                                            PromisDataContext.tbPRMDataObjectsFromDelta.get(i).getRELATIONSHIP_TYPE_NAME());
                                }
                                break;
                    }
            }
        }
    }

    @Given("^We get the (.*) random Promis History Excluding Query ids of (.*)$")
    public void getRandomPromisHistExclIds(String numberOfRecords, String HistExcltablename) {
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
        System.out.println(PromisETLDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.size());
    }

    @Then("^We get the Promis History Excluding records from (.*)$")
    public void getRecordsInHistExcltablename(String HistExcltablename) throws ParseException {
        Log.info("We get the History Excluding records..");
        switch (HistExcltablename) {
            case "promis_transform_history_subject_areas_part":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_pricing_part":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_person_roles_part":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_works_part":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_metrics_part":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_urls_part":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_history_work_rels_part":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS, HistExcltablename, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisETLDataContext.tbPRMDataObjectsFromHistoryExcluding = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records for History Excluding query and History Excluding tables of (.*)$")
    public void comparePromDataHistExclQuerytoHistExcl(String tablename) {
        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            switch (tablename) {
                case "subject_areas":
                    Log.info("Sorting the data to compare the Subject Area records for History Excluding..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));


                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " PUB_IDT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " EPR_ID => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " Priority => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRIORITY() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRIORITY());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRIORITY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRIORITY() != null)) {
                            Assert.assertEquals("The priority is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRIORITY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRIORITY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " SUBJECT_AREA_CODE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_AREA_CODE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_AREA_CODE());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_AREA_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_AREA_CODE() != null)) {
                            Assert.assertEquals("The SUBJECT_AREA_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_AREA_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_AREA_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " SUBJECT_AREA_NAME => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_AREA_NAME() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_AREA_NAME());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_AREA_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_AREA_NAME() != null)) {
                            Assert.assertEquals("The SUBJECT_AREA_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_AREA_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_AREA_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " SUBJECT_TYPE_CODE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_TYPE_CODE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_TYPE_CODE());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_TYPE_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_TYPE_CODE() != null)) {
                            Assert.assertEquals("The SUBJECT_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_TYPE_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_TYPE_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " SUBJECT_TYPE_NAME => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_TYPE_NAME() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_TYPE_NAME());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_TYPE_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_TYPE_NAME() != null)) {
                            Assert.assertEquals("The SUBJECT_TYPE_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSUBJECT_TYPE_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSUBJECT_TYPE_NAME());
                        }
                    }
                    break;
                case "pricing":
                    Log.info("Sorting the data to compare the Pricing records between History Excluding and HistoryExcluding_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " PUB_IDT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " EPR_ID => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " Product_type => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRODUCT_TYPE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRODUCT_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRODUCT_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRODUCT_TYPE() != null)) {
                            Assert.assertEquals("The Product_Type for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRODUCT_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRODUCT_TYPE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " CURRENCY => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCURRENCY() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCURRENCY());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCURRENCY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCURRENCY() != null)) {
                            Assert.assertEquals("The CURRENCY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCURRENCY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCURRENCY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " PRICE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRICE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRICE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRICE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRICE() != null)) {
                            Assert.assertEquals("The PRICE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRICE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRICE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " START_DATE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSTART_DATE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSTART_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSTART_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSTART_DATE() != null)) {
                            Assert.assertEquals("The START_DATE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSTART_DATE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSTART_DATE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " END_DATE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEND_DATE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEND_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEND_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEND_DATE() != null)) {
                            Assert.assertEquals("The END_DATE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEND_DATE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEND_DATE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " REGION => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getREGION() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getREGION());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getREGION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getREGION() != null)) {
                            Assert.assertEquals("The REGION is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getREGION(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getREGION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " QUANTITY => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getQUANTITY() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getQUANTITY());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getQUANTITY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getQUANTITY() != null)) {
                            Assert.assertEquals("The QUANTITY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getQUANTITY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getQUANTITY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " CUSTOMER_CATEGORY => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCUSTOMER_CATEGORY() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCUSTOMER_CATEGORY());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCUSTOMER_CATEGORY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCUSTOMER_CATEGORY() != null)) {
                            Assert.assertEquals("The CUSTOMER_CATEGORY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCUSTOMER_CATEGORY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCUSTOMER_CATEGORY());
                        }
                    }
                    break;
                case "person_roles":
                    Log.info("Sorting the data to compare the Person Roles records between History Excluding and HistoryExcluding_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " PUB_IDT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " EPR_ID => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " ROLE_DESCRIPTION => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getROLE_DESCRIPTION() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getROLE_DESCRIPTION());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getROLE_DESCRIPTION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getROLE_DESCRIPTION() != null)) {
                            Assert.assertEquals("The ROLE_DESCRIPTION for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getROLE_DESCRIPTION(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getROLE_DESCRIPTION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " SEQUENCE_NUMBER => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSEQUENCE_NUMBER() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSEQUENCE_NUMBER());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSEQUENCE_NUMBER() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSEQUENCE_NUMBER() != null)) {
                            Assert.assertEquals("The SEQUENCE_NUMBER is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getSEQUENCE_NUMBER(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getSEQUENCE_NUMBER());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " GROUP_NUMBER => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getGROUP_NUMBER() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getGROUP_NUMBER());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getGROUP_NUMBER() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getGROUP_NUMBER() != null)) {
                            Assert.assertEquals("The GROUP_NUMBER is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getGROUP_NUMBER(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getGROUP_NUMBER());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " INITIALS => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getINITIALS() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getINITIALS());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getINITIALS() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getINITIALS() != null)) {
                            Assert.assertEquals("The INITIALS is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getINITIALS(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getINITIALS());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " LAST_NAME => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getLAST_NAME() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getLAST_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getLAST_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getLAST_NAME() != null)) {
                            Assert.assertEquals("The LAST_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getLAST_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getLAST_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " TITLE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getTITLE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getTITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getTITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getTITLE() != null)) {
                            Assert.assertEquals("The TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getTITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getTITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " HONOURS => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getHONOURS() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getHONOURS());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getHONOURS() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getHONOURS() != null)) {
                            Assert.assertEquals("The HONOURS is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getHONOURS(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getHONOURS());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " AFFILIATION => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getAFFILIATION() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getAFFILIATION());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getAFFILIATION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getAFFILIATION() != null)) {
                            Assert.assertEquals("The AFFILIATION is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getAFFILIATION(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getAFFILIATION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " IMAGE_URL => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getIMAGE_URL() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getIMAGE_URL());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getIMAGE_URL() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getIMAGE_URL() != null)) {
                            Assert.assertEquals("The IMAGE_URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getIMAGE_URL(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getIMAGE_URL());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " FOOTNOTE_TXT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getFOOTNOTE_TXT() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getFOOTNOTE_TXT());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getFOOTNOTE_TXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getFOOTNOTE_TXT() != null)) {
                            Assert.assertEquals("The FOOTNOTE_TXT is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getFOOTNOTE_TXT(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getFOOTNOTE_TXT());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " NOTES_TXT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getNOTES_TXT() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getNOTES_TXT());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getNOTES_TXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getNOTES_TXT() != null)) {
                            Assert.assertEquals("The NOTES_TXT is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getNOTES_TXT(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getNOTES_TXT());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " WORK_TYPE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "works":
                    Log.info("Sorting the data to compare the Works records between History Excluding and HistoryExcluding_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " PUB_IDT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " EPR_ID => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " JOURNAL_AIMS_SCOPE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getJOURNAL_AIMS_SCOPE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getJOURNAL_AIMS_SCOPE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getJOURNAL_AIMS_SCOPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getJOURNAL_AIMS_SCOPE() != null)) {
                            Assert.assertEquals("The JOURNAL_AIMS_SCOPE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getJOURNAL_AIMS_SCOPE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getJOURNAL_AIMS_SCOPE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " ELSEVIER_COM_IND => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getELSEVIER_COM_IND() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getELSEVIER_COM_IND());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getELSEVIER_COM_IND() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getELSEVIER_COM_IND() != null)) {
                            Assert.assertEquals("The ELSEVIER_COM_IND is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getELSEVIER_COM_IND(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getELSEVIER_COM_IND());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " PRIMARY_AUTHOR => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRIMARY_AUTHOR() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRIMARY_AUTHOR());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRIMARY_AUTHOR() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRIMARY_AUTHOR() != null)) {
                            Assert.assertEquals("The PRIMARY_AUTHOR is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPRIMARY_AUTHOR(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPRIMARY_AUTHOR());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " PREVIOUS_TITLE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPREVIOUS_TITLE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPREVIOUS_TITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPREVIOUS_TITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPREVIOUS_TITLE() != null)) {
                            Assert.assertEquals("The PREVIOUS_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPREVIOUS_TITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPREVIOUS_TITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " WORK_TYPE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                        }

                       Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " WORK_TYPE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "metrics":
                    Log.info("Sorting the data to compare the Metrics records between History Excluding and HistoryExcluding_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " PUB_IDT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " EPR_ID => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " METRIC_CODE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_CODE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_CODE() != null)) {
                            Assert.assertEquals("The METRIC_CODE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " METRIC_NAME => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_NAME() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_NAME() != null)) {
                            Assert.assertEquals("The METRIC_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " METRIC => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC() != null)) {
                            Assert.assertEquals("The METRIC is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " METRIC_YEAR => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_YEAR() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_YEAR());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_YEAR() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_YEAR() != null)) {
                            Assert.assertEquals("The METRIC_YEAR is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_YEAR(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_YEAR());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " METRIC_URL => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_URL() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_URL());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_URL() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_URL() != null)) {
                            Assert.assertEquals("The METRIC_URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getMETRIC_URL(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getMETRIC_URL());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                " WORK_TYPE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() +
                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                        }
                    }
                        break;
                        case "urls":
                            Log.info("Sorting the data to compare the Urls records between History Excluding and HistoryExcluding_Query ..");
                            for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.size(); i++) {

                                PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                                PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                        " PUB_IDT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() +
                                        " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());
                                if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT() != null)) {
                                    Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPUB_IDT(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPUB_IDT());

                                }
                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                        " EPR_ID => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() +
                                        " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());

                                if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID() != null)) {
                                    Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getEPR_ID(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getEPR_ID());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                        " URL_CODE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_CODE() +
                                        " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_CODE());
                                if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_CODE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_CODE() != null)) {
                                    Assert.assertEquals("The URL_CODE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_CODE(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_CODE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                        " URL_NAME => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_NAME() +
                                        " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_NAME());
                                if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_NAME() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_NAME() != null)) {
                                    Assert.assertEquals("The URL_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_NAME(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_NAME());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                        " URL => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL() +
                                        " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL());
                                if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL() != null)) {
                                    Assert.assertEquals("The URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                        " URL_TITLE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_TITLE() +
                                        " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_TITLE());
                                if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_TITLE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_TITLE() != null)) {
                                    Assert.assertEquals("The URL_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getURL_TITLE(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getURL_TITLE());
                                }

                                Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                        " WORK_TYPE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() +
                                        " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                                if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE() != null ||
                                        (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE() != null)) {
                                    Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getWORK_TYPE(),
                                            PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getWORK_TYPE());
                                }
                            }
                                break;
                                case "work_rels":
                                    Log.info("Sorting the data to compare the Work_Rels records between History Excluding and HistoryExcluding_Query ..");
                                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.size(); i++) {

                                        PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                                        PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_PUB_IDT => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_PUB_IDT() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_PUB_IDT());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_PUB_IDT() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_PUB_IDT() != null)) {
                                            Assert.assertEquals("The CHILD_PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_PUB_IDT() + " is missing/not found in Data Lake",
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_PUB_IDT(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_PUB_IDT());

                                        }
                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " PARENT_EPR_ID => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPARENT_EPR_ID() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPARENT_EPR_ID());

                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPARENT_EPR_ID() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPARENT_EPR_ID() != null)) {
                                            Assert.assertEquals("The PARENT_EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getPARENT_EPR_ID(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getPARENT_EPR_ID());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_EPR_ID => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_EPR_ID() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_EPR_ID());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_EPR_ID() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_EPR_ID() != null)) {
                                            Assert.assertEquals("The CHILD_EPR_ID for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_EPR_ID(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_EPR_ID());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_TITLE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_TITLE() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_TITLE());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_TITLE() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_TITLE() != null)) {
                                            Assert.assertEquals("The CHILD_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_TITLE(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_TITLE());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_RELATED_TYPE_CODE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_TYPE_CODE() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_TYPE_CODE());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_TYPE_CODE() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_TYPE_CODE() != null)) {
                                            Assert.assertEquals("The CHILD_RELATED_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_TYPE_CODE(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_TYPE_CODE());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_RELATED => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED() != null)) {
                                            Assert.assertEquals("The CHILD_RELATED is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_RELATED_STATUS_CODE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_CODE() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_CODE());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_CODE() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_CODE() != null)) {
                                            Assert.assertEquals("The CHILD_RELATED_STATUS_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_CODE(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_CODE());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_RELATED_TYPE_ROLL_UP => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_TYPE_ROLL_UP());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_TYPE_ROLL_UP() != null)) {
                                            Assert.assertEquals("The CHILD_RELATED_TYPE_ROLL_UP is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_TYPE_ROLL_UP());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_RELATED_STATUS_NAME => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_NAME() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_NAME());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_NAME() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_NAME() != null)) {
                                            Assert.assertEquals("The CHILD_RELATED_STATUS_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_NAME(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_NAME());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " CHILD_RELATED_STATUS_ROLL_UP => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_ROLL_UP());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_ROLL_UP() != null)) {
                                            Assert.assertEquals("The CHILD_RELATED_STATUS_ROLL_UP is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getCHILD_RELATED_STATUS_ROLL_UP());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " RELATIONSHIP_TYPE_CODE => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getRELATIONSHIP_TYPE_CODE() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getRELATIONSHIP_TYPE_CODE());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getRELATIONSHIP_TYPE_CODE() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getRELATIONSHIP_TYPE_CODE() != null)) {
                                            Assert.assertEquals("The RELATIONSHIP_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getRELATIONSHIP_TYPE_CODE(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getRELATIONSHIP_TYPE_CODE());
                                        }

                                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY() +
                                                " RELATIONSHIP_TYPE_NAME => HistoryExcluding_Query=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getRELATIONSHIP_TYPE_NAME() +
                                                " HistoryExcluding=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getRELATIONSHIP_TYPE_NAME());
                                        if (PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getRELATIONSHIP_TYPE_NAME() != null ||
                                                (PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getRELATIONSHIP_TYPE_NAME() != null)) {
                                            Assert.assertEquals("The RELATIONSHIP_TYPE_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getU_KEY(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcludingQuery.get(i).getRELATIONSHIP_TYPE_NAME(),
                                                    PromisDataContext.tbPRMDataObjectsFromHistoryExcluding.get(i).getRELATIONSHIP_TYPE_NAME());
                                        }
                                        break;
                            }
                    }
            }
        }

    @Given("^We get the (.*) random Promis Latest Query ids of (.*)$")
    public void getRandomPromisLatestIds(String numberOfRecords, String Latesttablename) {
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
        System.out.println(PromisETLDataContext.tbPRMDataObjectsFromLatestQuery.size());
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
        PromisETLDataContext.tbPRMDataObjectsFromLatest = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records for Latest query and Latest tables of (.*)$")
    public void comparePromDataLatestQuerytoLatest(String tablename) {
        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            switch (tablename) {
                case "subject_areas":
                    Log.info("Sorting the data to compare the Subject Area records for Latest..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromLatest.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));


                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PUB_IDT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " EPR_ID => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " Priority => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRIORITY() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRIORITY());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRIORITY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRIORITY() != null)) {
                            Assert.assertEquals("The priority is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRIORITY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRIORITY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " SUBJECT_AREA_CODE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_AREA_CODE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_AREA_CODE());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_AREA_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_AREA_CODE() != null)) {
                            Assert.assertEquals("The SUBJECT_AREA_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_AREA_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_AREA_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " SUBJECT_AREA_NAME => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_AREA_NAME() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_AREA_NAME());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_AREA_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_AREA_NAME() != null)) {
                            Assert.assertEquals("The SUBJECT_AREA_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_AREA_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_AREA_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " SUBJECT_TYPE_CODE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_TYPE_CODE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_TYPE_CODE());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_TYPE_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_TYPE_CODE() != null)) {
                            Assert.assertEquals("The SUBJECT_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_TYPE_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_TYPE_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " SUBJECT_TYPE_NAME => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_TYPE_NAME() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_TYPE_NAME());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_TYPE_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_TYPE_NAME() != null)) {
                            Assert.assertEquals("The SUBJECT_TYPE_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSUBJECT_TYPE_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSUBJECT_TYPE_NAME());
                        }
                    }
                    break;
                case "pricing":
                    Log.info("Sorting the data to compare the Pricing records between Latest and Latest_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromLatestQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PUB_IDT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " EPR_ID => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " Product_type => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRODUCT_TYPE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRODUCT_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRODUCT_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRODUCT_TYPE() != null)) {
                            Assert.assertEquals("The Product_Type for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRODUCT_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRODUCT_TYPE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CURRENCY => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCURRENCY() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCURRENCY());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCURRENCY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCURRENCY() != null)) {
                            Assert.assertEquals("The CURRENCY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCURRENCY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCURRENCY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PRICE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRICE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRICE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRICE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRICE() != null)) {
                            Assert.assertEquals("The PRICE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRICE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRICE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " START_DATE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSTART_DATE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSTART_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSTART_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSTART_DATE() != null)) {
                            Assert.assertEquals("The START_DATE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSTART_DATE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSTART_DATE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " END_DATE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEND_DATE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEND_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEND_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEND_DATE() != null)) {
                            Assert.assertEquals("The END_DATE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEND_DATE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEND_DATE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " REGION => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getREGION() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getREGION());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getREGION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getREGION() != null)) {
                            Assert.assertEquals("The REGION is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getREGION(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getREGION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " QUANTITY => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getQUANTITY() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getQUANTITY());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getQUANTITY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getQUANTITY() != null)) {
                            Assert.assertEquals("The QUANTITY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getQUANTITY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getQUANTITY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CUSTOMER_CATEGORY => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCUSTOMER_CATEGORY() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCUSTOMER_CATEGORY());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCUSTOMER_CATEGORY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCUSTOMER_CATEGORY() != null)) {
                            Assert.assertEquals("The CUSTOMER_CATEGORY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCUSTOMER_CATEGORY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCUSTOMER_CATEGORY());
                        }
                    }
                    break;
                case "person_roles":
                    Log.info("Sorting the data to compare the Person Roles records between Latest and Latest_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromLatestQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PUB_IDT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " EPR_ID => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " ROLE_DESCRIPTION => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getROLE_DESCRIPTION() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getROLE_DESCRIPTION());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getROLE_DESCRIPTION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getROLE_DESCRIPTION() != null)) {
                            Assert.assertEquals("The ROLE_DESCRIPTION for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getROLE_DESCRIPTION(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getROLE_DESCRIPTION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " SEQUENCE_NUMBER => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSEQUENCE_NUMBER() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSEQUENCE_NUMBER());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSEQUENCE_NUMBER() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSEQUENCE_NUMBER() != null)) {
                            Assert.assertEquals("The SEQUENCE_NUMBER is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getSEQUENCE_NUMBER(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getSEQUENCE_NUMBER());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " GROUP_NUMBER => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getGROUP_NUMBER() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getGROUP_NUMBER());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getGROUP_NUMBER() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getGROUP_NUMBER() != null)) {
                            Assert.assertEquals("The GROUP_NUMBER is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getGROUP_NUMBER(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getGROUP_NUMBER());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " INITIALS => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getINITIALS() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getINITIALS());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getINITIALS() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getINITIALS() != null)) {
                            Assert.assertEquals("The INITIALS is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getINITIALS(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getINITIALS());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " LAST_NAME => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getLAST_NAME() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getLAST_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getLAST_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getLAST_NAME() != null)) {
                            Assert.assertEquals("The LAST_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getLAST_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getLAST_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " TITLE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getTITLE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getTITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getTITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getTITLE() != null)) {
                            Assert.assertEquals("The TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getTITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getTITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " HONOURS => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getHONOURS() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getHONOURS());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getHONOURS() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getHONOURS() != null)) {
                            Assert.assertEquals("The HONOURS is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getHONOURS(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getHONOURS());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " AFFILIATION => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getAFFILIATION() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getAFFILIATION());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getAFFILIATION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getAFFILIATION() != null)) {
                            Assert.assertEquals("The AFFILIATION is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getAFFILIATION(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getAFFILIATION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " IMAGE_URL => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getIMAGE_URL() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getIMAGE_URL());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getIMAGE_URL() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getIMAGE_URL() != null)) {
                            Assert.assertEquals("The IMAGE_URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getIMAGE_URL(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getIMAGE_URL());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " FOOTNOTE_TXT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getFOOTNOTE_TXT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getFOOTNOTE_TXT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getFOOTNOTE_TXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getFOOTNOTE_TXT() != null)) {
                            Assert.assertEquals("The FOOTNOTE_TXT is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getFOOTNOTE_TXT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getFOOTNOTE_TXT());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " NOTES_TXT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getNOTES_TXT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getNOTES_TXT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getNOTES_TXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getNOTES_TXT() != null)) {
                            Assert.assertEquals("The NOTES_TXT is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getNOTES_TXT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getNOTES_TXT());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " WORK_TYPE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "works":
                    Log.info("Sorting the data to compare the Works records between Latest and Latest_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromLatestQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PUB_IDT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " EPR_ID => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " JOURNAL_AIMS_SCOPE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getJOURNAL_AIMS_SCOPE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getJOURNAL_AIMS_SCOPE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getJOURNAL_AIMS_SCOPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getJOURNAL_AIMS_SCOPE() != null)) {
                            Assert.assertEquals("The JOURNAL_AIMS_SCOPE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getJOURNAL_AIMS_SCOPE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getJOURNAL_AIMS_SCOPE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " ELSEVIER_COM_IND => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getELSEVIER_COM_IND() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getELSEVIER_COM_IND());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getELSEVIER_COM_IND() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getELSEVIER_COM_IND() != null)) {
                            Assert.assertEquals("The ELSEVIER_COM_IND is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getELSEVIER_COM_IND(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getELSEVIER_COM_IND());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PRIMARY_AUTHOR => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRIMARY_AUTHOR() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRIMARY_AUTHOR());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRIMARY_AUTHOR() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRIMARY_AUTHOR() != null)) {
                            Assert.assertEquals("The PRIMARY_AUTHOR is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPRIMARY_AUTHOR(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPRIMARY_AUTHOR());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PREVIOUS_TITLE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPREVIOUS_TITLE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPREVIOUS_TITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPREVIOUS_TITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPREVIOUS_TITLE() != null)) {
                            Assert.assertEquals("The PREVIOUS_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPREVIOUS_TITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPREVIOUS_TITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " WORK_TYPE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "metrics":
                    Log.info("Sorting the data to compare the Metrics records between Latest and Latest_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromLatestQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PUB_IDT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " EPR_ID => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " METRIC_CODE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_CODE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_CODE() != null)) {
                            Assert.assertEquals("The METRIC_CODE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " METRIC_NAME => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_NAME() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_NAME() != null)) {
                            Assert.assertEquals("The METRIC_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " METRIC => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC() != null)) {
                            Assert.assertEquals("The METRIC is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " METRIC_YEAR => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_YEAR() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_YEAR());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_YEAR() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_YEAR() != null)) {
                            Assert.assertEquals("The METRIC_YEAR is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_YEAR(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_YEAR());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " METRIC_URL => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_URL() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_URL());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_URL() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_URL() != null)) {
                            Assert.assertEquals("The METRIC_URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getMETRIC_URL(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getMETRIC_URL());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " WORK_TYPE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "urls":
                    Log.info("Sorting the data to compare the Urls records between Latest and Latest_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromLatestQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PUB_IDT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " EPR_ID => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " URL_CODE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_CODE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_CODE() != null)) {
                            Assert.assertEquals("The URL_CODE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " URL_NAME => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_NAME() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_NAME() != null)) {
                            Assert.assertEquals("The URL_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " URL => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL() != null)) {
                            Assert.assertEquals("The URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " URL_TITLE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_TITLE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_TITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_TITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_TITLE() != null)) {
                            Assert.assertEquals("The URL_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getURL_TITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getURL_TITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " WORK_TYPE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "work_rels":
                    Log.info("Sorting the data to compare the Work_Rels records between Latest and Latest_Query ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromLatestQuery.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromLatestQuery.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromLatest.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_PUB_IDT => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_PUB_IDT() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_PUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_PUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_PUB_IDT() != null)) {
                            Assert.assertEquals("The CHILD_PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_PUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_PUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " PARENT_EPR_ID => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPARENT_EPR_ID() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPARENT_EPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPARENT_EPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPARENT_EPR_ID() != null)) {
                            Assert.assertEquals("The PARENT_EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getPARENT_EPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getPARENT_EPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_EPR_ID => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_EPR_ID() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_EPR_ID());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_EPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_EPR_ID() != null)) {
                            Assert.assertEquals("The CHILD_EPR_ID for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_EPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_EPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_TITLE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_TITLE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_TITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_TITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_TITLE() != null)) {
                            Assert.assertEquals("The CHILD_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_TITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_TITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_RELATED_TYPE_CODE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_TYPE_CODE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_TYPE_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_TYPE_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_TYPE_CODE() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_TYPE_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_TYPE_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_RELATED => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED() != null)) {
                            Assert.assertEquals("The CHILD_RELATED is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_RELATED_STATUS_CODE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_CODE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_CODE() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_STATUS_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_RELATED_TYPE_ROLL_UP => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_TYPE_ROLL_UP());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_TYPE_ROLL_UP() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_TYPE_ROLL_UP is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_TYPE_ROLL_UP(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_TYPE_ROLL_UP());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_RELATED_STATUS_NAME => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_NAME() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_NAME() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_STATUS_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " CHILD_RELATED_STATUS_ROLL_UP => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_ROLL_UP());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_ROLL_UP() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_STATUS_ROLL_UP is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getCHILD_RELATED_STATUS_ROLL_UP(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getCHILD_RELATED_STATUS_ROLL_UP());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " RELATIONSHIP_TYPE_CODE => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getRELATIONSHIP_TYPE_CODE() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getRELATIONSHIP_TYPE_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getRELATIONSHIP_TYPE_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getRELATIONSHIP_TYPE_CODE() != null)) {
                            Assert.assertEquals("The RELATIONSHIP_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getRELATIONSHIP_TYPE_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getRELATIONSHIP_TYPE_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY() +
                                " RELATIONSHIP_TYPE_NAME => Latest_Query=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getRELATIONSHIP_TYPE_NAME() +
                                " Latest=" + PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getRELATIONSHIP_TYPE_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getRELATIONSHIP_TYPE_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getRELATIONSHIP_TYPE_NAME() != null)) {
                            Assert.assertEquals("The RELATIONSHIP_TYPE_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromLatestQuery.get(i).getRELATIONSHIP_TYPE_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromLatest.get(i).getRELATIONSHIP_TYPE_NAME());
                        }
                        break;
                    }
            }
        }
    }

    @Given("^We get the (.*) random Promis transform mapping ids of (.*)$")
    public void getRandomPromisTransformMappingIds(String numberOfRecords, String Currenttablename) {
//        numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
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
        System.out.println(PromisETLDataContext.tbPRMDataObjectsFromTransformMapping.size());
    }

    @Then("^We get the Promis Transform mapping current records from (.*)$")
    public void getRecordsInCurrenttablename(String Currenttablename) throws ParseException {
        Log.info("We get the History Excluding records..");
        switch (Currenttablename) {
            case "promis_transform_Current_subject_areas":
                sql = String.format(PromisETLDataCheckSQL.GET_SUBJECT_AREAS, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_Current_pricing":
                sql = String.format(PromisETLDataCheckSQL.GET_PRICING, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_Current_person_roles":
                sql = String.format(PromisETLDataCheckSQL.GET_PERSON_ROLES, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_Current_works":
                sql = String.format(PromisETLDataCheckSQL.GET_WORKS, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_Current_metrics":
                sql = String.format(PromisETLDataCheckSQL.GET_METRICS, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_Current_urls":
                sql = String.format(PromisETLDataCheckSQL.GET_URLS, Currenttablename, Joiner.on("','").join(Ids));
                break;
            case "promis_transform_Current_work_rels":
                sql = String.format(PromisETLDataCheckSQL.GET_WORK_RELS, Currenttablename, Joiner.on("','").join(Ids));
                break;
        }
        Log.info(sql);
        PromisETLDataContext.tbPRMDataObjectsFromTransformMappingCurrent = DBManager.getDBResultAsBeanList(sql, PRMTablesETLObject.class, Constants.AWS_URL);
    }

    @And("^Compare Promis records for transform mapping and current of (.*)$")
    public void comparePromDataTransformMappingtoCurrent(String tablename) {
        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            switch (tablename) {
                case "subject_areas":
                    Log.info("Sorting the data to compare the Subject Area records for TransformMappingCurrent..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));


                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PUB_IDT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " EPR_ID => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " Priority => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRIORITY() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRIORITY());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRIORITY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRIORITY() != null)) {
                            Assert.assertEquals("The priority is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRIORITY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRIORITY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " SUBJECT_AREA_CODE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_AREA_CODE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_AREA_CODE());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_AREA_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_AREA_CODE() != null)) {
                            Assert.assertEquals("The SUBJECT_AREA_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_AREA_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_AREA_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " SUBJECT_AREA_NAME => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_AREA_NAME() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_AREA_NAME());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_AREA_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_AREA_NAME() != null)) {
                            Assert.assertEquals("The SUBJECT_AREA_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_AREA_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_AREA_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " SUBJECT_TYPE_CODE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_TYPE_CODE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_TYPE_CODE());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_TYPE_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_TYPE_CODE() != null)) {
                            Assert.assertEquals("The SUBJECT_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_TYPE_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_TYPE_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " SUBJECT_TYPE_NAME => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_TYPE_NAME() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_TYPE_NAME());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_TYPE_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_TYPE_NAME() != null)) {
                            Assert.assertEquals("The SUBJECT_TYPE_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSUBJECT_TYPE_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSUBJECT_TYPE_NAME());
                        }
                    }
                    break;
                case "pricing":
                    Log.info("Sorting the data to compare the Pricing records between TransformMappingCurrent and TransformMapping ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMapping.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PUB_IDT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " EPR_ID => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " Product_type => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRODUCT_TYPE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRODUCT_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRODUCT_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRODUCT_TYPE() != null)) {
                            Assert.assertEquals("The Product_Type for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRODUCT_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRODUCT_TYPE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CURRENCY => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCURRENCY() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCURRENCY());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCURRENCY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCURRENCY() != null)) {
                            Assert.assertEquals("The CURRENCY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCURRENCY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCURRENCY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PRICE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRICE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRICE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRICE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRICE() != null)) {
                            Assert.assertEquals("The PRICE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRICE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRICE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " START_DATE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSTART_DATE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSTART_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSTART_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSTART_DATE() != null)) {
                            Assert.assertEquals("The START_DATE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSTART_DATE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSTART_DATE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " END_DATE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEND_DATE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEND_DATE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEND_DATE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEND_DATE() != null)) {
                            Assert.assertEquals("The END_DATE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEND_DATE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEND_DATE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " REGION => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getREGION() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getREGION());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getREGION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getREGION() != null)) {
                            Assert.assertEquals("The REGION is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getREGION(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getREGION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " QUANTITY => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getQUANTITY() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getQUANTITY());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getQUANTITY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getQUANTITY() != null)) {
                            Assert.assertEquals("The QUANTITY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getQUANTITY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getQUANTITY());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CUSTOMER_CATEGORY => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCUSTOMER_CATEGORY() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCUSTOMER_CATEGORY());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCUSTOMER_CATEGORY() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCUSTOMER_CATEGORY() != null)) {
                            Assert.assertEquals("The CUSTOMER_CATEGORY is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCUSTOMER_CATEGORY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCUSTOMER_CATEGORY());
                        }
                    }
                    break;
                case "person_roles":
                    Log.info("Sorting the data to compare the Person Roles records between TransformMappingCurrent and TransformMapping ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMapping.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PUB_IDT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " EPR_ID => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " ROLE_DESCRIPTION => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getROLE_DESCRIPTION() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getROLE_DESCRIPTION());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getROLE_DESCRIPTION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getROLE_DESCRIPTION() != null)) {
                            Assert.assertEquals("The ROLE_DESCRIPTION for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getROLE_DESCRIPTION(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getROLE_DESCRIPTION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " SEQUENCE_NUMBER => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSEQUENCE_NUMBER() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSEQUENCE_NUMBER());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSEQUENCE_NUMBER() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSEQUENCE_NUMBER() != null)) {
                            Assert.assertEquals("The SEQUENCE_NUMBER is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getSEQUENCE_NUMBER(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getSEQUENCE_NUMBER());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " GROUP_NUMBER => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getGROUP_NUMBER() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getGROUP_NUMBER());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getGROUP_NUMBER() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getGROUP_NUMBER() != null)) {
                            Assert.assertEquals("The GROUP_NUMBER is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getGROUP_NUMBER(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getGROUP_NUMBER());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " INITIALS => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getINITIALS() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getINITIALS());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getINITIALS() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getINITIALS() != null)) {
                            Assert.assertEquals("The INITIALS is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getINITIALS(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getINITIALS());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " LAST_NAME => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getLAST_NAME() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getLAST_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getLAST_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getLAST_NAME() != null)) {
                            Assert.assertEquals("The LAST_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getLAST_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getLAST_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " TITLE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getTITLE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getTITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getTITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getTITLE() != null)) {
                            Assert.assertEquals("The TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getTITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getTITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " HONOURS => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getHONOURS() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getHONOURS());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getHONOURS() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getHONOURS() != null)) {
                            Assert.assertEquals("The HONOURS is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getHONOURS(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getHONOURS());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " AFFILIATION => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getAFFILIATION() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getAFFILIATION());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getAFFILIATION() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getAFFILIATION() != null)) {
                            Assert.assertEquals("The AFFILIATION is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getAFFILIATION(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getAFFILIATION());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " IMAGE_URL => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getIMAGE_URL() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getIMAGE_URL());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getIMAGE_URL() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getIMAGE_URL() != null)) {
                            Assert.assertEquals("The IMAGE_URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getIMAGE_URL(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getIMAGE_URL());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " FOOTNOTE_TXT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getFOOTNOTE_TXT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getFOOTNOTE_TXT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getFOOTNOTE_TXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getFOOTNOTE_TXT() != null)) {
                            Assert.assertEquals("The FOOTNOTE_TXT is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getFOOTNOTE_TXT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getFOOTNOTE_TXT());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " NOTES_TXT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getNOTES_TXT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getNOTES_TXT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getNOTES_TXT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getNOTES_TXT() != null)) {
                            Assert.assertEquals("The NOTES_TXT is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getNOTES_TXT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getNOTES_TXT());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " WORK_TYPE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "works":
                    Log.info("Sorting the data to compare the Works records between TransformMappingCurrent and TransformMapping ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMapping.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PUB_IDT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " EPR_ID => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " JOURNAL_AIMS_SCOPE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getJOURNAL_AIMS_SCOPE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getJOURNAL_AIMS_SCOPE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getJOURNAL_AIMS_SCOPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getJOURNAL_AIMS_SCOPE() != null)) {
                            Assert.assertEquals("The JOURNAL_AIMS_SCOPE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getJOURNAL_AIMS_SCOPE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getJOURNAL_AIMS_SCOPE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " ELSEVIER_COM_IND => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getELSEVIER_COM_IND() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getELSEVIER_COM_IND());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getELSEVIER_COM_IND() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getELSEVIER_COM_IND() != null)) {
                            Assert.assertEquals("The ELSEVIER_COM_IND is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getELSEVIER_COM_IND(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getELSEVIER_COM_IND());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PRIMARY_AUTHOR => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRIMARY_AUTHOR() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRIMARY_AUTHOR());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRIMARY_AUTHOR() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRIMARY_AUTHOR() != null)) {
                            Assert.assertEquals("The PRIMARY_AUTHOR is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPRIMARY_AUTHOR(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPRIMARY_AUTHOR());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PREVIOUS_TITLE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPREVIOUS_TITLE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPREVIOUS_TITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPREVIOUS_TITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPREVIOUS_TITLE() != null)) {
                            Assert.assertEquals("The PREVIOUS_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPREVIOUS_TITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPREVIOUS_TITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " WORK_TYPE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "metrics":
                    Log.info("Sorting the data to compare the Metrics records between TransformMappingCurrent and TransformMapping ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMapping.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PUB_IDT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " EPR_ID => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " METRIC_CODE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_CODE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_CODE() != null)) {
                            Assert.assertEquals("The METRIC_CODE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " METRIC_NAME => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_NAME() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_NAME() != null)) {
                            Assert.assertEquals("The METRIC_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " METRIC => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC() != null)) {
                            Assert.assertEquals("The METRIC is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " METRIC_YEAR => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_YEAR() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_YEAR());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_YEAR() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_YEAR() != null)) {
                            Assert.assertEquals("The METRIC_YEAR is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_YEAR(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_YEAR());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " METRIC_URL => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_URL() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_URL());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_URL() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_URL() != null)) {
                            Assert.assertEquals("The METRIC_URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getMETRIC_URL(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getMETRIC_URL());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " WORK_TYPE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "urls":
                    Log.info("Sorting the data to compare the Urls records between TransformMappingCurrent and TransformMapping ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMapping.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PUB_IDT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT() != null)) {
                            Assert.assertEquals("The PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " EPR_ID => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID() != null)) {
                            Assert.assertEquals("The EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getEPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getEPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " URL_CODE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_CODE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_CODE() != null)) {
                            Assert.assertEquals("The URL_CODE for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " URL_NAME => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_NAME() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_NAME() != null)) {
                            Assert.assertEquals("The URL_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " URL => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL() != null)) {
                            Assert.assertEquals("The URL is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " URL_TITLE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_TITLE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_TITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_TITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_TITLE() != null)) {
                            Assert.assertEquals("The URL_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getURL_TITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getURL_TITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " WORK_TYPE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE() != null)) {
                            Assert.assertEquals("The WORK_TYPE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getWORK_TYPE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getWORK_TYPE());
                        }
                    }
                    break;
                case "work_rels":
                    Log.info("Sorting the data to compare the Work_Rels records between TransformMappingCurrent and TransformMapping ..");
                    for (int i = 0; i < PromisDataContext.tbPRMDataObjectsFromTransformMapping.size(); i++) {

                        PromisDataContext.tbPRMDataObjectsFromTransformMapping.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY)); //sort data in the lists
                        PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.sort(Comparator.comparing(PRMTablesETLObject::getU_KEY));

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_PUB_IDT => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_PUB_IDT() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_PUB_IDT());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_PUB_IDT() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_PUB_IDT() != null)) {
                            Assert.assertEquals("The CHILD_PUB_IDT is =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_PUB_IDT() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_PUB_IDT(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_PUB_IDT());

                        }
                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " PARENT_EPR_ID => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPARENT_EPR_ID() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPARENT_EPR_ID());

                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPARENT_EPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPARENT_EPR_ID() != null)) {
                            Assert.assertEquals("The PARENT_EPR_ID is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getPARENT_EPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getPARENT_EPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_EPR_ID => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_EPR_ID() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_EPR_ID());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_EPR_ID() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_EPR_ID() != null)) {
                            Assert.assertEquals("The CHILD_EPR_ID for U_KEY =" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() + " is missing/not found in Data Lake",
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_EPR_ID(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_EPR_ID());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_TITLE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_TITLE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_TITLE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_TITLE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_TITLE() != null)) {
                            Assert.assertEquals("The CHILD_TITLE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_TITLE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_TITLE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_RELATED_TYPE_CODE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_TYPE_CODE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_TYPE_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_TYPE_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_TYPE_CODE() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_TYPE_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_TYPE_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_RELATED => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED() != null)) {
                            Assert.assertEquals("The CHILD_RELATED is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_RELATED_STATUS_CODE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_CODE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_CODE() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_STATUS_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_RELATED_TYPE_ROLL_UP => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_TYPE_ROLL_UP() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_TYPE_ROLL_UP());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_TYPE_ROLL_UP() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_TYPE_ROLL_UP() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_TYPE_ROLL_UP is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_TYPE_ROLL_UP(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_TYPE_ROLL_UP());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_RELATED_STATUS_NAME => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_NAME() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_NAME() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_STATUS_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_NAME());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " CHILD_RELATED_STATUS_ROLL_UP => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_ROLL_UP() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_ROLL_UP());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_ROLL_UP() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_ROLL_UP() != null)) {
                            Assert.assertEquals("The CHILD_RELATED_STATUS_ROLL_UP is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getCHILD_RELATED_STATUS_ROLL_UP(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getCHILD_RELATED_STATUS_ROLL_UP());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " RELATIONSHIP_TYPE_CODE => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getRELATIONSHIP_TYPE_CODE() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getRELATIONSHIP_TYPE_CODE());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getRELATIONSHIP_TYPE_CODE() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getRELATIONSHIP_TYPE_CODE() != null)) {
                            Assert.assertEquals("The RELATIONSHIP_TYPE_CODE is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getRELATIONSHIP_TYPE_CODE(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getRELATIONSHIP_TYPE_CODE());
                        }

                        Log.info("U_KEY => " + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY() +
                                " RELATIONSHIP_TYPE_NAME => TransformMapping=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getRELATIONSHIP_TYPE_NAME() +
                                " TransformMappingCurrent=" + PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getRELATIONSHIP_TYPE_NAME());
                        if (PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getRELATIONSHIP_TYPE_NAME() != null ||
                                (PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getRELATIONSHIP_TYPE_NAME() != null)) {
                            Assert.assertEquals("The RELATIONSHIP_TYPE_NAME is incorrect for U_KEY=" + PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getU_KEY(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMapping.get(i).getRELATIONSHIP_TYPE_NAME(),
                                    PromisDataContext.tbPRMDataObjectsFromTransformMappingCurrent.get(i).getRELATIONSHIP_TYPE_NAME());
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
    }
