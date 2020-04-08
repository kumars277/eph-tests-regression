package com.eph.automation.testing.web.steps.PROMISDataLake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityPRMContext;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesDLObject;
import com.eph.automation.testing.services.db.PROMISDataLakeSQL.PRMtoDataLakeDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class PRMTablesDataChecksSteps {

    public DataQualityPRMContext dataQualityPRMContext;
    private static String sql;
    private static List<String> Ids;
    private PRMtoDataLakeDataChecksSQL prmObj = new PRMtoDataLakeDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Given("^We get the (.*) random PRM ids of (.*)$")
    public void getRandomPRMIds(String numberOfRecords, String tableName) {
        Log.info("Get random record...");
        switch (tableName) {
            case "PRMAUTPUBT":
                sql = String.format(PRMtoDataLakeDataChecksSQL.GET_PUBIDS_IDS, numberOfRecords);
                List<Map<?, ?>> randomPubIds = DBManager.getDBResultMap(sql, Constants.PROMIS_URL);
                Ids = randomPubIds.stream().map(m -> (BigDecimal) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "PRMCLSCODT":
                sql = String.format(PRMtoDataLakeDataChecksSQL.GET_CLSCOD_IDS, numberOfRecords);
                List<Map<?, ?>> randomClscodIds = DBManager.getDBResultMap(sql, Constants.PROMIS_URL);
                Ids = randomClscodIds.stream().map(m -> (String) m.get("CLS_COD")).collect(Collectors.toList());
                break;
            case "PRMCLST":
                sql = String.format(PRMtoDataLakeDataChecksSQL.GET_CLSTPUBIDT_IDS, numberOfRecords);
                List<Map<?, ?>> randomPubIdtIds = DBManager.getDBResultMap(sql, Constants.PROMIS_URL);
                Ids = randomPubIdtIds.stream().map(m -> (BigDecimal) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "PRMLONDEST":
                sql = String.format(PRMtoDataLakeDataChecksSQL.GET_PUBIDT_LONDEST_IDS, numberOfRecords);
                List<Map<?, ?>> randomPrmLondestIds = DBManager.getDBResultMap(sql, Constants.PROMIS_URL);
                Ids = randomPrmLondestIds.stream().map(m -> (BigDecimal) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "PRMPRICEST":
                sql = String.format(PRMtoDataLakeDataChecksSQL.GET_PUBIDT_PRICEST_IDS, numberOfRecords);
                List<Map<?, ?>> randomFamilyIds = DBManager.getDBResultMap(sql, Constants.PROMIS_URL);
                Ids = randomFamilyIds.stream().map(m -> (BigDecimal) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "PRMPUBINFT":
                sql = String.format(PRMtoDataLakeDataChecksSQL.GET_PUBIDT_PUBINFT_IDS, numberOfRecords);
                List<Map<?, ?>> randomPrmPubinftIds = DBManager.getDBResultMap(sql, Constants.PROMIS_URL);
                Ids = randomPrmPubinftIds.stream().map(m -> (BigDecimal) m.get("PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "PRMPUBRELT":
                sql = String.format(PRMtoDataLakeDataChecksSQL.GET_PUBIDT_PUBRELT_IDS, numberOfRecords);
                List<Map<?, ?>> randomPubreltIds = DBManager.getDBResultMap(sql, Constants.PROMIS_URL);
                Ids = randomPubreltIds.stream().map(m -> (BigDecimal) m.get("PUB_PUB_IDT")).map(String::valueOf).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the PRM PRMAUTPUBT records from Oracle of (.*)$")
    public void getRecordsAutPubtOracle(String tableName) {
        Log.info("We get the PRMAUTPUBT records from PRM Oracle..");
        sql = String.format(prmObj.getAutPubtSql("oracle", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromOracle = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.PROMIS_URL);
        System.out.println(dataQualityPRMContext.tbPRMDataObjectsFromOracle.size());
    }

    @Then("^We get the PRM PRMAUTPUBT records from DL of (.*)$")
    public void getRecordsAutPubtDL(String tableName) {
        Log.info("We get the PRMAUTPUBT records from PRM DL..");
        sql = String.format(prmObj.getAutPubtSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare PRM PRMAUTPUBT records in PRM Oracle and DL of (.*)$")
    public void comparePRMAutPubtDataOracletoDL(String tableName) {
        if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the PRMAUTPUBT records in Oracle and DATA LAKE ..");
            for (int i = 0; i < dataQualityPRMContext.tbPRMDataObjectsFromOracle.size(); i++) {

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT));
                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getAUT_EDT_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getAUT_EDT_IDT));


                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null)) {
                    Assert.assertEquals("The PUB_IDT is =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());

                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT() != null)) {
                    Assert.assertEquals("The AUT_EDT_IDT is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_IDT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_TYP => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_TYP() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_TYP());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_TYP() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_TYP() != null)) {
                    Assert.assertEquals("The AUT_EDT_TYP is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_TYP(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_TYP());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " TYP_DES => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getTYP_DES() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getTYP_DES());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getTYP_DES() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getTYP_DES() != null)) {
                    Assert.assertEquals("The TYP_DES is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getTYP_DES(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getTYP_DES());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SEQ_NUM => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSEQ_NUM() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSEQ_NUM());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSEQ_NUM() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSEQ_NUM() != null)) {
                    Assert.assertEquals("The SEQ_NUM is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSEQ_NUM(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSEQ_NUM());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_PRE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_PRE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PRE());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_PRE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PRE() != null)) {
                    Assert.assertEquals("The AUT_EDT_PRE is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_PRE(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PRE());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_INI => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_INI() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_INI());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_INI() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_INI() != null)) {
                    Assert.assertEquals("The AUT_EDT_INI is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_INI(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_INI());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_NAM => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_NAM() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_NAM() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM() != null)) {
                    Assert.assertEquals("The AUT_EDT_NAM is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_NAM(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_SORT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_SORT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SORT());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_SORT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SORT() != null)) {
                    Assert.assertEquals("The AUT_EDT_SORT is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_SORT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SORT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_SUF => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_SUF() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SUF());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_SUF() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SUF() != null)) {
                    Assert.assertEquals("The AUT_EDT_SUF is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_SUF(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_SUF());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AFF_TXT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAFF_TXT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAFF_TXT());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAFF_TXT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAFF_TXT() != null)) {
                    Assert.assertEquals("The AFF_TXT is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAFF_TXT().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAFF_TXT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " FTN => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFTN());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFTN() != null)) {
                    Assert.assertEquals("The FTN is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFTN());
                }

     /*           Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " BIO => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO() != null)) {
                    Assert.assertEquals("The BIO is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO());
                }
     */
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_FAX => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_FAX() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_FAX());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_FAX() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_FAX() != null)) {
                    Assert.assertEquals("The AUT_EDT_FAX is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_FAX(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_FAX());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_PHONE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_PHONE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PHONE());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_PHONE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PHONE() != null)) {
                    Assert.assertEquals("The AUT_EDT_PHONE is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_PHONE(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_PHONE());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_EMAIL => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_EMAIL() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_EMAIL());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_EMAIL() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_EMAIL() != null)) {
                    Assert.assertEquals("The AUT_EDT_EMAIL is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_EMAIL(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_EMAIL());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_JCI => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_JCI() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_JCI());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_JCI() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_JCI() != null)) {
                    Assert.assertEquals("The AUT_EDT_JCI is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_JCI(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_JCI());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " BIO_IMAGE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO_IMAGE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO_IMAGE());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO_IMAGE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO_IMAGE() != null)) {
                    Assert.assertEquals("The BIO_IMAGE is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO_IMAGE(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO_IMAGE());
                }


            }
        }
    }

    @When("^We get the PRM PRMCLSCODT records from Oracle of (.*)$")
    public void getRecordsPrmClsCodtOracle(String tableName) {
        Log.info("We get the PRMCLSCODT records from PRM Oracle..");
        sql = String.format(prmObj.getClsCodSql("oracle", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromOracle = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.PROMIS_URL);
        System.out.println(dataQualityPRMContext.tbPRMDataObjectsFromOracle.size());
    }

    @Then("^We get the PRM PRMCLSCODT records from DL of (.*)$")
    public void getRecordsPrmClsCodtDL(String tableName) {
        Log.info("We get the PRMCLSCODT records from PRM DL..");
        sql = String.format(prmObj.getClsCodSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare PRM PRMCLSCODT records in PRM Oracle and DL of (.*)$")
    public void comparePrmClsCodtDataOracletoDL(String tableName) {
        if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the PRMCLSCODT records in Oracle and DATA LAKE ..");
            for (int i = 0; i < dataQualityPRMContext.tbPRMDataObjectsFromOracle.size(); i++) {

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getCLS_COD)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getCLS_COD));

                Log.info("CLS_COD => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD() +
                        " CLS_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() != null)) {
                    Assert.assertEquals("The CLS_COD =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD() + " is missing/not found in Data Lake",
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD());

                }

                Log.info("CLS_COD => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD() +
                        " CLS_DES => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_DES() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_DES());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_DES() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_DES() != null)) {
                    Assert.assertEquals("The CLS_DES is incorrect for CLS_COD=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_DES(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_DES());

                }

                Log.info("CLS_COD => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD() +
                        " CLS_GRP_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_GRP_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_GRP_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_GRP_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_GRP_COD() != null)) {
                    Assert.assertEquals("The CLS_GRP_COD is incorrect for CLS_COD=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_GRP_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_GRP_COD());

                }
            }
        }

    }

    @When("^We get the PRM PRMCLST records from Oracle of (.*)$")
    public void getRecordsClstOracle(String tableName) {
        Log.info("We get the PRMCLST records from PRM Oracle..");
        sql = String.format(prmObj.getClstSql("oracle", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromOracle = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.PROMIS_URL);
    }

    @Then("^We get the PRM PRMCLST records from DL of (.*)$")
    public void getRecordsClstDL(String tableName) {
        Log.info("We get the PRMCLST records from PRM DL..");
        sql = String.format(prmObj.getClstSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare PRM PRMCLST records in PRM Oracle and DL of (.*)$")
    public void comparePrmClstDataOracletoDL(String tableName) {
        if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the PRMCLST records in Oracle and DATA LAKE ..");
            for (int i = 0; i < dataQualityPRMContext.tbPRMDataObjectsFromOracle.size(); i++) {

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getCLS_COD)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getCLS_COD));

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null)) {
                    Assert.assertEquals("The PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " CLS_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD() != null)) {
                    Assert.assertEquals("The CLS_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCLS_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCLS_COD());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SCA_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSCA_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSCA_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSCA_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSCA_COD() != null)) {
                    Assert.assertEquals("The SCA_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSCA_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSCA_COD());

                }

            }
        }
    }

    @When("^We get the PRM PRMLONDEST records from Oracle of (.*)$")
    public void getRecordsLndestDLOracle(String tableName) {
        Log.info("We get the PRMLONDEST records from PRM Oracle..");
        sql = String.format(prmObj.getLonDestSql("oracle", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromOracle = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.PROMIS_URL);
    }

    @Then("^We get the PRM PRMLONDEST records from DL of (.*)$")
    public void getRecordsLndestDL(String tableName) {
        Log.info("We get the PRMLONDEST records from PRM DL..");
        sql = String.format(prmObj.getLonDestSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare PRM PRMLONDEST records in PRM Oracle and DL of (.*)$")
    public void comparePrmLondestDataOracletoDL(String tableName) {
        if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the PRMLONDEST records in Oracle and DATA LAKE ..");
            for (int i = 0; i < dataQualityPRMContext.tbPRMDataObjectsFromOracle.size(); i++) {

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPUB_VOL_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_VOL_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getVOL_PRT_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getVOL_PRT_IDT));
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null)) {
                    Assert.assertEquals("The PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_VOL_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_VOL_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_VOL_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT() != null)) {
                    Assert.assertEquals("The PUB_VOL_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_VOL_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " VOL_PRT_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getVOL_PRT_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getVOL_PRT_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT() != null)) {
                    Assert.assertEquals("The VOL_PRT_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getVOL_PRT_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT());

                }

            }
        }
    }

    @When("^We get the PRM PRMPRICEST records from Oracle of (.*)$")
    public void getRecordsPricestOracle(String tableName) {
        Log.info("We get the PRMPRICEST records from PRM Oracle..");
        sql = String.format(prmObj.getPriCestSql("oracle", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromOracle = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.PROMIS_URL);
        System.out.println(dataQualityPRMContext.tbPRMDataObjectsFromOracle.size());
    }

    @Then("^We get the PRM PRMPRICEST records from DL of (.*)$")
    public void getRecordsPricestDL(String tableName) {
        Log.info("We get the PRMPRICEST records from PRM DL..");
        sql = String.format(prmObj.getPriCestSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare PRM PRMPRICEST records in PRM Oracle and DL of (.*)$")
    public void comparePrmPricestDataOracletoDL(String tableName) {
        if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the PRMPRICEST records in Oracle and DATA LAKE ..");
            for (int i = 0; i < dataQualityPRMContext.tbPRMDataObjectsFromOracle.size(); i++) {

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPUB_VOL_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_VOL_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getVOL_PRT_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getVOL_PRT_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getEDN_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getEDN_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getEDN_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getEDN_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPRC_TYP)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPRC_TYP));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPRC_GEO)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPRC_GEO));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getIPT_COD)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getIPT_COD));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPRC_DAT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPRC_DAT));


                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null)) {
                    Assert.assertEquals("The PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_VOL_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_VOL_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_VOL_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT() != null)) {
                    Assert.assertEquals("The PUB_VOL_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_VOL_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_VOL_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " VOL_PRT_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getVOL_PRT_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getVOL_PRT_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT() != null)) {
                    Assert.assertEquals("The VOL_PRT_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getVOL_PRT_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getVOL_PRT_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " EDN_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEDN_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEDN_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEDN_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEDN_IDT() != null)) {
                    Assert.assertEquals("The EDN_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEDN_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEDN_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PRC_TYP => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_TYP() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_TYP());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_TYP() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_TYP() != null)) {
                    Assert.assertEquals("The PRC_TYP is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_TYP(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_TYP());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PRC_GEO => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_GEO() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_GEO());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_GEO() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_GEO() != null)) {
                    Assert.assertEquals("The PRC_GEO is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_GEO(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_GEO());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " IPT_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIPT_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIPT_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIPT_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIPT_COD() != null)) {
                    Assert.assertEquals("The IPT_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIPT_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIPT_COD());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PRC_DAT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_DAT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_DAT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_DAT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_DAT() != null)) {
                    Assert.assertEquals("The PRC_DAT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_DAT().substring(0, 10),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_DAT().substring(0, 10));
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " STD_CUR_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTD_CUR_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTD_CUR_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTD_CUR_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTD_CUR_COD() != null)) {
                    Assert.assertEquals("The STD_CUR_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTD_CUR_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTD_CUR_COD());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " STD_PRC => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTD_PRC() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTD_PRC());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTD_CUR_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTD_CUR_COD() != null)) {
                    Assert.assertEquals("The STD_PRC is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTD_PRC(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTD_PRC());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PRC_PRE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_PRE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_PRE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_PRE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_PRE() != null)) {
                    Assert.assertEquals("The PRC_PRE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRC_PRE().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRC_PRE());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " ADD_PRC => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getADD_PRC() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getADD_PRC());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getADD_PRC() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getADD_PRC() != null)) {
                    Assert.assertEquals("The ADD_PRC is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getADD_PRC().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getADD_PRC());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " FLAG => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFLAG() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFLAG());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFLAG() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFLAG() != null)) {
                    Assert.assertEquals("The FLAG is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFLAG(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFLAG());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " EXP_DAT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEXP_DAT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEXP_DAT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEXP_DAT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEXP_DAT() != null)) {
                    Assert.assertEquals("The EXP_DAT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEXP_DAT().substring(0, 10),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEXP_DAT().substring(0, 10));
                }

            }
        }
    }

    @When("^We get the PRM PRMPUBINFT records from Oracle of (.*)$")
    public void getRecordsPubInftOracle(String tableName) {
        Log.info("We get the PRMPUBINFT records from PRM Oracle..");
        sql = String.format(prmObj.getPubInftSql("oracle", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromOracle = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.PROMIS_URL);
        System.out.println(dataQualityPRMContext.tbPRMDataObjectsFromOracle.size());
    }

    @Then("^We get the PRM PRMPUBINFT records from DL of (.*)$")
    public void getRecordsPubInftDL(String tableName) {
        Log.info("We get the PRMPUBINFT records from PRM DL..");
        sql = String.format(prmObj.getPubInftSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare PRM PRMPUBINFT records in PRM Oracle and DL of (.*)$")
    public void comparePrmPubInftDataOracletoDL(String tableName) {
        if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the PRMPUBINFT records in Oracle and DATA LAKE ..");
            for (int i = 0; i < dataQualityPRMContext.tbPRMDataObjectsFromOracle.size(); i++) {

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_IDT));

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null)) {
                    Assert.assertEquals("The PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() + " is missing/not found in Data Lake",
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " STA_DAA => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTA_DAA() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTA_DAA());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTA_DAA() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTA_DAA() != null)) {
                    Assert.assertEquals("The STA_DAA is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTA_DAA(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTA_DAA());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " FUL_TIT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFUL_TIT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFUL_TIT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT() != null)) {
                    Assert.assertEquals("The FUL_TIT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFUL_TIT().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_TYP => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_TYP() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_TYP());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_TYP() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_TYP() != null)) {
                    Assert.assertEquals("The PUB_TYP is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_TYP(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_TYP());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " OWN_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getOWN_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getOWN_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getOWN_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getOWN_IDT() != null)) {
                    Assert.assertEquals("The OWN_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getOWN_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getOWN_IDT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PBL_ABR_NAM => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPBL_ABR_NAM() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPBL_ABR_NAM());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPBL_ABR_NAM() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPBL_ABR_NAM() != null)) {
                    Assert.assertEquals("The PBL_ABR_NAM is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPBL_ABR_NAM(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPBL_ABR_NAM());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " LOC => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLOC() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLOC());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLOC() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLOC() != null)) {
                    Assert.assertEquals("The LOC is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLOC(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLOC());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " STA_PRM => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTA_PRM() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTA_PRM());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTA_PRM() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTA_PRM() != null)) {
                    Assert.assertEquals("The STA_PRM is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSTA_PRM(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSTA_PRM());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " LNG_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLNG_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLNG_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLNG_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLNG_COD() != null)) {
                    Assert.assertEquals("The LNG_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLNG_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLNG_COD());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_IMP_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IMP_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IMP_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IMP_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IMP_IDT() != null)) {
                    Assert.assertEquals("The PUB_IMP_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IMP_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IMP_IDT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PUB_BGN_YEA => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_BGN_YEA() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_BGN_YEA());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_BGN_YEA() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_BGN_YEA() != null)) {
                    Assert.assertEquals("The PUB_BGN_YEA is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_BGN_YEA(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_BGN_YEA());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " LCO_NUM => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLCO_NUM() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLCO_NUM());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLCO_NUM() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLCO_NUM() != null)) {
                    Assert.assertEquals("The LCO_NUM is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLCO_NUM(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLCO_NUM());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " IMP_DAT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIMP_DAT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIMP_DAT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIMP_DAT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIMP_DAT() != null)) {
                    Assert.assertEquals("The IMP_DAT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIMP_DAT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIMP_DAT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " CDA => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCDA() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCDA());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCDA() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCDA() != null)) {
                    Assert.assertEquals("The CDA is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCDA(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCDA());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " CRE_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCRE_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCRE_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCRE_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCRE_IDT() != null)) {
                    Assert.assertEquals("The CRE_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCRE_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCRE_IDT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " DEP_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getDEP_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getDEP_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getDEP_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getDEP_IDT() != null)) {
                    Assert.assertEquals("The DEP_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getDEP_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getDEP_IDT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " LST_USR_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_USR_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_USR_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_USR_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_USR_IDT() != null)) {
                    Assert.assertEquals("The LST_USR_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_USR_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_USR_IDT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " LST_UPD_DAT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_UPD_DAT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DAT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_UPD_DAT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DAT() != null)) {
                    Assert.assertEquals("The LST_UPD_DAT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_UPD_DAT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DAT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " ABR_TIT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getABR_TIT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getABR_TIT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getABR_TIT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getABR_TIT() != null)) {
                    Assert.assertEquals("The ABR_TIT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getABR_TIT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getABR_TIT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " FUL_TIT_SRT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFUL_TIT_SRT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT_SRT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFUL_TIT_SRT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT_SRT() != null)) {
                    Assert.assertEquals("The FUL_TIT_SRT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFUL_TIT_SRT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT_SRT());
                }


                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SUB_TIT_1 => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_1() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_1());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_1() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_1() != null)) {
                    Assert.assertEquals("The SUB_TIT_1 is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_1().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_1());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SUB_TIT_2 => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_2() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_2());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_2() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_2() != null)) {
                    Assert.assertEquals("The SUB_TIT_2 is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_2().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_2());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SUB_TIT_3 => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_3() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_3());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_3() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_3() != null)) {
                    Assert.assertEquals("The SUB_TIT_3 is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUB_TIT_3().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUB_TIT_3());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PRG_SUB_TIT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRG_SUB_TIT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRG_SUB_TIT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRG_SUB_TIT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRG_SUB_TIT() != null)) {
                    Assert.assertEquals("The PRG_SUB_TIT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPRG_SUB_TIT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPRG_SUB_TIT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_EDT_NAM => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_NAM() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_NAM() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM() != null)) {
                    Assert.assertEquals("The AUT_EDT_NAM is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_EDT_NAM(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_EDT_NAM());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SLO_TXT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSLO_TXT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSLO_TXT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSLO_TXT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSLO_TXT() != null)) {
                    Assert.assertEquals("The SLO_TXT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSLO_TXT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSLO_TXT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " NTB => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getNTB() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getNTB());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getNTB() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getNTB() != null)) {
                    Assert.assertEquals("The NTB is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getNTB(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getNTB());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " FTN => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFTN());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFTN() != null)) {
                    Assert.assertEquals("The FTN is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFTN());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " FOR_TIT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFOR_TIT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFOR_TIT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFOR_TIT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFOR_TIT() != null)) {
                    Assert.assertEquals("The FOR_TIT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFOR_TIT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFOR_TIT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " RPN_INF => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRPN_INF() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRPN_INF());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRPN_INF() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRPN_INF() != null)) {
                    Assert.assertEquals("The RPN_INF is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRPN_INF(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRPN_INF());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " ADV_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getADV_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getADV_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getADV_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getADV_COD() != null)) {
                    Assert.assertEquals("The ADV_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getADV_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getADV_COD());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " RVW_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRVW_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRVW_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRVW_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRVW_COD() != null)) {
                    Assert.assertEquals("The RVW_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRVW_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRVW_COD());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SUP_APP => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUP_APP() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRVW_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUP_APP() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUP_APP() != null)) {
                    Assert.assertEquals("The SUP_APP is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSUP_APP(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSUP_APP());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " PHY_SZE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPHY_SZE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPHY_SZE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPHY_SZE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPHY_SZE() != null)) {
                    Assert.assertEquals("The PHY_SZE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPHY_SZE(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPHY_SZE());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " FS_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFS_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFS_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFS_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFS_IDT() != null)) {
                    Assert.assertEquals("The FS_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFS_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFS_IDT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " WTK_NUM => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getWTK_NUM() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getWTK_NUM());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getWTK_NUM() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getWTK_NUM() != null)) {
                    Assert.assertEquals("The WTK_NUM is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getWTK_NUM(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getWTK_NUM());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " WTK_CLS => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getWTK_CLS() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getWTK_CLS());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getWTK_CLS() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getWTK_CLS() != null)) {
                    Assert.assertEquals("The WTK_CLS is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getWTK_CLS(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getWTK_CLS());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " BWK => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBWK() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBWK());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBWK() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBWK() != null)) {
                    Assert.assertEquals("The BWK is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBWK(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBWK());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AEI => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAEI() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAEI());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAEI() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAEI() != null)) {
                    Assert.assertEquals("The AEI is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAEI(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAEI());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " REV_EDN_TI => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREV_EDN_TI() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREV_EDN_TI());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREV_EDN_TI() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREV_EDN_TI() != null)) {
                    Assert.assertEquals("The REV_EDN_TI is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREV_EDN_TI(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREV_EDN_TI());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " JNL_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getJNL_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getJNL_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getJNL_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getJNL_IDT() != null)) {
                    Assert.assertEquals("The JNL_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getJNL_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getJNL_IDT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " LST_UPD_DATE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_UPD_DATE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DATE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_UPD_DATE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DATE() != null)) {
                    Assert.assertEquals("The LST_UPD_DATE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getLST_UPD_DATE().substring(0, 10),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getLST_UPD_DATE().substring(0, 10));
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " CDA_DATE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCDA_DATE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCDA_DATE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCDA_DATE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCDA_DATE() != null)) {
                    Assert.assertEquals("The CDA_DATE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getCDA_DATE().substring(0, 10),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getCDA_DATE().substring(0, 10));
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AQS_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAQS_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAQS_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAQS_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAQS_COD() != null)) {
                    Assert.assertEquals("The AQS_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAQS_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAQS_COD());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " MKT_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getMKT_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getMKT_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getMKT_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getMKT_COD() != null)) {
                    Assert.assertEquals("The MKT_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getMKT_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getMKT_COD());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUD() != null)) {
                    Assert.assertEquals("The AUD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUD().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUD());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SHT_DES => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSHT_DES() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSHT_DES());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSHT_DES() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSHT_DES() != null)) {
                    Assert.assertEquals("The SHT_DES is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSHT_DES().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSHT_DES());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SHT_CTT_DES => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSHT_CTT_DES() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSHT_CTT_DES());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSHT_CTT_DES() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSHT_CTT_DES() != null)) {
                    Assert.assertEquals("The SHT_CTT_DES is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSHT_CTT_DES().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSHT_CTT_DES());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SLO_EXP_DAT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSLO_EXP_DAT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSLO_EXP_DAT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSLO_EXP_DAT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSLO_EXP_DAT() != null)) {
                    Assert.assertEquals("The SLO_EXP_DAT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSLO_EXP_DAT().substring(0, 10),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSLO_EXP_DAT().substring(0, 10));
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " IF_NO => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_NO() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_NO());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_NO() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_NO() != null)) {
                    Assert.assertEquals("The IF_NO is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_NO(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_NO());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " IF_YEAR => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_YEAR() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_YEAR());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_YEAR() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_YEAR() != null)) {
                    Assert.assertEquals("The IF_YEAR is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_YEAR(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_YEAR());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " IF_COMMENT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_COMMENT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_COMMENT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_COMMENT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_COMMENT() != null)) {
                    Assert.assertEquals("The IF_COMMENT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_COMMENT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_COMMENT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " AUT_BENEFITS => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_BENEFITS() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_BENEFITS());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_BENEFITS() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_BENEFITS() != null)) {
                    Assert.assertEquals("The AUT_BENEFITS is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAUT_BENEFITS().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAUT_BENEFITS());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " SOURCE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSOURCE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSOURCE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSOURCE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSOURCE() != null)) {
                    Assert.assertEquals("The SOURCE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getSOURCE(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getSOURCE());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " ARG_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getARG_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getARG_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getARG_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getARG_COD() != null)) {
                    Assert.assertEquals("The ARG_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getARG_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getARG_COD());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " IF_5 => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_5() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_5());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_5() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_5() != null)) {
                    Assert.assertEquals("The IF_5 is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_5(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_5());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " IF_RANKING => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_RANKING() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_RANKING());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_RANKING() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_RANKING() != null)) {
                    Assert.assertEquals("The IF_RANKING is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_RANKING(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_RANKING());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " IF_CAT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_CAT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_CAT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_CAT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_CAT() != null)) {
                    Assert.assertEquals("The IF_CAT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getIF_CAT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getIF_CAT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " OA_TYPE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getOA_TYPE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getOA_TYPE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getOA_TYPE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getOA_TYPE() != null)) {
                    Assert.assertEquals("The OA_TYPE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getOA_TYPE(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getOA_TYPE());
                }

            }
        }
    }


    @When("^We get the PRM PRMPUBRELT records from Oracle of (.*)$")
    public void getRecordsPubReltOracle(String tableName) {
        Log.info("We get the PRMPUBRELT records from PRM Oracle..");
        sql = String.format(prmObj.getPubReltSql("oracle", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromOracle = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.PROMIS_URL);
        System.out.println(dataQualityPRMContext.tbPRMDataObjectsFromOracle.size());
    }

    @Then("^We get the PRM PRMPUBRELT records from DL of (.*)$")
    public void getRecordsPubReltDL(String tableName) {
        Log.info("We get the PRMPUBRELT records from PRM DL..");
        sql = String.format(prmObj.getPubReltSql("aws", tableName), Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityPRMContext.tbPRMDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, PRMTablesDLObject.class, Constants.AWS_URL);
    }


    @And("^Compare PRM PRMPUBRELT records in PRM Oracle and DL of (.*)$")
    public void comparePrmPubReltDataOracletoDL(String tableName) {
        if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the PRMPUBRELT records in Oracle and DATA LAKE ..");
            for (int i = 0; i < dataQualityPRMContext.tbPRMDataObjectsFromOracle.size(); i++) {

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getPUB_PUB_IDT)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getPUB_PUB_IDT));

                dataQualityPRMContext.tbPRMDataObjectsFromOracle.sort(Comparator.comparing(PRMTablesDLObject::getREL_NO)); //sort data in the lists
                dataQualityPRMContext.tbPRMDataObjectsFromDL.sort(Comparator.comparing(PRMTablesDLObject::getREL_NO));


                Log.info("PUB_PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " PUB_PUB_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT() != null)) {
                    Assert.assertEquals("The PUB_PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() + " is missing/not found in Data Lake",
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_PUB_IDT());

                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " REL_NO => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_NO() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_NO());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_NO() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_NO() != null)) {
                    Assert.assertEquals("The REL_NO is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_NO(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_NO());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " REL_SRT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_NO() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_SRT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_SRT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_SRT() != null)) {
                    Assert.assertEquals("The REL_SRT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_SRT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_SRT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " REL_IDT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_IDT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_IDT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_IDT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_IDT() != null)) {
                    Assert.assertEquals("The REL_IDT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_IDT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " REL_TITLE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_TITLE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_TITLE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_TITLE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_TITLE() != null)) {
                    Assert.assertEquals("The REL_TITLE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_TITLE(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_TITLE());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " FOOTNOTE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFOOTNOTE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFOOTNOTE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFOOTNOTE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFOOTNOTE() != null)) {
                    Assert.assertEquals("The FOOTNOTE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFOOTNOTE(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFOOTNOTE());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " FRONT_TEXT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFRONT_TEXT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFRONT_TEXT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFRONT_TEXT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFRONT_TEXT() != null)) {
                    Assert.assertEquals("The FRONT_TEXT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFRONT_TEXT().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFRONT_TEXT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " END_TEXT => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEND_TEXT() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEND_TEXT());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEND_TEXT() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEND_TEXT() != null)) {
                    Assert.assertEquals("The END_TEXT is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getEND_TEXT().replace("\n", " ").replace("\r", " ").replace("\0", ""),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getEND_TEXT());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " RTP_RTP_COD => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRTP_RTP_COD() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRTP_RTP_COD());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRTP_RTP_COD() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRTP_RTP_COD() != null)) {
                    Assert.assertEquals("The RTP_RTP_COD is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getRTP_RTP_COD(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getRTP_RTP_COD());
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " REL_END_DATE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_END_DATE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_END_DATE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_END_DATE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_END_DATE() != null)) {
                    Assert.assertEquals("The REL_END_DATE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_END_DATE().substring(0,10),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_END_DATE().substring(0,10));
                }
                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT() +
                        " REL_START_DATE => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_START_DATE() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_START_DATE());
                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_START_DATE() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_START_DATE() != null)) {
                    Assert.assertEquals("The REL_START_DATE is incorrect for PUB_IDT =" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_PUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getREL_START_DATE().substring(0,10),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getREL_START_DATE().substring(0,10));
                }
            }
        }
    }
}