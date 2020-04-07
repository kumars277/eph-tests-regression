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
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getAFF_TXT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getAFF_TXT());
                }

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " FTN => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFTN());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFTN() != null)) {
                    Assert.assertEquals("The FTN is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFTN(),
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
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getFUL_TIT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getFUL_TIT());
                }


            }
        }
    }
}