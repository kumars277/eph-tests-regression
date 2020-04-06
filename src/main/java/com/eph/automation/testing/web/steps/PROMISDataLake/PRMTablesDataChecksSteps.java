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

    public DataQualityPRMContext dataQualityPRMContext ;
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
                Ids = randomClscodIds.stream().map(m -> (BigDecimal) m.get("CLS_COD")).map(String::valueOf).collect(Collectors.toList());
                break;
            case "PRMCLST":
                sql = String.format(PRMtoDataLakeDataChecksSQL.GET_PUBIDT_IDS, numberOfRecords);
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
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getPUB_IDT() != null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PUB_IDT is incorrect for id=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
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

                Log.info("PUB_IDT => " + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT() +
                        " BIO => Oracle=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO() +
                        " DL=" + dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO());

                if (dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO() != null ||
                        (dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO() != null)) {
                    Assert.assertEquals("The BIO is incorrect for PUB_IDT=" + dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getPUB_IDT(),
                            dataQualityPRMContext.tbPRMDataObjectsFromOracle.get(i).getBIO(),
                            dataQualityPRMContext.tbPRMDataObjectsFromDL.get(i).getBIO());
                }
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


}