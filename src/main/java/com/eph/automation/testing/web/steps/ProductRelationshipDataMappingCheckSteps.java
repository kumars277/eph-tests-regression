package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductRelationshipDataObject;
import com.eph.automation.testing.services.db.sql.ProductRelationshipChecksSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Created by Bistra Drazheva on 21/02/2019
 */
public class ProductRelationshipDataMappingCheckSteps {
    private String sql;
    private static int countProductsRelPMX;
    private static int countProductsRelEPHSTG;
    private static int countProductsRelEPHDQ;
    private static int countProductsRelEPHSA;
    private static int countProductsRelEPHGD;
    private static int countProductRelationshipsEPHAE;
    private List<Map<?, ?>> productRelIds;
    private static List<String> ids;
    private static List<String> idsSA;


    @StaticInjection
    public DataQualityContext dataQualityContext;

    @Given("We get the count of the product relationship records in PMX$")
    public void getCountProductRelPmx() {
        Log.info("Given We get the count of the product relationship records in PMX ..");

        sql = ProductRelationshipChecksSQL.GET_PMX_PRODUCT_RELATIONSHIPS_COUNT;
        Log.info(sql);
        List<Map<String, Object>> productsRelNumber = DBManager.getDBResultMap(sql, Constants.PMX_URL);
        countProductsRelPMX = ((BigDecimal) productsRelNumber.get(0).get("count")).intValue();
        Log.info("Count of product relationship records in PMX is: " + countProductsRelPMX);

        Log.info("Assert count of records is not null");
        assertTrue("No data found in PMX for product relationships", countProductsRelPMX != 0);
    }


    @When("We get the count of the product relationship records in EPH STG$")
    public void getCountProductRelEPHSTG() {
        Log.info("When We get the count of the product relationship records in EPH STG ..");


        sql = ProductRelationshipChecksSQL.GET_EPH_STG_PRODUCT_RELATIONSHIPS_COUNT;
        Log.info(sql);

        List<Map<String, Object>> productsRelNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countProductsRelEPHSTG = ((Long) productsRelNumber.get(0).get("count")).intValue();
        Log.info("Count of product relationship records in EPH STG is: " + countProductsRelEPHSTG);

        Log.info("Assert count of records is not null");

        assertTrue("No data found in EPH STG for product relationships", countProductsRelEPHSTG != 0);


    }


    @When("We get the count of the product relationship records in EPH STG going to SA$")
    public void getCountProductRelEPHSTGGoingToSA() {
        Log.info("When We get the count of the product relationship records in EPH STG going to SA..");

            if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = ProductRelationshipChecksSQL.GET_EPH_STG_PRODUCT_RELATIONSHIPS_COUNT_GOING_TO_SA;
                Log.info(sql);
            } else {
                sql = WorkCountSQL.GET_REFRESH_DATE;
                Log.info(sql);
                List<Map<String, Object>> refreshDateNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                String refreshDate = (String) refreshDateNumber.get(1).get("refresh_timestamp");
                sql = String.format( ProductRelationshipChecksSQL.GET_EPH_STG_PRODUCT_RELATIONSHIPS_COUNT_DELTA, refreshDate );
                Log.info(sql);
            }


        List<Map<String, Object>> productsRelNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countProductsRelEPHSTG = ((Long) productsRelNumber.get(0).get("count")).intValue();
        Log.info("Count of product relationship records in EPH STG is: " + countProductsRelEPHSTG);

//        Log.info("Assert count of records is not null");
//
//        assertTrue("No data found in EPH STG for product relationships", countProductsRelEPHSTG != 0);


    }


    @When("We get the count of the product relationship records in EPH DQ$")
    public void getCountProductRelEPHDQ() {
        Log.info("When We get the count of the product relationship records in EPH DQ ..");


        sql = ProductRelationshipChecksSQL.GET_EPH_STG_PRODUCT_RELATIONSHIPS_COUNT;
        Log.info(sql);

        List<Map<String, Object>> productsRelNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countProductsRelEPHDQ = ((Long) productsRelNumber.get(0).get("count")).intValue();
        Log.info("Count of product relationship records in EPH DQ is: " + countProductsRelEPHDQ);

        Log.info("Assert count of records is not null");

        assertTrue("No data found in EPH STG for product relationships", countProductsRelEPHDQ != 0);


    }


    @When("^We get the count of product relationship records in EPH SA$")
    public void getCountProductRelEPHSA() {
        Log.info("When We get the count of the product relationship records in EPH SA ..");
        sql = ProductRelationshipChecksSQL.GET_EPH_SA_PRODUCT_RELATIONSHIPS_COUNT;
        Log.info(sql);
        List<Map<String, Object>> productsRelNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countProductsRelEPHSA = ((Long) productsRelNumber.get(0).get("count")).intValue();
        Log.info("Count of product relationship records in EPH SA is: " + countProductsRelEPHSA);

        Log.info("Assert count of records is not null");

//        assertTrue("No data found in EPH SA for product relationships", countProductsRelEPHSA != 0);

    }

    @Given("^Get the count of records for product relationship records in EPH AE$")
    public void getCountProductRelationshipRecordsEPHAE() {
        Log.info("When We get the count of product relationship records in EPH AE ..");
        sql = ProductRelationshipChecksSQL.GET_COUNT_PRODUCT_RELATIONSHIP_EPHAE;
        Log.info(sql);
        List<Map<String, Object>> personsNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countProductRelationshipsEPHAE = ((Long) personsNumber.get(0).get("count")).intValue();
        Log.info("Count of product relationship records in EPH AE is: " + countProductRelationshipsEPHAE);
    }

    @Then("^Verify sum of records for product relationships in EPH GD and EPH AE is equal to number of records in EPH SA$")
    public void verifyCountOfManifestationsInEPHGDAndEPHAEIsEqualToEPHSA() {
        int sumOFRecords = countProductRelationshipsEPHAE + countProductsRelEPHGD;
        Assert.assertEquals("\nSum of the records for product relationships in EPH GD and EPH AE is NOT equal to number of records in EPH SA", sumOFRecords, countProductsRelEPHSA);
    }

    @When("^We get the count of product relationship records in EPH GD")
    public void getCountProductRelEPHGD() {
        Log.info("When We get the count of the product relationship records in EPH GD ..");
        sql = ProductRelationshipChecksSQL.GET_EPH_GD_PRODUCT_RELATIONSHIPS_COUNT;
        Log.info(sql);
        List<Map<String, Object>> productsRelNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countProductsRelEPHGD = ((Long) productsRelNumber.get(0).get("count")).intValue();
        Log.info("Count of product relationship records in EPH GD is: " + countProductsRelEPHGD);

        Log.info("Assert count of records is not null");

        assertTrue("No data found in EPH GD for product relationships", countProductsRelEPHGD != 0);

    }

    @Then("^The count of the product relationship records in PMX and EPH staging table is equal$")
    public void verifyCountOfManifestationDataInPMXAndEPHIsEqual() {
        Log.info("Then Check number of the product relationship records in PMX and EPH staging table is equal .. ");
        Assert.assertEquals("\nThe number of product relationship records in PMX GD_PRODUCT_MANIFESTATION and STG_PMX_MANIFESTATION is not equal", countProductsRelPMX, countProductsRelEPHSTG);
    }

    @Then("^The count of the product relationship records in EPH staging table and EPH SA is equal$")
    public void verifyCountOfManifestationDataInEPHSTGAndEPHSAIsEqual() {
        Log.info("Then Check number of the product relationship records in EPH STG and EPH SA is equal .. ");
        Assert.assertEquals("\nThe number of product relationship records in EPH STG and EPH SA is not equal", countProductsRelEPHSTG, countProductsRelEPHSA);
    }

    @Then("^The count of the product relationship records in in EPH SA and EPH GD is equal$")
    public void verifyCountOfManifestationDataInEPHSAAndEPHGDIsEqual() {
        Log.info("Then Check number of the product relationship records in EPH SA and EPH GD is equal .. ");
        Assert.assertEquals("\nThe number of product relationship records in EPH SA and EPH GD is not equal", countProductsRelEPHSA, countProductsRelEPHGD);
    }


    @Given("^We get (.*) random ids of product relationship records$")
    public void getProductRelationshipRandomIds(String numberOfRecords) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = String.format(ProductRelationshipChecksSQL.SELECT_RANDOM_RELATIONSHIP_PMX_SOURCEREF, numberOfRecords);
        Log.info(sql);

        productRelIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = productRelIds.stream().map(m -> m.get("RELATIONSHIP_PMX_SOURCEREF")).map(String::valueOf).collect(Collectors.toList());
        Log.info("ids : " + ids);
    }

    @When("^We get the product relationship records from PMX$")
    public void getProductRelationshipDataPMX() {
        Log.info("Get data from PMX ..");

        sql = String.format(ProductRelationshipChecksSQL.GET_PMX_PRODUCT_RELATIONSHIPS_DATA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.productRelationshipDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, ProductRelationshipDataObject.class, Constants.PMX_URL);
    }

    @Then("^We get the product relationship records from EPH STG$")
    public void getProductRelationshipDataEPHSTG() {
        Log.info("Get data from EPH STG ..");

        sql = String.format(ProductRelationshipChecksSQL.GET_EPH_STG_PRODUCT_RELATIONSHIPS_DATA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.productRelationshipDataObjectsFromEPHSTG = DBManager
                .getDBResultAsBeanList(sql, ProductRelationshipDataObject.class, Constants.EPH_URL);
    }

    @And("^Compare the product relationship records in PMX and EPH STG$")
    public void compareProductRelationshipDataInPMXAndEPHSTG() {/*
        dataQualityContext.productRelationshipDataObjectsFromPMX.sort(Comparator.comparing(ProductRelationshipDataObject::getRELATIONSHIP_PMX_SOURCEREF));
        dataQualityContext.productRelationshipDataObjectsFromEPHSTG.sort(Comparator.comparing(ProductRelationshipDataObject::getRELATIONSHIP_PMX_SOURCEREF));*/

        IntStream.range(0, dataQualityContext.productRelationshipDataObjectsFromPMX.size()).forEach(i -> {

            //verify RELATIONSHIP_PMX_SOURCEREF
            Log.info("RELATIONSHIP_PMX_SOURCEREF in PMX : " + dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getRELATIONSHIP_PMX_SOURCEREF());
            Log.info("RELATIONSHIP_PMX_SOURCEREF in EPH STG : " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getRELATIONSHIP_PMX_SOURCEREF());

            Log.info("Expecting RELATIONSHIP_PMX_SOURCEREF in PMX and EPH STG to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getRELATIONSHIP_PMX_SOURCEREF(), dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getRELATIONSHIP_PMX_SOURCEREF());

            //verify OWNER_PMX_SOURCE
            Log.info("OWNER_PMX_SOURCE in PMX : " + dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getOWNER_PMX_SOURCE());
            Log.info("OWNER_PMX_SOURCE in EPH STG : " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getOWNER_PMX_SOURCE());

            Log.info("Expecting OWNER_PMX_SOURCE in PMX and EPH STG to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getOWNER_PMX_SOURCE(), dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getOWNER_PMX_SOURCE());


            //verify COMPONENT_PMX_SOURCE
            Log.info("COMPONENT_PMX_SOURCE in PMX : " + dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getCOMPONENT_PMX_SOURCE());
            Log.info("COMPONENT_PMX_SOURCE in EPH STG : " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getCOMPONENT_PMX_SOURCE());

            Log.info("Expecting COMPONENT_PMX_SOURCE in PMX and EPH STG to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getCOMPONENT_PMX_SOURCE(), dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getCOMPONENT_PMX_SOURCE());


            //verify F_RELATIONSHIP_TYPE
            Log.info("F_RELATIONSHIP_TYPE in PMX : " + dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getF_RELATIONSHIP_TYPE());
            Log.info("F_RELATIONSHIP_TYPE in EPH STG : " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getF_RELATIONSHIP_TYPE());

            Log.info("Expecting F_RELATIONSHIP_TYPE in PMX and EPH STG to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getF_RELATIONSHIP_TYPE(), dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getF_RELATIONSHIP_TYPE());


            //verify EFFECTIVE_START_DATE
            if (dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getEFFECTIVE_START_DATE() != null || dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getEFFECTIVE_START_DATE() != null) {
                try {
                    Date pmxEffectiveStartDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s").parse(dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getEFFECTIVE_START_DATE());
                    Log.info("EFFECTIVE_START_DATE in PMX: " + pmxEffectiveStartDate);

                    Date ephEffectiveStartDate = new SimpleDateFormat("yyyy-MM-dd").parse(dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getEFFECTIVE_START_DATE());
                    Log.info("EFFECTIVE_START_DATE in EPH Staging: " + ephEffectiveStartDate);

                    assertEquals("EFFECTIVE_START_DATE in PMX and EPH STG is not equal ", pmxEffectiveStartDate, ephEffectiveStartDate);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            //ENDON
            Log.info("UPDATED in PMX: " + dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getENDON());
            Log.info("UPDATED in EPH Staging: " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getENDON());

            Log.info("Expecting UPDATED in PMX and EPH Staging are consistent for ");


               assertEquals("UPDATED in PMX and EPH STG is not equal ", dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getENDON(), dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getENDON());



            //UPDATED
            Log.info("UPDATED in PMX: " + dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getUPDATED());
            Log.info("UPDATED in EPH Staging: " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getUPDATED());

            Log.info("Expecting UPDATED in PMX and EPH Staging are consistent for ");


            try {
                Date pmxUpdatedDate = new SimpleDateFormat("dd-MMM-yy HH.mm.ss.SSSSSS").parse(dataQualityContext.productRelationshipDataObjectsFromPMX.get(i).getUPDATED());
                Date ephUpdatedDate = new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSSSSS aaa").parse(dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getUPDATED());

                assertEquals("UPDATED in PMX and EPH STG is not equal ", pmxUpdatedDate, ephUpdatedDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }


        });
    }


    @Then("^We get the product relationship records from EPH SA$")
    public void getProductRelationshipDataEPHSA() {
  /*      Log.info("Get PRODUCT_REL_PACK_ID from the lookup table map_identref_2_identid : ");
        idsSA = new ArrayList<>(ids);

        IntStream.range(0, idsSA.size()).forEach(i -> idsSA.set(i, "PROD_PACK-" + idsSA.get(i)));
        sql = String.format(ProductRelationshipChecksSQL.GET_PRODUCT_REL_PACK_ID_FROM_LOOKUP_TABLE,  Joiner.on("','").join(idsSA));
        Log.info(sql);

        List<Map<?, ?>> productRelIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        idsSA = productRelIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_REL_PACK_ID")).map(String::valueOf).collect(Collectors.toList());
*/
        Log.info("Get data from EPH SA ..");
        sql = String.format(ProductRelationshipChecksSQL.GET_EPH_SA_PRODUCT_RELATIONSHIPS_DATA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.productRelationshipDataObjectsFromEPHSA = DBManager
                .getDBResultAsBeanList(sql, ProductRelationshipDataObject.class, Constants.EPH_URL);
        sql.length();
    }

    @Then("^We get the product relationship records from EPH GD$")
    public void getProductRelationshipDataEPHGD() {
        Log.info("Get data from EPH GD ..");

        sql = String.format(ProductRelationshipChecksSQL.GET_EPH_GD_PRODUCT_RELATIONSHIPS_DATA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.productRelationshipDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductRelationshipDataObject.class, Constants.EPH_URL);
    }

    @And("^We check that mandatory columns for product relationship SA$")
    public void checkMandatoryColumnsRelationshipsSA() {
        Log.info("We check that mandatory columns are populated ...");

        IntStream.range(0, dataQualityContext.productRelationshipDataObjectsFromEPHSA.size()).forEach(i -> {
            //verify F_EVENT is not null
            assertNotNull(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_EVENT());
            //verify PRODUCT_REL_PACKAGE_ID
            assertNotNull(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getPRODUCT_REL_PACK_ID());
            //verify F_PACKAGE_OWNER
            assertNotNull(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_PACKAGE_OWNER());
            //verify F_COMPONENT
            assertNotNull(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_COMPONENT());
            //verify F_RELATIONSHIP_TYPE
            assertNotNull(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_RELATIONSHIP_TYPE());
        });
    }

    @And("^Compare the product relationship data between EPH STG and EPH SA$")
    public void compareProductRelationshipsDataBetweenEPHSTGAndEPHSA() {
        Log.info("Compare the product relationship data between EPH STG and EPH SA ...");

        if ( CollectionUtils.isEmpty(dataQualityContext.productRelationshipDataObjectsFromEPHSA) && System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Product Relationships");
        } else {
            IntStream.range(0, dataQualityContext.productRelationshipDataObjectsFromEPHSTG.size()).forEach(i -> {
/*

            String c = "PROD_PACK-"  + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getRELATIONSHIP_PMX_SOURCEREF();

            sql = String.format(ProductRelationshipChecksSQL.GET_PRODUCT_REL_PACK_ID_FROM_LOOKUP_TABLE, c);
            Log.info(sql);

            List<Map<?, ?>> productRelIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            idsSA = productRelIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_REL_PACK_ID")).map(String::valueOf).collect(Collectors.toList());

            Log.info("Get data from EPH SA ..");
            sql = String.format(ProductRelationshipChecksSQL.GET_EPH_SA_PRODUCT_RELATIONSHIPS_DATA, Joiner.on("','").join(idsSA));
            Log.info(sql);

*/
/*
            dataQualityContext.productRelationshipDataObjectsFromEPHSA = DBManager
                    .getDBResultAsBeanList(sql, ProductRelationshipDataObject.class, Constants.EPH_URL);
*/


                //B_CLASSNAME
                Log.info("B_CLASSNAME in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getB_CLASSNAME());

                Log.info("Expecting B_CLASSNAME in EPH SA to be correctly added");

                assertEquals("ProductRelationshipPackage", dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getB_CLASSNAME());

                //Get relationship_pmx_sourceref for the current PRODUCT_REL_PACK_ID from the lookup table
                Log.info("Get relationship_pmx_sourceref for the current PRODUCT_REL_PACK_ID from the lookup table ..");


                //F_PACKAGE_OWNER
//            map_sourceref_2_ephid('PRODUCT'::varchar, owner_pmx_source )

                Log.info("Get expected F_PACKAGE_OWNER from map_sourceref_2_ephid lookup table");
                String expectedF_PACKAGE_OWNER;
                sql = String.format(ProductRelationshipChecksSQL.GET_ID_FROM_SOURCEREF_LOOKUP_TABLE, dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getOWNER_PMX_SOURCE());
                Log.info((sql));

                productRelIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                expectedF_PACKAGE_OWNER = productRelIds.get(0).get("eph_id").toString();

                Log.info("Verify F_PACKAGE_OWNER is correctly populated");
                assertEquals(expectedF_PACKAGE_OWNER, dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_PACKAGE_OWNER());

                //F_COMPONENT
//            map_sourceref_2_ephid('PRODUCT'::varchar, component_pmx_source )

                Log.info("Get expected F_COMPONENT from map_sourceref_2_ephid lookup table");
                String expectedF_COMPONENT;
                sql = String.format(ProductRelationshipChecksSQL.GET_ID_FROM_SOURCEREF_LOOKUP_TABLE, dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getCOMPONENT_PMX_SOURCE());
                Log.info((sql));

                productRelIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                expectedF_COMPONENT = productRelIds.get(0).get("eph_id").toString();

                Log.info("Verify F_COMPONENT is correctly populated");
                assertEquals(expectedF_COMPONENT, dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_COMPONENT());

                //F_RELATIONSHIP_TYPE
                Log.info("F_RELATIONSHIP_TYPE in EPH STG : " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getF_RELATIONSHIP_TYPE());
                Log.info("F_RELATIONSHIP_TYPE in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_RELATIONSHIP_TYPE());

                Log.info("Expecting F_RELATIONSHIP_TYPE in EPH STG and EPH SA to be equal");

                assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getF_RELATIONSHIP_TYPE(), dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_RELATIONSHIP_TYPE());


                //EFFECTIVE_START_DATE
                Log.info("EFFECTIVE_START_DATE in EPH STG : " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getEFFECTIVE_START_DATE());
                Log.info("EFFECTIVE_START_DATE in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());

                Log.info("Expecting EFFECTIVE_START_DATE in EPH STG and EPH SA to be equal");

                assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getEFFECTIVE_START_DATE(), dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());

                //EFFECTIVE_END_DATE
                Log.info("EFFECTIVE_END_DATE in EPH STG : " + dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getENDON());
                Log.info("EFFECTIVE_END_DATE in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());

                Log.info("Expecting EFFECTIVE_END_DATE in EPH STG and EPH SA to be equal");

                if (dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getStatus1().equalsIgnoreCase("PST") ||
                        dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getStatus1().equalsIgnoreCase("NVP")
                        || dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getStatus2().equalsIgnoreCase("PST") ||
                        dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getStatus2().equalsIgnoreCase("NVP")) {
                    assertNotNull(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());

                } else {
                    assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSTG.get(i).getENDON(), dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());
                }
            });
        }
    }


    @And("^Compare the product relationship data between EPH SA and EPH GD$")
    public void compareProductRelationshipsDataBetweenEPHSAAndEPHGD() {
        Log.info("Compare the product relationship data between EPH SA and EPH GD ...");

        IntStream.range(0, dataQualityContext.productRelationshipDataObjectsFromEPHSA.size()).forEach(i -> {
            //F_EVENT
            Log.info("F_EVENT in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_EVENT());
            Log.info("F_EVENT in EPH GD : " + dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getF_EVENT());

            Log.info("Expecting F_EVENT in EPH SA and EPH GD to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_EVENT(), dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getF_EVENT());

            //B_CLASSNAME
            Log.info("B_CLASSNAME in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getB_CLASSNAME());
            Log.info("B_CLASSNAME in EPH GD : " + dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

            Log.info("Expecting B_CLASSNAME in EPH SA and EPH GD to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getB_CLASSNAME(), dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getB_CLASSNAME());

            //PRODUCT_REL_PACK_ID
            Log.info("PRODUCT_REL_PACK_ID in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getPRODUCT_REL_PACK_ID());
            Log.info("PRODUCT_REL_PACK_ID in EPH GD : " + dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getPRODUCT_REL_PACK_ID());

            Log.info("Expecting PRODUCT_REL_PACK_ID in EPH SA and EPH GD to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getPRODUCT_REL_PACK_ID(), dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getPRODUCT_REL_PACK_ID());

            //F_PACKAGE_OWNER
            Log.info("F_PACKAGE_OWNER in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_PACKAGE_OWNER());
            Log.info("F_PACKAGE_OWNER in EPH GD : " + dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getF_PACKAGE_OWNER());

            Log.info("Expecting F_PACKAGE_OWNER in EPH SA and EPH GD to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_PACKAGE_OWNER(), dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getF_PACKAGE_OWNER());

            //F_COMPONENT
            Log.info("F_COMPONENT in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_COMPONENT());
            Log.info("F_COMPONENT in EPH GD : " + dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getF_COMPONENT());

            Log.info("Expecting F_COMPONENT in EPH SA and EPH GD to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_COMPONENT(), dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getF_COMPONENT());

            //F_RELATIONSHIP_TYPE
            Log.info("F_RELATIONSHIP_TYPE in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_RELATIONSHIP_TYPE());
            Log.info("F_RELATIONSHIP_TYPE in EPH GD : " + dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getF_RELATIONSHIP_TYPE());

            Log.info("Expecting F_RELATIONSHIP_TYPE in EPH SA and EPH GD to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getF_RELATIONSHIP_TYPE(), dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getF_RELATIONSHIP_TYPE());

            //EFFECTIVE_START_DATE
            Log.info("EFFECTIVE_START_DATE in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE());
            Log.info("EFFECTIVE_START_DATE in EPH GD : " + dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getEFFECTIVE_START_DATE());

            Log.info("Expecting EFFECTIVE_START_DATE in EPH SA and EPH GD to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_START_DATE(), dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getEFFECTIVE_START_DATE());

            //EFFECTIVE_END_DATE
            Log.info("EFFECTIVE_END_DATE in EPH SA : " + dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE());
            Log.info("EFFECTIVE_END_DATE in EPH GD : " + dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getEFFECTIVE_END_DATE());

            Log.info("Expecting EFFECTIVE_END_DATE in EPH SA and EPH GD to be equal");

            assertEquals(dataQualityContext.productRelationshipDataObjectsFromEPHSA.get(i).getEFFECTIVE_END_DATE(), dataQualityContext.productRelationshipDataObjectsFromEPHGD.get(i).getEFFECTIVE_END_DATE());

        });
    }
}
