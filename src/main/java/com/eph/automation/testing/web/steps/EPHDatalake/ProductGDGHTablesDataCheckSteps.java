package com.eph.automation.testing.web.steps.EPHDatalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityEPHDLContext;
import com.eph.automation.testing.models.dao.EPHDataLake.ProductDataDLObject;
import com.eph.automation.testing.services.db.EPHDataLakeSql.GetDLDBUser;
import com.eph.automation.testing.services.db.EPHDataLakeSql.ProductGDGHTablesDataChecksSQL;
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


public class ProductGDGHTablesDataCheckSteps {

    @StaticInjection
    public DataQualityEPHDLContext dataQualityEPHDLContext;
    private static String sql;
    private static List<String> Ids;
    private ProductGDGHTablesDataChecksSQL productObj = new ProductGDGHTablesDataChecksSQL();


    @Given("^We get (.*) random product ids of (.*)")
    public void getRandomProductIds(String numberOfRecords, String tableName) {
       numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random records ..");

        //Get property when running with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);
        switch (tableName){
            case "gd_product":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_ID, numberOfRecords);
                List<Map<?, ?>> randomProductIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductIds.stream().map(m -> (String) m.get("PRODUCT_ID")).collect(Collectors.toList());
                break;

            case "gh_product":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_GH_PRODUCT_ID, numberOfRecords);
                List<Map<?, ?>> randomGHProductIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGHProductIds.stream().map(m -> (String) m.get("PRODUCT_ID")).collect(Collectors.toList());
                break;

            case "gd_accountable_product":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_ACCOUNTABLE_PRODUCT_ID, numberOfRecords);
                List<Map<?, ?>> randomAccProductIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomAccProductIds.stream().map(m -> (BigDecimal) m.get("ACCOUNTABLE_PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_accountable_product":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_ACCOUNTABLE_PRODUCT_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomGhAccProductIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomGhAccProductIds.stream().map(m -> (BigDecimal) m.get("ACCOUNTABLE_PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_product_rel_package":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_PACKAGE_ID, numberOfRecords);
                List<Map<?, ?>> randomProductPkgIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductPkgIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_REL_PACK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_product_rel_package":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_PACKAGE_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomProductPkgGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductPkgGHIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_REL_PACK_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_product_financial_attribs":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_FIN_ATTRIB_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomProductFinGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductFinGDIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_FIN_AATRIBS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_product_financial_attribs":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_FIN_ATTRIB_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomProductFinGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductFinGHIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_FIN_AATRIBS_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_product_identifier":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_IDENTIFIER_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomProductIdentifierGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductIdentifierGDIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_IDENTIFIER_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_product_identifier":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_IDENTIFIER_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomProductIdentifierGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductIdentifierGHIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_IDENTIFIER_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gd_product_relationship":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_RELATIONSHIP_ID_GD, numberOfRecords);
                List<Map<?, ?>> randomProductRelationGDIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductRelationGDIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_RELATIONSHIP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

            case "gh_product_relationship":
                sql = String.format(ProductGDGHTablesDataChecksSQL.GET_RANDOM_PRODUCT_RELATIONSHIP_ID_GH, numberOfRecords);
                List<Map<?, ?>> randomProductRelationGHIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                Ids = randomProductRelationGHIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_RELATIONSHIP_ID")).map(String::valueOf).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @When("^We get the product records from EPH of (.*)$")
    public void getProductEPH(String tableName) {
        Log.info("We get the product records from EPH..");
        sql = String.format(productObj.productBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the product records from DL of (.*)$")
    public void getProductDL(String tableName) {
        Log.info("We get the product records from DL..");
        sql = String.format(productObj.productBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product records in EPH and DL of (.*)$")
    public void compareProductDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the product records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_ID));
                dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_ID));
                if(tableName.equals("gh_product")){
                    dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_ID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_ID());

                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_product")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_UPDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " S_PRODUCT_ID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_PRODUCT_ID()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_PRODUCT_ID());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_PRODUCT_ID() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_PRODUCT_ID()!= null)) {
                    Assert.assertEquals("The S_PRODUCT_ID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_PRODUCT_ID());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " NAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getNAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getNAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getNAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getNAME()!= null)) {
                    Assert.assertEquals("The NAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getNAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getNAME());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " S_NAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_NAME());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_NAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_NAME()!= null)) {
                    Assert.assertEquals("The S_NAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_NAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_NAME());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " SHORT_NAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getSHORT_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getSHORT_NAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getSHORT_NAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getSHORT_NAME()!= null)) {
                    Assert.assertEquals("The SHORT_NAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getSHORT_NAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getSHORT_NAME());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " S_SHORT_NAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_SHORT_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_SHORT_NAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_SHORT_NAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_SHORT_NAME()!= null)) {
                    Assert.assertEquals("The S_SHORT_NAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_SHORT_NAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_SHORT_NAME());

                }
                //Converting Boolean to String
                String seperately_sale_indicator_eph = String.valueOf(dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getSEPARATELY_SALE_INDICATOR());
                String seperately_sale_indicator_dl = String.valueOf(dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getSEPARATELY_SALE_INDICATOR());
                if(seperately_sale_indicator_eph.equals("f")){
                    seperately_sale_indicator_eph = "false";
                }else{
                    seperately_sale_indicator_eph = "true";
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " SEPARATELY_SALE_INDICATOR => EPH="+seperately_sale_indicator_eph+
                        " DL="+seperately_sale_indicator_dl);

                if (seperately_sale_indicator_eph != null || (seperately_sale_indicator_dl!= null)) {
                    Assert.assertEquals("The SEPERATELY_SALE_INDICATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            seperately_sale_indicator_eph, seperately_sale_indicator_dl);
                }
                String trial_allowed_indicator_eph = String.valueOf(dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getTRIAL_ALLOWED_INDICATOR());
                String trial_allowed_indicator_dl = String.valueOf(dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getTRIAL_ALLOWED_INDICATOR());

                if(trial_allowed_indicator_eph.equals("f")){
                    trial_allowed_indicator_eph = "false";
                }else{
                    trial_allowed_indicator_eph = "true";
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " TRIAL_ALLOWED_INDICATOR => EPH="+trial_allowed_indicator_eph+
                        " DL="+trial_allowed_indicator_dl);

                if (seperately_sale_indicator_eph != null || (seperately_sale_indicator_dl!= null)) {
                    Assert.assertEquals("The TRIAL_ALLOWED_INDICATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            trial_allowed_indicator_eph, trial_allowed_indicator_dl);
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " LAUNCH_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getLAUNCH_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getLAUNCH_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getLAUNCH_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getLAUNCH_DATE()!= null)) {
                    Assert.assertEquals("The LAUNCH_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getLAUNCH_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getLAUNCH_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " CONTENT_FROM_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_FROM_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_FROM_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_FROM_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_FROM_DATE()!= null)) {
                    Assert.assertEquals("The CONTENT_FROM_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_FROM_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_FROM_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " CONTENT_TO_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_TO_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_TO_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_TO_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_TO_DATE()!= null)) {
                    Assert.assertEquals("The CONTENT_TO_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_TO_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_TO_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " CONTENT_DATE_OFFSET => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_DATE_OFFSET()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_DATE_OFFSET());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_DATE_OFFSET() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_DATE_OFFSET()!= null)) {
                    Assert.assertEquals("The CONTENT_DATE_OFFSET is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_DATE_OFFSET(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_DATE_OFFSET());
                }

                String summaryChanged_eph = String.valueOf(dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getT_SUMMARY_CHANGED());
                String summaryChanged_dl = String.valueOf(dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getT_SUMMARY_CHANGED());
                if(summaryChanged_eph.equals("f")){
                    summaryChanged_eph = "false";
                }else{
                    summaryChanged_eph = "true";
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " T_EVENT_DESCRIPTION => EPH="+summaryChanged_eph+
                        " DL="+summaryChanged_dl);
                if (summaryChanged_eph != null || (summaryChanged_dl!= null)) {
                    Assert.assertEquals("The T_SUMMARY_CHANGED is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            summaryChanged_eph, summaryChanged_dl);
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " T_EVENT_DESCRIPTION => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION()!= null)) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_TYPE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE()!= null)) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_STATUS => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_STATUS()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_STATUS());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_STATUS() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_STATUS()!= null)) {
                    Assert.assertEquals("The F_STATUS is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_STATUS(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_STATUS());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " f_accountable_product => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getf_accountable_product()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getf_accountable_product());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getf_accountable_product() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getf_accountable_product()!= null)) {
                    Assert.assertEquals("The f_accountable_product is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getf_accountable_product(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getf_accountable_product());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_TAX_CODE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TAX_CODE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TAX_CODE());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TAX_CODE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TAX_CODE()!= null)) {
                    Assert.assertEquals("The F_TAX_CODE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TAX_CODE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TAX_CODE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_REVENUE_MODEL => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_MODEL()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_MODEL());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_MODEL() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_MODEL()!= null)) {
                    Assert.assertEquals("The F_REVENUE_MODEL is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_MODEL(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_MODEL());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_REVENUE_ACCOUNT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_ACCOUNT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_ACCOUNT());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_ACCOUNT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_ACCOUNT()!= null)) {
                    Assert.assertEquals("The F_REVENUE_ACCOUNT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_ACCOUNT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_ACCOUNT());

                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_WWORK => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_WWORK()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_WWORK());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_WWORK()!= null)) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_WWORK());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_MANIFESTATION => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_MANIFESTATION()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_MANIFESTATION());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_MANIFESTATION() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_MANIFESTATION()!= null)) {
                    Assert.assertEquals("The F_MANIFESTATION is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_MANIFESTATION(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_MANIFESTATION());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_T_EVENT_TYPE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE()!= null)) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_ONE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_ONE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_ONE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_ONE()!= null)) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_ONE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_TWO => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_TWO()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_TWO());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_TWO() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_TWO()!= null)) {
                    Assert.assertEquals("The F_SELF_TWO is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_TWO());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_THREE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_THREE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_THREE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_THREE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_THREE()!= null)) {
                    Assert.assertEquals("The F_SELF_THREE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_THREE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_FOUR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FOUR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FOUR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FOUR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FOUR()!= null)) {
                    Assert.assertEquals("The F_SELF_FOUR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FOUR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_FIVE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FIVE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FIVE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FIVE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FIVE()!= null)) {
                    Assert.assertEquals("The F_SELF_FIVE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FIVE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FIVE());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_SIX => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SIX()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SIX());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SIX() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SIX()!= null)) {
                    Assert.assertEquals("The F_SELF_SIX is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SIX(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SIX());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_SEVEN => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SEVEN()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SEVEN());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SEVEN() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SEVEN()!= null)) {
                    Assert.assertEquals("The F_SELF_SEVEN is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SEVEN(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SEVEN());
                }
            }
        }
    }

    @When("^We get the accountable product records from EPH of (.*)$")
    public void getAccountableProductEPH(String tableName) {
        Log.info("We get the accountable product records from EPH..");
        sql = String.format(productObj.accProductBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the accountable product records from DL of (.*)$")
    public void getAccountableProductDL(String tableName) {
        Log.info("We get the accountable product records from DL..");
        sql = String.format(productObj.accProductBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare accountable product records in EPH and DL of (.*)$")
    public void compareGDAccountableProductDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the gd accountable product records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getACCOUNTABLE_PRODUCT_ID));
                dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getACCOUNTABLE_PRODUCT_ID));
                if(tableName.equals("gh_accountable_product")){
                    dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }
                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getACCOUNTABLE_PRODUCT_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The ACCOUNTABLE_PRODUCT_ID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getACCOUNTABLE_PRODUCT_ID());

                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_accountable_product")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_UPDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " GL_PRODUCT_SEGMENT_CODE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_CODE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_CODE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_CODE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_CODE()!= null)) {
                    Assert.assertEquals("The GL_PRODUCT_SEGMENT_CODE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_CODE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_CODE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " GL_PRODUCT_SEGMENT_NAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_NAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_NAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_NAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_NAME()!= null)) {
                    Assert.assertEquals("The GL_PRODUCT_SEGMENT_NAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_NAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_NAME());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " T_EVENT_DESCRIPTION => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION()!= null)) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " F_GL_PRODUCT_SEGMENT_PARENT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_PRODUCT_SEGMENT_PARENT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_PRODUCT_SEGMENT_PARENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_PRODUCT_SEGMENT_PARENT()!= null)) {
                    Assert.assertEquals("The F_GL_PRODUCT_SEGMENT_PARENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_PRODUCT_SEGMENT_PARENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " F_T_EVENT_TYPE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE()!= null)) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the product package records from EPH of (.*)$")
    public void getProductPkgEPH(String tableName) {
        Log.info("We get the product Package records from EPH..");
        sql = String.format(productObj.productPkgBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the product package records from DL of (.*)$")
    public void getProductPkgDL(String tableName) {
        Log.info("We get the product Package records from DL..");
        sql = String.format(productObj.productPkgBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product package records in EPH and DL of (.*)$")
    public void compareProductPkgDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the product package records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_REL_PACK_ID));
                dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_REL_PACK_ID));
                if(tableName.equals("gh_product_rel_package")){
                    dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_REL_PACK_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_REL_PACK_ID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_REL_PACK_ID());

                }

                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_CLASSNAME => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_product_rel_package")){
                    Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                            " B_FROMBATCHID => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                            " B_TOBATCHID => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                            " B_BATCHID => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_CREDATE => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_UPDDATE => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_CREATOR => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_UPDATOR => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " ALLOCATION => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getALLOCATION() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getALLOCATION());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getALLOCATION() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getALLOCATION()!= null)) {
                    Assert.assertEquals("The ALLOCATION is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getALLOCATION(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getALLOCATION());
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " EFFECTIVE_START_DATE => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }
                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " EFFECTIVE_END_DATE => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " F_PACKAGE_OWNER => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PACKAGE_OWNER() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PACKAGE_OWNER());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PACKAGE_OWNER() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PACKAGE_OWNER()!= null)) {
                    Assert.assertEquals("The F_PACKAGE_OWNER is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PACKAGE_OWNER(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PACKAGE_OWNER());
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " F_COMPONENT => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_COMPONENT() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_COMPONENT());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_COMPONENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_COMPONENT()!= null)) {
                    Assert.assertEquals("The F_COMPONENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_COMPONENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_COMPONENT());
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " F_RELATIONSHIP_TYPE => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE()!= null)) {
                    Assert.assertEquals("The F_RELATIONSHIP_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE());
                }

                Log.info("ID => " + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " F_EVENT => EPH=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() +
                        " DL=" + dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the product financial records from EPH of (.*)$")
    public void getProductFinAttrEPH(String tableName) {
        Log.info("We get the Financial Attribute product records from EPH..");
        sql = String.format(productObj.productFinAttrBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the product financial records from DL of (.*)$")
    public void getProductFinAttrDL(String tableName) {
        Log.info("We get the Financial Attribute product records from DL..");
        sql = String.format(productObj.productFinAttrBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product financial records in EPH and DL of (.*)$")
    public void compareGDProductFinAttrDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Financial Attribute product records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_FIN_AATRIBS_ID));
                dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_FIN_AATRIBS_ID));

                if(tableName.equals("gh_product_financial_attribs")){
                    dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_FIN_AATRIBS_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_FIN_AATRIBS_ID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_FIN_AATRIBS_ID());

                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_product_financial_attribs")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_UPDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " EFFECTIVE_START_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " EFFECTIVE_END_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_GL_COMPANY => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COMPANY()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COMPANY());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COMPANY() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COMPANY()!= null)) {
                    Assert.assertEquals("The F_GL_COMPANY is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COMPANY(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COMPANY());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_GL_COST_RESP_CENTRE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE()!= null)) {
                    Assert.assertEquals("The F_GL_COST_RESP_CENTRE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_GL_REVENUE_RESP_CENTRE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE()!= null)) {
                    Assert.assertEquals("The F_GL_REVENUE_RESP_CENTRE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_PRODUCT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT()!= null)) {
                    Assert.assertEquals("The F_PRODUCT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the product identifier records from EPH of (.*)$")
    public void getProductIdentifierEPH(String tableName) {
        Log.info("We get the product identifier records from EPH..");
        sql = String.format(productObj.productIdentifierBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the product identifier records from DL of (.*)$")
    public void getProductIdentifierDL(String tableName) {
        Log.info("We get the product identifier records from DL..");
        sql = String.format(productObj.productIdentifierBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product identifier records in EPH and DL of (.*)$")
    public void compareProductIdentifierDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Identifier records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_IDENTIFIER_ID));
                dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_IDENTIFIER_ID));
                if(tableName.equals("gh_product_identifier")){
                    dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_IDENTIFIER_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_IDENTIFIER_ID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_IDENTIFIER_ID());

                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());


                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_product_identifier")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_UPDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " IDENTIFIER => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getIDENTIFIER()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getIDENTIFIER());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getIDENTIFIER() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getIDENTIFIER()!= null)) {
                    Assert.assertEquals("The IDENTIFIER is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getIDENTIFIER(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getIDENTIFIER());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " S_IDENTIFIER => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_IDENTIFIER()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_IDENTIFIER());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_IDENTIFIER() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_IDENTIFIER()!= null)) {
                    Assert.assertEquals("The S_IDENTIFIER is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getS_IDENTIFIER(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getS_IDENTIFIER());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " EFFECTIVE_START_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " EFFECTIVE_END_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " F_TYPE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE()!= null)) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " F_PRODUCT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT()!= null)) {
                    Assert.assertEquals("The F_PRODUCT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT());
                }
                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the product relationship records from EPH of (.*)$")
    public void getProductRelationEPH(String tableName) {
        Log.info("We get the product relation records from EPH..");
        sql = String.format(productObj.productRelationshipBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the product relationship records from DL of (.*)$")
    public void getProductRelationDL(String tableName) {
        Log.info("We get the product relation records from DL..");
        sql = String.format(productObj.productRelationshipBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityEPHDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product relationship records in EPH and DL of (.*)$")
    public void compareProductRelationDataEPHtoDL(String tableName) {
        if (dataQualityEPHDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Relationship records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityEPHDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_RELATIONSHIP_ID));
                dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_RELATIONSHIP_ID));
                if(tableName.equals("gh_product_relationship")){
                    dataQualityEPHDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityEPHDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_RELATIONSHIP_ID()!= null)) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_RELATIONSHIP_ID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_RELATIONSHIP_ID());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_CLASSNAME => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME()!= null)) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_product_relationship")){
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                            " B_FROMBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                            " B_TOBATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID()!= null)) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                            " B_BATCHID => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID()!= null)) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_CREDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE()!= null)) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_UPDATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE()!= null)) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_CREATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR()!= null)) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_UPDATOR => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR()!= null)) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " EXTERNAL_REFERENCE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE()!= null)) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " F_PARENT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PARENT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PARENT());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PARENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PARENT()!= null)) {
                    Assert.assertEquals("The F_PARENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_PARENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_PARENT());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " F_CHILD => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_CHILD()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_CHILD());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_CHILD() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_CHILD()!= null)) {
                    Assert.assertEquals("The F_CHILD is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_CHILD(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_CHILD());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " EFFECTIVE_START_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " EFFECTIVE_END_DATE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE()!= null)) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " F_RELATIOSHIP_TYPE => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIOSHIP_TYPE()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIOSHIP_TYPE());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIOSHIP_TYPE() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIOSHIP_TYPE()!= null)) {
                    Assert.assertEquals("The F_RELATIOSHIP_TYPE is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIOSHIP_TYPE(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIOSHIP_TYPE());
                }

                Log.info("ID => "+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " F_EVENT => EPH="+ dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+ dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT()!= null)) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityEPHDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }

            }
        }
    }

}





