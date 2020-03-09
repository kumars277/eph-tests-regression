package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.ProductDataDLObject;
import com.eph.automation.testing.services.db.DataLakeSql.GetDLDBUser;
import com.eph.automation.testing.services.db.DataLakeSql.ProductGDGHTablesDataChecksSQL;
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
    public DataQualityDLContext dataQualityDLContext;
    private static String sql;
    private static List<String> Ids;
    private ProductGDGHTablesDataChecksSQL productObj = new ProductGDGHTablesDataChecksSQL();


    @Given("^We get (.*) random product ids of (.*)")
    public void getRandomProductIds(String numberOfRecords, String tableName) {
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
        dataQualityDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the product records from DL of (.*)$")
    public void getProductDL(String tableName) {
        Log.info("We get the product records from DL..");
        sql = String.format(productObj.productBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product records in EPH and DL of (.*)$")
    public void compareProductDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the product records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_ID));
                dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_ID));
                if(tableName.equals("gh_product")){
                    dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_ID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_ID());

                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }
                if(tableName.equals("gh_product")){
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_UPDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " S_PRODUCT_ID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_PRODUCT_ID()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_PRODUCT_ID());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_PRODUCT_ID() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_PRODUCT_ID().equals("null"))) {
                    Assert.assertEquals("The S_PRODUCT_ID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_PRODUCT_ID());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " NAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getNAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getNAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getNAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getNAME().equals("null"))) {
                    Assert.assertEquals("The NAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getNAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getNAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " S_NAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_NAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_NAME());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_NAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_NAME().equals("null"))) {
                    Assert.assertEquals("The S_NAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_NAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_NAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " SHORT_NAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getSHORT_NAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getSHORT_NAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getSHORT_NAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getSHORT_NAME().equals("null"))) {
                    Assert.assertEquals("The SHORT_NAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getSHORT_NAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getSHORT_NAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " S_SHORT_NAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_SHORT_NAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_SHORT_NAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_SHORT_NAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_SHORT_NAME().equals("null"))) {
                    Assert.assertEquals("The S_SHORT_NAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_SHORT_NAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_SHORT_NAME());

                }
                //Converting Boolean to String
                String seperately_sale_indicator_eph = String.valueOf(dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getSEPARATELY_SALE_INDICATOR());
                String seperately_sale_indicator_dl = String.valueOf(dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getSEPARATELY_SALE_INDICATOR());
                if(seperately_sale_indicator_eph.equals("f")){
                    seperately_sale_indicator_eph = "false";
                }else{
                    seperately_sale_indicator_eph = "true";
                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " SEPARATELY_SALE_INDICATOR => EPH="+seperately_sale_indicator_eph+
                        " DL="+seperately_sale_indicator_dl);

                if (seperately_sale_indicator_eph != null || (!seperately_sale_indicator_dl.equals("null"))) {
                    Assert.assertEquals("The SEPERATELY_SALE_INDICATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            seperately_sale_indicator_eph, seperately_sale_indicator_dl);
                }
                String trial_allowed_indicator_eph = String.valueOf(dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getTRIAL_ALLOWED_INDICATOR());
                String trial_allowed_indicator_dl = String.valueOf(dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getTRIAL_ALLOWED_INDICATOR());

                if(trial_allowed_indicator_eph.equals("f")){
                    trial_allowed_indicator_eph = "false";
                }else{
                    trial_allowed_indicator_eph = "true";
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " TRIAL_ALLOWED_INDICATOR => EPH="+trial_allowed_indicator_eph+
                        " DL="+trial_allowed_indicator_dl);

                if (seperately_sale_indicator_eph != null || (!seperately_sale_indicator_dl.equals("null"))) {
                    Assert.assertEquals("The TRIAL_ALLOWED_INDICATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            trial_allowed_indicator_eph, trial_allowed_indicator_dl);
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " LAUNCH_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getLAUNCH_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getLAUNCH_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getLAUNCH_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getLAUNCH_DATE().equals("null"))) {
                    Assert.assertEquals("The LAUNCH_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getLAUNCH_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getLAUNCH_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " CONTENT_FROM_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_FROM_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_FROM_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_FROM_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_FROM_DATE().equals("null"))) {
                    Assert.assertEquals("The CONTENT_FROM_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_FROM_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_FROM_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " CONTENT_TO_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_TO_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_TO_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_TO_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_TO_DATE().equals("null"))) {
                    Assert.assertEquals("The CONTENT_TO_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_TO_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_TO_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " CONTENT_DATE_OFFSET => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_DATE_OFFSET()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_DATE_OFFSET());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_DATE_OFFSET() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_DATE_OFFSET().equals("null"))) {
                    Assert.assertEquals("The CONTENT_DATE_OFFSET is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getCONTENT_DATE_OFFSET(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getCONTENT_DATE_OFFSET());
                }

                String summaryChanged_eph = String.valueOf(dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getT_SUMMARY_CHANGED());
                String summaryChanged_dl = String.valueOf(dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getT_SUMMARY_CHANGED());
                if(summaryChanged_eph.equals("f")){
                    summaryChanged_eph = "false";
                }else{
                    summaryChanged_eph = "true";
                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " T_EVENT_DESCRIPTION => EPH="+summaryChanged_eph+
                        " DL="+summaryChanged_dl);
                if (summaryChanged_eph != null || (!summaryChanged_dl.equals("null"))) {
                    Assert.assertEquals("The T_SUMMARY_CHANGED is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            summaryChanged_eph, summaryChanged_dl);
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " T_EVENT_DESCRIPTION => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_TYPE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_STATUS => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_STATUS()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_STATUS());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_STATUS() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_STATUS().equals("null"))) {
                    Assert.assertEquals("The F_STATUS is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_STATUS(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_STATUS());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " f_accountable_product => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getf_accountable_product()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getf_accountable_product());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getf_accountable_product() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getf_accountable_product().equals("null"))) {
                    Assert.assertEquals("The f_accountable_product is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getf_accountable_product(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getf_accountable_product());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_TAX_CODE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TAX_CODE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TAX_CODE());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TAX_CODE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TAX_CODE().equals("null"))) {
                    Assert.assertEquals("The F_TAX_CODE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TAX_CODE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TAX_CODE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_REVENUE_MODEL => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_MODEL()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_MODEL());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_MODEL() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_MODEL().equals("null"))) {
                    Assert.assertEquals("The F_REVENUE_MODEL is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_MODEL(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_MODEL());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_REVENUE_ACCOUNT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_ACCOUNT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_ACCOUNT());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_ACCOUNT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_ACCOUNT().equals("null"))) {
                    Assert.assertEquals("The F_REVENUE_ACCOUNT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_REVENUE_ACCOUNT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_REVENUE_ACCOUNT());

                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_WWORK => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_WWORK()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_WWORK());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_WWORK() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_WWORK().equals("null"))) {
                    Assert.assertEquals("The F_WWORK is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_WWORK(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_WWORK());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_MANIFESTATION => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_MANIFESTATION()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_MANIFESTATION());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_MANIFESTATION() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_MANIFESTATION().equals("null"))) {
                    Assert.assertEquals("The F_MANIFESTATION is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_MANIFESTATION(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_MANIFESTATION());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_T_EVENT_TYPE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_EVENT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_ONE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_ONE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_ONE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_ONE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_ONE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_ONE());
                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_TWO => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_TWO()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_TWO());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_TWO() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_TWO().equals("null"))) {
                    Assert.assertEquals("The F_SELF_TWO is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_TWO());
                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_THREE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_THREE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_THREE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_THREE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_THREE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_THREE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_THREE());
                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_FOUR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FOUR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FOUR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FOUR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FOUR().equals("null"))) {
                    Assert.assertEquals("The F_SELF_FOUR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FOUR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_FIVE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FIVE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FIVE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FIVE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FIVE().equals("null"))) {
                    Assert.assertEquals("The F_SELF_FIVE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_FIVE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_FIVE());
                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_SIX => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SIX()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SIX());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SIX() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SIX().equals("null"))) {
                    Assert.assertEquals("The F_SELF_SIX is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SIX(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SIX());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID()+
                        " F_SELF_SEVEN => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SEVEN()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SEVEN());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SEVEN() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SEVEN().equals("null"))) {
                    Assert.assertEquals("The F_SELF_SEVEN is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_SELF_SEVEN(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_SELF_SEVEN());
                }
            }
        }
    }

    @When("^We get the accountable product records from EPH of (.*)$")
    public void getAccountableProductEPH(String tableName) {
        Log.info("We get the accountable product records from EPH..");
        sql = String.format(productObj.accProductBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the accountable product records from DL of (.*)$")
    public void getAccountableProductDL(String tableName) {
        Log.info("We get the accountable product records from DL..");
        sql = String.format(productObj.accProductBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare accountable product records in EPH and DL of (.*)$")
    public void compareGDAccountableProductDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the gd accountable product records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getACCOUNTABLE_PRODUCT_ID));
                dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getACCOUNTABLE_PRODUCT_ID));
                if(tableName.equals("gh_accountable_product")){
                    dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }
                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getACCOUNTABLE_PRODUCT_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The ACCOUNTABLE_PRODUCT_ID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getACCOUNTABLE_PRODUCT_ID());

                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_accountable_product")){
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_UPDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " GL_PRODUCT_SEGMENT_CODE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_CODE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_CODE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_CODE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_CODE().equals("null"))) {
                    Assert.assertEquals("The GL_PRODUCT_SEGMENT_CODE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_CODE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_CODE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " GL_PRODUCT_SEGMENT_NAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_NAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_NAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_NAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_NAME().equals("null"))) {
                    Assert.assertEquals("The GL_PRODUCT_SEGMENT_NAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getGL_PRODUCT_SEGMENT_NAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getGL_PRODUCT_SEGMENT_NAME());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " T_EVENT_DESCRIPTION => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION().equals("null"))) {
                    Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " F_GL_PRODUCT_SEGMENT_PARENT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_PRODUCT_SEGMENT_PARENT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_PRODUCT_SEGMENT_PARENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_PRODUCT_SEGMENT_PARENT().equals("null"))) {
                    Assert.assertEquals("The F_GL_PRODUCT_SEGMENT_PARENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_PRODUCT_SEGMENT_PARENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_PRODUCT_SEGMENT_PARENT());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " F_T_EVENT_TYPE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID()+
                        " F_EVENT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getACCOUNTABLE_PRODUCT_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the product package records from EPH of (.*)$")
    public void getProductPkgEPH(String tableName) {
        Log.info("We get the product Package records from EPH..");
        sql = String.format(productObj.productPkgBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the product package records from DL of (.*)$")
    public void getProductPkgDL(String tableName) {
        Log.info("We get the product Package records from DL..");
        sql = String.format(productObj.productPkgBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product package records in EPH and DL of (.*)$")
    public void compareProductPkgDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the product package records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_REL_PACK_ID));
                dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_REL_PACK_ID));
                if(tableName.equals("gh_product_rel_package")){
                    dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_REL_PACK_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_REL_PACK_ID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_REL_PACK_ID());

                }

                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_CLASSNAME => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_product_rel_package")){
                    Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                            " B_FROMBATCHID => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID() +
                            " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                            " B_TOBATCHID => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() +
                            " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }
                }else{
                    Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                            " B_BATCHID => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() +
                            " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_CREDATE => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());
                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }
                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_UPDDATE => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }
                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_CREATOR => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }
                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " B_UPDATOR => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " EXTERNAL_REFERENCE => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }
                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " ALLOCATION => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getALLOCATION() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getALLOCATION());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getALLOCATION() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getALLOCATION().equals("null"))) {
                    Assert.assertEquals("The ALLOCATION is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getALLOCATION(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getALLOCATION());
                }

                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " EFFECTIVE_START_DATE => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }
                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " EFFECTIVE_END_DATE => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " F_PACKAGE_OWNER => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PACKAGE_OWNER() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PACKAGE_OWNER());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PACKAGE_OWNER() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PACKAGE_OWNER().equals("null"))) {
                    Assert.assertEquals("The F_PACKAGE_OWNER is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PACKAGE_OWNER(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PACKAGE_OWNER());
                }

                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " F_COMPONENT => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_COMPONENT() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_COMPONENT());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_COMPONENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_COMPONENT().equals("null"))) {
                    Assert.assertEquals("The F_COMPONENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_COMPONENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_COMPONENT());
                }

                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " F_RELATIONSHIP_TYPE => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_RELATIONSHIP_TYPE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIONSHIP_TYPE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIONSHIP_TYPE());
                }

                Log.info("ID => " + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID() +
                        " F_EVENT => EPH=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() +
                        " DL=" + dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_REL_PACK_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the product financial records from EPH of (.*)$")
    public void getProductFinAttrEPH(String tableName) {
        Log.info("We get the Financial Attribute product records from EPH..");
        sql = String.format(productObj.productFinAttrBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the product financial records from DL of (.*)$")
    public void getProductFinAttrDL(String tableName) {
        Log.info("We get the Financial Attribute product records from DL..");
        sql = String.format(productObj.productFinAttrBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product financial records in EPH and DL of (.*)$")
    public void compareGDProductFinAttrDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Financial Attribute product records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_FIN_AATRIBS_ID));
                dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_FIN_AATRIBS_ID));

                if(tableName.equals("gh_product_financial_attribs")){
                    dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_FIN_AATRIBS_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_FIN_AATRIBS_ID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_FIN_AATRIBS_ID());

                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_product_financial_attribs")){
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_UPDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " EFFECTIVE_START_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " EFFECTIVE_END_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_GL_COMPANY => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COMPANY()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COMPANY());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COMPANY() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COMPANY().equals("null"))) {
                    Assert.assertEquals("The F_GL_COMPANY is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COMPANY(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COMPANY());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_GL_COST_RESP_CENTRE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE().equals("null"))) {
                    Assert.assertEquals("The F_GL_COST_RESP_CENTRE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_COST_RESP_CENTRE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_COST_RESP_CENTRE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_GL_REVENUE_RESP_CENTRE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE().equals("null"))) {
                    Assert.assertEquals("The F_GL_REVENUE_RESP_CENTRE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_GL_REVENUE_RESP_CENTRE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_GL_REVENUE_RESP_CENTRE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_PRODUCT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT().equals("null"))) {
                    Assert.assertEquals("The F_PRODUCT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID()+
                        " F_EVENT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_FIN_AATRIBS_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the product identifier records from EPH of (.*)$")
    public void getProductIdentifierEPH(String tableName) {
        Log.info("We get the product identifier records from EPH..");
        sql = String.format(productObj.productIdentifierBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the product identifier records from DL of (.*)$")
    public void getProductIdentifierDL(String tableName) {
        Log.info("We get the product identifier records from DL..");
        sql = String.format(productObj.productIdentifierBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product identifier records in EPH and DL of (.*)$")
    public void compareProductIdentifierDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Identifier records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_IDENTIFIER_ID));
                dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_IDENTIFIER_ID));
                if(tableName.equals("gh_product_identifier")){
                    dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_IDENTIFIER_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_IDENTIFIER_ID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_IDENTIFIER_ID());

                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());


                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_product_identifier")){
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_UPDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " IDENTIFIER => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getIDENTIFIER()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getIDENTIFIER());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getIDENTIFIER() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getIDENTIFIER().equals("null"))) {
                    Assert.assertEquals("The IDENTIFIER is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getIDENTIFIER(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getIDENTIFIER());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " S_IDENTIFIER => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_IDENTIFIER()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_IDENTIFIER());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_IDENTIFIER() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_IDENTIFIER().equals("null"))) {
                    Assert.assertEquals("The S_IDENTIFIER is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getS_IDENTIFIER(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getS_IDENTIFIER());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " EFFECTIVE_START_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " EFFECTIVE_END_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " F_TYPE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_TYPE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_TYPE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_TYPE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " F_PRODUCT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT().equals("null"))) {
                    Assert.assertEquals("The F_PRODUCT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PRODUCT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PRODUCT());
                }
                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID()+
                        " F_EVENT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_IDENTIFIER_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }
            }
        }
    }

    @When("^We get the product relationship records from EPH of (.*)$")
    public void getProductRelationEPH(String tableName) {
        Log.info("We get the product relation records from EPH..");
        sql = String.format(productObj.productRelationshipBuildSql(Constants.EPH_SCHEMA,tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.EPH_URL);
    }

    @Then("^We get the product relationship records from DL of (.*)$")
    public void getProductRelationDL(String tableName) {
        Log.info("We get the product relation records from DL..");
        sql = String.format(productObj.productRelationshipBuildSql(GetDLDBUser.getDataBase(),tableName),Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityDLContext.tbProductDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, ProductDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare product relationship records in EPH and DL of (.*)$")
    public void compareProductRelationDataEPHtoDL(String tableName) {
        if (dataQualityDLContext.tbProductDataObjectsFromDL.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the data to compare the Product Relationship records in EPH and DATA LAKE ..");//sort data in the lists
            for (int i = 0; i < dataQualityDLContext.tbProductDataObjectsFromEPH.size(); i++) {
                dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_RELATIONSHIP_ID));
                dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getPRODUCT_RELATIONSHIP_ID));
                if(tableName.equals("gh_product_relationship")){
                    dataQualityDLContext.tbProductDataObjectsFromEPH.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                    dataQualityDLContext.tbProductDataObjectsFromDL.sort(Comparator.comparing(ProductDataDLObject::getB_FROMBATCHID));
                }

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_RELATIONSHIP_ID().equals("null"))) {  //In data lake null considering or getting as String
                    Assert.assertEquals("The PRODUCT_RELATIONSHIP_ID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getPRODUCT_RELATIONSHIP_ID());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_CLASSNAME => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME().equals("null"))) {
                    Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CLASSNAME());
                }

                if(tableName.equals("gh_product_relationship")){
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                            " B_FROMBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_FROMBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_FROMBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_FROMBATCHID());
                    }

                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                            " B_TOBATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID().equals("null"))) {
                        Assert.assertEquals("The B_TOBATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_TOBATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_TOBATCHID());
                    }

                }else{
                    Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                            " B_BATCHID => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID()+
                            " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());

                    if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID() != null ||
                            (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID().equals("null"))) {
                        Assert.assertEquals("The B_BATCHID is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                                dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_BATCHID(),
                                dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_BATCHID());
                    }
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_CREDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().equals("null"))) {
                    Assert.assertEquals("The B_CREDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_UPDATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().equals("null"))) {
                    Assert.assertEquals("The B_UPDATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDDATE().substring(0, 10),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDDATE().substring(0, 10));
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_CREATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR().equals("null"))) {
                    Assert.assertEquals("The B_CREATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_CREATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_CREATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " B_UPDATOR => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR().equals("null"))) {
                    Assert.assertEquals("The B_UPDATOR is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getB_UPDATOR(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getB_UPDATOR());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " EXTERNAL_REFERENCE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE().equals("null"))) {
                    Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " F_PARENT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PARENT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PARENT());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PARENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PARENT().equals("null"))) {
                    Assert.assertEquals("The F_PARENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_PARENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_PARENT());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " F_CHILD => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_CHILD()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_CHILD());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_CHILD() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_CHILD().equals("null"))) {
                    Assert.assertEquals("The F_CHILD is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_CHILD(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_CHILD());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " EFFECTIVE_START_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_START_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_START_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " EFFECTIVE_END_DATE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE().equals("null"))) {
                    Assert.assertEquals("The EFFECTIVE_END_DATE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getEFFECTIVE_END_DATE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getEFFECTIVE_END_DATE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " F_RELATIOSHIP_TYPE => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIOSHIP_TYPE()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIOSHIP_TYPE());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIOSHIP_TYPE() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIOSHIP_TYPE().equals("null"))) {
                    Assert.assertEquals("The F_RELATIOSHIP_TYPE is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_RELATIOSHIP_TYPE(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_RELATIOSHIP_TYPE());
                }

                Log.info("ID => "+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID()+
                        " F_EVENT => EPH="+dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT()+
                        " DL="+dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());

                if (dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT() != null ||
                        (!dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT().equals("null"))) {
                    Assert.assertEquals("The F_EVENT is incorrect for id=" + dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getPRODUCT_RELATIONSHIP_ID(),
                            dataQualityDLContext.tbProductDataObjectsFromEPH.get(i).getF_EVENT(),
                            dataQualityDLContext.tbProductDataObjectsFromDL.get(i).getF_EVENT());
                }

            }
        }
    }

}





