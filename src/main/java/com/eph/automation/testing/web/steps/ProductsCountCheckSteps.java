package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.ProductsCountContext;
import com.eph.automation.testing.models.dao.ProductCountObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.ProductCountSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import com.eph.automation.testing.helper.Log;

import java.util.List;

public class ProductsCountCheckSteps {
    @StaticInjection
    public ProductsCountContext productsCountContext;
    private String sql;
    private static int stgToCanonical;
    private static List<WorkDataObject> refreshDate;

    @Given("^We know the products count in PMX$")
    public void getPmxProducts() {
        sql= ProductCountSQL.PMX_PRODUCT_Count;
        Log.info(sql);
        productsCountContext.productCountPMX= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.PMX_URL);
        Log.info("\nThe number of products in PMX is: " + productsCountContext.productCountPMX.get(0).pmxCount);
    }

    @When("^The products are in PMX Staging$")
    public void getStgProducts(){

        if (System.getProperty("LOAD") != null) {
            if(System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = ProductCountSQL.EPH_STG_PRODUCT_Count;
                productsCountContext.productCountStg = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
                Log.info("\nThe number of products in Staging is: " + productsCountContext.productCountStg.get(0).stgCount);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_BOOKS;
                productsCountContext.productCountStgBooks = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Sub;
                productsCountContext.productCountStgSubscription = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Bulk;
                productsCountContext.productCountStgBulk = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Reprint;
                productsCountContext.productCountStgReprints = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Back;
                productsCountContext.productCountStgBack = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_OA;
                productsCountContext.productCountStgOA = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_OA_More;
                productsCountContext.productCountStgOAMore = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_AC;
                productsCountContext.productCountStgAC = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_AC_More;
                productsCountContext.productCountStgACMore = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Packages;
                productsCountContext.productCountStgPackages = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                stgToCanonical =
                        productsCountContext.productCountStgBooks.get(0).booksCount +
                                productsCountContext.productCountStgSubscription.get(0).subCount +
                                productsCountContext.productCountStgBulk.get(0).bulkCount +
                                productsCountContext.productCountStgReprints.get(0).reprintCount +
                                productsCountContext.productCountStgBack.get(0).backCount +
                                productsCountContext.productCountStgOA.get(0).oaCount +
                                productsCountContext.productCountStgOAMore.get(0).oaMoreCount +
                                productsCountContext.productCountStgAC.get(0).acCount +
                                productsCountContext.productCountStgACMore.get(0).acMoreCount +
                                productsCountContext.productCountStgPackages.get(0).packagesCount;

                Log.info("\nThe number of products in Staging for Canonical compare is: " + stgToCanonical);
            }else{
                sql = WorkCountSQL.GET_REFRESH_DATE;
                refreshDate =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                        Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count;
                productsCountContext.productCountStg = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
                Log.info("\nThe number of products in Staging is: " + productsCountContext.productCountStg.get(0).stgCount);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                Log.info(sql);
                productsCountContext.productCountStgDelta = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
                Log.info("\nThe number of products in Staging is: " + productsCountContext.productCountStg.get(0).stgCount);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_BOOKS_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                Log.info(sql);
                productsCountContext.productCountStgBooks = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Sub_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                Log.info(sql);
                productsCountContext.productCountStgSubscription = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Bulk_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                Log.info(sql);
                productsCountContext.productCountStgBulk = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Reprint_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                Log.info(sql);
                productsCountContext.productCountStgReprints = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Back_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                productsCountContext.productCountStgBack = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_OA_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                Log.info(sql);
                productsCountContext.productCountStgOA = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_OA_More_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                productsCountContext.productCountStgOAMore = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_AC_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                productsCountContext.productCountStgAC = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Count_AC_More_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                productsCountContext.productCountStgACMore = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                sql = ProductCountSQL.EPH_STG_PRODUCT_Packages_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                productsCountContext.productCountStgPackages = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

                stgToCanonical =
                        productsCountContext.productCountStgBooks.get(0).booksCount +
                                productsCountContext.productCountStgSubscription.get(0).subCount +
                                productsCountContext.productCountStgBulk.get(0).bulkCount +
                                productsCountContext.productCountStgReprints.get(0).reprintCount +
                                productsCountContext.productCountStgBack.get(0).backCount +
                                productsCountContext.productCountStgOA.get(0).oaCount +
                                productsCountContext.productCountStgOAMore.get(0).oaMoreCount +
                                productsCountContext.productCountStgAC.get(0).acCount +
                                productsCountContext.productCountStgACMore.get(0).acMoreCount +
                                productsCountContext.productCountStgPackages.get(0).packagesCount;

                Log.info("\nThe number of products in Staging for Canonical compare is: " + stgToCanonical);
            }
        }else {
/*            sql = WorkCountSQL.GET_REFRESH_DATE;
            refreshDate =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                    Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count;
            productsCountContext.productCountStg = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
            Log.info("\nThe number of products in Staging is: " + productsCountContext.productCountStg.get(0).stgCount);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgDelta = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
            Log.info("\nThe number of products in Staging is: " + productsCountContext.productCountStg.get(0).stgCount);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_BOOKS_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgBooks = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Sub_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgSubscription = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Bulk_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgBulk = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Reprint_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgReprints = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Back_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgBack = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_OA_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgOA = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_OA_More_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgOAMore = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_AC_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgAC = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_AC_More_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgACMore = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Packages_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            productsCountContext.productCountStgPackages = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            stgToCanonical =
                    productsCountContext.productCountStgBooks.get(0).booksCount +
                            productsCountContext.productCountStgSubscription.get(0).subCount +
                            productsCountContext.productCountStgBulk.get(0).bulkCount +
                            productsCountContext.productCountStgReprints.get(0).reprintCount +
                            productsCountContext.productCountStgBack.get(0).backCount +
                            productsCountContext.productCountStgOA.get(0).oaCount +
                            productsCountContext.productCountStgOAMore.get(0).oaMoreCount +
                            productsCountContext.productCountStgAC.get(0).acCount +
                            productsCountContext.productCountStgACMore.get(0).acMoreCount +
                            productsCountContext.productCountStgPackages.get(0).packagesCount;

            Log.info("\nThe number of products in Staging for Canonical compare is: " + stgToCanonical);*/
            sql = ProductCountSQL.EPH_STG_PRODUCT_Count;
            productsCountContext.productCountStg = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
            Log.info("\nThe number of products in Staging is: " + productsCountContext.productCountStg.get(0).stgCount);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_BOOKS;
            productsCountContext.productCountStgBooks = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Sub;
            productsCountContext.productCountStgSubscription = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Bulk;
            productsCountContext.productCountStgBulk = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Reprint;
            productsCountContext.productCountStgReprints = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_Back;
            productsCountContext.productCountStgBack = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_OA;
            productsCountContext.productCountStgOA = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_OA_More;
            productsCountContext.productCountStgOAMore = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_AC;
            productsCountContext.productCountStgAC = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Count_AC_More;
            productsCountContext.productCountStgACMore = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            sql = ProductCountSQL.EPH_STG_PRODUCT_Packages;
            productsCountContext.productCountStgPackages = DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);

            stgToCanonical =
                    productsCountContext.productCountStgBooks.get(0).booksCount +
                            productsCountContext.productCountStgSubscription.get(0).subCount +
                            productsCountContext.productCountStgBulk.get(0).bulkCount +
                            productsCountContext.productCountStgReprints.get(0).reprintCount +
                            productsCountContext.productCountStgBack.get(0).backCount +
                            productsCountContext.productCountStgOA.get(0).oaCount +
                            productsCountContext.productCountStgOAMore.get(0).oaMoreCount +
                            productsCountContext.productCountStgAC.get(0).acCount +
                            productsCountContext.productCountStgACMore.get(0).acMoreCount +
                            productsCountContext.productCountStgPackages.get(0).packagesCount;

            Log.info("\nThe number of products in Staging for Canonical compare is: " + stgToCanonical);
        }

    }

    @When("^We know the number of Products in EPH$")
    public void getEPHProducts(){
        sql= ProductCountSQL.EPH_SA_PRODUCT_Count;
        productsCountContext.productCountEPHSA= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
        Log.info("\nThe number of products in EPH SA is: " + productsCountContext.productCountEPHSA.get(0).ephSACount);

        sql= ProductCountSQL.EPH_GD_PRODUCT_Count;
        productsCountContext.productCountEPHGD= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
        Log.info("\nThe number of products in EPH GD is: " + productsCountContext.productCountEPHGD.get(0).ephGDCount);

        sql= ProductCountSQL.EPH_AE_PRODUCT_Count;
        productsCountContext.productCountEPHAE= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
        Log.info("\nThe number of products in EPH AE is: " + productsCountContext.productCountEPHAE.get(0).aeCount);
    }

    @When("^We know the number of products in EPH DQ$")
    public void getCanonicalCount(){
        sql= ProductCountSQL.EPH_STG_CAN_Count;
        productsCountContext.productCountStgCan= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
        Log.info("\nThe number of products in Canonical is: " + productsCountContext.productCountStgCan.get(0).ephCanCount);

        sql= ProductCountSQL.EPH_STG_CAN_DQ_Count;
        productsCountContext.productCountStgCanDQ= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_URL);
        Log.info("\nThe number of products in Canonical with DQ is: " + productsCountContext.productCountStgCanDQ.get(0).ephCanDqCount);
    }

    @Then("^The number of products between (.*) and (.*) is equal$")
    public void comparePMXtoEPHCount(String source, String target){
        if (source.contentEquals("PMX")) {
            Assert.assertEquals("The number of products in PMX and PMX Staging is not equal!", productsCountContext.productCountPMX.get(0).pmxCount,
                    productsCountContext.productCountStg.get(0).stgCount);
           }else if (target.contentEquals("SA")){
            Assert.assertEquals("\nThe number of products in EPH DQ and EPH SA is not equal!", productsCountContext.productCountStgCanDQ.get(0).ephCanDqCount,
                    productsCountContext.productCountEPHSA.get(0).ephSACount);
        } else if (target.contentEquals("GD")){
            Assert.assertEquals("\nThe number of products in SA and GD is not equal!", productsCountContext.productCountEPHSA.get(0).ephSACount,
                    productsCountContext.productCountEPHGD.get(0).ephGDCount);
        }else if (target.contentEquals("EPH DQ")){
            Assert.assertEquals("\nThe number of products in Staging and EPH DQ is not equal!", stgToCanonical,
                    productsCountContext.productCountStgCan.get(0).ephCanCount);
        } else {
            Assert.assertEquals("\nThe number of products in SA and GD with AE is not equal!", productsCountContext.productCountEPHSA.get(0).ephSACount,
                    productsCountContext.productCountEPHGD.get(0).ephGDCount + productsCountContext.productCountEPHAE.get(0).aeCount);
        }
    }
}
