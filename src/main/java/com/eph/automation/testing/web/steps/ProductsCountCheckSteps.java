package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.ProductsCountContext;
import com.eph.automation.testing.models.dao.ProductCountObject;
import com.eph.automation.testing.services.db.sql.ProductCountSQL;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

public class ProductsCountCheckSteps {
    @StaticInjection
    public ProductsCountContext productsCountContext;
    private String sql;
    private static int stgToSA;

    @Given("^We know the products count in PMX$")
    public void getPmxProducts() {
        sql= ProductCountSQL.PMX_PRODUCT_Count;
        System.out.println(sql);
        productsCountContext.productCountPMX= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.PMX_SIT_URL);
        System.out.println("\nThe number of products in PMX is: " + productsCountContext.productCountPMX.get(0).pmxCount);
    }

    @When("^The products are in PMX Staging$")
    public void getStgProducts(){
        sql= ProductCountSQL.EPH_STG_PRODUCT_Count;
        productsCountContext.productCountStg= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);
        System.out.println("\nThe number of products in Staging is: " + productsCountContext.productCountStg.get(0).stgCount);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_BOOKS;
        productsCountContext.productCountStgBooks= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_Sub;
        productsCountContext.productCountStgSubscription= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_Bulk;
        productsCountContext.productCountStgBulk= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_Reprint;
        productsCountContext.productCountStgReprints= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_Back;
        productsCountContext.productCountStgBack= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_OA;
        productsCountContext.productCountStgOA= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_OA_More;
        productsCountContext.productCountStgOAMore= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_AC;
        productsCountContext.productCountStgAC=DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        sql= ProductCountSQL.EPH_STG_PRODUCT_Count_AC_More;
        productsCountContext.productCountStgACMore= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);

        stgToSA =
                productsCountContext.productCountStgBooks.get(0).booksCount +
                productsCountContext.productCountStgSubscription.get(0).subCount +
                productsCountContext.productCountStgBulk.get(0).bulkCount +
                productsCountContext.productCountStgReprints.get(0).reprintCount +
                productsCountContext.productCountStgBack.get(0).backCount +
                productsCountContext.productCountStgOA.get(0).oaCount +
                productsCountContext.productCountStgOAMore.get(0).oaMoreCount +
                productsCountContext.productCountStgAC.get(0).acCount +
                productsCountContext.productCountStgACMore.get(0).acMoreCount;

        System.out.println("\nThe number of products in Staging for SA compare is: " + stgToSA);

    }

    @When("^We know the number of Products in EPH$")
    public void getEPHProducts(){
        sql= ProductCountSQL.EPH_SA_PRODUCT_Count;
        productsCountContext.productCountEPHSA= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);
        System.out.println("\nThe number of products in EPH SA is: " + productsCountContext.productCountEPHSA.get(0).ephSACount);

        sql= ProductCountSQL.EPH_GD_PRODUCT_Count;
        productsCountContext.productCountEPHGD= DBManager.getDBResultAsBeanList(sql, ProductCountObject.class, Constants.EPH_SIT_URL);
        System.out.println("\nThe number of products in EPH GD is: " + productsCountContext.productCountEPHGD.get(0).ephGDCount);
    }

    @Then("^The number of products between (.*) and (.*) is equal$")
    public void comparePMXtoEPHCount(String source, String target){
        if (source.contentEquals("PMX")) {
            Assert.assertEquals("The number of works in PMX and PMX Staging is not equal!", productsCountContext.productCountPMX.get(0).pmxCount,
                    productsCountContext.productCountStg.get(0).stgCount);
           }else if (target.contentEquals("SA")){
            Assert.assertEquals("\nThe number of works in PMX Staging and EPH SA is not equal!", stgToSA,
                    productsCountContext.productCountEPHSA.get(0).ephSACount);
        } else {
            Assert.assertEquals("\nThe number of works in SA and GD is not equal!", productsCountContext.productCountEPHSA.get(0).ephSACount,
                    productsCountContext.productCountEPHGD.get(0).ephGDCount);
        }
    }
}
