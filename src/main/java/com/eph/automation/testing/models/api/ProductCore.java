package com.eph.automation.testing.models.api;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/*
* created by Nishant @ 8 May 2020
* */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductCore
{
    private static List<ProductDataObject> productDataObjectsFromEPHGD;

    private String name;
    public String getName() {return name;}
    public void setName(String name) {this.name = name;}

    private String shortName;
    public String getShortName() {return shortName;}
    public void setShortName(String shortName) {this.shortName = shortName;}

    private Boolean separatelySaleableInd;
    public Boolean getSeparatelySaleableInd() {return separatelySaleableInd;}
    public void setSeparatelySaleableInd(Boolean separatelySaleableInd) {this.separatelySaleableInd = separatelySaleableInd;}

    private Boolean trialAllowedInd;
    public Boolean getTrialAllowedInd() {return trialAllowedInd;}
    public void setTrialAllowedInd(Boolean trialAllowedInd) {this.trialAllowedInd = trialAllowedInd;}

    private String launchDate;
    public String getLaunchDate() {return launchDate;}
    public void setLaunchDate(String launchDate) {this.launchDate = launchDate;}

    private HashMap<String,Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private HashMap<String,Object> status;
    public HashMap<String, Object> getStatus() {return status;}
    public void setStatus(HashMap<String, Object> status) {this.status = status;}

    private HashMap<String,Object> revenueModel;
    public HashMap<String, Object> getRevenueModel() {return revenueModel;}
    public void setRevenueModel(HashMap<String, Object> revenueModel) {this.revenueModel = revenueModel;}

    private HashMap<String,Object> etaxProductCode;
    public HashMap<String, Object> getEtaxProductCode() {return etaxProductCode;}
    public void setEtaxProductCode(HashMap<String, Object> etaxProductCode) {this.etaxProductCode = etaxProductCode;}

    private PersonsApiObject[] productPersons;
    public PersonsApiObject[] getProductPersons() {return productPersons;}
    public void setProductPersons(PersonsApiObject[] productPersons) {this.productPersons = productPersons;}


    public void compareWithDB(String productId){
        getProductDataFromEPHGD(productId);
        Log.info("verifying productCode data ..."+productId);
    //    Log.info("\n-name\n-shortName\n-separatelySaleableInd\n-trialAllowedInd\n-launchDate\n-product type code\n-product status code\n" +
    //            "-revenueModel code\n-etaxProductCode code\n");
        Assert.assertEquals(name, this.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
      printLog("name");
        Assert.assertEquals(shortName, this.productDataObjectsFromEPHGD.get(0).getPRODUCT_SHORT_NAME());
        printLog("shortName");
        if(separatelySaleableInd!=null){
        Assert.assertEquals(separatelySaleableInd,this.productDataObjectsFromEPHGD.get(0).getBoolSEPARATELY_SALEABLE_IND());
        printLog("separatelySaleableInd");}
        if(trialAllowedInd!=null) {
        Assert.assertEquals(trialAllowedInd, this.productDataObjectsFromEPHGD.get(0).getBoolTRIAL_ALLOWED_IND());
        printLog("trialAllowedInd");
        }
        Assert.assertEquals(launchDate, this.productDataObjectsFromEPHGD.get(0).getFIRST_PUB_DATE());
        printLog("launchDate");

        Assert.assertEquals(type.get("code"), this.productDataObjectsFromEPHGD.get(0).getF_TYPE());
        printLog("product type");

        Assert.assertEquals(status.get("code"), this.productDataObjectsFromEPHGD.get(0).getF_STATUS());
        printLog("product status");

        if(!(revenueModel==null && this.productDataObjectsFromEPHGD.get(0).getF_REVENUE_MODEL()==null)) {
            Assert.assertEquals(revenueModel.get("code"), this.productDataObjectsFromEPHGD.get(0).getF_REVENUE_MODEL());
        printLog("revenueModel");}
        else Log.info("revenueModel is null for the product");
        if(!(etaxProductCode==null)&&!(this.productDataObjectsFromEPHGD.get(0).getTAX_CODE()==null)) {
            Assert.assertEquals(etaxProductCode.get("code"), this.productDataObjectsFromEPHGD.get(0).getTAX_CODE());
            printLog("etaxProductCode");
        }
        else Log.info("etaxProductCode is null for the product");

        //validation for packages - EPR-11BBFJ, EPR-11BBFK

        if(!(productPersons==null)&&!(productPersons.length==0))  //for product person - EPR-11119M
        {for (PersonsApiObject person : productPersons) {person.compareWithDB_product();}}

    }

    private void getProductDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        productDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    private void printLog(String verified){Log.info("verified..."+verified);}
}

