package com.eph.automation.testing.models.api;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
Created by Nishant @ 27 Nov 2019
updated by Nishant @ 04 Feb 2021, EPHD-2747
* */
public class ManifestationProductAPIObject {

    private static List<ProductDataObject> productDataObjectsFromEPHGD;

    private String id;
    public String getId(){return id;}
    public void setId(String id){this.id=id;}

    private ProductSummary productSummary;
    public ProductSummary getProductSummary() {return productSummary;}
    public void setProductSummary(ProductSummary productSummary) {this.productSummary = productSummary;}

    private ManifestationCore manifestationCore;
    public ManifestationCore getManifestationCore() {return manifestationCore;}
    public void setManifestationCore(ManifestationCore manifestationCore) {this.manifestationCore = manifestationCore;}

    private ManifestationWorkApiObject work;
    public ManifestationWorkApiObject getWork() {return work;}
    public void setWork(ManifestationWorkApiObject work) {this.work = work;}
/*
    static class ProductSummary {
        String name;
        public String getName() {return name;}
        public void setName(String name) {this.name = name;}

        HashMap<String, Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        HashMap<String, Object> status;
        public HashMap<String, Object> getStatus() {return status;}
        public void setStatus(HashMap status) {this.status = status;}
    }*/

    private void getProductDataFromEPHGD(String productID) {
        List<String> ids = new ArrayList<>();
        ids.add(productID);
        String sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        productDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    public void compareWithDB(){
        Log.info("verifying product..."+this.id);

        getProductDataFromEPHGD(this.id);
        Assert.assertEquals(this.productSummary.name,productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
        printLog("product name");

        Assert.assertEquals(this.productSummary.type.get("code"),productDataObjectsFromEPHGD.get(0).getF_TYPE());
        printLog("product type");

        Assert.assertEquals(this.productSummary.status.get("code"),productDataObjectsFromEPHGD.get(0).getF_STATUS());
        printLog("product status");
    }

    private void printLog(String verified){Log.info("verified..."+verified);}
}
