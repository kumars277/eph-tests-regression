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
* */
public class ManifestationProductAPIObject {

//    public ManifestationProductAPIObject() {}

    private static List<ProductDataObject> productDataObjectsFromEPHGD;

    private String id;
    public String getId(){return id;}
    public void setId(String id){this.id=id;}

    private ProductSummary productSummary;
    public ProductSummary getProductSummary() {return productSummary;}
    public void setProductSummary(ProductSummary productSummary) {
        this.productSummary = productSummary;
    }

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
    }

    private void getProductDataFromEPHGD(String productID) {
        List<String> ids = new ArrayList<>();
        ids.add(productID);
        String sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        productDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    public void compareWithDB(){
        Log.info("comparing product..."+this.id);
        Log.info( "\n-product name\n"+
                "-product type\n"+
                "-product status");
        getProductDataFromEPHGD(this.id);
        Assert.assertEquals(this.productSummary.name,productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
        Assert.assertEquals(this.productSummary.type.get("code"),productDataObjectsFromEPHGD.get(0).getF_TYPE());
        Assert.assertEquals(this.productSummary.status.get("code"),productDataObjectsFromEPHGD.get(0).getF_STATUS());
    }


}
