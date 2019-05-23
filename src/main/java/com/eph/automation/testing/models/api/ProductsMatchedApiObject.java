package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductsMatchedApiObject {

    public ProductsMatchedApiObject() {
    }

    public int getTotalMatchCount() {
        return totalMatchCount;
    }

    public void setTotalMatchCount(int totalMatchCount) {
        this.totalMatchCount = totalMatchCount;
    }

    public ProductApiObject[] getItems() {
        return items;
    }

    public void setItems(ProductApiObject[] items) {
        this.items = items;
    }

    int totalMatchCount;
    ProductApiObject[] items;

    public void verifyProductsAreReturned(){
        Assert.assertNotEquals(0, totalMatchCount);
    }

    public void verifyProductsReturned(int productsInDB){
        Assert.assertEquals(totalMatchCount, productsInDB);
    }

    public void verifyProductWithIdIsReturned(String productID){
        int i=0;
        boolean found=false;
        int bound = items.length;
        while(i<bound&&!found){
            if(items[i].getId().equals(productID)){
                found=true;
            }
            i++;
        }
        Assert.assertTrue(found);
    }


}