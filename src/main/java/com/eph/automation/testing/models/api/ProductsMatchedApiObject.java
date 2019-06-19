package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
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
        Assert.assertNotEquals("Verify that more than zero items are returned from the API",0, totalMatchCount);
    }

    public void verifyProductsReturned(int productsInDB){
        Assert.assertEquals("Verify that returned products count match whats in the database", totalMatchCount, productsInDB);
    }

    public void verifyProductWithIdIsReturned(String productID){
        int i=0;
        boolean found=false;
        int bound = items.length;
        while(i<bound&&!found){
            if(items[i].getId().equals(productID)){
                found=true;
                items[i].compareWithDB();
            }
            i++;
        }
        Assert.assertTrue(found);
    }


}