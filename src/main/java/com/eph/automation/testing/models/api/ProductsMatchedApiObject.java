package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.helper.Log;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductsMatchedApiObject {

    public ProductsMatchedApiObject() {}

    public int getTotalMatchCount() {return totalMatchCount;}

    public void setTotalMatchCount(int totalMatchCount) {this.totalMatchCount = totalMatchCount;}

    public ProductApiObject[] getItems() {return items;}

    public void setItems(ProductApiObject[] items) {this.items = items;}

    private int totalMatchCount;
    private ProductApiObject[] items;

    public void verifyProductsAreReturned(){Assert.assertNotEquals("Verify more than zero items returned by API",0, totalMatchCount);}

    public void verifyProductsReturned(int productsInDB){Assert.assertEquals("Verify returned products count match with database", totalMatchCount, productsInDB);}

    //created by Nishant @ 8 May 2020 to verify getProductByPersonName returns expected productId (only boolean return)
    //based on this output we will call API again
    public boolean verifyProductWithIdIsReturnedOnly(String productID){
        int i=0;    boolean found=false;
        while(i<items.length&&!found){
            if(items[i].getId().equals(productID)){found=true;}
            i++;}
        return found;
    }

    public boolean verifyProductWithIdIsReturned(String productID){
        int i=0;    boolean found=false;
        int bound = items.length;
        while(i<bound&&!found){
            if(items[i].getId().equals(productID)){
                found=true;
                Log.info("comparing product info with DB..."+productID);
                items[i].compareWithDB();
            }  i++;
        }
        return found;
    }


}