package com.eph.automation.testing.models.api;
/** Created by GVLAYKOV */
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.text.ParseException;
import java.util.regex.Pattern;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductsMatchedApiObject {

  public ProductsMatchedApiObject() {}

  private int totalMatchCount;
  public int getTotalMatchCount() {return totalMatchCount;}
  public void setTotalMatchCount(int totalMatchCount) {this.totalMatchCount = totalMatchCount;}

  int pageableMatchCount;
  public int getPageableMatchCount() {return pageableMatchCount;}
  public void setPageableMatchCount(int pageableMatchCount) {this.pageableMatchCount = pageableMatchCount;}

  String scrollId;
  public void setScrollId(String scrollId) {this.scrollId = scrollId;}
  public String getScrollId() {return scrollId;}

  private ProductApiObject[] items;
  public ProductApiObject[] getItems() {return items;}
  public void setItems(ProductApiObject[] items) {this.items = items;}

  public void verifyProductsAreReturned() {
    Assert.assertNotEquals("Verify more than zero items returned by API", 0, totalMatchCount);
  }

  public void verifyNoProductReturned() {
    Assert.assertEquals("Verify zero items returned by API", 0, totalMatchCount);
  }

  public void verifyAPIReturnedProductsCount(int productsInDB) {
    Assert.assertEquals(
        DataQualityContext.breadcrumbMessage
            + " Verify returned products count match with database",
        totalMatchCount,
        productsInDB);
  }

  // created by Nishant @ 8 May 2020 to verify getProductByPersonName returns expected productId
  // (only boolean return)
  // based on this output we will call API again
  public boolean verifyProductWithIdIsReturnedOnly(String productID) {
    int i = 0;
    boolean found = false;
    while (i < items.length && !found) {
      if (items[i].getId().equals(productID)) {
        found = true;
      }
      i++;
    }
    return found;
  }

  public boolean verifyProductWithIdIsReturned(String productID) throws ParseException {
    int i = 0;
    boolean found = false;
    int bound = items.length;
    while (i < bound && !found) {
      if (items[i].getId().equals(productID)) {
        found = true;
        Log.info("comparing product info with DB..." + productID);
        items[i].compareWithDB();
      }
      i++;
    }
    Assert.assertTrue(DataQualityContext.breadcrumbMessage + "id not found in response", found);
    return found;
  }

  public boolean verifyAllTitleContainsSearchKey(String searchKey) {
    // created by Nishant @ 04 June 2021
    int i = 0;
    boolean notFound = false;
    int bound = items.length;
    while (i < bound && !notFound) {
      if (!Pattern.compile(Pattern.quote(searchKey), Pattern.CASE_INSENSITIVE)
          .matcher(items[i].getProductCore().getName())
          .find())
      {
        notFound = true;
          Log.info(items[i].getId()+ " at counter "+ i+ " - title does not include search string -"
                  + searchKey +", title - "+items[i].getProductCore().getName());

          //check if manifestation title contains search key
          if (items[i].getManifestation() != null) {
            if (Pattern.compile(Pattern.quote(searchKey), Pattern.CASE_INSENSITIVE)
                    .matcher(items[i].getManifestation().getManifestationCore().getKeyTitle()).find()) {
              Log.info(items[i].getManifestation().getId()+ " at counter "+ i+ " - manifestation title includes search string -"
                      + searchKey +", title - "+items[i].getManifestation().getManifestationCore().getKeyTitle());
              notFound = false;
            }
          }
          //check product work title
          if(notFound){
            if (items[i].getWork() != null) {  Log.info(items[i].getWork().getWorkCore().getTitle().toString());}
          }

          Assert.assertFalse(DataQualityContext.breadcrumbMessage + " product/manifestation title does not contain searchKey "+ searchKey, notFound);
      }
      i++;
    }

    return notFound;
  }
}
