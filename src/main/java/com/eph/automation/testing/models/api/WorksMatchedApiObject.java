package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorksMatchedApiObject {

    public WorksMatchedApiObject() {
    }

    public int getTotalMatchCount() {
        return totalMatchCount;
    }

    public void setTotalMatchCount(int totalMatchCount) {
        this.totalMatchCount = totalMatchCount;
    }

    public WorkApiObject[] getItems() {
        return items;
    }

    public void setItems(WorkApiObject[] items) {
        this.items = items;
    }

    int totalMatchCount;
    WorkApiObject[] items;

    public void verifyWorksAreReturned(){
        Assert.assertNotEquals(0, totalMatchCount);
    }

    public void verifyWorksReturned(int worksInDB){
        Assert.assertEquals(totalMatchCount, worksInDB);
    }

    public void verifyWorkWithIdIsReturned(String workID){
        int i=0;
        boolean found=false;
        while(i<items.length&&!found){
            if(items[i].getId().equals(workID)){
                found=true;
                items[i].compareWithDB();
            }
            i++;
        }
        Assert.assertTrue(found);
    }

}