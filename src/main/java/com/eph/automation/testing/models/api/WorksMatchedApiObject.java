package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Verify;
import org.junit.Assert;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorksMatchedApiObject {

    public WorksMatchedApiObject() {}

    int totalMatchCount;
    public int getTotalMatchCount() {return totalMatchCount;}
    public void setTotalMatchCount(int totalMatchCount) {this.totalMatchCount = totalMatchCount;}

    WorkApiObject[] items;
    public WorkApiObject[] getItems() {return items;}
    public void setItems(WorkApiObject[] items) {this.items = items;}

    public void verifyWorksAreReturned(){Assert.assertNotEquals("Verify more than 0 items returned from API", 0, totalMatchCount);}

    public void verifyWorksReturnedCount(int worksInDB){
        //API count could be higher than DB count as it comes from Elastic search
        Verify.verify(totalMatchCount>= worksInDB,"API count less than DB count");}

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

    //created by Nishant @ 24 Apr 2020 to verify getWorkByPersonName returns expected workId (only boolean return)
    //based on this output we will call API again
    public boolean verifyWorkWithIdIsReturnedOnly(String workID){
        int i=0;
        boolean found=false;
        while(i<items.length&&!found){
            if(items[i].getId().equals(workID))
                found=true;
            i++;
        }
        return found;
    }

}