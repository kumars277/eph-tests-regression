package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.helper.Log;
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
        Assert.assertTrue("work with person id found",found);
    }


    public void verifyEnddatedPerson(String searchedPerson){//created by Nishant @ 13 Jul 2020
        int i=0;
        boolean found=false;
        int activePerson=0;
        if(items!=null)
        {
        while(i<items.length&&!found){
          PersonsApiObject[] persons=  items[i].getWorkCore().getWorkPersons().clone();
           for(PersonsApiObject person:persons)
           {
              if(searchedPerson.contains(person.getPerson().get("firstName").toString())) {
                  if(person.getEffectiveEndDate()!=null) {
                      found = true;
                      Log.info("Enddated person found for work id " + items[i].getId());
                      Log.info("end dated person id = " + person.getId());
                  }
              }
               if(person.getEffectiveEndDate()==null) {activePerson+=1;            }
           }

         //  WorkExtendedPersons[] extendedPerson=items[i].getWorkExtended().getWorkExtendedPersons().clone();
         //  for(WorkExtendedPersons extPerson:extendedPerson) { if(searchedPerson.contains(extPerson.getExtendedPerson().get("firstName").toString())) {}        }
            i++;
        }
        //activePerson+=extendedPerson.length;
        Assert.assertFalse(found);
    }
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