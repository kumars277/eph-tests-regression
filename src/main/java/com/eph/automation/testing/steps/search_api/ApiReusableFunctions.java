package com.eph.automation.testing.steps.search_api;
//created by Nishant @ 07 Jan 2022
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.services.api.APIService;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import org.junit.Assert;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;

import static com.eph.automation.testing.models.contexts.DataQualityContext.getBreadcrumbMessage;

public class ApiReusableFunctions {

    static String from = "&from=";
    static String size = "&size=";

    public WorksMatchedApiObject workByTitle_Iterative(String title, int i) {
        //created by Nishant @ 5 Jan 2022
        int fromCntr = 0;
        int sizeCntr = 500;
        WorksMatchedApiObject returnedWorks = null;
        try {
            returnedWorks = APIService.getWorkByTitle(title+ from + fromCntr + size + sizeCntr);

            Log.info("Total work found for title... - "+ returnedWorks.getTotalMatchCount());
            while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID()) &&
                    fromCntr + sizeCntr < returnedWorks.getTotalMatchCount()) {
                fromCntr += sizeCntr;
                Log.info("scanned productID from record " + (fromCntr - sizeCntr) + " to " + fromCntr);
                returnedWorks =APIService.getWorkByTitle(title+ from + fromCntr + size + sizeCntr);
            }
        } catch (AzureOauthTokenFetchingException e) {
            e.printStackTrace();
        }
        return returnedWorks;
    }

    public WorksMatchedApiObject workBySeachOption_Iterative(String searcOption,int i,boolean resultOnTop){
        //created by Nishant @ 5 Jan 2022
        /*api returns sometimes 70000+ records and most probable intended work id does not appear in first 20 records
        hence we need to send API request with size 500 and check if intended workID is returned
        if not, send request again for next 500 records*/
        WorksMatchedApiObject returnedWorks = null;
        try {
            if(resultOnTop){
                returnedWorks =APIService.getWorksBySearchOption(searcOption+ from+ 0+ size+ 20);
            }
            else{
                int fromCntr = 0;
                int sizeCntr = 500;
                returnedWorks =APIService.getWorksBySearchOption(searcOption+ from+ fromCntr+ size+ sizeCntr);
                Log.info("Total work found... - " + returnedWorks.getTotalMatchCount());
                while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID())
                        && fromCntr + sizeCntr < returnedWorks.getTotalMatchCount()
                        &&(fromCntr+sizeCntr)<10000) {

                    fromCntr += sizeCntr;
                    Log.info("scanned workID from " + (fromCntr - sizeCntr) + " to " + fromCntr + " records...");
                    returnedWorks =APIService.getWorksBySearchOption(searcOption+ from+ fromCntr+ size+ sizeCntr);
                }
            }
        } catch (AzureOauthTokenFetchingException e) {
            e.printStackTrace();
        }
        return returnedWorks;
    }

    public WorksMatchedApiObject workByParam_Iterative(String queryType,String queryValue,int i){
        //created by Nishant @ 19 Jan 2022
        int fromCntr = 0;
        int sizeCntr = 500;
        WorksMatchedApiObject returnedWorks = null;
        try {
            returnedWorks = APIService.getWorksByParam(queryType,queryValue+ from + fromCntr + size + sizeCntr);

            Log.info("Total work found by "+queryType+" - "+ returnedWorks.getTotalMatchCount());
      if (returnedWorks.getTotalMatchCount() > 0) {
        while (!returnedWorks.verifyWorkWithIdIsReturnedOnly(
                DataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID())
            && fromCntr + sizeCntr < returnedWorks.getTotalMatchCount()) {
          fromCntr += sizeCntr;
          Log.info("scanned productID from record " + (fromCntr - sizeCntr) + " to " + fromCntr);
          returnedWorks =  APIService.getWorksByParam(queryType, queryValue + from + fromCntr + size + sizeCntr);
        }
            }
        } catch (AzureOauthTokenFetchingException e) {
             Assert.assertFalse(getBreadcrumbMessage()+"API error",true );
            e.printStackTrace();
        }

        return returnedWorks;
    }

    public static String getSearchKeyword(String title)
    {
        //created by Nishant @ 28 Feb 2022
        //updated by Nishant @ 12 May 2022
        List<String> ignoreKeywords = Arrays.asList("EDITION","THE","NET","III","SCIENTIFIC","PART");
        String keyword = "";
        //by nishant @ 08 Jul 2022 to fix mismatch with DB
        //translate all accents characters in normal characters
        String normalisedTitle = Normalizer.normalize(title, Normalizer.Form.NFD);
        normalisedTitle = normalisedTitle.replaceAll("[^\\p{ASCII}]", "");
        String[] arr_title= normalisedTitle.replaceAll("[^a-zA-Z0-9]", " ").split(" ");

        for(int i=0;i<arr_title.length;i++)
        {
            try{
                int tmp = Integer.parseInt(arr_title[i]);
                continue;
            }
            catch (NumberFormatException nfe)
            {
                if(!ignoreKeywords.contains(arr_title[i].toUpperCase()))
                {
                    keyword=arr_title[i];

                    /*ngram filter in API splits the title into all the combinations
        of consecutive characters between 3 and 50 characters long.
        Because the minimum length is 3, CD isn’t a valid match.
        hence valid keyword should be more than 2 characters*/
                    if(keyword.length()<4)continue;
                    break;
                }
            }
        }


        return keyword;
    }

}
