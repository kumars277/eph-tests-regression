package com.eph.automation.testing.models.api;
//created by Nishant @ 18 May 2021, EPHD-3122

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.eph.automation.testing.models.contexts.DataQualityContext.getBreadcrumbMessage;


public class WorkLegalOwners {

    List<Map<String,Object>> workLegalOwners;
    private LegalOwner legalOwner;
    public LegalOwner getLegalOwner() {return legalOwner;}
    public void setLegalOwner(LegalOwner legalOwner) {this.legalOwner = legalOwner;}

    private HashMap<String, Object> ownershipDescription;
    public HashMap<String, Object> getOwnershipDescription() {return ownershipDescription;}
    public void setOwnershipDescription(HashMap<String, Object> ownershipDescription) {this.ownershipDescription = ownershipDescription;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

    private String effectiveEndDate;
    public String getEffectiveEndDate() {return effectiveEndDate;}
    public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}

    public class LegalOwner{
        private String name;
        public String getName() {return name;}
        public void setName(String name) {this.name = name;}

        private HashMap<String, Object> legalOwnership;
        public HashMap<String, Object> getLegalOwnership() {return legalOwnership;}
        public void setLegalOwnership(HashMap<String, Object> legalOwnership) {this.legalOwnership = legalOwnership;}
    }

    public void compareWithDB(String workId)
    {
        boolean legalOwnerfound = false;

        getWorkLegalOwnersById(workId);
        for(int i=0;i<workLegalOwners.size();i++)
        {

            String[] legalOwnerValue = getLegalOwnerValues(workLegalOwners.get(i).get("f_legal_owner").toString());

            if(ownershipDescription.get("code").equals(workLegalOwners.get(i).get("f_ownership_description"))&
                    legalOwner.name.equalsIgnoreCase(legalOwnerValue[0]))
            {
                legalOwnerfound = true;

                Log.info("ownershipDescription code - " + ownershipDescription.get("code"));
                printLog("legalOwner name - "+legalOwner.name);

                Assert.assertEquals(getBreadcrumbMessage()+ " - ownershipDescription",ownershipDescription.get("name"),getownershipDescriptionName(workLegalOwners.get(i).get("f_ownership_description").toString()));
                printLog("ownershipDescription name");

                    if (effectiveStartDate != null)
                    {
                        Assert.assertEquals(getBreadcrumbMessage() + " - ownership effectiveStartDate",effectiveStartDate,workLegalOwners.get(i).get("effective_start_date").toString());
                printLog("effectiveStartDate");
                    }
                if(effectiveEndDate!=null)
                {
                    Log.info("workLegalOwners effectiveStartDate present");
                    Assert.assertEquals(getBreadcrumbMessage()+" - ownership effectiveStartDate",effectiveEndDate,workLegalOwners.get(i).get("effective_end_date").toString());
                    printLog("effectiveEndDate");

                }

             //   String[] legalOwnerValue = getLegalOwnerValues(workLegalOwners.get(i).get("f_legal_owner").toString());
            //    Assert.assertEquals(workId + " - legalOwner",legalOwner.name,legalOwnerValue[0]);


                Assert.assertEquals(getBreadcrumbMessage()+" - legalOwnership",legalOwner.legalOwnership.get("code"),legalOwnerValue[1]);
                printLog("legalOwnership code");

                String[] legalOwnershipValue = getLegalOwnershipValues(legalOwner.legalOwnership.get("code").toString());

                Assert.assertEquals(getBreadcrumbMessage() + " - legalOwnership",legalOwner.legalOwnership.get("name"),legalOwnershipValue[0]);
                printLog("legalOwnership name");

                Assert.assertEquals(getBreadcrumbMessage() + " - ",legalOwner.legalOwnership.get("ownershipRollUp"),legalOwnershipValue[1]);
                printLog("legalOwnership ownershiRollUp");

                break;
            }
        }
        Assert.assertTrue(getBreadcrumbMessage()+ " LegalOwnerFound - "+ownershipDescription.get("code"),legalOwnerfound);

    }

    private List<Map<String,Object>> getWorkLegalOwnersById(String workId){
        String sql = APIDataSQL.GET_GD_DATA_WORKLEGELOWNDER_BY_WORKID.replace("PARAM1", workId);
        workLegalOwners = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return workLegalOwners;
    }

    private String getownershipDescriptionName(String code){
        String sql = String.format(APIDataSQL.SELECT_GD_LOV_DESCRIPTION_OF_OWNER,code);
        List<Map<String,Object>> ownershipDescriptionName = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String) ownershipDescriptionName.get(0).get("l_description");
    }

    private String[] getLegalOwnerValues(String code){
        String sql = String.format(APIDataSQL.SELECT_GD_LOV_OF_LEGALOWNER,code);
        List<Map<String,Object>> legalOwnerValues = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        String[] arr_temp={legalOwnerValues.get(0).get("name").toString(),legalOwnerValues.get(0).get("f_legal_ownership").toString()};
        return arr_temp;
    }

    private String[] getLegalOwnershipValues(String code){
        String sql = String.format(APIDataSQL.SELECT_GD_LOV_OF_LEGALOWNERSHIP,code);
        List<Map<String,Object>> legalOwnershipValues = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        String[] arr_temp={legalOwnershipValues.get(0).get("l_description").toString(),
                legalOwnershipValues.get(0).get("roll_up_ownership").toString()};
        return arr_temp;
    }

    private void printLog(String verified) {
        System.out.println("verified..." + verified);
    }
}
