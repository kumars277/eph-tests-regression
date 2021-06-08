package com.eph.automation.testing.models.api;
/*
* created by Nishant @ 02 Jul 2020
* to validate manifestation Extended data with API v3
* updated by Nishant @ 05 Feb 2021 to validate stch_manifestation_ext_json with API
* Updated by Nishant @ 02 Mar 2021, EPHD-2898
* */
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestationExtended {
    private String journalProdSiteCode;
    public String getJournalProdSiteCode() {return journalProdSiteCode;}
    public void setJournalProdSiteCode(String journalProdSiteCode) {this.journalProdSiteCode = journalProdSiteCode;}

    private String journalIssueTrimSize;
    public String getJournalIssueTrimSize() {return journalIssueTrimSize;}
    public void setJournalIssueTrimSize(String journalIssueTrimSize) {this.journalIssueTrimSize = journalIssueTrimSize;}

    public String warReference;
    public String getWarReference() { return warReference;}
    public void setWarReference(String warReference) {this.warReference = warReference;}

    private String ukTextbookInd;
    public String getUkTextbookInd() {return ukTextbookInd;}
    public void setUkTextbookInd(String ukTextbookInd) {this.ukTextbookInd = ukTextbookInd;}

    private String usTextbookInd;
    public String getUsTextbookInd() {return usTextbookInd;}
    public void setUsTextbookInd(String usTextbookInd) {this.usTextbookInd = usTextbookInd;}

    private String manifestationTrimText;
    public String getManifestationTrimText() {return manifestationTrimText;}
    public void setManifestationTrimText(String manifestationTrimText) {this.manifestationTrimText = manifestationTrimText;}

    private String commodityCode;
    public String getCommodityCode() {return commodityCode;}
    public void setCommodityCode(String commodityCode) {this.commodityCode = commodityCode;}

    private String discountCodeEMEA;
    public String getDiscountCodeEMEA() {return discountCodeEMEA;}
    public void setDiscountCodeEMEA(String discountCodeEMEA) {this.discountCodeEMEA = discountCodeEMEA;}

    private String discountCodeUS;
    public String getDiscountCodeUS() {return discountCodeUS;}
    public void setDiscountCodeUS(String discountCodeUS) {this.discountCodeUS = discountCodeUS;}

    private ManifestationExtendedPageCountsAPIObj[] manifestationExtendedPageCounts;
    public ManifestationExtendedPageCountsAPIObj[] getManifestationExtendedPageCounts() {return manifestationExtendedPageCounts;}
    public void setManifestationExtendedPageCounts(ManifestationExtendedPageCountsAPIObj[] manifestationExtendedPageCounts) {this.manifestationExtendedPageCounts = manifestationExtendedPageCounts;}

    private ManifestationExtendedRestrictions[] manifestationExtendedRestrictions;
    public ManifestationExtendedRestrictions[] getManifestationExtendedRestrictions() {return manifestationExtendedRestrictions;}
    public void setManifestationExtendedRestrictions(ManifestationExtendedRestrictions[] manifestationExtendedRestrictions) {this.manifestationExtendedRestrictions = manifestationExtendedRestrictions;}

    public void compareWithDB() {

        Log.info("----- verifying ManifestationExtended data...");

        Assert.assertEquals(ukTextbookInd, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getUkTextbookInd());
        printLog("ukTextbookInd");

        Assert.assertEquals(usTextbookInd, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getUsTextbookInd());
        printLog("usTextbookInd");

        Assert.assertEquals(manifestationTrimText, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getManifestationTrimText());
        printLog("manifestationTrimText");

        Assert.assertEquals(commodityCode, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getCommodityCode());
        printLog("commodityCode");

        Assert.assertEquals(discountCodeEMEA, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getDiscountCodeEMEA());
        printLog("discountCodeEMEA");

        Assert.assertEquals(discountCodeUS, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getDiscountCodeUS());
        printLog("discountCodeUS");

        Assert.assertEquals(journalProdSiteCode,DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalProdSiteCode());
        printLog("journalProdSiteCode");

        Assert.assertEquals(journalIssueTrimSize, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalIssueTrimSize());
        printLog("primarySiteSystem");

     //   String DBWarReference = "";
     //   if(DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getWarReference()!=null)DBWarReference=DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getWarReference();
        Assert.assertEquals(warReference,DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getWarReference());
        printLog("warReference");


        if(manifestationExtendedPageCounts!=null)
        {
            ArrayList<ManifestationExtendedPageCountsAPIObj> api_manifestationExtendedPageCounts = new ArrayList<>(Arrays.asList(manifestationExtendedPageCounts));
            ArrayList<ManifestationExtendedPageCountsAPIObj> db_manifestationExtendedPageCounts = new ArrayList<>(Arrays.asList(DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().manifestationExtendedPageCounts));

            List<Integer> ignore = new ArrayList<>();
            for(int mpc=0;mpc<api_manifestationExtendedPageCounts.size();mpc++)
            {
                Log.info("----->verification for manifestationExtendedPageCounts "+mpc);

                boolean extPageFound = false;
                for(int cnt =0;cnt<db_manifestationExtendedPageCounts.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if( api_manifestationExtendedPageCounts.get(mpc).getExtendedPageCount().getType().get("code").toString().equalsIgnoreCase(db_manifestationExtendedPageCounts.get(cnt).getExtendedPageCount().getType().get("code").toString()))
                    {
                        extPageFound = true;
                        printLog("ExtendedPageCount code");

                        Assert.assertEquals(api_manifestationExtendedPageCounts.get(mpc).getExtendedPageCount().getType().get("name"),db_manifestationExtendedPageCounts.get(cnt).getExtendedPageCount().getType().get("name"));
                        printLog("ExtendedPageCount name");

                        Assert.assertEquals(api_manifestationExtendedPageCounts.get(mpc).getExtendedPageCount().getCount(),db_manifestationExtendedPageCounts.get(cnt).getExtendedPageCount().getCount());
                        printLog("ExtendedPageCount count");

                        ignore.add(cnt);break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage + " - manifestationExtendedPageCounts-> "+mpc+" not found in DB",extPageFound);

            }
        }

    }

    public static class ManifestationExtendedRestrictions{
        //created by Nishant @ 05 Feb 2021
        private ExtendedRestriction extendedRestriction;
        public ExtendedRestriction getExtendedRestriction() {return extendedRestriction;}
        public void setExtendedRestriction(ExtendedRestriction extendedRestriction) {this.extendedRestriction = extendedRestriction;}
    }

    public static class ExtendedRestriction{
        private HashMap<String,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}
    }

    private void printLog(String verified){Log.info("verified..."+verified);}
}
