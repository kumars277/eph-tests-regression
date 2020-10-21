package com.eph.automation.testing.models.api;
/*
* created by Nishant @ 02 Jul 2020
* to validate manifestation Extended data with API v3
* */
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestationExtended {
    private String journalProdSiteCode;
    public String getJournalProdSiteCode() {return journalProdSiteCode;}
    public void setJournalProdSiteCode(String journalProdSiteCode) {this.journalProdSiteCode = journalProdSiteCode;}

    private String journalIssueTrimSize;
    public String getJournalIssueTrimSize() {return journalIssueTrimSize;}
    public void setJournalIssueTrimSize(String journalIssueTrimSize) {this.journalIssueTrimSize = journalIssueTrimSize;}

    public String warReference;
    public String getWarReference() {
        if(warReference==null) return "";
        else return warReference;
    }
    public void setWarReference(String warReference) {this.warReference = warReference;}

    public void compareWithDB() {
        Assert.assertEquals(journalProdSiteCode,DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalProdSiteCode());
        printLog("journalProdSiteCode");

        Assert.assertEquals(journalIssueTrimSize, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalIssueTrimSize());
        printLog("primarySiteSystem");

        Assert.assertEquals(warReference,DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getWarReference());
        printLog("warReference");

    }
    private void printLog(String verified){Log.info("verified..."+verified);}
}
