package com.eph.automation.testing.models.api;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import org.junit.Assert;

public class ManifestationExtended {
    private String journalProdSiteCode;
    public String getJournalProdSiteCode() {return journalProdSiteCode;}
    public void setJournalProdSiteCode(String journalProdSiteCode) {this.journalProdSiteCode = journalProdSiteCode;}

    private String journalIssueTrimSize;
    public String getJournalIssueTrimSize() {return journalIssueTrimSize;}
    public void setJournalIssueTrimSize(String journalIssueTrimSize) {this.journalIssueTrimSize = journalIssueTrimSize;}

    public String warReference;
    public String getWarReference() {return warReference;}
    public void setWarReference(String warReference) {this.warReference = warReference;}

    public void compareWithDB() {
        Assert.assertEquals(journalIssueTrimSize, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalIssueTrimSize());
        printLog("primarySiteSystem");

    }
    private void printLog(String verified){Log.info("verified..."+verified);}
}
