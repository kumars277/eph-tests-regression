package com.eph.automation.testing.models.dao.JRBIDataLakeAccess;
//package com.eph.automation.testing.models.api;
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

    /*public void compareWithDB() {
        Assert.assertEquals(journalProdSiteCode,DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalProdSiteCode());
        printLog("journalProdSiteCode");

        Assert.assertEquals(journalIssueTrimSize, DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getJournalIssueTrimSize());
        printLog("primarySiteSystem");

        Assert.assertEquals(warReference,DataQualityContext.manifestationExtendedTestClass.getManifestationExtended().getWarReference());
        printLog("warReference");

    }
    private void printLog(String verified){Log.info("verified..."+verified);}*/
}