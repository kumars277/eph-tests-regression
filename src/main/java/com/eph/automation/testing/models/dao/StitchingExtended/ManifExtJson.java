package com.eph.automation.testing.models.dao.StitchingExtended;
//package com.eph.automation.testing.models.api;

import com.eph.automation.testing.models.dao.StitchingExtended.ManifExtPgCountJson;

public class ManifExtJson {

    private String journalIssueTrimSize;
    public String getjournalIssueTrimSize() {
        return journalIssueTrimSize;
    }
    public void setjournalIssueTrimSize(String journalIssueTrimSize) {
        this.journalIssueTrimSize = journalIssueTrimSize;
    }

    private String manifestationTrimText;
    public String getmanifestationTrimText() {
        return manifestationTrimText;
    }
    public void setmanifestationTrimText(String manifestationTrimText) {
        this.manifestationTrimText = manifestationTrimText;
    }

    private String manifestationWeight;
    public String getmanifestationWeight() {
        return manifestationWeight;
    }
    public void setmanifestationWeight(String manifestationWeight) {
        this.manifestationWeight = manifestationWeight;
    }

    private String journalProdSiteCode;
    public String getjournalProdSiteCode() {
        return journalProdSiteCode;
    }
    public void setjournalProdSiteCode(String journalProdSiteCode) {
        this.journalProdSiteCode = journalProdSiteCode;
    }

    private String ukTextbookInd;
    public String getukTextbookInd() {
        return ukTextbookInd;
    }
    public void setukTextbookInd(String ukTextbookInd) {
        this.ukTextbookInd = ukTextbookInd;
    }

    private String usTextbookInd;
    public String getusTextbookInd() {
        return usTextbookInd;
    }
    public void setusTextbookInd(String usTextbookInd) {
        this.usTextbookInd = usTextbookInd;
    }

    private String commodityCode;
    public String getcommodityCode() {
        return commodityCode;
    }
    public void setcommodityCode(String commodityCode) {
        this.commodityCode = commodityCode;
    }

    private String discountCodeEMEA;
    public String getdiscountCodeEMEA() {
        return discountCodeEMEA;
    }
    public void setdiscountCodeEMEA(String discountCodeEMEA) {
        this.discountCodeEMEA = discountCodeEMEA;
    }

    private String discountCodeUS;
    public String getdiscountCodeUS() {
        return discountCodeUS;
    }
    public void setdiscountCodeUS(String discountCodeUS) {
        this.discountCodeUS = discountCodeUS;
    }

    private String exportToWebInd;
    public String getexportToWebInd() {
        return exportToWebInd;
    }
    public void setexportToWebInd(String exportToWebInd) {
        this.exportToWebInd = exportToWebInd;
    }


    private String warReference;
    public String getWarReference() {
        return warReference;
    }
    public void setWarReference(String warReference) {
        this.warReference = warReference;
    }

    private ManifExtPgCountJson[] manifestationExtendedPageCounts;
    public ManifExtPgCountJson[] getManifestationExtendedPageCounts() {return manifestationExtendedPageCounts;}
    public void setManifestationExtendedPageCounts(ManifExtPgCountJson[] manifestationExtendedPageCounts) {this.manifestationExtendedPageCounts = manifestationExtendedPageCounts;}

    private ManifExtRestrictionJson[] manifestationExtendedRestrictions;
    public ManifExtRestrictionJson[] getManifestationExtendedRestrictions() {return manifestationExtendedRestrictions;}
    public void setManifestationExtendedRestrictions(ManifExtRestrictionJson[] manifestationExtendedRestrictions) {this.manifestationExtendedRestrictions = manifestationExtendedRestrictions;}
}

