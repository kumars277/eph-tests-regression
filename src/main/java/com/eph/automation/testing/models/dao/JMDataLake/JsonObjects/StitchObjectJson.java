package com.eph.automation.testing.models.dao.JMDataLake.JsonObjects;

public class StitchObjectJson {
    private String title;
    public String gettitle() {
        return title;
    }
    public void settitle(String title) {
        this.title = title;
    }

    private String name;
    public String getname() {
        return name;
    }
    public void setname(String name) {
        this.name = name;
    }

    private String shortName;
    public String getshortName() {
        return shortName;
    }
    public void setshortName(String shortName) {
        this.shortName = shortName;
    }

    private String copyrightYear;
    public String getcopyrightYear() {return copyrightYear;}
    public void setcopyrightYear(String copyrightYear) {this.copyrightYear = copyrightYear;}

    private StitchSubWorkCoreJson imprint;
    public StitchSubWorkCoreJson getimprint() {return imprint;}
    public void setimprint(StitchSubWorkCoreJson imprint) {this.imprint = imprint;}

    private StitchSubIdentifierJson workRelationships;
    public StitchSubIdentifierJson getworkRelationships() {return workRelationships;}
    public void setworkRelationships(StitchSubIdentifierJson workRelationships) {this.workRelationships = workRelationships;}

    private StitchPersonJson [] workPersons;
    public StitchPersonJson [] getworkPersons() {return workPersons;}
    public void setworkPersons (StitchPersonJson[] workPersons) {this.workPersons = workPersons;}

    private StitchPersonJson [] workSubjectAreas;
    public StitchPersonJson [] getworkSubjectAreas() {return workSubjectAreas;}
    public void setworkSubjectAreas (StitchPersonJson[] workSubjectAreas) {this.workSubjectAreas = workSubjectAreas;}

    private StitchSubIdentifierJson [] identifiers;
    public StitchSubIdentifierJson [] getidentifiers() {return identifiers;}
    public void setidentifiers (StitchSubIdentifierJson[] identifiers) {this.identifiers = identifiers;}

    private StitchSubWorkCoreJson accountableProduct;
    public StitchSubWorkCoreJson getaccountableProduct() {return accountableProduct;}
    public void setaccountableProduct(StitchSubWorkCoreJson accountableProduct) {this.accountableProduct = accountableProduct;}

    private StitchSubWorkCoreJson pmc;
    public StitchSubWorkCoreJson getpmc() {return pmc;}
    public void setpmc(StitchSubWorkCoreJson pmc) {this.pmc = pmc;}

    private StitchSubWorkCoreJson etaxProductCode;
    public StitchSubWorkCoreJson getetaxProductCode() {return etaxProductCode;}
    public void setetaxProductCode(StitchSubWorkCoreJson etaxProductCode) {this.etaxProductCode = etaxProductCode;}

    private StitchSubWorkCoreJson revenueModel;
    public StitchSubWorkCoreJson getrevenueModel() {return revenueModel;}
    public void setrevenueModel(StitchSubWorkCoreJson revenueModel) {this.revenueModel = revenueModel;}

    private StitchSubWorkCoreJson revenueAccount;
    public StitchSubWorkCoreJson getrevenueAccount() {return revenueAccount;}
    public void setrevenueAccount(StitchSubWorkCoreJson revenueAccount) {this.revenueAccount = revenueAccount;}

    private StitchSubWorkCoreJson status;
    public StitchSubWorkCoreJson getstatus() {return status;}
    public void setstatus(StitchSubWorkCoreJson status) {this.status = status;}

    private StitchSubWorkCoreJson type;
    public StitchSubWorkCoreJson gettype() {return type;}
    public void settype(StitchSubWorkCoreJson type) {this.type = type;}

    private StitchObjectJson work;
    public StitchObjectJson getwork() {return work;}
    public void setwork(StitchObjectJson work) {this.work = work;}

    private StitchObjectJson workCore;
    public StitchObjectJson getworkCore() {return workCore;}
    public void setworkCore(StitchObjectJson workCore) {this.workCore = workCore;}
}
