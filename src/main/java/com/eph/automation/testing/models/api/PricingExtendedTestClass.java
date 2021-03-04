package com.eph.automation.testing.models.api;
//created by Nishant @ 03 Feb 2021, EPHD-2747
public class PricingExtendedTestClass {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private PricingExtendedAPIObj pricingExtended;
    public PricingExtendedAPIObj getPricingExtended() {return pricingExtended;}
    public void setPricingExtended(PricingExtendedAPIObj pricingExtended) {this.pricingExtended = pricingExtended;}
}
