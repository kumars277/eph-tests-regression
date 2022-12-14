package com.eph.automation.testing.models.dao.StitchingExtended;

//package com.eph.automation.testing.models.api;
public class ProdExtJsonObject {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private AvailExtJson availabilityExtended;
    public AvailExtJson getAvailabilityExtended() {return availabilityExtended;}
    public void setAvailabilityExtended(AvailExtJson availabilityExtended) {this.availabilityExtended = availabilityExtended;}

    private PricingExtJson pricingExtended;
    public PricingExtJson getPricingExtended() {return pricingExtended;}
    public void setPricingExtended(PricingExtJson pricingExtended) {this.pricingExtended = pricingExtended;}


}
