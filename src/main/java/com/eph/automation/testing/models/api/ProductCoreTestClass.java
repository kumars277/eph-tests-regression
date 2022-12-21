package com.eph.automation.testing.models.api;

//created by Nishant @ 03 Feb 2021, EPHD-2747
public class ProductCoreTestClass {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String createdDate;
    public String getCreatedDate() {return createdDate;}
    public void setCreatedDate(String createdDate) {this.createdDate = createdDate;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private ProductCore productCore;
    public ProductCore getProductCore() {return productCore;}
    public void setProductCore(ProductCore productCore) {this.productCore = productCore;}

    private ManifestationProductAPIObject manifestation;
    public ManifestationProductAPIObject getManifestation() {return manifestation;}
    public void setManifestation(ManifestationProductAPIObject manifestation) {this.manifestation = manifestation;}
}
