package com.eph.automation.testing.models.api;
//created by Nishant @16 Feb 2021, EPHD-2747
public class WorkCoreTestClass {

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

    private workCore workCore;
    public workCore getWorkCore() {return workCore;}
    public void setWorkCore(workCore workCore) {this.workCore = workCore;}

    private WorkManifestationApiObject[] manifestations;
    public WorkManifestationApiObject[] getManifestations() {return manifestations;}
    public void setManifestations(WorkManifestationApiObject[] manifestations) {this.manifestations = manifestations;}

    private WorkProductsApiObj[] products;
    public WorkProductsApiObj[] getProducts() {return products;}
    public void setProducts(WorkProductsApiObj[] products) {this.products = products;}
}
