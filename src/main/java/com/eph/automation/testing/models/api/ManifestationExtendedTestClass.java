package com.eph.automation.testing.models.api;

import org.apache.xmlbeans.impl.xb.xsdschema.Public;

public class ManifestationExtendedTestClass {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private ManifestationExtended manifestationExtended;
    public ManifestationExtended getManifestationExtended() {return manifestationExtended;}
    public void setManifestationExtended(ManifestationExtended manifestationExtended) {this.manifestationExtended = manifestationExtended;}
}
