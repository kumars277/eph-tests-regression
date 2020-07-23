package com.eph.automation.testing.models.dao.JRBIDataLakeAccess;
//package com.eph.automation.testing.models.api;
public class JRBIManifExtJsonObject {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private JRBIManifExtJson manifestationExtended;
    public JRBIManifExtJson getManifestationExtended() {return manifestationExtended;}
    public void setManifestationExtended(JRBIManifExtJson manifestationExtended) {this.manifestationExtended = manifestationExtended;}
}
