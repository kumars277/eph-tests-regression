package com.eph.automation.testing.models.dao.StitchingExtended;

import com.eph.automation.testing.models.dao.StitchingExtended.ManifExtJson;

//package com.eph.automation.testing.models.api;
public class ManifExtJsonObject {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private ManifExtJson manifestationExtended;
    public ManifExtJson getManifestationExtended() {return manifestationExtended;}
    public void setManifestationExtended(ManifExtJson manifestationExtended) {this.manifestationExtended = manifestationExtended;}
}
