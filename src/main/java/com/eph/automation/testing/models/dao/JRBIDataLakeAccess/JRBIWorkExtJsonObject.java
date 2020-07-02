package com.eph.automation.testing.models.dao.JRBIDataLakeAccess;
//package com.eph.automation.testing.models.api;
public class JRBIWorkExtJsonObject {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private JRBIWorkExtJson workExtended;
    public JRBIWorkExtJson getWorkExtended() {return workExtended;}
    public void setWorkExtended(JRBIWorkExtJson workExtended) {this.workExtended = workExtended;}
}

