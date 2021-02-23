package com.eph.automation.testing.models.dao.StitchingExtended;

//package com.eph.automation.testing.models.api;
public class WorkExtJsonObject {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private WorkExtJson workExtended;
    public WorkExtJson getWorkExtended() {return workExtended;}
    public void setWorkExtended(WorkExtJson workExtended) {this.workExtended = workExtended;}
}
