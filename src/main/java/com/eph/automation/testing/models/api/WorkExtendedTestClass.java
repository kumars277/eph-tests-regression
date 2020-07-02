package com.eph.automation.testing.models.api;
/**
 * created by Nishant @ 19 Jun 2020
 * this convertes json string to java object
 */
public class WorkExtendedTestClass {

    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private WorkExtended workExtended;
    public WorkExtended getWorkExtended() {return workExtended;}
    public void setWorkExtended(WorkExtended workExtended) {this.workExtended = workExtended;}
}
