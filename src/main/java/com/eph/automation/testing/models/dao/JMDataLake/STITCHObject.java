package com.eph.automation.testing.models.dao.JMDataLake;

import com.eph.automation.testing.models.dao.JMDataLake.JsonObjects.StitchObjectJson;

public class STITCHObject {
    private String schemaVersion;
    public String getSchemaVersion() {return schemaVersion;}
    public void setSchemaVersion(String schemaVersion) {this.schemaVersion = schemaVersion;}

    private String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getid() {return id;}
    public void setid(String id) {this.id = id;}

    private String epr_id;
    public String getepr_id() {return epr_id;}
    public void setepr_id(String epr_id) {this.epr_id = epr_id;}

    private StitchObjectJson productCore;
    public StitchObjectJson getproductCore() {return productCore;}
    public void setproductCore(StitchObjectJson productCore) {this.productCore = productCore;}

    private StitchObjectJson manifestation;
    public StitchObjectJson getmanifestation() {return manifestation;}
    public void setmanifestation(StitchObjectJson manifestation) {this.manifestation = manifestation;}

    private StitchObjectJson manifestationCore;
    public StitchObjectJson getmanifestationCore() {return manifestationCore;}
    public void setmanifestationCore(StitchObjectJson manifestationCore) {this.manifestationCore = manifestationCore;}

    private StitchObjectJson work;
    public StitchObjectJson getwork() {return work;}
    public void setwork(StitchObjectJson work) {this.work = work;}

    private StitchObjectJson workCore;
    public StitchObjectJson getworkCore() {return workCore;}
    public void setworkCore(StitchObjectJson workCore) {this.workCore = workCore;}
}
