package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 28 Apr 2020
 */

import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductManifestationApiObject {
    public ProductManifestationApiObject() {}
    private List<ManifestationDataObject> manifestationDataObjectFromEPHGD;

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private manifestationCore manifestationCore;
    public manifestationCore getManifestationCore() {return manifestationCore;}
    public void setManifestationCore(manifestationCore manifestationCore) {this.manifestationCore = manifestationCore;}

    private work work;
    public ProductManifestationApiObject.work getWork() {return work;}
    public void setWork(ProductManifestationApiObject.work work) {this.work = work;}

    public class work{
        private String id;
        public String getId() {return id;}
        public void setId(String id) {this.id = id;}

        private workCore workCore;
        public workCore getWorkCore() {return workCore;}
        public void setWorkCore(workCore workCore) {this.workCore = workCore;}
    }

    public void compareWithDB(){
        manifestationCore.compareWithDB(this.id);
        work.workCore.compareWithDB(work.id);
    }
}