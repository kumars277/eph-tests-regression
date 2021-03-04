package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 28 Apr 2020
 * updated by Nishant @ 04 Feb 2021, EPHD-2747
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

    private ManifestationCore manifestationCore;
    public ManifestationCore getManifestationCore() {return manifestationCore;}
    public void setManifestationCore(ManifestationCore manifestationCore) {this.manifestationCore = manifestationCore;}

    private ManifestationExtended manifestationExtended;
    public ManifestationExtended getManifestationExtended() {return manifestationExtended;}
    public void setManifestationExtended(ManifestationExtended manifestationExtended) {this.manifestationExtended = manifestationExtended;}

    private WorkApiObject work;
    public WorkApiObject getWork() {return work;}
    public void setWork(WorkApiObject work) {this.work = work;}

    //manifestationExtended need to add, by Nishant @ 29 Jan 2021
/*
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

        //added by Nishant @ 29 Jan 2021
        private WorkExtended workExtended;
        public WorkExtended getWorkExtended(){return  workExtended;}
        public void setWorkExtended(WorkExtended workExtended){this.workExtended=workExtended;}

    }
*/
    public void compareWithDB(){
        manifestationCore.compareWithDB(this.id);
        work.getWorkCore().compareWithDB(work.getId());
    }
}