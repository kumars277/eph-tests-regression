package com.eph.automation.testing.models.dao.JMDataLake.JsonObjects;

import com.eph.automation.testing.models.dao.JMDataLake.STITCHObject;

public class StitchManifestationObjectJson {
    private STITCHObject manifestation;
    public STITCHObject getmanifestation() {return manifestation;}
    public void setmanifestation(STITCHObject manifestation) {this.manifestation = manifestation;}

    private STITCHObject [] manifestations;
    public STITCHObject [] getmanifestations() {return manifestations;}
    public void setmanifestations (STITCHObject[] manifestations) {this.manifestations = manifestations;}

    private String id;
    public String getid() {return id;}
    public void setid(String id) {this.id = id;}
}
