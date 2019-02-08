package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 24/01/2019
 */
public class ManifestationIdentifierObject {
   public String b_loadid;
   public String f_event;
   public String b_classname;
   public String manif_identifier_id;
   public int identifier;
   public String f_type;
   public String f_manifestation;

    public String getB_loadid() {
        return b_loadid;
    }

    public void setB_loadid(String b_loadid) {
        this.b_loadid = b_loadid;
    }

    public String getF_event() {
        return f_event;
    }

    public void setF_event(String f_event) {
        this.f_event = f_event;
    }

    public String getB_classname() {
        return b_classname;
    }

    public void setB_classname(String b_classname) {
        this.b_classname = b_classname;
    }

    public String getManif_identifier_id() {
        return manif_identifier_id;
    }

    public void setManif_identifier_id(String manif_identifier_id) {
        this.manif_identifier_id = manif_identifier_id;
    }

    public int getIdentifier() {
        return identifier;
    }

    public void setIdentifier(int identifier) {
        this.identifier = identifier;
    }

    public String getF_type() {
        return f_type;
    }

    public void setF_type(String f_type) {
        this.f_type = f_type;
    }

    public String getF_manifestation() {
        return f_manifestation;
    }

    public void setF_manifestation(String f_manifestation) {
        this.f_manifestation = f_manifestation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ManifestationIdentifierObject that = (ManifestationIdentifierObject) o;
        return Objects.equals(b_loadid, that.b_loadid) &&
                Objects.equals(f_event, that.f_event) &&
                Objects.equals(b_classname, that.b_classname) &&
                Objects.equals(manif_identifier_id, that.manif_identifier_id) &&
                Objects.equals(identifier, that.identifier) &&
                Objects.equals(f_type, that.f_type) &&
                Objects.equals(f_manifestation, that.f_manifestation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(b_loadid, f_event, b_classname, manif_identifier_id, identifier, f_type, f_manifestation);
    }
}
