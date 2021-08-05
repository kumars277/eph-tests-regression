package com.eph.automation.testing.models.dao;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 24/01/2019
 */
public class ManifestationIdentifierObject {
   private String b_loadid;
   private String f_event;
   private String b_classname;
   private String manif_identifier_id;
   private String identifier;
   private String f_type;
   private String f_manifestation;
   private String effective_start_date;
   private String effective_end_date;
   private String external_reference;


    public String getExternal_reference() {
        return external_reference;
    }

    public void setExternal_reference(String external_reference) {
        this.external_reference = external_reference;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

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

    public String getEffective_start_date() {
        return effective_start_date;
    }

    public void setEffective_start_date(String effective_start_date) {
        this.effective_start_date = effective_start_date;
    }

    public String getEffective_end_date() {
        return effective_end_date;
    }

    public void setEffective_end_date(String effective_end_date) {
        this.effective_end_date = effective_end_date;
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
                Objects.equals(f_manifestation, that.f_manifestation) &&
                Objects.equals(effective_start_date, that.effective_start_date) &&
                Objects.equals(effective_end_date, that.effective_end_date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(b_loadid, f_event, b_classname, manif_identifier_id, identifier, f_type, f_manifestation, effective_start_date, effective_end_date);
    }
}
