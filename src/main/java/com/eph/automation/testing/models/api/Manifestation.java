package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
   public class Manifestation {
    public Manifestation() {
    }

    private String manifestationId;
    private String manifestationKeyTitle;
    private Boolean internationalEditionFlag;
    private String firstPubDate;
    private ManifestationIdentifiers[] manifestationIdentifiers;
    private HashMap<String, Object> manifestationType;
    private HashMap<String, Object> manifestationStatus;
    private HashMap<String, Object> manifestationFormat;
    private Work work;

    public ManifestationIdentifiers[] getManifestationIdentifiers() {
        return  manifestationIdentifiers;
    }

    public HashMap<String, Object> getManifestationFormat() {
        return manifestationFormat;
    }

    public void setManifestationFormat(HashMap<String, Object> manifestationFormat) {
        this.manifestationFormat = manifestationFormat;
    }

    public void setManifestationIdentifiers(ManifestationIdentifiers[] manifestationIdentifiers) {
        this.manifestationIdentifiers = manifestationIdentifiers;
    }

    public String getManifestationId() {
        return manifestationId;
    }

    public void setManifestationId(String manifestationId) {
        this.manifestationId = manifestationId;
    }

    public Work getWork() {
        return work;
    }

    public void setWork(Work work) {
        this.work = work;
    }

    public String getManifestationKeyTitle() {
        return manifestationKeyTitle;
    }

    public void setManifestationKeyTitle(String manifestationKeyTitle) {
        this.manifestationKeyTitle = manifestationKeyTitle;
    }

    public Boolean getInternationalEditionFlag() {
        return internationalEditionFlag;
    }

    public void setInternationalEditionFlag(Boolean internationalEditionFlag) {
        this.internationalEditionFlag = internationalEditionFlag;
    }

    public String getFirstPubDate() {
        return firstPubDate;
    }

    public void setFirstPubDate(String firstPubDate) {
        this.firstPubDate = firstPubDate;
    }

    public HashMap<String, Object> getManifestationType() {
        return manifestationType;
    }

    public void setManifestationType(HashMap<String, Object> manifestationType) {
        this.manifestationType = manifestationType;
    }

    public HashMap<String, Object> getManifestationStatus() {
        return manifestationStatus;
    }

    public void setManifestationStatus(HashMap<String, Object> manifestationStatus) {
        this.manifestationStatus = manifestationStatus;
    }

}


