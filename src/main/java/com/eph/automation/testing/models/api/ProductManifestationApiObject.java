package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
   public class ProductManifestationApiObject {
    public ProductManifestationApiObject() {
    }

    public String getManifestationId() {
        return manifestationId;
    }

    public void setManifestationId(String manifestationId) {
        this.manifestationId = manifestationId;
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

    public ManifestationIdentifiersApiObject[] getManifestationIdentifiers() {
        return manifestationIdentifiers;
    }

    public void setManifestationIdentifiers(ManifestationIdentifiersApiObject[] manifestationIdentifiers) {
        this.manifestationIdentifiers = manifestationIdentifiers;
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

    public HashMap<String, Object> getManifestationFormat() {
        return manifestationFormat;
    }

    public void setManifestationFormat(HashMap<String, Object> manifestationFormat) {
        this.manifestationFormat = manifestationFormat;
    }

    public ManifestationWorkApiObject getWork() {
        return work;
    }

    public void setWork(ManifestationWorkApiObject work) {
        this.work = work;
    }

    private String manifestationId;
    private String manifestationKeyTitle;
    private Boolean internationalEditionFlag;
    private String firstPubDate;
    private ManifestationIdentifiersApiObject[] manifestationIdentifiers;
    private HashMap<String, Object> manifestationType;
    private HashMap<String, Object> manifestationStatus;
    
    private HashMap<String, Object> manifestationFormat;
    private ManifestationWorkApiObject work;



}


