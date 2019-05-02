package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
   public class ProductManifestationApiObject {
    public ProductManifestationApiObject() {
    }

    private String manifestationId;
    private String manifestationKeyTitle;
    private Boolean internationalEditionFlag;
    private String firstPubDate;
    private ManifestationIdentifiersApiObject[] manifestationIdentifierApiObjects;
    private HashMap<String, Object> manifestationType;
    private HashMap<String, Object> manifestationStatus;
    
    private HashMap<String, Object> manifestationFormat;
    private ManifestationWorkApiObject manifestationWorkApiObject;

    public ManifestationIdentifiersApiObject[] getManifestationIdentifierApiObjects() {
        return manifestationIdentifierApiObjects;
    }

    public HashMap<String, Object> getManifestationFormat() {
        return manifestationFormat;
    }

    public void setManifestationFormat(HashMap<String, Object> manifestationFormat) {
        this.manifestationFormat = manifestationFormat;
    }

    public void setManifestationIdentifierApiObjects(ManifestationIdentifiersApiObject[] manifestationIdentifierApiObjects) {
        this.manifestationIdentifierApiObjects = manifestationIdentifierApiObjects;
    }

    public String getManifestationId() {
        return manifestationId;
    }

    public void setManifestationId(String manifestationId) {
        this.manifestationId = manifestationId;
    }

    public ManifestationWorkApiObject getManifestationWorkApiObject() {
        return manifestationWorkApiObject;
    }

    public void setManifestationWorkApiObject(ManifestationWorkApiObject manifestationWorkApiObject) {
        this.manifestationWorkApiObject = manifestationWorkApiObject;
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


