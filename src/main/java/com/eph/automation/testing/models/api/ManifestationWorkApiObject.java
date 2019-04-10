package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class ManifestationWorkApiObject {
    public ManifestationWorkApiObject() {
    }

    private String workTitle;
    private WorkPersonsApiObject[] workPersonApiObjects;
    private Boolean electronicRightsInd;
    private int volume;
    private int copyrightYear;
    private WorkIdentifiersApiObject[] workIdentifierApiObjects;
    private HashMap<String, Object> workType;
    private HashMap<String, Object> workStatus;
    private HashMap<String, Object> imprint;
    private PMCApiObject pmcApiObject;

    public String getWorkTitle() {
        return workTitle;
    }

    public void setWorkTitle(String workTitle) {
        this.workTitle = workTitle;
    }

    public Boolean getElectronicRightsInd() {
        return electronicRightsInd;
    }

    public void setElectronicRightsInd(Boolean electronicRightsInd) {
        this.electronicRightsInd = electronicRightsInd;
    }

    public int getVolume() {
        return volume;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }

    public int getCopyrightYear() {
        return copyrightYear;
    }

    public void setCopyrightYear(int copyrightYear) {
        this.copyrightYear = copyrightYear;
    }

    public HashMap<String, Object> getWorkType() {
        return workType;
    }

    public void setWorkType(HashMap<String, Object> workType) {
        this.workType = workType;
    }

    public HashMap<String, Object> getWorkStatus() {
        return workStatus;
    }

    public void setWorkStatus(HashMap<String, Object> workStatus) {
        this.workStatus = workStatus;
    }

    public HashMap<String, Object> getImprint() {
        return imprint;
    }

    public void setImprint(HashMap<String, Object> imprint) {
        this.imprint = imprint;
    }

    public WorkIdentifiersApiObject[] getWorkIdentifierApiObjects() {
        return workIdentifierApiObjects;
    }

    public void setWorkIdentifierApiObjects(WorkIdentifiersApiObject[] workIdentifierApiObjects) {
        this.workIdentifierApiObjects = workIdentifierApiObjects;
    }

    public PMCApiObject getPmcApiObject() {
        return pmcApiObject;
    }

    public void setPmcApiObject(PMCApiObject pmcApiObject) {
        this.pmcApiObject = pmcApiObject;
    }

    public WorkPersonsApiObject[] getWorkPersonApiObjects() {
        return workPersonApiObjects;
    }

    public void setWorkPersonApiObjects(WorkPersonsApiObject[] workPersonApiObjects) {
        this.workPersonApiObjects = workPersonApiObjects;
    }

}