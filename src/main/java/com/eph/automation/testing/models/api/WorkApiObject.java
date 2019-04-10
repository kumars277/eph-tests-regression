package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkApiObject {

    public WorkApiObject() {
    }

    private String workId;
    private String workTitle;
    private String electronicRightsInd;
    private String editionNumber;
    private String volume;
    private String copyrightYear;
    private HashMap<String,Object> workType;
    private HashMap<String,Object> workStatus;
    private HashMap<String,Object> imprint;
    private CopyrightOwnersApiObject[] copyrightOwnerApiObjects;
    private WorkIdentifiersApiObject[] workIdentifierApiObjects;

    public String getWorkId() {
        return workId;
    }

    public void setWorkId(String workId) {
        this.workId = workId;
    }

    public String getWorkTitle() {
        return workTitle;
    }

    public void setWorkTitle(String workTitle) {
        this.workTitle = workTitle;
    }

    public String getElectronicRightsInd() {
        return electronicRightsInd;
    }

    public void setElectronicRightsInd(String electronicRightsInd) {
        this.electronicRightsInd = electronicRightsInd;
    }

    public String getEditionNumber() {
        return editionNumber;
    }

    public void setEditionNumber(String editionNumber) {
        this.editionNumber = editionNumber;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getCopyrightYear() {
        return copyrightYear;
    }

    public void setCopyrightYear(String copyrightYear) {
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

    public CopyrightOwnersApiObject[] getCopyrightOwnerApiObjects() {
        return copyrightOwnerApiObjects;
    }

    public void setCopyrightOwnerApiObjects(CopyrightOwnersApiObject[] copyrightOwnerApiObjects) {
        this.copyrightOwnerApiObjects = copyrightOwnerApiObjects;
    }
}