package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class ManifestationWorkApiObject {
    public ManifestationWorkApiObject() {
    }

    private String workId;
    private String workTitle;
    private String workSubTitle;
    private WorkPersonsApiObject[] workPersonApiObjects;
    private Boolean electronicRightsInd;
    private int editionNumber;
    private int volume;
    private int copyrightYear;
    private WorkIdentifiersApiObject[] workIdentifiers;
    private HashMap<String, Object> workType;
    private HashMap<String, Object> workStatus;
    private HashMap<String, Object> imprint;
    private PMCApiObject pmc;

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

    public String getWorkSubTitle() {
        return workSubTitle;
    }

    public void setWorkSubTitle(String workSubTitle) {
        this.workSubTitle = workSubTitle;
    }

    public WorkPersonsApiObject[] getWorkPersonApiObjects() {
        return workPersonApiObjects;
    }

    public void setWorkPersonApiObjects(WorkPersonsApiObject[] workPersonApiObjects) {
        this.workPersonApiObjects = workPersonApiObjects;
    }

    public Boolean getElectronicRightsInd() {
        return electronicRightsInd;
    }

    public void setElectronicRightsInd(Boolean electronicRightsInd) {
        this.electronicRightsInd = electronicRightsInd;
    }

    public int getEditionNumber() {
        return editionNumber;
    }

    public void setEditionNumber(int editionNumber) {
        this.editionNumber = editionNumber;
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

    public WorkIdentifiersApiObject[] getWorkIdentifiers() {
        return workIdentifiers;
    }

    public void setWorkIdentifiers(WorkIdentifiersApiObject[] workIdentifiers) {
        this.workIdentifiers = workIdentifiers;
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

    public PMCApiObject getPmc() {
        return pmc;
    }

    public void setPmc(PMCApiObject pmc) {
        this.pmc = pmc;
    }
}