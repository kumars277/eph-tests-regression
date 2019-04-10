package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class Work {
    public Work() {
    }

    private String workTitle;
    private WorkPersons[] workPersons;
    private Boolean electronicRightsInd;
    private int volume;
    private int copyrightYear;
    private WorkIdentifiers[] workIdentifiers;
    private HashMap<String, Object> workType;
    private HashMap<String, Object> workStatus;
    private HashMap<String, Object> imprint;
    private PMC pmc;

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

    public WorkIdentifiers[] getWorkIdentifiers() {
        return workIdentifiers;
    }

    public void setWorkIdentifiers(WorkIdentifiers[] workIdentifiers) {
        this.workIdentifiers = workIdentifiers;
    }

    public PMC getPmc() {
        return pmc;
    }

    public void setPmc(PMC pmc) {
        this.pmc = pmc;
    }

    public WorkPersons[] getWorkPersons() {
        return workPersons;
    }

    public void setWorkPersons(WorkPersons[] workPersons) {
        this.workPersons = workPersons;
    }

}