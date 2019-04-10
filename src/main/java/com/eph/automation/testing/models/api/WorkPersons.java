package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class WorkPersons {
    public WorkPersons() {
    }

    private int workPersonId;
    private HashMap<String, Object> role;
    private HashMap<String, Object> person;

    public int getWorkPersonId() {
        return workPersonId;
    }

    public void setWorkPersonId(int workPersonId) {
        this.workPersonId = workPersonId;
    }

    public HashMap<String, Object> getRole() {
        return role;
    }

    public void setRole(HashMap<String, Object> role) {
        this.role = role;
    }

    public HashMap<String, Object> getPerson() {
        return person;
    }

    public void setPerson(HashMap<String, Object> person) {
        this.person = person;
    }
}