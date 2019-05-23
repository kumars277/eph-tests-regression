package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class WorkPersonsApiObject {
    public WorkPersonsApiObject() {
    }

    public void compareWithDB(){

    }
    private int id;
    private HashMap<String, Object> role;
    private HashMap<String, Object> person;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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