package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class WorkIdentifiersApiObject {

    public String getWorkIdentifier() {
        return workIdentifier;
    }

    public void setWorkIdentifier(String workIdentifier) {
        this.workIdentifier = workIdentifier;
    }

    private String workIdentifier;
        private HashMap<String, Object> identifierType;

        public WorkIdentifiersApiObject() {
        }


        public HashMap<String, Object> getIdentifierType() {
            return identifierType;
        }

        public void setIdentifierType(HashMap<String, Object> identifierType) {
            this.identifierType = identifierType;
        }

}