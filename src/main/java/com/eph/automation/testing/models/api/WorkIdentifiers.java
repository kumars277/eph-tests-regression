package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class WorkIdentifiers {
    private List<WorkIdentifier> workIdentifiers;

    public WorkIdentifiers() {
    }

    public List<WorkIdentifier> getWorkIdentifiers() {
        return this.workIdentifiers;
    }

    public void setWorkIdentifiers(List<WorkIdentifier> workIdentifiers) {
        this.workIdentifiers = workIdentifiers;
    }


    private class WorkIdentifier {

        private String workIdentifier;
        private HashMap<String, Object> identifierType;

        public WorkIdentifier() {
        }

        public String getWorkId() {
            return workIdentifier;
        }

        public void setWorkId(String workIdentifier) {
            this.workIdentifier = workIdentifier;
        }

        public HashMap<String, Object> getIdentifierType() {
            return identifierType;
        }

        public void setIdentifierType(HashMap<String, Object> identifierType) {
            this.identifierType = identifierType;
        }
    }
}