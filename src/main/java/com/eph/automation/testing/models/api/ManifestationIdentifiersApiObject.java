package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class ManifestationIdentifiersApiObject {

    public String getManifestationIdentifier() {
        return manifestationIdentifier;
    }

    public void setManifestationIdentifier(String manifestationIdentifier) {
        this.manifestationIdentifier = manifestationIdentifier;
    }

    private String manifestationIdentifier;
        private HashMap<String, Object> identifierType;

        public ManifestationIdentifiersApiObject() {
        }

        public HashMap<String, Object> getIdentifierType() {
            return identifierType;
        }

        public void setIdentifierType(HashMap<String, Object> identifierType) {
            this.identifierType = identifierType;
        }

}