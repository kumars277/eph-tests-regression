package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class ManifestationIdentifiersApiObject {

    private String manifestationIdentifier;
    private HashMap<String, Object> identifierType;

    public String getManifestationIdentifier() {
        return manifestationIdentifier;
    }

    public void setManifestationIdentifier(String manifestationIdentifier) {
        this.manifestationIdentifier = manifestationIdentifier;
    }


        public ManifestationIdentifiersApiObject() {
        }

        public HashMap<String, Object> getIdentifierType() {
            return identifierType;
        }

        public void setIdentifierType(HashMap<String, Object> identifierType) {
            this.identifierType = identifierType;
        }

}