package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class ManifestationIdentifiersApiObject {
    private List<ManifestationIdentifier> manifestationIdentifiers;

    public ManifestationIdentifiersApiObject() {
    }

    public List<ManifestationIdentifier> getManifestationIdentifiers() {
        return this.manifestationIdentifiers;
    }

    public void setManifestationIdentifiers(List<ManifestationIdentifier> manifestationIdentifiers) {
        this.manifestationIdentifiers = manifestationIdentifiers;
    }


    private class ManifestationIdentifier {

        private String manifestationIdentifier;
        private HashMap<String, Object> identifierType;

        public ManifestationIdentifier() {
        }

        public String getManifestationId() {
            return manifestationIdentifier;
        }

        public void setManifestationId(String manifestationIdentifier) {
            this.manifestationIdentifier = manifestationIdentifier;
        }

        public HashMap<String, Object> getIdentifierType() {
            return identifierType;
        }

        public void setIdentifierType(HashMap<String, Object> identifierType) {
            this.identifierType = identifierType;
        }
    }
}