package org.morris.unofficial.models;

/**
 * {@code org.morris.unofficial.models.KeyPhraseType} models a type of key phrase that can be used in conjunction
 * with transformation processes on metro line data dumps. Specifically, transformation processes should focus on
 * which type of data is being transformed on metro line dumps.
 * <p></p>
 * Example: "stop-times" lets any transformation process know that "stop-times" type key phrases will be processed
 * in the following functional logic.
 * <p></p>
 */
public enum KeyPhraseType {
    STOPS("stops"),
    STOP_TIMES("stop-times");

    private final String type;

    KeyPhraseType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
