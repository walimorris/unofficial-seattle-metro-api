package org.morris.unofficial.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * MetroLine is a model used to model the line, lineName, and lineScheduleUrl of the initial processed data from
 * SEA Metro site crawl. This model will allow the {@link com.fasterxml.jackson.databind.ObjectMapper} to map the
 * json file pulled from the processed document bucket to a POJO.
 */
public class MetroLine {
    @JsonProperty("line")
    private String line;

    @JsonProperty("line_name")
    private String lineName;

    @JsonProperty("line_schedule_url")
    private String lineScheduleUrl;

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public String getLineScheduleUrl() {
        return lineScheduleUrl;
    }

    public void setLineScheduleUrl(String lineScheduleUrl) {
        this.lineScheduleUrl = lineScheduleUrl;
    }
}
