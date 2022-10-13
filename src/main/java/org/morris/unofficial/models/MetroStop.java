package org.morris.unofficial.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * {@code org.morris.unofficial.models.MetroStop} models a Metro stop that contains a name for
 * its stop line and a list of stop times.
 */
public class MetroStop {

    @JsonProperty("line_stop")
    private String lineStop;

    @JsonProperty("stop_times")
    private List<String> stopTimes;

    public String getLineStop() {
        return lineStop;
    }

    public void setLineStop(String lineStop) {
        this.lineStop = lineStop;
    }

    public List<String> getStopTimes() {
        return stopTimes;
    }

    public void setStopTimes(List<String> stopTimes) {
        this.stopTimes = stopTimes;
    }
}
