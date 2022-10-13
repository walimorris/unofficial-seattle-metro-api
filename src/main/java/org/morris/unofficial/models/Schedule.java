package org.morris.unofficial.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * {@code org.morris.unofficial.models.Schedule} models a Metro schedule that contains a list of
 * {@link MetroStop}s.
 */
public class Schedule {

    @JsonProperty("schedule")
    List<MetroStop> schedule;

    public List<MetroStop> getSchedule() {
        return schedule;
    }

    public void setSchedule(List<MetroStop> schedule) {
        this.schedule = schedule;
    }
}
