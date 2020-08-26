package com.capitalone.dashboard.model;

import javax.annotation.Nonnull;
import java.util.List;

public class ArtifactSyncRequest {

    private List<String> dashboards;
    @Nonnull
    private long startTime;
    @Nonnull
    private long endTime;

    public boolean isMetrics() {
        return metrics;
    }

    public void setMetrics(boolean metrics) {
        this.metrics = metrics;
    }

    private boolean metrics;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public List<String> getDashboards() {
        return dashboards;
    }

    public void setDashboards(List<String> dashboards) {
        this.dashboards = dashboards;
    }

}
