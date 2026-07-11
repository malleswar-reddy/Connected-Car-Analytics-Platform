package com.demo.gpsspark.web;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobStatus {
    private boolean running;
    private String queryId;
    private String runId;
    /** Raw JSON from Spark's StreamingQueryProgress — cheapest honest way to
     *  expose batch/throughput info without hand-mapping every field. */
    private String lastProgressJson;
    private String lastErrorMessage;
}
