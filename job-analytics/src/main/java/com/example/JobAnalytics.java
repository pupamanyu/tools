/*
 * Copyright 2022 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import com.example.pojo.DataFlowJobMetric;
import com.example.pojo.DataFlowMetric;
import com.example.pojo.ImmutableDataFlowJobMetric;
import com.example.pojo.ImmutableDataFlowMetric;
import com.google.dataflow.v1beta3.GetJobMetricsRequest;
import com.google.dataflow.v1beta3.JobMetrics;
import com.google.dataflow.v1beta3.MetricUpdate;
import com.google.dataflow.v1beta3.MetricsV1Beta3Client;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.util.Timestamps;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "job-analytics", mixinStandardHelpOptions = true, version = "job-analytics 1.0",
        description = "Print the metrics for a completed DataFlow job to STDOUT. GCP projectId, GCP region name, and DataFlow jobId needs to be supplied as options.")
public class JobAnalytics implements Callable<Integer> {
    @CommandLine.Option(names = {"-p", "--project"}, description = "GCP project ID", required = true)
    private String projectId;

    @CommandLine.Option(names = {"-r", "--region"}, description = "GCP Region Name", required = true)
    private String region;

    @CommandLine.Option(names = {"-j", "--job"}, description = "DataFlow Job ID", required = true)
    private String jobId;

    public static void main(String... args) {
        int exitCode = new CommandLine(new JobAnalytics()).execute(args);
        System.exit(exitCode);
    }

    private JobMetrics getMetrics(String projectId, String region, String jobId) throws IOException {
        try (MetricsV1Beta3Client metricsV1Beta3Client = MetricsV1Beta3Client.create()) {
            GetJobMetricsRequest metricsRequest =
                    GetJobMetricsRequest.newBuilder()
                            .setProjectId(projectId)
                            .setJobId(jobId)
                            .setLocation(region)
                            .build();
            return metricsV1Beta3Client.getJobMetrics(metricsRequest);
        }
    }

    private List<DataFlowMetric> getDataFlowMetrics(String metricsOrigin, JobMetrics jobMetrics) {
        List<DataFlowMetric> dataFlowJobMetrics = new ArrayList<>();
        for (MetricUpdate metricUpdate : jobMetrics.getMetricsList()) {
            // "dataflow/v1b3" is the metrics origin for dataflow service metrics
            if (metricUpdate.getName().getOrigin().equals(metricsOrigin) && (!metricUpdate.getName().containsContext("tentative")) && (!metricUpdate.getName().containsContext("execution_step")) && (!metricUpdate.getName().containsContext("step")) && (!metricUpdate.getName().getName().equals("ElementCount")) && (!metricUpdate.getName().getName().equals("MeanByteCount")) && ((metricUpdate.getName().getName().startsWith("Total")) || (metricUpdate.getName().getName().startsWith("Billable"))) && metricUpdate.getScalar().hasNumberValue()) {
                dataFlowJobMetrics.add(ImmutableDataFlowMetric.builder()
                        .metricName(metricUpdate.getName().getName())
                        .metricOriginalName(metricUpdate.getName().getContextOrDefault("original_name", metricUpdate.getName().getName()))
                        .metricValue(metricUpdate.getScalar().getNumberValue())
                        .metricMillis(Timestamps.toMillis(metricUpdate.getUpdateTime()))
                        .build());
            }
        }
        return dataFlowJobMetrics;
    }

    @Override
    public Integer call() throws Exception {
        try {
            JobMetrics jobMetrics = getMetrics(projectId, region, jobId);
            List<DataFlowMetric> dataFlowMetrics = getDataFlowMetrics("dataflow/v1b3", jobMetrics);
            DataFlowJobMetric dataFlowJobMetric = ImmutableDataFlowJobMetric.builder().projectId(projectId).region(region).jobId(jobId).addAllDataflowMetrics(dataFlowMetrics).build();
            Gson gson = new GsonBuilder().create();
            System.out.println(gson.toJson(dataFlowJobMetric));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }
}