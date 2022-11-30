## DataFlow job analytics
> The artifact can be used to get the dataflow job metrics.
> projectId, region, and DataFlow jobId are the required parameters.
 
- To build the fat jar, execute the below command from within the project root directory
```bash
$ mvn clean package
```
-  To get the DataFlow job metrics, execute the below command from the directory where JAR file exists
```bash
$ java -jar job-analytics-1.0.tar --project=<projectId> --region=<regionName> --job=<dataflowJobId>
```