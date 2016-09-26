/*
 * Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

// [START all]
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * Example of authorizing with Bigquery and reading from a public dataset.
 *
 * Specifically, this queries the shakespeare dataset to fetch the 10 of
 * Shakespeare's works with the greatest number of distinct words.
 */
public class BigQueryUploadMain {
	// [START build_service]
	/**
	 * Creates an authorized Bigquery client service using Application Default
	 * Credentials.
	 *
	 * @return an authorized Bigquery client
	 * @throws IOException
	 *             if there's an error getting the default credentials.
	 */
	public static Bigquery createAuthorizedClient() throws IOException {
		// Create the credential
		HttpTransport transport = new NetHttpTransport();
		JsonFactory jsonFactory = new JacksonFactory();
		GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);

		// Depending on the environment that provides the default credentials
		// (e.g. Compute Engine, App
		// Engine), the credentials may require us to specify the scopes we need
		// explicitly.
		// Check for this case, and inject the Bigquery scope if required.
		if (credential.createScopedRequired()) {
			credential = credential.createScoped(BigqueryScopes.all());
		}

		return new Bigquery.Builder(transport, jsonFactory, credential).setApplicationName("Bigquery Samples").build();
	}
	// [END build_service]

	// [START run_query]
	private static JobReference executeJobs(Bigquery bigquery, String projectId, Integer year, Integer month,
			Integer gram) throws IOException {
		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationLoad loadConfig = new JobConfigurationLoad();
		config.setLoad(loadConfig);

		job.setConfiguration(config);

		// Set where you are importing from (i.e. the Google Cloud Storage
		// paths).
		List<String> sources = new ArrayList<String>();
		String monthStr = "";
		if (month < 10) {
			monthStr = "0" + month;
		} else {
			monthStr = "" + month;
		}
		sources.add("gs://reddit-corpus/GRAM/" + year + "/RC_" + year + "-" + monthStr + "-comments-grams" + gram);
		loadConfig.setSourceUris(sources);
		loadConfig.setIgnoreUnknownValues(false);
		loadConfig.setAllowJaggedRows(false);
		loadConfig.setFieldDelimiter(",");
		loadConfig.setMaxBadRecords(99999999);

		// Describe the resulting table you are importing to:
		TableReference tableRef = new TableReference();
		tableRef.setDatasetId("NGram");
		tableRef.setTableId("GRAM_" + year + "_" + monthStr + "_" + gram);
		tableRef.setProjectId(projectId);
		loadConfig.setDestinationTable(tableRef);

		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
		TableFieldSchema fieldFoo = new TableFieldSchema();
		fieldFoo.setName("WORD");
		fieldFoo.setType("string");
		TableFieldSchema fieldBar = new TableFieldSchema();
		fieldBar.setName("COUNT");
		fieldBar.setType("integer");
		fields.add(fieldFoo);
		fields.add(fieldBar);
		TableSchema schema = new TableSchema();
		schema.setFields(fields);
		loadConfig.setSchema(schema);

		// Also set custom delimiter or header rows to skip here....
		// [not shown].

		Insert insert;

		insert = bigquery.jobs().insert(projectId, job);
		insert.setProjectId(projectId);
		JobReference jobRef = insert.execute().getJobReference();

		System.out.format("\nJob ID of Query Job is: %s\n", jobRef.getJobId());

		return jobRef;
	}
	
	@SuppressWarnings("unused")
	private static JobReference exportJobs(Bigquery bigquery, String projectId, Integer topicId) throws IOException {
		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationExtract extractConfig = new JobConfigurationExtract();
		config.setExtract(extractConfig);

		job.setConfiguration(config);

		// Set where you are importing from (i.e. the Google Cloud Storage
//		// paths).
//	
		TableReference tableRef = new TableReference();
		tableRef.setDatasetId("NGram");
		tableRef.setTableId("AATGRAM_" + topicId);
		tableRef.setProjectId(projectId);		
		extractConfig.setSourceTable(tableRef);
		
		extractConfig.setDestinationUri("gs://ngram-dalhousie1/results/" + topicId + "*");

		// Describe the resulting table you are importing to:

		Insert insert;

		insert = bigquery.jobs().insert(projectId, job);
		insert.setProjectId(projectId);
		JobReference jobRef = insert.execute().getJobReference();

		System.out.format("\nJob ID of Query Job is: %s\n", jobRef.getJobId());

		return jobRef;
	}

	@SuppressWarnings("unused")
	private static JobReference executeGoogle1TJobs(Bigquery bigquery, String projectId, Integer gram, String number)
			throws IOException {
		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationLoad loadConfig = new JobConfigurationLoad();
		config.setLoad(loadConfig);

		job.setConfiguration(config);

		// Set where you are importing from (i.e. the Google Cloud Storage
		// paths).
		List<String> sources = new ArrayList<String>();
		if (gram == 1){
			sources.add("gs://reddit-corpus/ngram-dalhousie/" + gram + "gm/vocab");
		}else{
			sources.add("gs://ngram-dalhousie1/ngram-dalhousie/" + gram + "gms/" + gram + "gm-" + number);
		}
		
		loadConfig.setSourceUris(sources);
		loadConfig.setIgnoreUnknownValues(false);
		loadConfig.setAllowJaggedRows(false);
		loadConfig.setFieldDelimiter("\t");
		loadConfig.setMaxBadRecords(99999999);

		// Describe the resulting table you are importing to:
		TableReference tableRef = new TableReference();
		tableRef.setDatasetId("NGram");
		tableRef.setTableId("GRAM_WEB_1T_" + gram + "_" + number);
		tableRef.setProjectId(projectId);
		loadConfig.setDestinationTable(tableRef);
		loadConfig.setWriteDisposition("WRITE_APPEND");

		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
		TableFieldSchema fieldFoo = new TableFieldSchema();
		fieldFoo.setName("WORD");
		fieldFoo.setType("string");
		TableFieldSchema fieldBar = new TableFieldSchema();
		fieldBar.setName("COUNT");
		fieldBar.setType("integer");
		fields.add(fieldFoo);
		fields.add(fieldBar);
		TableSchema schema = new TableSchema();
		schema.setFields(fields);
		loadConfig.setSchema(schema);

		// Also set custom delimiter or header rows to skip here....
		// [not shown].

		Insert insert;

		insert = bigquery.jobs().insert(projectId, job);
		insert.setProjectId(projectId);
		JobReference jobRef = insert.execute().getJobReference();

		System.out.format("\nJob ID of Query Job is: %s\n", jobRef.getJobId());

		return jobRef;
	}

	@SuppressWarnings("unused")
	private static Job checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId)
			throws IOException, InterruptedException {
		// Variables to keep track of total query time
		long startTime = System.currentTimeMillis();
		long elapsedTime;

		while (true) {
			Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
			elapsedTime = System.currentTimeMillis() - startTime;
			System.out.format("Job status (%dms) %s: %s\n", elapsedTime, jobId.getJobId(),
					pollJob.getStatus().getState());
			if (pollJob.getStatus().getState().equals("DONE")) {
				return pollJob;
			}
			// Pause execution for one second before polling job status again,
			// to
			// reduce unnecessary calls to the BigQUery API and lower overall
			// application bandwidth.
			Thread.sleep(1000);
		}
	}
	// [END start_query]

	/**
	 * Executes the given query synchronously.
	 *
	 * @param querySql
	 *            the query to execute.
	 * @param bigquery
	 *            the Bigquery service object.
	 * @param projectId
	 *            the id of the project under which to run the query.
	 * @return a list of the results of the query.
	 * @throws IOException
	 *             if there's an error communicating with the API.
	 */
	@SuppressWarnings("unused")
	private static List<TableRow> executeQuery(String querySql, Bigquery bigquery, String projectId)
			throws IOException {
		QueryResponse query = bigquery.jobs().query(projectId, new QueryRequest().setQuery(querySql)).execute();

		// Execute it
		GetQueryResultsResponse queryResult = bigquery.jobs()
				.getQueryResults(query.getJobReference().getProjectId(), query.getJobReference().getJobId()).execute();

		return queryResult.getRows();
	}
	// [END run_query]

	// [START print_results]
	/**
	 * Prints the results to standard out.
	 *
	 * @param rows
	 *            the rows to print.
	 */
	@SuppressWarnings("unused")
	private static void printResults(List<TableRow> rows) {
		System.out.print("\nQuery Results:\n------------\n");
		for (TableRow row : rows) {
			for (TableCell field : row.getF()) {
				System.out.printf("%-50s", field.getV());
			}
			System.out.println();
		}
	}
	// [END print_results]

	/**
	 * Exercises the methods defined in this class.
	 *
	 * In particular, it creates an authorized Bigquery service object using
	 * Application Default Credentials, then executes a query against the public
	 * Shakespeare dataset and prints out the results.
	 *
	 * @param args
	 *            the first argument, if it exists, should be the id of the
	 *            project to run the test under. If no arguments are given, it
	 *            will prompt for it.
	 * @throws IOException
	 *             if there's an error communicating with the API.
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		String projectId = "northern-timer-134923";

		// Create a new Bigquery client authorized via Application Default
		// Credentials.
		Bigquery bigquery = createAuthorizedClient();

		//2008,2013,2014,2015
	    for (int i=1;i<=12;i++){
    	for (int j=1;j<=5;j++){
    		 executeJobs(bigquery,projectId,2008,i,j);
    		 executeJobs(bigquery,projectId,2013,i,j);
    		 executeJobs(bigquery,projectId,2014,i,j);
    		 executeJobs(bigquery,projectId,2015,i,j);
	    	}
	    }
	    //2007
	    for (int i=10;i<=12;i++){
	    	for (int j=1;j<=5;j++){
	    		 executeJobs(bigquery,projectId,2007,i,j);
		    	}
		    }
	   
	    //2009 - 2012
	    for (int i=1;i<=12;i++){
    	for (int j=1;j<=5;j++){
    		 executeJobs(bigquery,projectId,2009,i,j);
    		 executeJobs(bigquery,projectId,2010,i,j);
    		 executeJobs(bigquery,projectId,2011,i,j);
    		 executeJobs(bigquery,projectId,2012,i,j);
	    	}
	    }
		
//		List<TableRow> rows1 = null;
//		int year = 2007;
//		for (year=2007;year <=2015;year++){
//			
//			for (int month=1;month<=12;month++){
//				if (year==2007){
//					if (month < 10){
//						continue;
//					}
//				}
//				if (year==2015){
//					if (month > 5){
//						continue;
//					}
//				}
//				try{
//					rows1 = executeQuery("SELECT word,count FROM [NGram.GRAM_" + year + "_" + month + "1] where word='ISIS'", bigquery, projectId);
//					for (TableRow row : rows1) {
//						for (TableCell field : row.getF()) {
//							int count = Integer.valueOf(field.getV().toString());
//						}
//					}
//				}catch(Exception ex){
//					
//				}
//			}
//			
//		}
//		

//		checkQueryResults(bigquery, projectId, job);

	}
}

// }
// [END all]