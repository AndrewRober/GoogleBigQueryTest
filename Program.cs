using Google;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Services;

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GoogleBigQueryTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string projectId = "quickstart-1564761649643"; // Replace with your GCP project ID
            string datasetId = "newtest";
            string tableId = "mytable";
            string jsonKeyFilePath = @"C:\Users\andre\Downloads\quickstart-1564761649643-678d86e71db5.json"; // Replace with the path to your JSON key file

            GoogleCredential credentials;
            using (var stream = new FileStream(jsonKeyFilePath, FileMode.Open, FileAccess.Read))
            {
                credentials = GoogleCredential.FromStream(stream).CreateScoped(BigqueryService.Scope.Bigquery);
            }

            var bigqueryService = new BigqueryService(new BaseClientService.Initializer
            {
                HttpClientInitializer = credentials,
                ApplicationName = "BigQuery API Demo"
            });

            // Create a dataset
            var dataset = new Dataset
            {
                DatasetReference = new DatasetReference { ProjectId = projectId, DatasetId = datasetId }
            };
            if (!TableExists(bigqueryService, projectId, datasetId, tableId))
                bigqueryService.Datasets.Insert(dataset, projectId).Execute();

            // Create a table
            var schema = new TableSchema
            {
                Fields = new List<TableFieldSchema>
                {
                    new TableFieldSchema { Name = "name", Type = "STRING" },
                    new TableFieldSchema { Name = "age", Type = "INTEGER" }
                }
            };
            var table = new Table
            {
                TableReference = new TableReference { ProjectId = projectId, DatasetId = datasetId, TableId = tableId },
                Schema = schema
            };
            if (!TableExists(bigqueryService, projectId, datasetId, tableId))
                bigqueryService.Tables.Insert(table, projectId, datasetId).Execute();

            // Insert values into the table
            var rows = new List<TableRow>
            {
                new TableRow { F = new List<TableCell> { new TableCell { V = "Alice" }, new TableCell { V = 30 } } },
                new TableRow { F = new List<TableCell> { new TableCell { V = "Bob" }, new TableCell { V = 25 } } }
            };
            string ndjsonFilePath = "data.ndjson"; // Choose a file name and path for the NDJSON file

            foreach (var row in rows)
            {
                string name = row.F[0].V.ToString();
                string age = row.F[1].V.ToString();

                var dmlQueryRequest = new QueryRequest
                {
                    Query = $@" INSERT INTO `{projectId}.{datasetId}.{tableId}` (name, age) VALUES ('{name}', {age})",
                    UseLegacySql = false
                };

                bigqueryService.Jobs.Query(dmlQueryRequest, projectId).Execute();
            }

            // Fetch table data
            // SQL query to fetch records where age >= 30
            string query = $"SELECT * FROM `{projectId}.{datasetId}.{tableId}` WHERE age >= 30";

            // Run a query job
            var queryRequest = new QueryRequest
            {
                Query = query
            };

            var queryResponse = bigqueryService.Jobs.Query(queryRequest, projectId).Execute();

            // Print the results
            Console.WriteLine("Results:");
            foreach (var row in queryResponse.Rows)
            {
                Console.WriteLine($"{row.F[0].V}, {row.F[1].V}");
            }
        }

        public static bool TableExists(BigqueryService bigqueryService, string projectId, string datasetId, string tableId)
        {
            try
            {
                var table = bigqueryService.Tables.Get(projectId, datasetId, tableId).Execute();
                return true;
            }
            catch (GoogleApiException ex)
            {
                if (ex.HttpStatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }
                throw;
            }
        }
    }
}