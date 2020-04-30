using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace cosmos_dotnet_core_getting_started
{
    class Program
    {
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string _cosmosDbEndpointUri = Environment.GetEnvironmentVariable("COSMOS_DB_ENDPOINT_URI");
        // The primary key for the Azure Cosmos account.
        private static readonly string _cosmosAccountPrimaryKey = Environment.GetEnvironmentVariable("COSMOS_ACCOUNT_PRIMARY_KEY");

        // The Cosmos client instance
        private CosmosClient _cosmosClient;

        // The database we will create
        private Database _database;

        // The container we will create.
        private Container _container;

        // The name of the database and container we will create
        private string _databaseId = "FamilyDatabase";
        private string _containerId = "FamilyContainer";

        public static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Beginning operations...");
                Program p = new Program();
                await p.GetStartedDemoAsync();
            }
            catch (CosmosException ce)
            {
                Exception baseException = ce.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}\n", ce.StatusCode, ce);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}\n", e);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Entry point to call methods that operate on Azure Cosmos DB resources in this sample
        /// </summary>
        public async Task GetStartedDemoAsync()
        {
            // Create a new instance of the Cosmos Client
            _cosmosClient = new CosmosClient(_cosmosDbEndpointUri, _cosmosAccountPrimaryKey);
            await CreateDatabaseAsync();
            await CreateContainerAsync();
            await AddItemsToContainerAsync();
            await QueryItemsAsync();
            await ReplaceFamilyItemAsync();
            await DeleteFamilyItemAsync();
            await DeleteDatabaseAndCleanupAsync();
        }

        /// <summary>
        /// Create the database if it does not exist
        /// </summary>
        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            _database = await _cosmosClient.CreateDatabaseIfNotExistsAsync(_databaseId);
            Console.WriteLine("Created Database: {0}\n", _database.Id);
        }

        /// <summary>
        /// Create the container if it does not exist.
        /// Specifiy "/LastName" as the partition key since we're storing family information, to ensure good distribution of requests and storage.
        /// </summary>
        /// <returns></returns>
        private async Task CreateContainerAsync()
        {
            // Create a new container
            _container = await _database.CreateContainerIfNotExistsAsync(_containerId, "/LastName", 400);
            Console.WriteLine("Created Container: {0}\n", _container.Id);
        }
        /// <summary>
        /// Add Family items to the container
        /// </summary>
        private async Task AddItemsToContainerAsync()
        {
            // Create a family object for the Andersen family
            var andersenFamily = new Family
            {
                Id = "Andersen.1",
                LastName = "Andersen",
                Parents = new Parent[]
                {
                    new Parent { FirstName = "Thomas" },
                    new Parent { FirstName = "Mary Kay" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FirstName = "Henriette Thaulow",
                        Gender = "female",
                        Grade = 5,
                        Pets = new []
                        {
                            new Pet { GivenName = "Fluffy" }
                        }
                    }
                },
                Address = new Address { State = "WA", County = "King", City = "Seattle" },
                IsRegistered = false
            };

            try
            {
                // Read the item to see if it exists.
                var andersenFamilyResponse = await _container.ReadItemAsync<Family>(andersenFamily.Id, new PartitionKey(andersenFamily.LastName));
                Console.WriteLine("Item in database with id: {0} already exists\n", andersenFamilyResponse.Resource.Id);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Create an item in the container representing the Andersen family. Note we provide the value of the partition key for this item, which is "Andersen"
                var andersenFamilyResponse = await _container.CreateItemAsync<Family>(andersenFamily, new PartitionKey(andersenFamily.LastName));

                // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", andersenFamilyResponse.Resource.Id, andersenFamilyResponse.RequestCharge);
            }

            // Create a family object for the Wakefield family
            var wakefieldFamily = new Family
            {
                Id = "Wakefield.7",
                LastName = "Wakefield",
                Parents = new[]
                          {
                              new Parent { FamilyName = "Wakefield", FirstName = "Robin" },
                              new Parent { FamilyName = "Miller", FirstName = "Ben" }
                          },
                Children = new[]
                           {
                               new Child
                               {
                                   FamilyName = "Merriam",
                                   FirstName = "Jesse",
                                   Gender = "female",
                                   Grade = 8,
                                   Pets = new []
                                   {
                                       new Pet { GivenName = "Goofy" },
                                       new Pet { GivenName = "Shadow" }
                                   }
                               },
                               new Child
                               {
                                   FamilyName = "Miller",
                                   FirstName = "Lisa",
                                   Gender = "female",
                                   Grade = 1
                               }
                           },
                Address = new Address
                {
                    State = "NY",
                    County = "Manhattan",
                    City = "NY"
                },
                IsRegistered = true
            };

            try
            {
                // Read the item to see if it exists
                var wakefieldFamilyResponse = await _container.ReadItemAsync<Family>(wakefieldFamily.Id, new PartitionKey(wakefieldFamily.LastName));
                Console.WriteLine("Item in database with id: {0} already exists\n", wakefieldFamilyResponse.Resource.Id);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Create an item in the container representing the Wakefield family. Note we provide the value of the partition key for this item, which is "Wakefield"
                var wakefieldFamilyResponse = await _container.CreateItemAsync<Family>(wakefieldFamily, new PartitionKey(wakefieldFamily.LastName));

                // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", wakefieldFamilyResponse.Resource.Id, wakefieldFamilyResponse.RequestCharge);
            }
        }
        /// <summary>
        /// Run a query (using Azure Cosmos DB SQL syntax) against the container
        /// </summary>
        private async Task QueryItemsAsync()
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.LastName = 'Andersen'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            var queryDefinition = new QueryDefinition(sqlQueryText);
            var queryResultSetIterator = _container.GetItemQueryIterator<Family>(queryDefinition);

            var families = new List<Family>();

            while (queryResultSetIterator.HasMoreResults)
            {
                var currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (var family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine("\tRead {0}\n", family);
                }
            }
        }

        /// <summary>
        /// Replace an item in the container
        /// </summary>
        private async Task ReplaceFamilyItemAsync()
        {
            var wakefieldFamilyResponse = await _container.ReadItemAsync<Family>("Wakefield.7", new PartitionKey("Wakefield"));
            var itemBody = wakefieldFamilyResponse.Resource;

            // update registration status from false to true
            itemBody.IsRegistered = true;
            // update grade of child
            itemBody.Children[0].Grade = 6;

            // replace the item with the updated content
            wakefieldFamilyResponse = await _container.ReplaceItemAsync<Family>(itemBody, itemBody.Id, new PartitionKey(itemBody.LastName));
            Console.WriteLine("Updated Family [{0},{1}].\n \tBody is now: {2}\n", itemBody.LastName, itemBody.Id, wakefieldFamilyResponse.Resource);
        }
        /// <summary>
        /// Delete an item in the container
        /// </summary>
        private async Task DeleteFamilyItemAsync()
        {
            var partitionKeyValue = "Wakefield";
            var familyId = "Wakefield.7";

            // Delete an item. Note we must provide the partition key value and id of the item to delete
            var wakefieldFamilyResponse = await _container.DeleteItemAsync<Family>(familyId, new PartitionKey(partitionKeyValue));
            Console.WriteLine("Deleted Family [{0},{1}]\n", partitionKeyValue, familyId);
        }
        /// <summary>
        /// Delete the database and dispose of the Cosmos Client instance
        /// </summary>
        private async Task DeleteDatabaseAndCleanupAsync()
        {
            var databaseResourceResponse = await _database.DeleteAsync();
            // Also valid: await _cosmosClient.Databases["FamilyDatabase"].DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", _databaseId);

            //Dispose of CosmosClient
            _cosmosClient.Dispose();
        }
    }
}