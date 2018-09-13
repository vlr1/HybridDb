using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Docker.DotNet;
using Docker.DotNet.Models;
using Shouldly;
using Xunit;
using Xunit.Abstractions;
using static HybridDb.Helpers;

namespace HybridDb.Tests
{
    public class DocumentStoreTests
    {
        readonly ITestOutputHelper output;
        readonly DocumentStore store;

        public DocumentStoreTests(ITestOutputHelper output)
        {
            this.output = output;

            var client = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")).CreateClient();

            client.Images.CreateImageAsync(new ImagesCreateParameters
            {
                FromImage = "microsoft/mssql-server-windows-developer",
                Tag = "2017-latest",
            }, null, new Progress<JSONMessage>(x => { output.WriteLine(x.Status); })).Wait();

            var list = client.Containers.ListContainersAsync(new ContainersListParameters
            {
                All = true,
                Filters = new Dictionary<string, IDictionary<string, bool>>()
                {
                    ["name"] = new Dictionary<string, bool>
                    {
                        ["hybriddb-testing"] = true
                    }
                }
            }).Result;

            var found = list.SingleOrDefault();

            if (found == null)
            {
                var response = client.Containers.CreateContainerAsync(new CreateContainerParameters
                {
                    Image = "microsoft/mssql-server-windows-developer:2017-latest",
                    Name = "hybriddb-testing",
                    Env = new[] {"ACCEPT_EULA=Y", $"SA_PASSWORD=Qalid4wiro!"},
                    ExposedPorts = new Dictionary<string, EmptyStruct>
                    {
                        ["1433"] = new EmptyStruct()
                    },
                    HostConfig = new HostConfig
                    {
                        PortBindings = new Dictionary<string, IList<PortBinding>>
                        {
                            ["1433"] = new List<PortBinding> {new PortBinding {HostPort = "1401"}}
                        }
                    }
                }).Result;
            }

            if (found == null || !found.Status.StartsWith("Up"))
            {
                var b = client.Containers.StartContainerAsync("hybriddb-testing", new ContainerStartParameters()).Result;
            }

            var connectionString = "Server=127.0.0.1,1401;User=sa;Password=Qalid4wiro!;";

            var db = Guid.NewGuid().ToString();

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                connection.Execute($"create database [{db}]");
                connection.Execute($"alter database [{db}] set allow_snapshot_isolation on;");
            }

            store = new DocumentStore($"{connectionString};Initial Catalog={db}");

            store.Up();
        }

        [Fact]
        public async Task Test() { }

        [Fact]
        public async Task InsertGet()
        {
            await store.Execute(ListOf(new Insert("a", new Dictionary<string, string[]>(), "{}")));

            var result = await store.Get("a");

            result.Id.ShouldBe("a");
            result.Metadata.ShouldBe("{}");
            result.Document.ShouldBe("{}");
        }
    }
}