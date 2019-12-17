using System.Data;
using Dapper.ExtendedCRUDBaseTests;
using Npgsql;
using TestContainers.Container.Abstractions.Hosting;
using TestContainers.Container.Database.Hosting;
using TestContainers.Container.Database.PostgreSql;

namespace Dapper.ExtendedCRUDPostgreSQLTests
{
    public class PostresSQLTestFixture : ITestClassFixture
    {
        public bool CanProcessTestsWithSchema => true;
        public bool CanProcessTestsWithArrays => true;
        public string Encapsulate(string value)
        {
            return $"\"{value}\"";
        }

        private PostgreSqlContainer Container { get; }

        private string ConnectionString => Container.GetConnectionString();

        public PostresSQLTestFixture()
        {
            Container = new ContainerBuilder<PostgreSqlContainer>()
                .ConfigureDatabaseConfiguration("Test", "TestTest*R1", "Tests")
                .Build();
            Container.AutoRemove = true;
            Container.StartAsync().Wait();
            var connection = new NpgsqlConnection(ConnectionString);
            using (connection)
            {
                connection.Open();
                connection.Execute(" create table \"Users\" (\"Id\" SERIAL PRIMARY KEY, \"Name\" varchar not null, \"Age\" int not null, \"ScheduledDayOff\" int null, \"CreatedDate\" date not null default CURRENT_DATE) ");
                connection.Execute(" create table \"Car\" (\"CarId\" SERIAL PRIMARY KEY, \"Id\" int null, \"Make\" varchar not null, \"Model\" varchar not null) ");
                connection.Execute(" create table \"BigCar\" (\"CarId\" BIGSERIAL PRIMARY KEY, \"Make\" varchar not null, \"Model\" varchar not null) ");
                connection.Execute(" create table \"City\" (\"Name\" varchar not null, \"Population\" int not null) ");
                connection.Execute(" CREATE SCHEMA \"Log\"; ");
                connection.Execute(" create table \"Log\".\"CarLog\" (\"Id\" SERIAL PRIMARY KEY, \"LogNotes\" varchar NOT NULL) ");
                connection.Execute(" CREATE TABLE \"GUIDTest\"(\"Id\" uuid PRIMARY KEY,\"Name\" varchar NOT NULL)");
                connection.Execute(" create table \"StrangeColumnNames\" (\"ItemId\" Serial PRIMARY KEY, \"Word\" varchar not null, \"colstringstrangeword\" varchar, \"KeywordedProperty\" varchar) ");
                connection.Execute(" create table \"TypeMapColumnNames\" (\"ItemId\" Serial PRIMARY KEY, \"typemappedcolumn\" varchar)");
                connection.Execute(" create table \"UserWithoutAutoIdentity\" (\"Id\" int PRIMARY KEY, \"Name\" varchar not null, \"Age\" int not null) ");
                connection.Execute(" create table \"IgnoreColumns\" (\"Id\" SERIAL PRIMARY KEY, \"IgnoreInsert\" varchar null, \"IgnoreUpdate\" varchar null, \"IgnoreSelect\" varchar  null, \"IgnoreAll\" varchar null) ");
                connection.Execute(" CREATE TABLE \"KeyMaster\" (\"Key1\" INTEGER NOT NULL, \"Key2\" INTEGER NOT NULL, PRIMARY KEY (\"Key1\", \"Key2\"))");
                connection.Execute(" CREATE TABLE \"StringTest\" (\"stringkey\" varchar NOT NULL,\"name\" varchar NOT NULL, PRIMARY KEY (\"stringkey\"))");
                connection.Execute(" CREATE TABLE \"IntegerArraysTest\" (\"integerarraykey\" SERIAL NOT NULL,\"integerarray\" integer[] NOT NULL, PRIMARY KEY (\"integerarraykey\"))");
            }
        }
        public IDbConnection GetOpenConnection()
        {
            var connection = new NpgsqlConnection(ConnectionString);
            ExtendedCRUD.SetDialect(ExtendedCRUD.Dialect.PostgreSQL);
            connection.Open();
            return connection;
        }

        public void Dispose()
        {
            Container.StopAsync().Wait();
        }
    }
}
