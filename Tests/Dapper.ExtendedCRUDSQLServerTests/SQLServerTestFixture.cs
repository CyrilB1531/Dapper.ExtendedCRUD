using System.Data;
using System.Data.SqlClient;
using Dapper.ExtendedCRUDBaseTests;
using TestContainers.Container.Abstractions.Hosting;
using TestContainers.Container.Database.Hosting;
using TestContainers.Container.Database.MsSql;

namespace Dapper.ExtendedCRUDSQLServerTests
{
    public class SQLServerTestFixture : ITestClassFixture
    {
        public bool CanProcessTestsWithSchema => true;
        public bool CanProcessTestsWithArrays => false;
        private MsSqlContainer Container { get; }

        public string Encapsulate(string value)
        {
            return $"[{value}]";
        }

        private string ConnectionString => Container.GetConnectionString();

        public SQLServerTestFixture()
        {
            Container = new ContainerBuilder<MsSqlContainer>()
                .ConfigureDatabaseConfiguration("Test", "TestTest*R1", "Tests")
                .Build();
            Container.AutoRemove = true;
            Container.StartAsync().Wait();
            var connection = new SqlConnection(ConnectionString);
            using (connection)
            {
                connection.Open();
                connection.Execute(@" create table Users (Id int IDENTITY(1,1) not null, Name nvarchar(100) not null, Age int not null, ScheduledDayOff int null, CreatedDate datetime DEFAULT(getdate())) ");
                connection.Execute(@" create table Car (CarId int IDENTITY(1,1) not null, Id int null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table BigCar (CarId bigint IDENTITY(2147483650,1) not null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table City (Name nvarchar(100) not null, Population int not null) ");
                connection.Execute(@" CREATE SCHEMA Log; ");
                connection.Execute(@" create table Log.CarLog (Id int IDENTITY(1,1) not null, LogNotes nvarchar(100) NOT NULL) ");
                connection.Execute(@" CREATE TABLE [dbo].[GUIDTest]([Id] [uniqueidentifier] NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_GUIDTest] PRIMARY KEY CLUSTERED ([Id] ASC))");
                connection.Execute(@" create table StrangeColumnNames (ItemId int IDENTITY(1,1) not null Primary Key, word nvarchar(100) not null, colstringstrangeword nvarchar(100) not null, KeywordedProperty nvarchar(100) null)");
                connection.Execute(@" create table TypeMapColumnNames (ItemId int IDENTITY(1,1) not null Primary Key, typemappedcolumn nvarchar(100) not null)");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id int not null Primary Key, Name nvarchar(100) not null, Age int not null) ");
                connection.Execute(@" create table IgnoreColumns (Id int IDENTITY(1,1) not null Primary Key, IgnoreInsert nvarchar(100) null, IgnoreUpdate nvarchar(100) null, IgnoreSelect nvarchar(100)  null, IgnoreAll nvarchar(100) null) ");
                connection.Execute(@" CREATE TABLE GradingScale ([ScaleID] [int] IDENTITY(1,1) NOT NULL, [AppID] [int] NULL, [ScaleName] [nvarchar](50) NOT NULL, [IsDefault] [bit] NOT NULL)");
                connection.Execute(@" CREATE TABLE KeyMaster ([Key1] [int] NOT NULL, [Key2] [int] NOT NULL, CONSTRAINT [PK_KeyMaster] PRIMARY KEY CLUSTERED ([Key1] ASC, [Key2] ASC))");
                connection.Execute(@" CREATE TABLE [dbo].[stringtest]([stringkey] [varchar](50) NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_stringkey] PRIMARY KEY CLUSTERED ([stringkey] ASC))");
            }
        }
        public IDbConnection GetOpenConnection()
        {
            var connection = new SqlConnection(ConnectionString);
            ExtendedCRUD.SetDialect(ExtendedCRUD.Dialect.SQLServer);
            connection.Open();
            return connection;
        }

        public void Dispose()
        {
            Container.StopAsync().Wait();
        }
    }
}
