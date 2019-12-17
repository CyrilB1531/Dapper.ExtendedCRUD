using System.Data;
using System.IO;
using Dapper.ExtendedCRUDBaseTests;
using Microsoft.Data.Sqlite;

namespace Dapper.ExtendedCRUDSQLiteTests
{
    public class SQLiteTestFixture : ITestClassFixture
    {
        public bool CanProcessTestsWithSchema => false;
        public bool CanProcessTestsWithArrays => false;
        private const string FileName = "MyDatabase.sqlite";
        private string ConnectionString => $"Filename=./{FileName};Mode=ReadWriteCreate;";
        public string Encapsulate(string value)
        {
            return $"\"{value}\"";
        }


        public SQLiteTestFixture()
        {
            if (File.Exists(FileName))
            {
                File.Delete(FileName);
            }
            var connection = new SqliteConnection(ConnectionString);
            using (connection)
            {
                connection.Open();
                connection.Execute(@" create table Users (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name nvarchar(100) not null, Age int not null, ScheduledDayOff int null, CreatedDate datetime default current_timestamp ) ");
                connection.Execute(@" create table Car (CarId INTEGER PRIMARY KEY AUTOINCREMENT, Id INTEGER null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table BigCar (CarId INTEGER PRIMARY KEY AUTOINCREMENT, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" insert into BigCar (CarId,Make,Model) Values (2147483649,'car','car') ");
                connection.Execute(@" create table City (Name nvarchar(100) not null, Population int not null) ");
                connection.Execute(@" CREATE TABLE GUIDTest([Id] [uniqueidentifier] NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_GUIDTest] PRIMARY KEY  ([Id] ASC))");
                connection.Execute(@" create table StrangeColumnNames (ItemId INTEGER PRIMARY KEY AUTOINCREMENT, word nvarchar(100) not null, colstringstrangeword nvarchar(100) not null, KeywordedProperty nvarchar(100) null) ");
                connection.Execute(@" create table TypeMapColumnNames (ItemId INTEGER PRIMARY KEY AUTOINCREMENT, typemappedcolumn nvarchar(100) not null)");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id INTEGER PRIMARY KEY, Name nvarchar(100) not null, Age int not null) ");
                connection.Execute(@" create table IgnoreColumns (Id INTEGER PRIMARY KEY AUTOINCREMENT, IgnoreInsert nvarchar(100) null, IgnoreUpdate nvarchar(100) null, IgnoreSelect nvarchar(100)  null, IgnoreAll nvarchar(100) null) ");
                connection.Execute(@" CREATE TABLE KeyMaster (Key1 INTEGER NOT NULL, Key2 INTEGER NOT NULL, PRIMARY KEY ([Key1], [Key2]))");
                connection.Execute(@" CREATE TABLE stringtest (stringkey nvarchar(50) NOT NULL,name nvarchar(50) NOT NULL, PRIMARY KEY ([stringkey] ASC))");

            }
        }
        public IDbConnection GetOpenConnection()
        {
            //if (_fixture.Dialect == SimpleCRUD.Dialect.PostgreSQL)
            //{
            //    connection = new NpgsqlConnection(String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};", "localhost", "5432", "postgres", "postgrespass", "testdb"));
            //    SimpleCRUD.SetDialect(SimpleCRUD.Dialect.PostgreSQL);
            //}
            //else if (_fixture.Dialect == SimpleCRUD.Dialect.SQLite)
            //{
            var connection = new SqliteConnection(ConnectionString);
            ExtendedCRUD.SetDialect(ExtendedCRUD.Dialect.SQLite);
            //}
            //else if (_fixture.Dialect == SimpleCRUD.Dialect.MySQL)
            //{
            //    connection = new MySqlConnection(String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};", "localhost", "3306", "root", "admin", "testdb"));
            //    SimpleCRUD.SetDialect(SimpleCRUD.Dialect.MySQL);
            //}
            //else
            //{
            //    connection = new SqlConnection(@"Data Source = .\sqlexpress;Initial Catalog=DapperSimpleCrudTestDb;Integrated Security=True;MultipleActiveResultSets=true;");
            //    SimpleCRUD.SetDialect(SimpleCRUD.Dialect.SQLServer);
            //}

            connection.Open();
            return connection;
        }

        public void Dispose()
        {
        }
    }
}
