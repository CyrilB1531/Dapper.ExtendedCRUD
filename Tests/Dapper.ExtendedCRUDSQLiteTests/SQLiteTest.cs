using System;
using System.Data;
using Dapper.ExtendedCRUDBaseTests;
using Xunit;

namespace Dapper.ExtendedCRUDSQLiteTests
{
    public class SQLiteTest : TestSuite, IDisposable, IClassFixture<SQLiteTestFixture>
    {
        private readonly SQLiteTestFixture _fixture;
        public SQLiteTest(SQLiteTestFixture fixture) : base(fixture)
        {
            _fixture = fixture;
        }

        protected override IDbConnection GetOpenConnection()
        {
            return _fixture.GetOpenConnection();
        }

        public void Dispose()
        {

        }
    }
}
