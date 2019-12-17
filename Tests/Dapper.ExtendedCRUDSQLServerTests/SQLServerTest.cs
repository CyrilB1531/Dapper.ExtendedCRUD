using System;
using System.Data;
using Dapper.ExtendedCRUDBaseTests;
using Xunit;

namespace Dapper.ExtendedCRUDSQLServerTests
{
    public class SQLServerTest : TestSuite, IDisposable, IClassFixture<SQLServerTestFixture>
    {
        private SQLServerTestFixture _fixture;
        public SQLServerTest(SQLServerTestFixture fixture) : base(fixture)
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
