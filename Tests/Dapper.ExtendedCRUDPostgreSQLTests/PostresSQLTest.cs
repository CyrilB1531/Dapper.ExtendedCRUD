using System;
using System.Data;
using Dapper.ExtendedCRUDBaseTests;
using Xunit;

namespace Dapper.ExtendedCRUDPostgreSQLTests
{
    public class PostresSQLTest : TestSuite, IDisposable, IClassFixture<PostresSQLTestFixture>
    {
        private PostresSQLTestFixture _fixture;
        public PostresSQLTest(PostresSQLTestFixture fixture) : base(fixture)
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
