using System;

namespace Dapper.ExtendedCRUDBaseTests
{
    public interface ITestClassFixture : IDisposable
    {
        bool CanProcessTestsWithSchema { get; }
        bool CanProcessTestsWithArrays { get; }
        string Encapsulate(string value);
    }
}
