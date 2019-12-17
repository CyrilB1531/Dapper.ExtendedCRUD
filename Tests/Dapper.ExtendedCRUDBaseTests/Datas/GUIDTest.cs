using System;

namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    public class GUIDTest
    {
        [Key]
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
}
