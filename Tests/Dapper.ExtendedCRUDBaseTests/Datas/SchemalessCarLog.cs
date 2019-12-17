namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    /// <summary>
    /// This class should be used for failing tests, since no schema is specified and 'CarLog' is not on dbo
    /// </summary>
    [Table("CarLog")]
    public class SchemalessCarLog
    {
        public int Id { get; set; }
        public string LogNotes { get; set; }
    }
}
