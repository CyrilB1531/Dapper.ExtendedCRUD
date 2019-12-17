namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    [Table("CarLog", Schema = "Log")]
    public class CarLog
    {
        public int Id { get; set; }
        public string LogNotes { get; set; }
    }
}
