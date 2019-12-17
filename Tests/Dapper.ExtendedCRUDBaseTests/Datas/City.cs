namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    public class City
    {
        [Key]
        public string Name { get; set; }
        public int Population { get; set; }
    }
}
