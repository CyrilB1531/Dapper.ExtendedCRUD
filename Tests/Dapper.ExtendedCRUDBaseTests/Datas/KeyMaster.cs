namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    public class KeyMaster
    {
        [Key, Required]
        public int Key1 { get; set; }
        [Key, Required]
        public int Key2 { get; set; }
    }
}
