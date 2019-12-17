namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    public class IgnoreColumns
    {
        [Key]
        public int Id { get; set; }
        [IgnoreInsert]
        public string IgnoreInsert { get; set; }
        [IgnoreUpdate]
        public string IgnoreUpdate { get; set; }
        [IgnoreSelect]
        public string IgnoreSelect { get; set; }
        [IgnoreInsert]
        [IgnoreUpdate]
        [IgnoreSelect]
        public string IgnoreAll { get; set; }
    }
}
