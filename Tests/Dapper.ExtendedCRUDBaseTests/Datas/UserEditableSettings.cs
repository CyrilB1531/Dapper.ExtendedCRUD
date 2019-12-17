namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    [Table("Users")]
    public class UserEditableSettings
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }
}
