namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    [Table("Users")]
    public class User1
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public int? ScheduledDayOff { get; set; }
    }
}
