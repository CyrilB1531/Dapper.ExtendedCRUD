using System;

namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    //For .Net 4.5> [System.ComponentModel.DataAnnotations.Schema.Table("Users")]  or the attribute built into SimpleCRUD
    [Table("Users")]
    public class User : UserEditableSettings
    {
        //we modified so enums were automatically handled, we should also automatically handle nullable enums
        public DayOfWeek? ScheduledDayOff { get; set; }

        [ReadOnly(true)]
        public DateTime CreatedDate { get; set; }

        [NotMapped]
        public int NotMappedInt { get; set; }
    }

}
