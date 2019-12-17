namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    [Table("IntegerArraysTest")]
    public class IntegerArrays
    {
        [Key, Column("integerarraykey")]
        public int IntegerArrayKey { get; set; }

        [Column("integerarray")]
        public int[] IntegerArray { get; set; }
    }
}
