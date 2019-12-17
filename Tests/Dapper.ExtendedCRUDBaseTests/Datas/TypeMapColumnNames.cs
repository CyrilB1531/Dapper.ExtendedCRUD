namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    public class TypeMapColumnNames
    {
        [Key]
        [Column("ItemId")]
        public int Id { get; set; }
        [Column("typemappedcolumn")]
        public TypeMapColumnName TypeMappedColumn { get; set; }
    }
}
