namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    public class StrangeColumnNames
    {
        [Key]
        [Column("ItemId")]
        public int Id { get; set; }
        public string Word { get; set; }
        [Column("colstringstrangeword")]
        public string StrangeWord { get; set; }
        [Column("KeywordedProperty")]
        public string Select { get; set; }
        [Editable(false)]
        public string ExtraProperty { get; set; }
    }
}
