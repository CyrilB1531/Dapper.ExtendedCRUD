namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    public class BigCar
    {
        #region DatabaseFields
        //System.ComponentModel.DataAnnotations.Key
        [Key]
        public long CarId { get; set; }
        public string Make { get; set; }
        public string Model { get; set; }
        #endregion

    }
}
