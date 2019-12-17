using System.Collections.Generic;

namespace Dapper.ExtendedCRUDBaseTests.Datas
{
    public class Car
    {
        #region DatabaseFields
        //System.ComponentModel.DataAnnotations.Key
        [Key]
        public int CarId { get; set; }
        public int? Id { get; set; }
        public string Make { get; set; }
        public string Model { get; set; }
        #endregion

        #region RelatedTables
        public List<User> Users { get; set; }
        #endregion

        #region AdditionalFields
        [Editable(false)]
        public string MakeWithModel { get { return Make + " (" + Model + ")"; } }
        #endregion

    }

}
