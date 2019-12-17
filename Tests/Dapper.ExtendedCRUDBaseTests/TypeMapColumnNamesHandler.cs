using System;
using System.Data;
using Dapper.ExtendedCRUDBaseTests.Datas;

namespace Dapper.ExtendedCRUDBaseTests
{
    class TypeMapColumnNamesHandler : SqlMapper.TypeHandler<TypeMapColumnName>
    {
        private TypeMapColumnNamesHandler() { }
        public static TypeMapColumnNamesHandler Instance { get; } = new TypeMapColumnNamesHandler();
        public override TypeMapColumnName Parse(object value)
        {
            var content = (string)value;
            return content == null ? null : new TypeMapColumnName{ Content = content};
        }
        public override void SetValue(IDbDataParameter parameter, TypeMapColumnName value)
        {
            if(value == null)
                parameter.Value = DBNull.Value;
            else
                parameter.Value = value.Content;
            parameter.DbType = DbType.String;
        }
    }
}
