﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dapper
{

    internal static class TypeExtension
    {
        //You can't insert or update complex types. Lets filter them out.
        public static bool IsSimpleType(this Type type, ExtendedCRUD.Dialect dialect)
        {
            var underlyingType = Nullable.GetUnderlyingType(type);
            type = underlyingType ?? type;
            var simpleTypes = new List<Type>
            {
                typeof(byte),
                typeof(sbyte),
                typeof(short),
                typeof(ushort),
                typeof(int),
                typeof(uint),
                typeof(long),
                typeof(ulong),
                typeof(float),
                typeof(double),
                typeof(decimal),
                typeof(bool),
                typeof(string),
                typeof(char),
                typeof(Guid),
                typeof(DateTime),
                typeof(DateTimeOffset),
                typeof(TimeSpan),
                typeof(byte[])
            };
            return simpleTypes.Contains(type) || type.IsEnum || dialect == ExtendedCRUD.Dialect.PostgreSQL &&
                   type.IsArray && simpleTypes.Any(a => a.FullName + "[]" == type.FullName);
        }

        public static string CacheKey(this IEnumerable<PropertyInfo> props)
        {
            return string.Join(",", props.Select(p => p.DeclaringType.FullName + "." + p.Name).ToArray());
        }
    }

}