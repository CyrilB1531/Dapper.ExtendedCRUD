using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper.ExtendedCRUDBaseTests.Datas;
using Xunit;

namespace Dapper.ExtendedCRUDBaseTests
{
    public abstract partial class TestSuite
    {
        protected abstract IDbConnection GetOpenConnection();

        private ITestClassFixture _fixture;

        protected TestSuite(ITestClassFixture fixture)
        {
            _fixture = fixture;
            ExtendedCRUD.AddTypeHandler(typeof(TypeMapColumnName), TypeMapColumnNamesHandler.Instance);
        }

        //basic tests
        [Fact]
        public void TestInsertWithSpecifiedTableName()
        {
            using (var connection = GetOpenConnection())
            {

                var id = connection.Insert(new User { Name = "TestInsertWithSpecifiedTableName", Age = 10 });
                var user = connection.Get<User>(id);
                Assert.Equal("TestInsertWithSpecifiedTableName", user.Name);
                connection.Delete<User>(id);

            }
        }

        [Fact]
        public void TestMassInsert()
        {
            //With cached strinb builder, this tests runs 2.5X faster (From 400ms to 180ms)
            using (var connection = GetOpenConnection())
            using (var transaction = connection.BeginTransaction())
            {
                for (int i = 0; i < 1000; i++)
                {
                    connection.Insert(new User { Name = $"Name #{i}", Age = i }, transaction);
                }
            }
        }

        [Fact]
        public void TestMassUpdate() //356
        {
            //With cached strinb builder, this tests runs 2.5X faster (From 375ms to 140ms)
            using (var connection = GetOpenConnection())
            using (var transaction = connection.BeginTransaction())
            {
                var id = connection.Insert(new User { Name = "New User", Age = 0 }, transaction);
                var user = connection.Get<User>(id, transaction);

                for (int i = 1; i <= 1000; i++)
                {
                    user.Age = i;
                    connection.Update(user, transaction);
                }
            }
        }

        [Fact]
        public void TestInsertUsingBigIntPrimaryKey()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert<long, BigCar>(new BigCar { Make = "Big", Model = "Car" });
                connection.Delete<BigCar>(id);

            }
        }

        [Fact]
        public void TestInsertUsingGenericLimitedFields()
        {
            using (var connection = GetOpenConnection())
            {
                //arrange
                var user = new User { Name = "User1", Age = 10, ScheduledDayOff = DayOfWeek.Friday };

                //act
                var id = connection.Insert<int?, UserEditableSettings>(user);

                //assert
                var insertedUser = connection.Get<User>(id);
                Assert.Null(insertedUser.ScheduledDayOff);

                connection.Delete<User>(id);
            }
        }

        [Fact]
        public void TestSimpleGet()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "UserTestSimpleGet", Age = 10 });
                var user = connection.Get<User>(id);
                Assert.Equal("UserTestSimpleGet", user.Name);
                connection.Delete<User>(id);

            }
        }

        [Fact]
        public void TestDeleteById()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "UserTestDeleteById", Age = 10 });
                connection.Delete<User>(id);
                Assert.Null(connection.Get<User>(id));
            }
        }

        [Fact]
        public void TestDeleteByObject()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestDeleteByObject", Age = 10 });
                var user = connection.Get<User>(id);
                connection.Delete(user);
                Assert.Null(connection.Get<User>(id));
            }
        }

        [Fact]
        public void TestSimpleGetList()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestSimpleGetList1", Age = 10 });
                connection.Insert(new User { Name = "TestSimpleGetList2", Age = 10 });
                var user = connection.GetList<User>(new { });
                Assert.Equal(2, user.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestFilteredGetList()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestFilteredGetList1", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetList2", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetList3", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetList4", Age = 11 });

                var user = connection.GetList<User>(new { Age = 10 });
                Assert.Equal(3, user.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }


        [Fact]
        public void TestFilteredGetListWithMultipleKeys()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new KeyMaster { Key1 = 1, Key2 = 1 });
                connection.Insert(new KeyMaster { Key1 = 1, Key2 = 2 });
                connection.Insert(new KeyMaster { Key1 = 1, Key2 = 3 });
                connection.Insert(new KeyMaster { Key1 = 2, Key2 = 4 });

                var keyMasters = connection.GetList<KeyMaster>(new { Key1 = 1 });
                Assert.Equal(3, keyMasters.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("KeyMaster")}");
            }
        }


        [Fact]
        public void TestFilteredWithSQLGetList()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestFilteredWithSQLGetList1", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredWithSQLGetList2", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredWithSQLGetList3", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredWithSQLGetList4", Age = 11 });

                var user = connection.GetList<User>($"where {_fixture.Encapsulate("Name")} like 'TestFilteredWithSQLGetList%' and {_fixture.Encapsulate("Age")} = 10");
                Assert.Equal(3, user.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestGetListWithNullWhere()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestGetListWithNullWhere", Age = 10 });
                var user = connection.GetList<User>(null);
                Assert.Single(user);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestGetListWithoutWhere()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestGetListWithoutWhere", Age = 10 });
                var user = connection.GetList<User>();
                Assert.Single(user);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestsGetListWithParameters()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    connection.Insert(new User {Name = "TestsGetListWithParameters1", Age = 10});
                    connection.Insert(new User {Name = "TestsGetListWithParameters2", Age = 10});
                    connection.Insert(new User {Name = "TestsGetListWithParameters3", Age = 10});
                    connection.Insert(new User {Name = "TestsGetListWithParameters4", Age = 11});

                    var user = connection.GetList<User>($"where {_fixture.Encapsulate("Age")} > @Age", new {Age = 10});
                    Assert.Single(user);
                }
                finally
                {
                    connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
                }
            }
        }

        [Fact]
        public void TestGetWithReadonlyProperty()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestGetWithReadonlyProperty", Age = 10 });
                var user = connection.Get<User>(id);
                Assert.Equal(DateTime.Now.Year, user.CreatedDate.Year);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestInsertWithReadonlyProperty()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestInsertWithReadonlyProperty", Age = 10, CreatedDate = new DateTime(2001, 1, 1) });
                var user = connection.Get<User>(id);
                //the date can't be 2001 - it should be the autogenerated date from the database
                Assert.Equal(DateTime.Now.Year, user.CreatedDate.Year);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestUpdateWithReadonlyProperty()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestUpdateWithReadonlyProperty", Age = 10 });
                var user = connection.Get<User>(id);
                user.Age = 11;
                user.CreatedDate = new DateTime(2001, 1, 1);
                connection.Update(user);
                user = connection.Get<User>(id);
                //don't allow changing created date since it has a readonly attribute
                Assert.Equal(DateTime.Now.Year, user.CreatedDate.Year);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestGetWithNotMappedProperty()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestGetWithNotMappedProperty", Age = 10, NotMappedInt = 1000 });
                var user = connection.Get<User>(id);
                Assert.Equal(0, user.NotMappedInt);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestInsertWithNotMappedProperty()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestInsertWithNotMappedProperty", Age = 10, CreatedDate = new DateTime(2001, 1, 1), NotMappedInt = 1000 });
                var user = connection.Get<User>(id);
                Assert.Equal(0, user.NotMappedInt);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestUpdateWithNotMappedProperty()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestUpdateWithNotMappedProperty", Age = 10 });
                var user = connection.Get<User>(id);
                user.Age = 11;
                user.CreatedDate = new DateTime(2001, 1, 1);
                user.NotMappedInt = 1234;
                connection.Update(user);
                user = connection.Get<User>(id);

                Assert.Equal(0, user.NotMappedInt);

                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestInsertWithSpecifiedKey()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new Car { Make = "Honda", Model = "Civic" });
                Assert.NotNull(id);
                Assert.NotEqual(0, id);
            }
        }

        [Fact]
        public void TestInsertWithExtraPropertiesShouldSkipNonSimpleTypesAndPropertiesMarkedEditableFalse()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new Car { Make = "Honda", Model = "Civic", Users = new List<User> { new User { Age = 12, Name = "test" } } });
                Assert.NotNull(id);
                Assert.NotEqual(0, id);
            }
        }

        [Fact]
        public void TestUpdate()
        {
            using (var connection = GetOpenConnection())
            {
                var newid = connection.Insert<int, Car>(new Car { Make = "Honda", Model = "Civic" });
                var newitem = connection.Get<Car>(newid);
                newitem.Make = "Toyota";
                connection.Update(newitem);
                var updateditem = connection.Get<Car>(newid);
                Assert.Equal("Toyota", updateditem.Make);
                connection.Delete<Car>(newid);
            }
        }

        /// <summary>
        /// We expect scheduled day off to NOT be updated, since it's not a property of UserEditableSettings
        /// </summary>
        [Fact]
        public void TestUpdateUsingGenericLimitedFields()
        {
            using (var connection = GetOpenConnection())
            {
                //arrange
                var user = new User { Name = "User1", Age = 10, ScheduledDayOff = DayOfWeek.Friday };
                user.Id = connection.Insert(user) ?? 0;

                user.ScheduledDayOff = DayOfWeek.Thursday;
                var userAsEditableSettings = (UserEditableSettings)user;
                userAsEditableSettings.Name = "User++";

                connection.Update(userAsEditableSettings);

                //act
                var insertedUser = connection.Get<User>(user.Id);

                //assert
                Assert.Equal("User++", insertedUser.Name);
                Assert.Equal(DayOfWeek.Friday, insertedUser.ScheduledDayOff);

                connection.Delete<User>(user.Id);
            }
        }

        [Fact]
        public void TestDeleteByObjectWithAttributes()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new Car { Make = "Honda", Model = "Civic" });
                var car = connection.Get<Car>(id);
                connection.Delete(car);
                Assert.Null(connection.Get<Car>(id));
            }
        }

        [Fact]
        public void TestDeleteByMultipleKeyObjectWithAttributes()
        {
            using (var connection = GetOpenConnection())
            {
                var keyMaster = new KeyMaster { Key1 = 1, Key2 = 2 };
                connection.Insert(keyMaster);
                var car = connection.Get<KeyMaster>(new { Key1 = 1, Key2 = 2 });
                connection.Delete(car);
                Assert.Null(connection.Get<KeyMaster>(keyMaster));
            }
        }

        [Fact]
        public void TestComplexTypesMarkedEditableAreSaved()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert<int, User>(new User { Name = "User", Age = 11, ScheduledDayOff = DayOfWeek.Friday });
                var user1 = connection.Get<User>(id);
                Assert.Equal(DayOfWeek.Friday, user1.ScheduledDayOff);
                connection.Delete(user1);
            }
        }

        [Fact]
        public void TestNullableSimpleTypesAreSaved()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert<int, User1>(new User1 { Name = "User", Age = 11, ScheduledDayOff = 2 });
                var user1 = connection.Get<User1>(id);
                Assert.Equal(2, user1.ScheduledDayOff);
                connection.Delete(user1);
            }
        }

        [SkippableFact]
        public void TestInsertIntoDifferentSchema()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithSchema);
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new CarLog { LogNotes = "blah blah blah" });
                Assert.NotNull(id);
                Assert.NotEqual(0, id);
                connection.Delete<CarLog>(id);

            }
        }

        [SkippableFact]
        public void TestGetFromDifferentSchema()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithSchema);
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new CarLog { LogNotes = "TestGetFromDifferentSchema" });
                var carlog = connection.Get<CarLog>(id);
                Assert.Equal("TestGetFromDifferentSchema", carlog.LogNotes);
                connection.Delete<CarLog>(id);
            }
        }

        [SkippableFact]
        public void TestTryingToGetFromTableInSchemaWithoutDataAnnotationShouldFail()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithSchema);
            using (var connection = GetOpenConnection())
            {
                try
                {
                    connection.Get<SchemalessCarLog>(1);
                }
                catch (Exception)
                {
                    //we expect to get an exception, so return
                    return;
                }

                //if we get here without throwing an exception, the test failed.
                throw new ApplicationException("Expected exception");
            }
        }

        [Fact]
        public void TestGetFromTableWithNonIntPrimaryKey()
        {
            using (var connection = GetOpenConnection())
            {
                //note - there's not support for inserts without a non-int id, so drop down to a normal execute
                connection.Execute($"INSERT INTO {_fixture.Encapsulate("City")} ({_fixture.Encapsulate("Name")}, {_fixture.Encapsulate("Population")}) VALUES ('Morgantown', 31000)");
                var city = connection.Get<City>("Morgantown");
                Assert.Equal(31000, city.Population);
            }
        }

        [Fact]
        public void TestDeleteFromTableWithNonIntPrimaryKey()
        {
            using (var connection = GetOpenConnection())
            {
                //note - there's not support for inserts without a non-int id, so drop down to a normal execute
                connection.Execute($"INSERT INTO {_fixture.Encapsulate("City")} ({_fixture.Encapsulate("Name")}, {_fixture.Encapsulate("Population")}) VALUES ('Fairmont', 18737)");
                Assert.Equal(1, connection.Delete<City>("Fairmont"));
            }
        }

        [Fact]
        public void TestNullableEnumInsert()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "Enum-y", Age = 10, ScheduledDayOff = DayOfWeek.Thursday });
                var user = connection.GetList<User>(new { Name = "Enum-y" }).FirstOrDefault() ?? new User();
                Assert.Equal(DayOfWeek.Thursday, user.ScheduledDayOff);
                connection.Delete<User>(user.Id);
            }
        }

        ////dialect test 

        //[Fact]
        //public void TestChangeDialect()
        //{
        //    SimpleCRUD.SetDialect(SimpleCRUD.Dialect.SQLServer);
        //    Assert.Equal(SimpleCRUD.Dialect.SQLServer.ToString(), SimpleCRUD.GetDialect());
        //    SimpleCRUD.SetDialect(SimpleCRUD.Dialect.PostgreSQL);
        //    Assert.Equal(SimpleCRUD.Dialect.PostgreSQL.ToString(), SimpleCRUD.GetDialect());
        //}


        //        A GUID is being created and returned on insert but never actually
        //applied to the insert query.

        //This can be seen on a table where the key
        //is a GUID and defaults to (newid()) and no GUID is provided on the
        //insert. Dapper will generate a GUID but it is not applied so the GUID is
        //generated by newid() but the Dapper GUID is returned instead which is
        //incorrect.


        //GUID primary key tests

        [SkippableFact]
        public void TestInsertIntoTableWithUnspecifiedGuidKey()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithSchema);
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert<Guid, GUIDTest>(new GUIDTest { Name = "GuidUser" });
                Assert.Equal("Guid", id.GetType().Name);
                var record = connection.Get<GUIDTest>(id);
                Assert.Equal("GuidUser", record.Name);
                connection.Delete<GUIDTest>(id);
            }
        }

        [SkippableFact]
        public void TestInsertIntoTableWithGuidKey()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithSchema);
            using (var connection = GetOpenConnection())
            {
                var guid = new Guid("1a6fb33d-7141-47a0-b9fa-86a1a1945da9");
                var id = connection.Insert<Guid, GUIDTest>(new GUIDTest { Name = "InsertIntoTableWithGuidKey", Id = guid });
                Assert.Equal(guid, id);
                connection.Delete<GUIDTest>(id);
            }
        }

        [SkippableFact]
        public void TestGetRecordWithGuidKey()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithSchema);
            using (var connection = GetOpenConnection())
            {
                var guid = new Guid("2a6fb33d-7141-47a0-b9fa-86a1a1945da9");
                connection.Insert<Guid, GUIDTest>(new GUIDTest { Name = "GetRecordWithGuidKey", Id = guid });
                var id = connection.GetList<GUIDTest>().First().Id;
                var record = connection.Get<GUIDTest>(id);
                Assert.Equal("GetRecordWithGuidKey",  record.Name);
                connection.Delete<GUIDTest>(id);

            }
        }

        [SkippableFact]
        public void TestDeleteRecordWithGuidKey()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithSchema);
            using (var connection = GetOpenConnection())
            {
                var guid = new Guid("3a6fb33d-7141-47a0-b9fa-86a1a1945da9");
                connection.Insert<Guid, GUIDTest>(new GUIDTest { Name = "DeleteRecordWithGuidKey", Id = guid });
                var id = connection.GetList<GUIDTest>().First().Id;
                connection.Delete<GUIDTest>(id);
                Assert.Null(connection.Get<GUIDTest>(id));
            }
        }
        [Fact]
        public void TestInsertIntoTableWithStringKey()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert<string, StringTest>(new StringTest { stringkey = "123xyz", name = "Bob" });
                Assert.Equal("123xyz", id);
                connection.Delete<StringTest>(id);
            }
        }

        //column attribute tests

        [Fact]
        public void TestInsertWithSpecifiedColumnName()
        {
            using (var connection = GetOpenConnection())
            {
                var itemId = connection.Insert(new StrangeColumnNames { Word = "InsertWithSpecifiedColumnName", StrangeWord = "Strange 1" });
                Assert.NotNull(itemId);
                Assert.NotEqual(0, itemId);
                connection.Delete<StrangeColumnNames>(itemId);
            }
        }

        [Fact]
        public void TestDeleteByObjectWithSpecifiedColumnName()
        {
            using (var connection = GetOpenConnection())
            {
                var itemId = connection.Insert(new StrangeColumnNames { Word = "TestDeleteByObjectWithSpecifiedColumnName", StrangeWord = "Strange 1" });
                var strange = connection.Get<StrangeColumnNames>(itemId);
                connection.Delete(strange);
                Assert.Null(connection.Get<StrangeColumnNames>(itemId));
            }
        }

        [Fact]
        public void TestSimpleGetListWithSpecifiedColumnName()
        {
            using (var connection = GetOpenConnection())
            {
                var id1 = connection.Insert(new StrangeColumnNames { Word = "TestSimpleGetListWithSpecifiedColumnName1", StrangeWord = "Strange 2", });
                var id2 = connection.Insert(new StrangeColumnNames { Word = "TestSimpleGetListWithSpecifiedColumnName2", StrangeWord = "Strange 3", });
                var strange = connection.GetList<StrangeColumnNames>(new { });
                Assert.Equal("Strange 2", strange.First().StrangeWord);
                Assert.Equal(2, strange.Count());
                connection.Delete<StrangeColumnNames>(id1);
                connection.Delete<StrangeColumnNames>(id2);
            }
        }

        [Fact]
        public void TestUpdateWithSpecifiedColumnName()
        {
            using (var connection = GetOpenConnection())
            {
                var newid = connection.Insert<int,StrangeColumnNames>(new StrangeColumnNames { Word = "Word Insert", StrangeWord = "Strange Insert" });
                var newitem = connection.Get<StrangeColumnNames>(newid);
                newitem.Word = "Word Update";
                connection.Update(newitem);
                var updateditem = connection.Get<StrangeColumnNames>(newid);
                Assert.Equal("Word Update", updateditem.Word);
                connection.Delete<StrangeColumnNames>(newid);
            }
        }

        [Fact]
        public void TestFilteredGetListWithSpecifiedColumnName()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new StrangeColumnNames { Word = "Word 5", StrangeWord = "Strange 1", });
                connection.Insert(new StrangeColumnNames { Word = "Word 6", StrangeWord = "Strange 2", });
                connection.Insert(new StrangeColumnNames { Word = "Word 7", StrangeWord = "Strange 2", });
                connection.Insert(new StrangeColumnNames { Word = "Word 8", StrangeWord = "Strange 2", });

                var strange = connection.GetList<StrangeColumnNames>(new { StrangeWord = "Strange 2" });
                Assert.Equal(3, strange.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("StrangeColumnNames")}");
            }
        }

        [Fact]
        public void TestGetListPaged()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new User { Name = "Person " + x, Age = x, CreatedDate = DateTime.Now, ScheduledDayOff = DayOfWeek.Thursday });
                    x++;
                } while (x < 30);

                var resultlist = connection.GetListPaged<User>(2, 10, null, null);
                Assert.Equal(10, resultlist.Count());
                Assert.Equal("Person 14",  resultlist.Skip(4).First().Name);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestGetListPagedWithParameters()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new User { Name = "Person " + x, Age = x, CreatedDate = DateTime.Now, ScheduledDayOff = DayOfWeek.Thursday });
                    x++;
                } while (x < 30);

                var resultlist = connection.GetListPaged<User>(1, 30, $"where {_fixture.Encapsulate("Age")} > @Age", null, new { Age = 14 });
                Assert.Equal(15,  resultlist.Count());
                Assert.Equal("Person 15",  resultlist.First().Name);
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }


        [Fact]
        public void TestGetListPagedWithSpecifiedPrimaryKey()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new StrangeColumnNames { Word = "Word " + x, StrangeWord = "Strange " + x });
                    x++;
                } while (x < 30);

                var resultlist = connection.GetListPaged<StrangeColumnNames>(2, 10, null, null);
                Assert.Equal(10,  resultlist.Count());
                Assert.Equal("Word 14", resultlist.Skip(4).First().Word);
                connection.Execute($"Delete from {_fixture.Encapsulate("StrangeColumnNames")}");
            }
        }
        [Fact]
        public void TestGetListPagedWithWhereClause()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new User { Name = "Person " + x, Age = x, CreatedDate = DateTime.Now, ScheduledDayOff = DayOfWeek.Thursday });
                    x++;
                } while (x < 30);

                var resultlist1 = connection.GetListPaged<User>(1, 3, $"Where {_fixture.Encapsulate("Name")} LIKE 'Person 2%'", $"{_fixture.Encapsulate("Age")} desc");
                Assert.Equal(3,  resultlist1.Count());

                var resultlist = connection.GetListPaged<User>(2, 3, $"Where {_fixture.Encapsulate("Name")} LIKE 'Person 2%'", $"{_fixture.Encapsulate("Age")} desc");
                Assert.Equal(3,  resultlist.Count());
                Assert.Equal("Person 25", resultlist.Skip(1).First().Name);

                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestDeleteListWithWhereClause()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new User { Name = "Person " + x, Age = x, CreatedDate = DateTime.Now, ScheduledDayOff = DayOfWeek.Thursday });
                    x++;
                } while (x < 30);

                connection.DeleteList<User>($"Where {_fixture.Encapsulate("Age")} > 9");
                var resultlist = connection.GetList<User>();
                Assert.Equal(10, resultlist.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestDeleteListWithWhereObject()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new User { Name = "Person " + x, Age = x, CreatedDate = DateTime.Now, ScheduledDayOff = DayOfWeek.Thursday });
                    x++;
                } while (x < 10);

                connection.DeleteList<User>(new { Age = 9 });
                var resultlist = connection.GetList<User>();
                Assert.Equal(9,  resultlist.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public void TestDeleteListWithParameters()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    int x = 1;
                    do
                    {
                        connection.Insert(new User
                        {
                            Name = "Person " + x, Age = x, CreatedDate = DateTime.Now,
                            ScheduledDayOff = DayOfWeek.Thursday
                        });
                        x++;
                    } while (x < 10);

                    connection.DeleteList<User>($"where {_fixture.Encapsulate("Age")} >= @Age", new {Age = 9});
                    var resultlist = connection.GetList<User>();
                    Assert.Equal(8, resultlist.Count());
                }
                finally
                {
                    connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
                }
            }
        }

        [Fact]
        public void TestRecordCountWhereClause()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new User { Name = "Person " + x, Age = x, CreatedDate = DateTime.Now, ScheduledDayOff = DayOfWeek.Thursday });
                    x++;
                } while (x < 30);

                var resultlist = connection.GetList<User>();
                Assert.Equal(30, resultlist.Count());
                Assert.Equal(30,  connection.RecordCount<User>());

                Assert.Equal(2,  connection.RecordCount<User>($"where {_fixture.Encapsulate("Age")} = 10 or {_fixture.Encapsulate("Age")} = 11"));


                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }

        }

        [Fact]
        public void TestRecordCountWhereObject()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new User { Name = "Person " + x, Age = x, CreatedDate = DateTime.Now, ScheduledDayOff = DayOfWeek.Thursday });
                    x++;
                } while (x < 30);

                var resultlist = connection.GetList<User>();
                Assert.Equal(30, resultlist.Count());
                Assert.Equal(30, connection.RecordCount<User>());

                Assert.Equal(1, connection.RecordCount<User>(new { Age = 10 }));


                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }

        }

        [Fact]
        public void TestRecordCountParameters()
        {
            using (var connection = GetOpenConnection())
            {
                int x = 0;
                do
                {
                    connection.Insert(new User { Name = "Person " + x, Age = x, CreatedDate = DateTime.Now, ScheduledDayOff = DayOfWeek.Thursday });
                    x++;
                } while (x < 30);

                var resultlist = connection.GetList<User>();
                Assert.Equal(30, resultlist.Count());
                Assert.Equal(14, connection.RecordCount<User>($"where {_fixture.Encapsulate("Age")} > 15"));


                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }

        }

        [Fact]
        public void TestInsertWithSpecifiedPrimaryKey()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new UserWithoutAutoIdentity { Id = 999, Name = "User999", Age = 10 });
                Assert.Equal(999, id);
                var user = connection.Get<UserWithoutAutoIdentity>(999);
                Assert.Equal("User999", user.Name);
                connection.Execute($"Delete from {_fixture.Encapsulate("UserWithoutAutoIdentity")}");
            }
        }

        [Fact]
        public void TestGetListNullableWhere()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestGetListWithoutWhere", Age = 10, ScheduledDayOff = DayOfWeek.Friday });
                connection.Insert(new User { Name = "TestGetListWithoutWhere", Age = 10 });

                //test with null property
                var list = connection.GetList<User>(new { ScheduledDayOff = (DayOfWeek?)null });
                Assert.Single(list);


                // test with db.null value
                list = connection.GetList<User>(new { ScheduledDayOff = DBNull.Value });
                Assert.Single(list);

                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        //ignore attribute tests
        //i cheated here and stuffed all of these in one test
        //didn't implement in postgres or mysql tests yet
        [Fact]
        public void IgnoreProperties()
        {
            using (var connection = GetOpenConnection())
            {
                var itemId = connection.Insert(new IgnoreColumns { IgnoreInsert = "OriginalInsert", IgnoreUpdate = "OriginalUpdate", IgnoreSelect = "OriginalSelect", IgnoreAll = "OriginalAll" });
                var item = connection.Get<IgnoreColumns>(itemId);
                //verify insert column was ignored
                Assert.Null(item.IgnoreInsert);

                //verify select value wasn't selected 
                Assert.Null(item.IgnoreSelect);

                //verify the column is really there via straight dapper
                var fromDapper = connection.Query<IgnoreColumns>($"Select * from {_fixture.Encapsulate("IgnoreColumns")} where {_fixture.Encapsulate("Id")} = @Id", new { Id = itemId }).First();
                Assert.Equal("OriginalSelect",  fromDapper.IgnoreSelect);

                //change value and update
                item.IgnoreUpdate = "ChangedUpdate";
                connection.Update(item);

                //verify that update didn't take effect
                item = connection.Get<IgnoreColumns>(itemId);
                Assert.Equal("OriginalUpdate",  item.IgnoreUpdate);

                var allColumnDapper = connection.Query<IgnoreColumns>($"Select {_fixture.Encapsulate("IgnoreAll")} from {_fixture.Encapsulate("IgnoreColumns")} where {_fixture.Encapsulate("Id")} = @Id", new { Id = itemId }).First();
                Assert.Null(allColumnDapper.IgnoreAll);

                connection.Delete<IgnoreColumns>(itemId);
            }
        }

        [Fact]
        public void TestDeleteByMultipleKeyObject()
        {
            using (var connection = GetOpenConnection())
            {
                var keyMaster = new KeyMaster { Key1 = 1, Key2 = 2 };
                connection.Insert(keyMaster);
                connection.Get<KeyMaster>(keyMaster);
                connection.Delete<KeyMaster>(new { Key1 = 1, Key2 = 2 });
                Assert.Null(connection.Get<KeyMaster>(keyMaster));
                connection.Delete(keyMaster);
            }
        }

        // TypeHandler Tests
        [Fact]
        public void TestInsertWithTypeHandledColumn()
        {
            using (var connection = GetOpenConnection())
            {
                var itemId = connection.Insert(new TypeMapColumnNames { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName 1" } });
                Assert.NotNull(itemId);
                Assert.NotEqual(0, itemId);
                connection.Delete<TypeMapColumnNames>(itemId);
            }
        }

        [Fact]
        public void TestSimpleGetListWithTypeHandledColumn()
        {
            using (var connection = GetOpenConnection())
            {
                var id1 = connection.Insert(new TypeMapColumnNames { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName 2" } });
                var id2 = connection.Insert(new TypeMapColumnNames { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName 3" } });
                var mappedColumns = connection.GetList<TypeMapColumnNames>(new { });
                Assert.Equal("TypeMapColumnName 2",  mappedColumns.First().TypeMappedColumn.Content);
                Assert.Equal(2,   mappedColumns.Count());
                connection.Delete<TypeMapColumnNames>(id1);
                connection.Delete<TypeMapColumnNames>(id2);
            }
        }

        [Fact]
        public void TestUpdateWithTypeHandledColumn()
        {
            using (var connection = GetOpenConnection())
            {
                var newid = connection.Insert<int, TypeMapColumnNames>(new TypeMapColumnNames { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName Insert" } });
                var newitem = connection.Get<TypeMapColumnNames>(newid);
                newitem.TypeMappedColumn.Content = "TypeMapColumnName Update";
                connection.Update(newitem);
                var updateditem = connection.Get<TypeMapColumnNames>(newid);
                Assert.Equal("TypeMapColumnName Update",  updateditem.TypeMappedColumn.Content);
                connection.Delete<TypeMapColumnNames>(newid);
            }
        }

        [Fact]
        public void TestFilteredGetListWithTypeHandledColumn()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new TypeMapColumnNames { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName 1" } });
                connection.Insert(new TypeMapColumnNames { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName 2" } });
                connection.Insert(new TypeMapColumnNames { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName 2" } });
                connection.Insert(new TypeMapColumnNames { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName 2" } });

                var strange = connection.GetList<TypeMapColumnNames>(new { TypeMappedColumn = new TypeMapColumnName { Content = "TypeMapColumnName 2" } });
                Assert.Equal(3,  strange.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("TypeMapColumnNames")}");
            }
        }

        // Arrays Tests
        [SkippableFact]
        public void TestInsertWithArraysColumn()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithArrays);
            using (var connection = GetOpenConnection())
            {
                var itemId = connection.Insert(new IntegerArrays { IntegerArray = new []{1,2} });
                Assert.NotNull(itemId);
                Assert.NotEqual(0, itemId);
                connection.Delete<IntegerArrays>(itemId);
            }
        }

        [SkippableFact]
        public void TestSimpleGetListWithArraysColumn()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithArrays);
            using (var connection = GetOpenConnection())
            {
                var id1 = connection.Insert(new IntegerArrays { IntegerArray = new []{3,4} });
                var id2 = connection.Insert(new IntegerArrays { IntegerArray = new []{5,6} });
                var mappedColumns = connection.GetList<IntegerArrays>(new { });
                Assert.Equal(2,  mappedColumns.First().IntegerArray.Length);
                Assert.Equal(3,  mappedColumns.First().IntegerArray[0]);
                Assert.Equal(4,  mappedColumns.First().IntegerArray[1]);
                Assert.Equal(2,   mappedColumns.Count());
                connection.Delete<IntegerArrays>(id1);
                connection.Delete<IntegerArrays>(id2);
            }
        }

        [SkippableFact]
        public void TestUpdateWithIntegerArraysColumn()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithArrays);
            using (var connection = GetOpenConnection())
            {
                var newid = connection.Insert<int, IntegerArrays>(new IntegerArrays { IntegerArray = new []{1,2} });
                var newitem = connection.Get<IntegerArrays>(newid);
                newitem.IntegerArray = new []{3,4};
                connection.Update(newitem);
                var updateditem = connection.Get<IntegerArrays>(newid);
                Assert.Equal(2,  updateditem.IntegerArray.Length);
                Assert.Equal(3,  updateditem.IntegerArray[0]);
                Assert.Equal(4,  updateditem.IntegerArray[1]);
                connection.Delete<IntegerArrays>(newid);
            }
        }

        [SkippableFact]
        public void TestFilteredGetListWithIntegerArraysColumn()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithArrays);
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new IntegerArrays { IntegerArray = new []{1,2}});
                connection.Insert(new IntegerArrays { IntegerArray = new []{3,4}});
                connection.Insert(new IntegerArrays { IntegerArray = new []{5,6}});

                var integerArrays = connection.GetList<IntegerArrays>( );
                Assert.Equal(3,  integerArrays.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("IntegerArraysTest")}");
            }
        }
    }
}
