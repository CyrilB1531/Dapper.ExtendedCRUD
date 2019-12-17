using System;
using System.Linq;
using System.Threading.Tasks;
using Dapper.ExtendedCRUDBaseTests.Datas;
using Xunit;

namespace Dapper.ExtendedCRUDBaseTests
{
    public abstract partial class TestSuite
    {
#if !NET40
        //async  tests
        [Fact]
        public async Task TestMultiInsertAsync()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    await connection.InsertAsync(new User {Name = "TestMultiInsertASync1", Age = 10});
                    await connection.InsertAsync(new User {Name = "TestMultiInsertASync2", Age = 10});
                    await connection.InsertAsync(new User {Name = "TestMultiInsertASync3", Age = 10});
                    await connection.InsertAsync(new User {Name = "TestMultiInsertASync4", Age = 11});
                    //tiny wait to let the inserts happen
                    var list = connection.GetList<User>(new {Age = 10});
                    Assert.Equal(3, list.Count());
                }
                finally
                {
                    connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
                }
            }
        }

        [SkippableFact]
        public async Task MultiInsertWithGuidAsync()
        {
            Skip.IfNot(_fixture.CanProcessTestsWithSchema);
            using (var connection = GetOpenConnection())
            {
                await connection.InsertAsync<Guid, GUIDTest>(new GUIDTest { Name = "MultiInsertWithGuidAsync" });
                await connection.InsertAsync<Guid, GUIDTest>(new GUIDTest { Name = "MultiInsertWithGuidAsync" });
                await connection.InsertAsync<Guid, GUIDTest>(new GUIDTest { Name = "MultiInsertWithGuidAsync" });
                await connection.InsertAsync<Guid, GUIDTest>(new GUIDTest { Name = "MultiInsertWithGuidAsync" });
                //tiny wait to let the inserts happen
                System.Threading.Thread.Sleep(300);
                var list = connection.GetList<GUIDTest>(new { Name = "MultiInsertWithGuidAsync" });
                Assert.Equal(4,list.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("GUIDTest")}");
            }
        }

        [Fact]
        public async Task TestSimpleGetAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestSimpleGetAsync", Age = 10 });
                var user = await connection.GetAsync<User>(id);
                Assert.Equal("TestSimpleGetAsync",user.Name);
                connection.Delete<User>(id);
            }
        }

        [Fact]
        public async Task TestMultipleKeyGetAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var keyMaster = new KeyMaster { Key1 = 1, Key2 = 2 };
                connection.Insert(keyMaster);
                var result = await connection.GetAsync<KeyMaster>(new { Key1 = 1, Key2 = 2 });
                Assert.Equal(1,result.Key1);
                Assert.Equal(2,result.Key2);
                connection.Delete(keyMaster);
            }
        }

        [Fact]
        public async Task TestDeleteByIdAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "UserAsyncDelete", Age = 10 });
                await connection.DeleteAsync<User>(id);
                //tiny wait to let the delete happen
                System.Threading.Thread.Sleep(300);
                Assert.Null(connection.Get<User>(id));
            }
        }

        [Fact]
        public async Task TestDeleteByObjectAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var id = connection.Insert(new User { Name = "TestDeleteByObjectAsync", Age = 10 });
                var user = connection.Get<User>(id);
                await connection.DeleteAsync(user);
                Assert.Null(connection.Get<User>(id));
                connection.Delete<User>(id);
            }
        }

        [Fact]
        public async Task TestSimpleGetListAsync()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestSimpleGetListAsync1", Age = 10 });
                connection.Insert(new User { Name = "TestSimpleGetListAsync2", Age = 10 });
                var user =await connection.GetListAsync<User>(new { });
                Assert.Equal(2, user.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public async Task TestFilteredGetListAsync()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestFilteredGetListAsync1", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetListAsync2", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetListAsync3", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetListAsync4", Age = 11 });

                var user =await connection.GetListAsync<User>(new { Age = 10 });
                Assert.Equal(3,user.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public async Task TestFilteredGetListParametersAsync()
        {
            using (var connection = GetOpenConnection())
            {
                connection.Insert(new User { Name = "TestFilteredGetListParametersAsync1", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetListParametersAsync2", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetListParametersAsync3", Age = 10 });
                connection.Insert(new User { Name = "TestFilteredGetListParametersAsync4", Age = 11 });

                var user =await connection.GetListAsync<User>($"where {_fixture.Encapsulate("Age")} = @Age", new { Age = 10 });
                Assert.Equal(3,user.Count());
                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }
        }

        [Fact]
        public async Task TestRecordCountAsync()
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
                Assert.Equal(30,resultlist.Count());
                Assert.Equal(30,await connection.RecordCountAsync<User>());

                Assert.Equal(2,await connection.RecordCountAsync<User>($"where {_fixture.Encapsulate("Age")} = 10 or {_fixture.Encapsulate("Age")} = 11"));


                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }

        }

        [Fact]
        public async Task TestRecordCountByObjectAsync()
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
                Assert.Equal(30,resultlist.Count());
                Assert.Equal(30,await connection.RecordCountAsync<User>());

                Assert.Equal(1, await connection.RecordCountAsync<User>(new { Age = 10 }));


                connection.Execute($"Delete from {_fixture.Encapsulate("Users")}");
            }

        }

        [Fact]
        public async Task TestInsertWithSpecifiedPrimaryKeyAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var id = await connection.InsertAsync(new UserWithoutAutoIdentity { Id = 999, Name = "User999Async", Age = 10 });
                Assert.Equal(999,id);
                var user = await connection.GetAsync<UserWithoutAutoIdentity>(999);
                Assert.Equal("User999Async",user.Name);
                connection.Execute($"Delete from {_fixture.Encapsulate("UserWithoutAutoIdentity")}");
            }
        }


        [Fact]
        public async Task TestInsertWithMultiplePrimaryKeysAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var keyMaster = new KeyMaster { Key1 = 1, Key2 = 2 };
                await connection.InsertAsync(keyMaster);
                var result =await connection.GetAsync<KeyMaster>(new { Key1 = 1, Key2 = 2 });
                Assert.Equal(1,result.Key1);
                Assert.Equal(2,result.Key2);
                connection.Execute($"Delete from {_fixture.Encapsulate("KeyMaster")}");
            }
        }
        [Fact]
        public void TestInsertUsingGenericLimitedFieldsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                //arrange
                var user = new User { Name = "User1", Age = 10, ScheduledDayOff = DayOfWeek.Friday };

                //act
                var idTask = connection.InsertAsync<int?, UserEditableSettings>(user);
                idTask.Wait();
                var id = idTask.Result;

                //assert
                var insertedUser = connection.Get<User>(id);
                Assert.Null(insertedUser.ScheduledDayOff);

                connection.Delete<User>(id);
            }
        }

        /// <summary>
        /// We expect scheduled day off to NOT be updated, since it's not a property of UserEditableSettings
        /// </summary>
        [Fact]
        public void TestUpdateUsingGenericLimitedFieldsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                //arrange
                var user = new User { Name = "User1", Age = 10, ScheduledDayOff = DayOfWeek.Friday };
                user.Id = connection.Insert(user) ?? 0;

                user.ScheduledDayOff = DayOfWeek.Thursday;
                var userAsEditableSettings = (UserEditableSettings)user;
                userAsEditableSettings.Name = "User++";

                connection.UpdateAsync(userAsEditableSettings).Wait();

                //act
                var insertedUser = connection.Get<User>(user.Id);

                //assert
                Assert.Equal("User++", insertedUser.Name);
                Assert.Equal(DayOfWeek.Friday,insertedUser.ScheduledDayOff);

                connection.Delete<User>(user.Id);
            }
        }
#endif    
    }
}
