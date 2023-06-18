// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections;
using Npgsql.EntityFrameworkCore.PostgreSQL.TestUtilities;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query;

public class BitArrayQueryTest : QueryTestBase<BitArrayQueryTest.BitArrayQueryFixture>
{
    public BitArrayQueryTest(BitArrayQueryFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    [ConditionalFact]
    public async Task Length()
    {
        await using var ctx = CreateContext();
        var count = ctx.Entities.Count(e => e.BitArray.Length == 4);
        Assert.Equal(4, count);

        AssertSql(
"""
SELECT count(*)::int
FROM "Entities" AS e
WHERE length(e."BitArray") = 4
""");
    }

    [ConditionalFact]
    public async Task Count()
    {
        await using var ctx = CreateContext();
        var count = ctx.Entities.Count(e => e.BitArray.Count == 4);
        Assert.Equal(4, count);

        AssertSql(
"""
SELECT count(*)::int
FROM "Entities" AS e
WHERE length(e."BitArray") = 4
""");
    }

    [ConditionalFact]
    public async Task Not()
    {
        await using var ctx = CreateContext();
        var count = ctx.Entities.Count(e => e.BitArray.Not() == new BitArray(new[] { false, false, false, false }));
        Assert.Equal(1, count);

        AssertSql(
"""
SELECT count(*)::int
FROM "Entities" AS e
WHERE ~e."BitArray" = B'0000'
""");
    }

    [ConditionalFact]
    public async Task And()
    {
        await using var ctx = CreateContext();
        var count = ctx.Entities.Count(
            e => e.BitArray
                    .And(new BitArray(new[] { true, true, false, false }))
                == new BitArray(new[] { true, false, false, false }));
        Assert.Equal(1, count);

        AssertSql(
"""
SELECT count(*)::int
FROM "Entities" AS e
WHERE e."BitArray" & B'1100' = B'1000'
""");
    }

    [ConditionalFact]
    public async Task Or()
    {
        await using var ctx = CreateContext();
        var count = ctx.Entities.Count(
            e => e.BitArray
                    .Or(new BitArray(new[] { true, false, false, false }))
                == new BitArray(new[] { true, true, false, true }));
        Assert.Equal(1, count);

        AssertSql(
"""
SELECT count(*)::int
FROM "Entities" AS e
WHERE e."BitArray" | B'1000' = B'1101'
""");
    }

    protected BitArrayQueryContext CreateContext() => Fixture.CreateContext();

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    public class BitArrayQueryContext : PoolableDbContext
    {
        public DbSet<BitArrayEntity> Entities { get; set; }

        public BitArrayQueryContext(DbContextOptions options) : base(options) {}

        public static void Seed(BitArrayQueryContext context)
        {
            context.Entities.AddRange(BitArrayData.CreateEntities());
            context.SaveChanges();
        }
    }

    public class BitArrayEntity
    {
        public int Id { get; set; }
        public BitArray BitArray { get; set; }
    }

    public class BitArrayQueryFixture : SharedStoreFixtureBase<BitArrayQueryContext>, IQueryFixtureBase
    {
        private BitArrayData _expectedData;

        protected override string StoreName => "BitArrayQueryTest";

        protected override ITestStoreFactory TestStoreFactory => NpgsqlTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override void Seed(BitArrayQueryContext context) => BitArrayQueryContext.Seed(context);

        public Func<DbContext> GetContextCreator()
            => CreateContext;

        public ISetSource GetExpectedData()
            => _expectedData ??= new BitArrayData();

        public IReadOnlyDictionary<Type, object> EntitySorters
            => new Dictionary<Type, Func<object, object>> { { typeof(BitArrayEntity), e => ((BitArrayEntity)e)?.Id } }
                .ToDictionary(e => e.Key, e => (object)e.Value);

        public IReadOnlyDictionary<Type, object> EntityAsserters
            => new Dictionary<Type, Action<object, object>>
            {
                {
                    typeof(BitArrayEntity), (e, a) =>
                    {
                        Assert.Equal(e is null, a is null);
                        if (a is not null)
                        {
                            var ee = (BitArrayEntity)e;
                            var aa = (BitArrayEntity)a;

                            Assert.Equal(ee.Id, aa.Id);
                            Assert.Equal(ee.BitArray, aa.BitArray);
                        }
                    }
                }
            }.ToDictionary(e => e.Key, e => (object)e.Value);
    }

    protected class BitArrayData : ISetSource
    {
        public IReadOnlyList<BitArrayEntity> Entities { get; }

        public BitArrayData()
            => Entities = CreateEntities();

        public IQueryable<TEntity> Set<TEntity>()
            where TEntity : class
        {
            if (typeof(TEntity) == typeof(BitArrayEntity))
            {
                return (IQueryable<TEntity>)Entities.AsQueryable();
            }

            throw new InvalidOperationException("Invalid entity type: " + typeof(TEntity));
        }

        public static IReadOnlyList<BitArrayEntity> CreateEntities()
            => new List<BitArrayEntity>
            {
                new() { Id = 1, BitArray = new BitArray(new[] { true, true, true, true }) },
                new() { Id = 2, BitArray = new BitArray(new[] { false, false, false, false }) },
                new() { Id = 3, BitArray = new BitArray(new[] { true, false, true, false }) },
                new() { Id = 4, BitArray = new BitArray(new[] { false, true, false, true }) },
                // new() { Id = 99, BitArray = new BitArray(0) },
            };
    }
}
