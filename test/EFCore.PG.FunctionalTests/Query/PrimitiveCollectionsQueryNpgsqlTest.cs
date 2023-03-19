// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Npgsql.EntityFrameworkCore.PostgreSQL.TestUtilities;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query;

public class PrimitiveCollectionsQueryNpgsqlTest : PrimitiveCollectionsQueryTestBase<
    PrimitiveCollectionsQueryNpgsqlTest.PrimitiveCollectionsQueryNpgsqlFixture>
{
    public PrimitiveCollectionsQueryNpgsqlTest(PrimitiveCollectionsQueryNpgsqlFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Constant_of_ints_Contains(bool async)
    {
        await base.Constant_of_ints_Contains(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Int" IN (10, 999)
""");
    }

    public override async Task Constant_of_nullable_ints_Contains(bool async)
    {
        await base.Constant_of_nullable_ints_Contains(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."NullableInt" IN (10, 999)
""");
    }

    public override async Task Constant_of_nullable_ints_Contains_null(bool async)
    {
        await base.Constant_of_nullable_ints_Contains_null(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."NullableInt" = 999 OR p."NullableInt" IS NULL
""");
    }

    public override Task Constant_Count_with_zero_values(bool async)
        => AssertTranslationFailedWithDetails(
            () => base.Constant_Count_with_zero_values(async),
            RelationalStrings.EmptyCollectionNotSupportedAsConstantQueryRoot);

    public override async Task Constant_Count_with_one_value(bool async)
    {
        await base.Constant_Count_with_one_value(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE (
    SELECT count(*)::int
    FROM (VALUES (2::int)) AS v("Value")
    WHERE v."Value" > p."Id") = 1
""");
    }

    public override async Task Constant_Count_with_two_values(bool async)
    {
        await base.Constant_Count_with_two_values(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE (
    SELECT count(*)::int
    FROM (VALUES (2::int), (999)) AS v("Value")
    WHERE v."Value" > p."Id") = 1
""");
    }

    public override async Task Constant_Count_with_three_values(bool async)
    {
        await base.Constant_Count_with_three_values(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE (
    SELECT count(*)::int
    FROM (VALUES (2::int), (999), (1000)) AS v("Value")
    WHERE v."Value" > p."Id") = 2
""");
    }

    public override Task Constant_Contains_with_zero_values(bool async)
        => AssertTranslationFailedWithDetails(
            () => base.Constant_Contains_with_zero_values(async),
            RelationalStrings.EmptyCollectionNotSupportedAsConstantQueryRoot);

    public override async Task Constant_Contains_with_one_value(bool async)
    {
        await base.Constant_Contains_with_one_value(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Id" = 2
""");
    }

    public override async Task Constant_Contains_with_two_values(bool async)
    {
        await base.Constant_Contains_with_two_values(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Id" IN (2, 999)
""");
    }

    public override async Task Constant_Contains_with_three_values(bool async)
    {
        await base.Constant_Contains_with_three_values(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Id" IN (2, 999, 1000)
""");
    }

    public override async Task Parameter_Count(bool async)
    {
        await base.Parameter_Count(async);

        AssertSql(
"""
@__ids_0={ '2', '999' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE (
    SELECT count(*)::int
    FROM unnest(@__ids_0) AS u(value)
    WHERE u.value > p."Id") = 1
""");
    }

    public override async Task Parameter_of_ints_Contains(bool async)
    {
        await base.Parameter_of_ints_Contains(async);

        AssertSql(
"""
@__ints_0={ '10', '999' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Int" = ANY (@__ints_0)
""");
    }

    public override async Task Parameter_of_nullable_ints_Contains(bool async)
    {
        await base.Parameter_of_nullable_ints_Contains(async);

        AssertSql(
"""
@__nullableInts_0={ '10', '999' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."NullableInt" = ANY (@__nullableInts_0) OR (p."NullableInt" IS NULL AND array_position(@__nullableInts_0, NULL) IS NOT NULL)
""");
    }

    public override async Task Parameter_of_nullable_ints_Contains_null(bool async)
    {
        await base.Parameter_of_nullable_ints_Contains_null(async);

        AssertSql(
"""
@__nullableInts_0={ NULL, '999' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."NullableInt" = ANY (@__nullableInts_0) OR (p."NullableInt" IS NULL AND array_position(@__nullableInts_0, NULL) IS NOT NULL)
""");
    }

    public override async Task Parameter_of_strings_Contains(bool async)
    {
        await base.Parameter_of_strings_Contains(async);

        AssertSql(
"""
@__strings_0={ '10', '999' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."String" = ANY (@__strings_0) OR (p."String" IS NULL AND array_position(@__strings_0, NULL) IS NOT NULL)
""");
    }

    public override async Task Parameter_of_DateTimes_Contains(bool async)
    {
        await base.Parameter_of_DateTimes_Contains(async);

        AssertSql(
"""
@__dateTimes_0={ '2020-01-10T12:30:00.0000000Z', '9999-01-01T00:00:00.0000000Z' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."DateTime" = ANY (@__dateTimes_0)
""");
    }

    public override async Task Parameter_of_bools_Contains(bool async)
    {
        await base.Parameter_of_bools_Contains(async);

        AssertSql(
"""
@__bools_0={ 'True' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Bool" = ANY (@__bools_0)
""");
    }

    public override async Task Parameter_of_enums_Contains(bool async)
    {
        await base.Parameter_of_enums_Contains(async);

        AssertSql(
"""
@__enums_0={ '0', '3' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Enum" = ANY (@__enums_0)
""");
    }

    public override async Task Column_of_ints_Contains(bool async)
    {
        await base.Column_of_ints_Contains(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE 10 = ANY (p."Ints")
""");
    }

    public override async Task Column_of_nullable_ints_Contains(bool async)
    {
        await base.Column_of_nullable_ints_Contains(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE 10 = ANY (p."NullableInts")
""");
    }

    public override async Task Column_of_nullable_ints_Contains_null(bool async)
    {
        await base.Column_of_nullable_ints_Contains_null(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE NULL = ANY (p."NullableInts") OR (NULL IS NULL AND array_position(p."NullableInts", NULL) IS NOT NULL)
""");
    }

    public override async Task Column_of_bools_Contains(bool async)
    {
        await base.Column_of_bools_Contains(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE TRUE = ANY (p."Bools")
""");
    }

    public override async Task Column_Count_method(bool async)
    {
        await base.Column_Count_method(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE cardinality(p."Ints") = 2
""");
    }

    public override async Task Column_Length(bool async)
    {
        await base.Column_Length(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE cardinality(p."Ints") = 2
""");
    }

    public override async Task Column_index(bool async)
    {
        await base.Column_index(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Ints"[2] = 10
""");
    }

    public override async Task Column_ElementAt(bool async)
    {
        await base.Column_ElementAt(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Ints"[2] = 10
""");
    }

    public override async Task Column_Any(bool async)
    {
        await base.Column_Any(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE cardinality(p."Ints") > 0
""");
    }

    public override async Task Column_projection_from_top_level(bool async)
    {
        await base.Column_projection_from_top_level(async);

        AssertSql(
"""
SELECT p."Ints"
FROM "PrimitiveCollectionsEntity" AS p
ORDER BY p."Id" NULLS FIRST
""");
    }

    public override async Task Column_and_parameter_Join(bool async)
    {
        await base.Column_and_parameter_Join(async);

        AssertSql(
"""
@__ints_0={ '11', '111' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE (
    SELECT count(*)::int
    FROM unnest(p."Ints") AS u(value)
    INNER JOIN unnest(@__ints_0) AS u0(value) ON u.value = u0.value) = 2
""");
    }

    public override async Task Parameter_Concat_column(bool async)
    {
        await base.Parameter_Concat_column(async);

        AssertSql(
"""
@__ints_0={ '11', '111' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE cardinality(@__ints_0 || p."Ints") = 2
""");
    }

    public override async Task Column_Union_parameter(bool async)
    {
        await base.Column_Union_parameter(async);

        AssertSql(
"""
@__ints_0={ '11', '111' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE (
    SELECT count(*)::int
    FROM (
        SELECT u.value
        FROM unnest(p."Ints") AS u(value)
        UNION
        SELECT u0.value
        FROM unnest(@__ints_0) AS u0(value)
    ) AS t) = 2
""");
    }

    public override async Task Column_Intersect_constant(bool async)
    {
        await base.Column_Intersect_constant(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE (
    SELECT count(*)::int
    FROM (
        SELECT u.value
        FROM unnest(p."Ints") AS u(value)
        INTERSECT
        VALUES (11::int), (111)
    ) AS t) = 2
""");
    }

    public override async Task Constant_Except_column(bool async)
    {
        await base.Constant_Except_column(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE (
    SELECT count(*)::int
    FROM (
        SELECT v."Value"
        FROM (VALUES (11::int), (111)) AS v("Value")
        EXCEPT
        SELECT u.value AS "Value"
        FROM unnest(p."Ints") AS u(value)
    ) AS t
    WHERE t."Value" % 2 = 1) = 2
""");
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public virtual async Task Parameter_Concat_column_Concat_parameter(bool async)
    {
        var ints1 = new[] { 11 };
        var ints2 = new[] { 12 };

        await AssertQuery(
            async,
            ss => ss.Set<PrimitiveCollectionsEntity>().Where(c => ints1.Concat(c.Ints).Concat(ints2).Count() == 4),
            entryCount: 1);

        AssertSql(
"""
@__ints1_0={ '11' } (DbType = Object)
@__ints2_1={ '12' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE cardinality(@__ints1_0 || p."Ints" || @__ints2_1) = 4
""");
    }

    public override async Task Column_Concat_parameter_equality_constant_not_supported(bool async)
    {
        await base.Column_Concat_parameter_equality_constant_not_supported(async);

        AssertSql();
    }

    public override async Task Column_equality_parameter(bool async)
    {
        await base.Column_equality_parameter(async);

        AssertSql(
"""
@__ints_0={ '1', '10' } (DbType = Object)

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Ints" = @__ints_0
""");
    }

    public override async Task Column_equality_constant(bool async)
    {
        await base.Column_equality_constant(async);

        AssertSql(
"""
SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."Ints" = ARRAY[1,10]::integer[]
""");
    }

    public override async Task Column_equality_parameter_with_custom_converter(bool async)
    {
        await base.Column_equality_parameter_with_custom_converter(async);

        AssertSql(
"""
@__ints_0='1,10'

SELECT p."Id", p."Bool", p."Bools", p."CustomConvertedInts", p."DateTime", p."DateTimes", p."Enum", p."Enums", p."Int", p."Ints", p."NullableInt", p."NullableInts", p."String", p."Strings"
FROM "PrimitiveCollectionsEntity" AS p
WHERE p."CustomConvertedInts" = @__ints_0
""");
    }

    [ConditionalFact]
    public virtual void Check_all_tests_overridden()
        => TestHelpers.AssertAllMethodsOverridden(GetType());

    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    private PrimitiveCollectionsContext CreateContext()
        => Fixture.CreateContext();

    public class PrimitiveCollectionsQueryNpgsqlFixture : PrimitiveCollectionsQueryFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory
            => NpgsqlTestStoreFactory.Instance;
    }
}
