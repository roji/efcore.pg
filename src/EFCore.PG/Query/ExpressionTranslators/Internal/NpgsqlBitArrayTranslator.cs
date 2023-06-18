using System.Collections;
using static Npgsql.EntityFrameworkCore.PostgreSQL.Utilities.Statics;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query.ExpressionTranslators.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class NpgsqlBitArrayTranslator : IMethodCallTranslator, IMemberTranslator
{
    private readonly NpgsqlSqlExpressionFactory _sqlExpressionFactory;

    private static readonly MemberInfo BitArray_Count = typeof(BitArray).GetProperty(nameof(BitArray.Count))!;
    private static readonly MemberInfo BitArray_Length = typeof(BitArray).GetProperty(nameof(BitArray.Length))!;

    private static readonly MethodInfo BitArray_Not = typeof(BitArray).GetMethod(nameof(BitArray.Not), Array.Empty<Type>())!;
    private static readonly MethodInfo BitArray_And = typeof(BitArray).GetMethod(nameof(BitArray.And), new[] { typeof(BitArray) })!;
    private static readonly MethodInfo BitArray_Or = typeof(BitArray).GetMethod(nameof(BitArray.Or), new[] { typeof(BitArray) })!;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public NpgsqlBitArrayTranslator(NpgsqlSqlExpressionFactory sqlExpressionFactory)
        => _sqlExpressionFactory = sqlExpressionFactory;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(BitArray) || instance is null)
        {
            return null;
        }

        if (method == BitArray_Not)
        {
            return _sqlExpressionFactory.Not(instance);
        }

        if (method == BitArray_And)
        {
            return _sqlExpressionFactory.And(instance, arguments[0]);
        }

        if (method == BitArray_Or)
        {
            return _sqlExpressionFactory.Or(instance, arguments[0]);
        }

        return null;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (member.DeclaringType == typeof(BitArray) && instance is not null)
        {
            if (member == BitArray_Length || member == BitArray_Count)
            {
                return _sqlExpressionFactory.Function("length", new[] { instance }, true, TrueArrays[1], typeof(int));
            }
        }

        return null;
    }
}
