using Microsoft.EntityFrameworkCore.Query.Internal;
using static System.Linq.Expressions.Expression;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class NpgsqlQueryableMethodNormalizingExpressionVisitor : QueryableMethodNormalizingExpressionVisitor
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public NpgsqlQueryableMethodNormalizingExpressionVisitor(QueryCompilationContext queryCompilationContext)
        : base(queryCompilationContext)
    {
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override Expression VisitMethodCall(MethodCallExpression methodCall)
    {
        var method = methodCall.Method;

        if (method.DeclaringType == typeof(NpgsqlQueryableExtensions))
        {
            if (method.GetGenericMethodDefinition() == NpgsqlQueryableExtensions.EnumerableWithNullsFirstMethodInfo)
            {
                var source = Visit(methodCall.Arguments[0]);
                var elementType = method.GetGenericArguments()[0];
                methodCall = Call(
                    NpgsqlQueryableExtensions.QueryableWithNullsFirstMethodInfo.MakeGenericMethod(elementType),
                    [source, methodCall.Arguments[1]]);
            }
        }

        return base.VisitMethodCall(methodCall);
    }
}
