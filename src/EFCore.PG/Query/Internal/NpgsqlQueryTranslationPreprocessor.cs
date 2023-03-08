namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class NpgsqlQueryTranslationPreprocessor : RelationalQueryTranslationPreprocessor
{
    /// <summary>
    ///     Creates a new instance of the <see cref="QueryTranslationPreprocessor" /> class.
    /// </summary>
    /// <param name="dependencies">Parameter object containing dependencies for this class.</param>
    /// <param name="relationalDependencies">Parameter object containing relational dependencies for this class.</param>
    /// <param name="queryCompilationContext">The query compilation context object to use.</param>
    public NpgsqlQueryTranslationPreprocessor(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public override Expression NormalizeQueryableMethod(Expression expression)
    {
        expression = new QueryableParameterToToQueryRootConverter().Visit(expression);
        return base.NormalizeQueryableMethod(expression);
    }

    private class QueryableParameterToToQueryRootConverter : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression parameter)
            => parameter.Name?.StartsWith(QueryCompilationContext.QueryParameterPrefix, StringComparison.Ordinal) == true
               && parameter.Type.TryGetSequenceType() is Type elementType
                ? new NpgsqlQueryableParameterQueryRootExpression(elementType, parameter)
                : base.VisitParameter(parameter);
    }
}

