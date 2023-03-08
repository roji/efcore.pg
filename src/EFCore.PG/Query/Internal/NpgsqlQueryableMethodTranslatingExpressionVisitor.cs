using System.Diagnostics.CodeAnalysis;
using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal.Mapping;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class NpgsqlQueryableMethodTranslatingExpressionVisitor : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public NpgsqlQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _sqlExpressionFactory = relationalDependencies.SqlExpressionFactory;
        _typeMappingSource = relationalDependencies.TypeMappingSource;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected NpgsqlQueryableMethodTranslatingExpressionVisitor(NpgsqlQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor)
    {
        _sqlExpressionFactory = parentVisitor._sqlExpressionFactory;
        _typeMappingSource = parentVisitor._typeMappingSource;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override bool IsValidSelectExpressionForExecuteUpdate(
        SelectExpression selectExpression,
        EntityShaperExpression entityShaperExpression,
        [NotNullWhen(true)] out TableExpression? tableExpression)
    {
        if (!base.IsValidSelectExpressionForExecuteUpdate(selectExpression, entityShaperExpression, out tableExpression))
        {
            return false;
        }

        // PostgreSQL doesn't support referencing the main update table from anywhere except for the UPDATE WHERE clause.
        // This specifically makes it impossible to have joins which reference the main table in their predicate (ON ...).
        // Because of this, we detect all such inner joins and lift their predicates to the main WHERE clause (where a reference to the
        // main table is allowed) - see NpgsqlQuerySqlGenerator.VisitUpdate.
        // For any other type of join which contains a reference to the main table, we return false to trigger a subquery pushdown instead.
        OuterReferenceFindingExpressionVisitor? visitor = null;

        for (var i = 0; i < selectExpression.Tables.Count; i++)
        {
            var table = selectExpression.Tables[i];

            if (ReferenceEquals(table, tableExpression))
            {
                continue;
            }

            visitor ??= new OuterReferenceFindingExpressionVisitor(tableExpression);

            // For inner joins, if the predicate contains a reference to the main table, NpgsqlQuerySqlGenerator will lift the predicate
            // to the WHERE clause; so we only need to check the inner join's table (i.e. subquery) for such a reference.
            // Cross join and cross/outer apply (lateral joins) don't have predicates, so just check the entire join for a reference to
            // the main table, and switch to subquery syntax if one is found.
            // Left join does have a predicate, but it isn't possible to lift it to the main WHERE clause; so also check the entire
            // join.
            if (table is InnerJoinExpression innerJoin)
            {
                table = innerJoin.Table;
            }

            if (visitor.ContainsReferenceToMainTable(table))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override bool IsValidSelectExpressionForExecuteDelete(
        SelectExpression selectExpression,
        EntityShaperExpression entityShaperExpression,
        [NotNullWhen(true)] out TableExpression? tableExpression)
    {
        // The default relational behavior is to allow only single-table expressions, and the only permitted feature is a predicate.
        // Here we extend this to also inner joins to tables, which we generate via the PostgreSQL-specific USING construct.
        if (selectExpression.Offset == null
            && selectExpression.Limit == null
            // If entity type has primary key then Distinct is no-op
            && (!selectExpression.IsDistinct || entityShaperExpression.EntityType.FindPrimaryKey() != null)
            && selectExpression.GroupBy.Count == 0
            && selectExpression.Having == null
            && selectExpression.Orderings.Count == 0)
        {
            TableExpressionBase? table = null;
            if (selectExpression.Tables.Count == 1)
            {
                table = selectExpression.Tables[0];
            }
            else if (selectExpression.Tables.All(t => t is TableExpression or InnerJoinExpression))
            {
                var projectionBindingExpression = (ProjectionBindingExpression)entityShaperExpression.ValueBufferExpression;
                var entityProjectionExpression = (EntityProjectionExpression)selectExpression.GetProjection(projectionBindingExpression);
                var column = entityProjectionExpression.BindProperty(entityShaperExpression.EntityType.GetProperties().First());
                table = column.Table;
                if (table is JoinExpressionBase joinExpressionBase)
                {
                    table = joinExpressionBase.Table;
                }
            }

            if (table is TableExpression te)
            {
                tableExpression = te;
                return true;
            }
        }

        tableExpression = null;
        return false;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new NpgsqlQueryableMethodTranslatingExpressionVisitor(this);

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override Expression VisitExtension(Expression extensionExpression)
    {
        if (extensionExpression is NpgsqlQueryableParameterQueryRootExpression parameterQueryRootExpression)
        {
            if (_typeMappingSource.FindMapping(parameterQueryRootExpression.ParameterExpression.Type) is not NpgsqlArrayTypeMapping typeMapping)
            {
                throw new InvalidOperationException("No array type mapping for parameter type: " + parameterQueryRootExpression.ParameterExpression.Type.Name);
            }

            Check.DebugAssert(
                _sqlExpressionFactory.Constant(parameterQueryRootExpression.ParameterExpression) is not null,
                "_sqlExpressionFactory.Constant(parameterQueryRootExpression.ParameterExpression) is not null");

            var translation = new TableValuedFunctionExpression(
                alias: "u",
                "unnest",
                schema: null,
                builtIn: true,
                new[] { new SqlParameterExpression(parameterQueryRootExpression.ParameterExpression, typeMapping) },
                annotations: null);

            var selectExpression = new SelectExpression(
                parameterQueryRootExpression.Type,
                typeMapping.ElementMapping,
                translation);

            Expression shaperExpression = new ProjectionBindingExpression(
                selectExpression, new ProjectionMember(), parameterQueryRootExpression.ElementType.MakeNullable());

            if (parameterQueryRootExpression.ElementType != shaperExpression.Type)
            {
                Check.DebugAssert(
                    parameterQueryRootExpression.ElementType.MakeNullable() == shaperExpression.Type,
                    "expression.Type must be nullable of targetType");

                shaperExpression = Expression.Convert(shaperExpression, parameterQueryRootExpression.ElementType);
            }

            return new ShapedQueryExpression(selectExpression, shaperExpression);
        }

        return base.VisitExtension(extensionExpression);
    }

    private sealed class OuterReferenceFindingExpressionVisitor : ExpressionVisitor
    {
        private readonly TableExpression _mainTable;
        private bool _containsReference;

        public OuterReferenceFindingExpressionVisitor(TableExpression mainTable)
            => _mainTable = mainTable;

        public bool ContainsReferenceToMainTable(TableExpressionBase tableExpression)
        {
            _containsReference = false;

            Visit(tableExpression);

            return _containsReference;
        }

        [return: NotNullIfNotNull("expression")]
        public override Expression? Visit(Expression? expression)
        {
            if (_containsReference)
            {
                return expression;
            }

            if (expression is ColumnExpression columnExpression
                && columnExpression.Table == _mainTable)
            {
                _containsReference = true;

                return expression;
            }

            return base.Visit(expression);
        }
    }
}
