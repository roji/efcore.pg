using System.Diagnostics.CodeAnalysis;
using Npgsql.EntityFrameworkCore.PostgreSQL.Query.Expressions.Internal;
using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal;
using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal.Mapping;
using static Npgsql.EntityFrameworkCore.PostgreSQL.Utilities.Statics;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class NpgsqlQueryableMethodTranslatingExpressionVisitor : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly NpgsqlTypeMappingSource _typeMappingSource;
    private readonly NpgsqlSqlExpressionFactory _sqlExpressionFactory;

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
        _typeMappingSource = (NpgsqlTypeMappingSource)relationalDependencies.TypeMappingSource;
        _sqlExpressionFactory = (NpgsqlSqlExpressionFactory)relationalDependencies.SqlExpressionFactory;
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
        _typeMappingSource = parentVisitor._typeMappingSource;
        _sqlExpressionFactory = parentVisitor._sqlExpressionFactory;
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
    protected override ShapedQueryExpression? TranslatePrimitiveCollection(SqlExpression sqlExpression)
    {
        var elementClrType = sqlExpression.Type.GetSequenceType();
        RelationalTypeMapping? elementTypeMapping = null;

        switch (sqlExpression)
        {
            case ColumnExpression { TypeMapping: NpgsqlArrayTypeMapping arrayTypeMapping }:
                elementTypeMapping = arrayTypeMapping.ElementTypeMapping;
                break;

            case SqlParameterExpression:
                break;

            default:
                return null;
        }

        var unnestExpression = new PostgresUnnestExpression(sqlExpression, "value");

        // TODO: Probably move this up to relational...
        if (elementTypeMapping is null)
        {
            RegisterUntypedTable(unnestExpression);
        }

        // TODO: When we have metadata to determine if the element is nullable, pass that here to SelectExpression
        var selectExpression = new SelectExpression(elementClrType, elementTypeMapping, unnestExpression, "value");

        Expression shaperExpression = new ProjectionBindingExpression(
            selectExpression, new ProjectionMember(), elementClrType.MakeNullable());

        if (elementClrType != shaperExpression.Type)
        {
            Check.DebugAssert(
                elementClrType.MakeNullable() == shaperExpression.Type,
                "expression.Type must be nullable of targetType");

            shaperExpression = Expression.Convert(shaperExpression, elementClrType);
        }

        return new ShapedQueryExpression(selectExpression, shaperExpression);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override Expression ProcessTypeMappings(
        Expression expression,
        Dictionary<TableExpressionBase, RelationalTypeMapping?> inferredTypeMappings)
        => new NpgsqlTypeMappingProcessor(_typeMappingSource, inferredTypeMappings).Visit(expression);

    protected override ShapedQueryExpression? TranslateAny(ShapedQueryExpression source, LambdaExpression? predicate)
    {
        if (source.QueryExpression is SelectExpression
            {
                Tables: [PostgresUnnestExpression { Array: var array } unnestTable],
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null
            })
        {
            // Simplify x.Array.Any() => cardinality(x.array) > 0 instead of EXISTS (SELECT 1 FROM FROM unnest(x.Array))
            if (predicate is null)
            {
                var translation =
                    _sqlExpressionFactory.GreaterThan(
                        _sqlExpressionFactory.Function(
                            "cardinality",
                            new[] { array },
                            nullable: true,
                            argumentsPropagateNullability: TrueArrays[1],
                            typeof(int)),
                        _sqlExpressionFactory.Constant(0));

                return source.Update(_sqlExpressionFactory.Select(translation), source.ShaperExpression);
            }

            // Simplify x.Array.Contains(y) => y = ANY (x.Array) instead of EXISTS (SELECT 1 FROM FROM unnest(x.Array) WHERE ...)
            if (TranslateLambdaExpression(source, predicate) is SqlBinaryExpression
                {
                    OperatorType: ExpressionType.Equal,
                    Left: var left,
                    Right: var right
                })
            {
                var otherColumn =
                    left is ColumnExpression leftColumn && ReferenceEquals(leftColumn.Table, unnestTable)
                        ? right
                        : right is ColumnExpression rightColumn && ReferenceEquals(rightColumn.Table, unnestTable)
                            ? left
                            : null;

                if (otherColumn is not null)
                {
                    // TODO: All the column/parameter/constant variations
                    var translation = _sqlExpressionFactory.Any(otherColumn, unnestTable.Arguments[0], PostgresAnyOperatorType.Equal);
                    return source.Update(_sqlExpressionFactory.Select(translation), source.ShaperExpression);
                }
            }
        }

        return base.TranslateAny(source, predicate);
    }

    protected override ShapedQueryExpression? TranslateCount(ShapedQueryExpression source, LambdaExpression? predicate)
    {
        // TODO: Does json_array_length pass through here? Most probably not, since it's not mapped with ElementTypeMapping...
        // Simplify x.Array.Count() => cardinality(x.Array) instead of SELECT COUNT(*) FROM unnest(x.Array)
        if (predicate is null && source.QueryExpression is SelectExpression
            {
                Tables: [PostgresUnnestExpression { Array: var array }],
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null
            })
        {
            var translation = _sqlExpressionFactory.Function(
                "cardinality",
                new[] { array },
                nullable: true,
                argumentsPropagateNullability: TrueArrays[1],
                typeof(int));

            return source.Update(_sqlExpressionFactory.Select(translation), source.ShaperExpression);
        }

        return base.TranslateCount(source, predicate);
    }

    protected override ShapedQueryExpression? TranslateConcat(ShapedQueryExpression source1, ShapedQueryExpression source2)
    {
        // Simplify x.Array.Concat(y.Array) => x.Array || y.Array instead of:
        // SELECT u.value FROM unnest(x.Array) UNION ALL SELECT u.value FROM unnest(y.Array)
        // TODO: Detect ValuesExpression as well, convert that the an array literal instead so that we can concat.
        // TODO: Be mindful of ordering and do this only after we change ValuesExpression to be ordered.
        if (source1.QueryExpression is SelectExpression
            {
                Tables: [PostgresUnnestExpression { Array: var array1 } unnest1],
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null,
                Orderings: []
            } select1
            && source2.QueryExpression is SelectExpression
            {
                Tables: [PostgresUnnestExpression { Array: var array2 } unnest2],
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null,
                Orderings: []
            } select2)
        {
            // TODO: Allow peeking into projection mapping
            var (clonedSelect1, clonedSelect2) = (select1.Clone(), select2.Clone());
            clonedSelect1.ApplyProjection();
            clonedSelect2.ApplyProjection();

            Check.DebugAssert(clonedSelect1.Projection.Count == 1 && clonedSelect2.Projection.Count == 1,
                "Multiple projections out of unnest");
            var elementClrType = clonedSelect1.Projection[0].Expression.Type;
            var typeMapping1 = clonedSelect1.Projection[0].Expression.TypeMapping;
            var typeMapping2 = clonedSelect2.Projection[0].Expression.TypeMapping;

            Check.DebugAssert(typeMapping1 is not null || typeMapping2 is not null,
                "Concat with no type mapping on either side (operation should be client-evaluated over parameters/constants");
            if (typeMapping1 is null)
            {
                RegisteredInferredTableMapping(unnest1, typeMapping2!);
            }
            else if (typeMapping2 is null)
            {
                RegisteredInferredTableMapping(unnest2, typeMapping1);
            }
            // TODO: Conflicting type mappings from both sides?

            var inferredTypeMapping = typeMapping1 ?? typeMapping2;
            var unnestExpression = new PostgresUnnestExpression(_sqlExpressionFactory.Add(array1, array2), "value");
            var selectExpression = new SelectExpression(elementClrType, inferredTypeMapping, unnestExpression, "value");

            return source1.Update(selectExpression, source1.ShaperExpression);
        }

        return base.TranslateConcat(source1, source2);
    }

    protected override ShapedQueryExpression? TranslateElementAtOrDefault(
        ShapedQueryExpression source,
        Expression index,
        bool returnDefault)
    {
        // TODO: Does json_array_length pass through here? Most probably not, since it's not mapped with ElementTypeMapping...
        // Simplify x.Array[1] => x.Array[1] (using the PG array subscript operator) instead of a subquery with LIMIT/OFFSET
        if (!returnDefault && source.QueryExpression is SelectExpression
            {
                Tables: [PostgresUnnestExpression { Array: var array }],
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Orderings: [],
                Limit: null,
                Offset: null
            })
        {
            var translatedIndex = TranslateExpression(index);
            if (translatedIndex == null)
            {
                return base.TranslateElementAtOrDefault(source, index, returnDefault);
            }

            // Index on array - but PostgreSQL arrays are 1-based, so adjust the index.
            var translation = _sqlExpressionFactory.ArrayIndex(array, GenerateOneBasedIndexExpression(translatedIndex));
            return source.Update(_sqlExpressionFactory.Select(translation), source.ShaperExpression);
        }

        return base.TranslateElementAtOrDefault(source, index, returnDefault);
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
    /// PostgreSQL array indexing is 1-based. If the index happens to be a constant,
    /// just increment it. Otherwise, append a +1 in the SQL.
    /// </summary>
    private SqlExpression GenerateOneBasedIndexExpression(SqlExpression expression)
        => expression is SqlConstantExpression constant
            ? _sqlExpressionFactory.Constant(Convert.ToInt32(constant.Value) + 1, constant.TypeMapping)
            : _sqlExpressionFactory.Add(expression, _sqlExpressionFactory.Constant(1));

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

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected class NpgsqlTypeMappingProcessor : RelationalTypeMappingProcessor
    {
        private readonly NpgsqlTypeMappingSource _typeMappingSource;

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public NpgsqlTypeMappingProcessor(
            NpgsqlTypeMappingSource typeMappingSource,
            Dictionary<TableExpressionBase, RelationalTypeMapping?> inferredTypeMappings)
            : base(inferredTypeMappings)
            => _typeMappingSource = typeMappingSource;

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        protected override Expression VisitExtension(Expression expression)
            => expression switch
            {
                PostgresUnnestExpression unnestExpression
                    when InferredTypeMappings.TryGetValue(unnestExpression, out var typeMapping)
                    && typeMapping is not null
                    => ApplyTypeMappingsOnUnnestExpression(unnestExpression, new[] { typeMapping }),

                _ => base.VisitExtension(expression)
            };

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public virtual PostgresUnnestExpression ApplyTypeMappingsOnUnnestExpression(
            PostgresUnnestExpression unnestExpression,
            IReadOnlyList<RelationalTypeMapping> typeMappings)
        {
            Check.DebugAssert(typeMappings.Count == 1, "typeMappings.Count == 1");
            var elementTypeMapping = typeMappings[0];

            // Constant queryables are translated to VALUES, no need for JSON.
            // Column queryables have their type mapping from the model, so we should never need to be here to apply an inferred mapping.
            var parameterExpression = unnestExpression.Array as SqlParameterExpression;
            Check.DebugAssert(parameterExpression is not null, "Non-parameter array expression when applying inferred type mapping");

            var parameterTypeMapping = _typeMappingSource.FindContainerMapping(parameterExpression.Type, elementTypeMapping);
            return unnestExpression.Update(parameterExpression.ApplyTypeMapping(parameterTypeMapping));
        }
    }
}
