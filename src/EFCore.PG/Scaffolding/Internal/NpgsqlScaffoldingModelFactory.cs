using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#pragma warning disable EF1001 // EF Core internal API

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Scaffolding.Internal
{
    public class NpgsqlScaffoldingModelFactory : RelationalScaffoldingModelFactory
    {
        public NpgsqlScaffoldingModelFactory(
            [NotNull] IOperationReporter reporter,
            [NotNull] ICandidateNamingService candidateNamingService,
            [NotNull] IPluralizer pluralizer,
            [NotNull] ICSharpUtilities cSharpUtilities,
            [NotNull] IScaffoldingTypeMapper scaffoldingTypeMapper,
            [NotNull] LoggingDefinitions loggingDefinitions)
            : base(reporter, candidateNamingService, pluralizer, cSharpUtilities, scaffoldingTypeMapper, loggingDefinitions)
        {
        }

        protected override ModelBuilder VisitDatabaseModel(ModelBuilder modelBuilder, DatabaseModel databaseModel)
        {
            var builder = base.VisitDatabaseModel(modelBuilder, databaseModel);

            // foreach (var postgresEnum in databaseModel.GetPostgresEnums())
            // {
            //     // PostgresEnum.RemovePostgresEnum(databaseModel, postgresEnum.Schema, postgresEnum.Name);
            // }

            return builder;
        }
    }
}
