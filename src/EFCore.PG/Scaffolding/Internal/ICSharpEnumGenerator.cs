using JetBrains.Annotations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Scaffolding.Internal
{
    public interface ICSharpEnumGenerator
    {
        string WriteCode([NotNull] PostgresEnum enumType, [NotNull] string @namespace);
    }
}
