using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Scaffolding.Internal
{
    public class CSharpEnumGenerator : ICSharpEnumGenerator
    {
        IndentedStringBuilder _sb = null!;

        public string WriteCode(PostgresEnum enumType, string @namespace)
        {
            // TODO: Name translation, snake_case to CamelCase
            _sb = new IndentedStringBuilder();

            _sb.AppendLine($"namespace {@namespace}");
            _sb.AppendLine("{");

            using (_sb.Indent())
            {
                GenerateEnum(enumType);
            }

            _sb.AppendLine("}");

            return _sb.ToString();
        }

        protected virtual void GenerateEnum(PostgresEnum enumType)
        {
            var labels = enumType.Labels;
            for (var i = 0; i < labels.Count; i++)
            {
                _sb.Append(labels[i]);
                if (i < labels.Count - 1)
                    _sb.Append(",");
                _sb.AppendLine();
            }
        }
    }
}
