using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Internal;

#pragma warning disable EF1001 // EF Core internal API

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Scaffolding.Internal
{
    public class NpgsqlModelCodeGenerator : CSharpModelGenerator
    {
        const string FileExtension = ".cs";

        public ICSharpEnumGenerator CSharpEnumGenerator { get; }

        public NpgsqlModelCodeGenerator(
            [NotNull] ModelCodeGeneratorDependencies dependencies,
            [NotNull] ICSharpDbContextGenerator cSharpDbContextGenerator,
            [NotNull] ICSharpEntityTypeGenerator cSharpEntityTypeGenerator)
            : base(dependencies, cSharpDbContextGenerator, cSharpEntityTypeGenerator)
        {
            CSharpEnumGenerator = new CSharpEnumGenerator();
        }

        public override ScaffoldedModel GenerateModel(IModel model, ModelCodeGenerationOptions options)
        {
            var scaffoldedModel = base.GenerateModel(model, options);

            foreach (var enumType in model.GetPostgresEnums())
            {
                // TODO: Make the generator pluggable

                var generatedCode = CSharpEnumGenerator.WriteCode(enumType, options.ModelNamespace);

                // output EntityType poco .cs file
                var entityTypeFileName = enumType.Name + FileExtension;
                scaffoldedModel.AdditionalFiles.Add(
                    new ScaffoldedFile { Path = entityTypeFileName, Code = generatedCode });
            }

            return scaffoldedModel;
        }
    }
}
