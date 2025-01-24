using System;
using System.IO;
using System.Reflection;
using System.Threading;
using PRISM;

namespace PgSqlProcedureListUpdater
{
    internal class Program
    {
        // Ignore Spelling: Conf, Sql

        private static int Main(string[] args)
        {
            var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);       // Alternatively: System.AppDomain.CurrentDomain.FriendlyName
            var version = ProcedureListUpdaterOptions.GetAppVersion();

            var parser = new CommandLineParser<ProcedureListUpdaterOptions>(asmName.Name, version)
            {
                ProgramInfo = "This program processes a PostgreSQL DDL file with `CREATE OR REPLACE PROCEDURE` and " +
                              "`CREATE OR REPLACE FUNCTION` statements and updates each procedure or function found " +
                              "using a file created by the DB Schema Export Tool (https://github.com/PNNL-Comp-Mass-Spec/DB-Schema-Export-Tool).",

                ContactInfo = "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                              Environment.NewLine + Environment.NewLine +
                              "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                              "Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics",

                UsageExamples =
                {
                    exeName + " StoredProcedures_edited.sql /D:\\Database_Schema\\DMS_PgSQL"
                }
            };

            parser.AddParamFileKey("Conf");

            var result = parser.ParseArgs(args);
            var options = result.ParsedResults;

            try
            {
                if (!result.Success)
                {
                    if (parser.CreateParamFileProvided)
                    {
                        return 0;
                    }

                    // Delay for 1500 msec in case the user double-clicked this file from within Windows Explorer (or started the program via a shortcut)
                    Thread.Sleep(1500);
                    return -1;
                }

                if (!options.ValidateArgs(out var errorMessage))
                {
                    parser.PrintHelp();

                    Console.WriteLine();
                    ConsoleMsgUtils.ShowWarning("Validation error:");
                    ConsoleMsgUtils.ShowWarning(errorMessage);

                    Thread.Sleep(1500);
                    return -1;
                }

                options.OutputSetOptions();
            }
            catch (Exception e)
            {
                Console.WriteLine();
                Console.Write($"Error running {exeName}");
                Console.WriteLine(e.Message);
                Console.WriteLine($"See help with {exeName} --help");
                return -1;
            }

            try
            {
                var processor = new ProcedureListUpdater(options);

                processor.ErrorEvent += Processor_ErrorEvent;
                processor.StatusEvent += Processor_StatusEvent;
                processor.WarningEvent += Processor_WarningEvent;

                var success = processor.ProcessInputFile();

                if (success)
                {
                    Console.WriteLine();
                    Console.WriteLine("Processing complete");
                    Thread.Sleep(500);
                    return 0;
                }

                ConsoleMsgUtils.ShowWarning("Processing error");
                Thread.Sleep(1500);
                return -1;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                Thread.Sleep(1500);
                return -1;
            }
        }

        private static void Processor_ErrorEvent(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowErrorCustom(message, ex, false);
        }

        private static void Processor_StatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void Processor_WarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }
    }
}
