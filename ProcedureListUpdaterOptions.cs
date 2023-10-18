using System;
using System.IO;
using System.Reflection;
using PRISM;

namespace PgSqlProcedureListUpdater
{
    public class ProcedureListUpdaterOptions
    {
        // Ignore Spelling: pre

        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "October 17, 2023";

        [Option("Input", "I", ArgPosition = 1, HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "SQL script file to process")]
        public string InputFilePath { get; set; }

        [Option("SqlFilesDirectory", "SqlFiles", "D", HelpShowsDefault = false,
            HelpText = "Directory with one .sql file for each procedure or function in the database, as created by the DB Schema Export Tool")]
        public string ScriptFileDirectory { get; set; }

        [Option("Recurse", "R", HelpShowsDefault = true,
            HelpText = "When true, look for .sql files in both the SQL Files directory and its subdirectories")]
        public bool Recurse { get; set; } = true;

        [Option("Verbose", "V", HelpShowsDefault = true,
            HelpText = "When true, show additional status messages while processing the input file")]
        public bool VerboseOutput { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public ProcedureListUpdaterOptions()
        {
            InputFilePath = string.Empty;
            ScriptFileDirectory = string.Empty;
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        public static string GetAppVersion()
        {
            return Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        /// <summary>
        /// Show the options at the console
        /// </summary>
        public void OutputSetOptions()
        {
            Console.WriteLine("Options:");

            Console.WriteLine(" {0,-25} {1}", "Input script file:", PathUtils.CompactPathString(InputFilePath, 120));

            Console.WriteLine(" {0,-25} {1}", "Script file directory:", PathUtils.CompactPathString(ScriptFileDirectory, 120));

            Console.WriteLine(" {0,-25} {1}", "Search recursively:", Recurse);

            Console.WriteLine(" {0,-25} {1}", "Verbose Output:", VerboseOutput);

            Console.WriteLine();
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        /// <returns>True if options are valid, false if /I or /M is missing</returns>
        public bool ValidateArgs(out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(InputFilePath))
            {
                errorMessage = "Use /I to specify the SQL script file to process";
                return false;
            }

            if (string.IsNullOrWhiteSpace(ScriptFileDirectory))
            {
                try
                {
                    var inputFile = new FileInfo(InputFilePath);

                    if (inputFile.Directory == null)
                    {
                        errorMessage = string.Format("Unable to determine the parent directory of the input file: {0}", InputFilePath);
                        return false;
                    }

                    ScriptFileDirectory = inputFile.Directory.FullName;
                }
                catch (Exception ex)
                {
                    errorMessage = string.Format("Error determining the parent directory of the input file ({0}): {1}", InputFilePath, ex.Message);
                    return false;
                }
            }

            errorMessage = string.Empty;

            return true;
        }
    }
}
