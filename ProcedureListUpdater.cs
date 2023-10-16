using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using PRISM;

namespace PgSqlProcedureListUpdater
{
    public class ProcedureListUpdater : EventNotifier
    {
        // Ignore Spelling: Postgres

        private readonly ProcedureListUpdaterOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public ProcedureListUpdater(ProcedureListUpdaterOptions options)
        {
            mOptions = options;
        }

        private bool GetObjectNameAndType(string dataLine, Regex objectNameMatcher, out string objectName, out string objectType)
        {
            var nameMatch = objectNameMatcher.Match(dataLine);

            if (!nameMatch.Success)
            {
                objectName = string.Empty;
                objectType = string.Empty;

                return false;
            }

            objectName = nameMatch.Groups["ObjectName"].Value;
            objectType = nameMatch.Groups["ObjectType"].Value;

            return true;
        }

        private void ParseHeaderLinesAfterArguments(
            string headerTextAfterArguments,
            string objectBodyDelimiter,
            string objectType,
            FileSystemInfo inputFile,
            ICollection<string> headerLinesAfterArguments)
        {
            var languageIndex = headerTextAfterArguments.IndexOf("LANGUAGE", StringComparison.OrdinalIgnoreCase);

            var asBodyDelimiterIndex = headerTextAfterArguments.IndexOf(
                string.Format(" AS {0}", objectBodyDelimiter), StringComparison.OrdinalIgnoreCase);

            if (languageIndex >= 0 && asBodyDelimiterIndex >= 0)
            {
                if (languageIndex > 0)
                {
                    headerLinesAfterArguments.Add(headerTextAfterArguments.Substring(0, languageIndex));
                }

                headerLinesAfterArguments.Add(headerTextAfterArguments.Substring(languageIndex, asBodyDelimiterIndex - languageIndex));
                headerLinesAfterArguments.Add(headerTextAfterArguments.Substring(asBodyDelimiterIndex));

                return;
            }

            if (asBodyDelimiterIndex > 0)
            {
                headerLinesAfterArguments.Add(headerTextAfterArguments.Substring(0, asBodyDelimiterIndex));
                headerLinesAfterArguments.Add(headerTextAfterArguments.Substring(asBodyDelimiterIndex));

                return;
            }

            OnWarningEvent("Did not find \"LANGUAGE\" and \"{0}\" in headerLinesAfterArguments for the {0}, file: {1}", objectType, inputFile.FullName);

            headerLinesAfterArguments.Add(headerTextAfterArguments);
        }

        /// <summary>
        /// Process the input file
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public bool ProcessInputFile()
        {
            const string PUBLIC_SCHEMA_PREFIX = "public.";
            try
            {
                var inputFile = new FileInfo(mOptions.InputFilePath);

                if (!inputFile.Exists)
                {
                    OnErrorEvent("Input file not found: " + inputFile.FullName);
                    return false;
                }

                if (inputFile.Directory == null || inputFile.DirectoryName == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of the input file: " + inputFile.FullName);
                    return false;
                }

                var outputFilePath = Path.Combine(
                    inputFile.DirectoryName,
                    Path.GetFileNameWithoutExtension(inputFile.Name) + "_updated" + inputFile.Extension);

                var sqlFilesDirectory = new DirectoryInfo(mOptions.ScriptFileDirectory);

                Console.WriteLine();
                OnStatusEvent("Reading " + PathUtils.CompactPathString(inputFile.FullName, 100));

                using var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                var objectNameMatcher = new Regex("CREATE OR REPLACE (?<ObjectType>PROCEDURE|FUNCTION) *(?<ObjectName>[^(]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var currentLineNumber = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    currentLineNumber++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        writer.WriteLine();
                        continue;
                    }

                    if (!dataLine.Trim().StartsWith("CREATE OR REPLACE", StringComparison.OrdinalIgnoreCase))
                    {
                        writer.WriteLine(dataLine);
                        continue;
                    }

                    if (!GetObjectNameAndType(dataLine, objectNameMatcher, out var objectNameWithSchema, out var objectType))
                    {
                        OnWarningEvent("Unable to parse out the procedure or function name from {0}", dataLine);
                        writer.WriteLine(dataLine);
                        continue;
                    }

                    string fileToFind;

                    if (objectNameWithSchema.StartsWith(PUBLIC_SCHEMA_PREFIX, StringComparison.OrdinalIgnoreCase) &&
                        objectNameWithSchema.Length > PUBLIC_SCHEMA_PREFIX.Length)
                    {
                        fileToFind = objectNameWithSchema.Substring(PUBLIC_SCHEMA_PREFIX.Length) + ".sql";
                    }
                    else
                    {
                        fileToFind = objectNameWithSchema + ".sql";
                    }

                    var foundFiles = sqlFilesDirectory.GetFiles(fileToFind, mOptions.Recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly).ToList();

                    if (foundFiles.Count == 0)
                    {
                        OnWarningEvent("DDL file not found in the SQL files directory: {0}", fileToFind);
                        writer.WriteLine(dataLine);
                        continue;
                    }

                    var sourceHeaderLines = new List<string> { dataLine };

                    // This is the object header, without any line feeds
                    var sourceObjectHeader = new StringBuilder();
                    sourceObjectHeader.Append(dataLine);

                    // Typically $$, but is sometimes $_$
                    var objectBodyDelimiter = string.Empty;

                    var sourceDescription = string.Format("{0} {1}", objectType.ToLower(), objectNameWithSchema);

                    var readSuccess = ReadHeaderAndBody(
                        reader,
                        objectNameMatcher,
                        objectType,
                        sourceDescription,
                        sourceHeaderLines,
                        sourceObjectHeader,
                        out var sourceObjectBody,
                        ref objectBodyDelimiter);

                    if (!readSuccess)
                    {
                        OnWarningEvent("Aborting since ReadHeaderAndBody returned false");
                        return false;
                    }

                    // Load the object info from the DDL file with the updated version of the procedure or function
                    var sqlFileRead = ReadSqlFile(
                        foundFiles[0],
                        objectType.ToLower(),
                        objectNameWithSchema,
                        objectNameMatcher,
                        out var objectArgumentList,
                        out var headerLinesAfterArguments,
                        out var objectBody);

                    if (!sqlFileRead)
                    {
                        // Write out the source file lines
                        foreach (var headerLine in sourceHeaderLines)
                        {
                            writer.WriteLine(headerLine);
                        }

                        foreach (var bodyLine in sourceObjectBody)
                        {
                            writer.WriteLine(bodyLine);
                        }

                        continue;
                    }

                    var writeSuccess = WriteHeaderAndBody(
                        writer,
                        objectType,
                        objectNameWithSchema,
                        objectArgumentList,
                        headerLinesAfterArguments,
                        objectBody);

                    if (writeSuccess)
                        continue;

                    OnWarningEvent("Aborting since WriteHeaderAndBody returned false");
                    return false;

                    // Find the procedure or function arguments (if any)
                    // Steps to implement:

                    // 1) Find the $$ (or similar) by reading forward line-by-line and caching the lines, looking for @"AS +\$[^$]*\$"

                    // 2) Find the corresponding $$ (or similar), again, reading forward line-by-line and caching the lines
                    //    If another "CREATE OR REPLACE" is found and the matching $$ is not found, show a warning and do not update this procedure or function

                    // 3) Parse the procedure or function argument names in the input file to determine the preferred casing

                    // 4) Read the .sql file
                    //    - Parse the argument names
                    //    - If it's a function that has RETURNS TABLE, parse the column names between ( and ) so we can format things nicely in the output file
                    //    - Find the text between $$ (or similar) and the corresponding $$
                    //    - If found, write the updating procedure or function DDL to the output file

                }

                Console.WriteLine();
                OnStatusEvent("Processed {0} lines in the input file", currentLineNumber);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessInputFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Read the object body text
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="objectBody"></param>
        /// <param name="objectBodyDelimiter">Typically $$, but is sometimes $_$</param>
        /// <param name="objectNameMatcher"></param>
        /// <param name="sourceDescription"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool ReadBody(
            StreamReader reader,
            out List<string> objectBody,
            string objectBodyDelimiter,
            Regex objectNameMatcher,
            string sourceDescription)
        {
            objectBody = new List<string>();

            try
            {
                // Cache the object body text in objectBody

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        objectBody.Add(string.Empty);
                        continue;
                    }

                    if (dataLine.Contains(objectBodyDelimiter))
                    {
                        objectBody.Add(dataLine);
                        return true;
                    }

                    var nameMatch = objectNameMatcher.Match(dataLine);

                    if (nameMatch.Success)
                    {
                        OnWarningEvent("Found the next object before finding closing body delimiter {0} for {1}: {2}",
                            objectBodyDelimiter, sourceDescription, dataLine);

                        return false;
                    }

                    objectBody.Add(dataLine);
                }

                OnWarningEvent("Did not find closing body delimiter {0} for {1}",
                    objectBodyDelimiter, sourceDescription);

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent(ex, "Error in ReadBody for {0}", sourceDescription);
                return false;
            }
        }

        private bool ReadHeader(
            StreamReader reader,
            ICollection<string> headerLines,
            StringBuilder objectHeader,
            ref string objectBodyDelimiter,
            Regex objectNameMatcher,
            string sourceDescription)
        {
            try
            {
                var dollarMatcher = new Regex(@"AS +(?<Delimiter>\$[^$]*\$)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        headerLines.Add(string.Empty);
                        continue;
                    }

                    headerLines.Add(dataLine);

                    var nameMatch = objectNameMatcher.Match(dataLine);

                    if (nameMatch.Success)
                    {
                        OnWarningEvent("Found the next object before finding the body delimiter for {0}: {1}",
                            sourceDescription, dataLine);

                        return false;
                    }

                    objectHeader.AppendFormat(" {0}", dataLine);

                    var match = dollarMatcher.Match(dataLine);

                    if (!match.Success)
                        continue;

                    objectBodyDelimiter = match.Groups["Delimiter"].Value;
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent(ex, "Error in ReadHeader for {0}", sourceDescription);
                return false;
            }
        }

        private bool ReadHeaderAndBody(
            StreamReader reader,
            Regex objectNameMatcher,
            string objectType,
            string sourceDescription,
            ICollection<string> headerLines,
            StringBuilder objectHeader,
            out List<string> objectBody,
            ref string objectBodyDelimiter)
        {
            var headerSuccess = ReadHeader(
                reader,
                headerLines,
                objectHeader,
                ref objectBodyDelimiter,
                objectNameMatcher,
                sourceDescription);

            if (!headerSuccess)
            {
                OnWarningEvent("Unable to parse {0} header in {1}",
                    objectType, sourceDescription);

                objectBody = new List<string>();
                return false;
            }

            // Read the object body

            return ReadBody(
                reader,
                out objectBody,
                objectBodyDelimiter,
                objectNameMatcher,
                sourceDescription);
        }

        /// <summary>
        /// Read a procedure or function's DDL file
        /// </summary>
        /// <param name="inputFile"></param>
        /// <param name="objectType"></param>
        /// <param name="objectNameWithSchema"></param>
        /// <param name="objectNameMatcher"></param>
        /// <param name="argumentList"></param>
        /// <param name="headerLinesAfterArguments">Tracks the text that occurs after the closing parenthesis of the procedure or function's argument list</param>
        /// <param name="objectBody">Function or procedure text between the starting and ending $$</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ReadSqlFile(
            FileSystemInfo inputFile,
            string objectType,
            string objectNameWithSchema,
            Regex objectNameMatcher,
            out List<string> argumentList,
            out List<string> headerLinesAfterArguments,
            out List<string> objectBody)
        {
            argumentList = new List<string>();
            headerLinesAfterArguments = new List<string>();

            // Typically $$, but is sometimes $_$
            var objectBodyDelimiter = string.Empty;

            try
            {
                var argumentListMatcher = new Regex(@"\((?<Arguments>[^)]+)\)", RegexOptions.Compiled);

                // Each line of the object header
                var headerLines = new List<string>();

                using var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (!dataLine.Trim().StartsWith("CREATE OR REPLACE", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (!GetObjectNameAndType(dataLine, objectNameMatcher, out var foundName, out var foundType))
                    {
                        OnWarningEvent("Unable to parse out the {0} name from line \"{1}\" in file {2}",
                            objectType, dataLine, PathUtils.CompactPathString(inputFile.FullName, 120));

                        objectBody = new List<string>();
                        return false;
                    }

                    if (!objectNameWithSchema.Equals(foundName, StringComparison.OrdinalIgnoreCase))
                    {
                        OnWarningEvent("{0} name in .sql file did not match the expected name ({1}): see \"{2}\" in file {3}",
                            objectType, objectNameWithSchema, dataLine, PathUtils.CompactPathString(inputFile.FullName, 120));

                        objectBody = new List<string>();
                        return false;
                    }

                    if (!objectType.Equals(foundType, StringComparison.OrdinalIgnoreCase))
                    {
                        OnWarningEvent("Object type in .sql file did not match the expected type ({0}): see \"{1}\" in file {2}",
                            objectType, dataLine, PathUtils.CompactPathString(inputFile.FullName, 120));

                        objectBody = new List<string>();
                        return false;
                    }

                    headerLines.Add(dataLine);

                    // This is the object header, without any line feeds
                    var objectHeader = new StringBuilder();
                    objectHeader.Append(dataLine);

                    var sourceDescription = string.Format("file {0}", PathUtils.CompactPathString(inputFile.FullName, 120));

                    var success = ReadHeaderAndBody(
                        reader,
                        objectNameMatcher,
                        objectType,
                        sourceDescription,
                        headerLines,
                        objectHeader,
                        out objectBody,
                        ref objectBodyDelimiter);

                    if (!success)
                    {
                        return false;
                    }

                    // Determine the procedure or function argument names and types
                    var argumentMatch = argumentListMatcher.Match(objectHeader.ToString());

                    if (!argumentMatch.Success)
                    {
                        OnWarningEvent("Header found, but unable to parse the arguments for the {0}, file: {1}", objectType, inputFile.FullName);
                        return false;
                    }

                    // ReSharper disable once LoopCanBeConvertedToQuery
                    foreach (var argument in argumentMatch.Groups["Arguments"].Value.Split(','))
                    {
                        argumentList.Add(argument.Trim());
                    }

                    // Find the text that occurs after the closing parenthesis

                    // This tracks the text that occurs after the closing parenthesis of the procedure or function's argument list
                    // (it does not include line feeds)
                    var headerTextAfterArguments = new StringBuilder();

                    var appendHeaderLines = false;

                    foreach (var headerLine in headerLines)
                    {
                        if (appendHeaderLines)
                        {
                            headerTextAfterArguments.AppendFormat(" {0}", headerLine);
                            continue;
                        }

                        var parenthesisIndex = headerLine.IndexOf(')');
                        if (parenthesisIndex < 0)
                            continue;

                        if (parenthesisIndex < headerLine.Length - 1)
                        {
                            headerTextAfterArguments.Append(headerLine.Substring(parenthesisIndex + 1).Trim());
                        }

                        appendHeaderLines = true;
                    }

                    if (!objectType.Equals("FUNCTION", StringComparison.OrdinalIgnoreCase))
                    {
                        // Populate headerLinesAfterArguments using headerTextAfterArguments

                        ParseHeaderLinesAfterArguments(
                            headerTextAfterArguments.ToString(),
                            objectBodyDelimiter,
                            objectType,
                            inputFile,
                            headerLinesAfterArguments);

                        return true;
                    }

                    // Determine the return type of the function
                    // If it is a table, parse the column names

                    var returnsMatcher = new Regex(@"\bRETURNS +(?<ReturnType>[^ (]+) *(?<LanguageAndOptions>)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var returnMatch = returnsMatcher.Match(headerTextAfterArguments.ToString());

                    if (!returnMatch.Success)
                    {
                        OnWarningEvent("Header found, but unable to determine the return type of function {0}", objectNameWithSchema);
                        return false;
                    }

                    var returnType = returnMatch.Groups["ReturnType"].Value;

                    if (!returnType.Equals("Table", StringComparison.OrdinalIgnoreCase))
                    {
                        headerLinesAfterArguments.Add(string.Format("RETURNS {0}", returnType));

                        ParseHeaderLinesAfterArguments(
                            returnMatch.Groups["LanguageAndOptions"].Value,
                            objectBodyDelimiter,
                            objectType,
                            inputFile,
                            headerLinesAfterArguments);

                        return true;
                    }

                    var returnedColumnListMatcher = new Regex(@"\bTABLE *\((?<TableColumns>[^)]+)\) *(?<LanguageAndOptions>)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var columnListMatch = returnedColumnListMatcher.Match(headerTextAfterArguments.ToString());

                    if (!columnListMatch.Success)
                    {
                        OnWarningEvent("Header found, but unable to determine the column list for the table returned by function {0}",
                            objectNameWithSchema);

                        return false;
                    }

                    headerLinesAfterArguments.Add("RETURNS TABLE(");

                    var tableColumns = columnListMatch.Groups["TableColumns"].Value.Split(',').ToList();
                    var indexEnd = tableColumns.Count - 1;

                    for (var i = 0; i <= indexEnd; i++)
                    {
                        headerLinesAfterArguments.Add(string.Format("    {0}{1}", tableColumns[i].Trim(), i < indexEnd ? "," : string.Empty));
                    }

                    headerLinesAfterArguments.Add(")");

                    ParseHeaderLinesAfterArguments(
                        returnMatch.Groups["LanguageAndOptions"].Value,
                        objectBodyDelimiter,
                        objectType,
                        inputFile,
                        headerLinesAfterArguments);

                    return true;
                }

                objectBody = new List<string>();
                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent(ex, "Error in ReadSqlFile for {0}", PathUtils.CompactPathString(inputFile.FullName, 120));
                objectBody = new List<string>();
                return false;
            }
        }


        /// <summary>
        /// Write the updated header and body for the procedure of function
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="objectType"></param>
        /// <param name="objectNameWithSchema"></param>
        /// <param name="objectArgumentList"></param>
        /// <param name="headerLinesAfterArguments"></param>
        /// <param name="objectBody"></param>
        /// <param name="argumentNameMap">Argument names for the procedure or function, as read from the input file</param>
        /// <returns>True if successful, false if an error</returns>
        private bool WriteHeaderAndBody(
            TextWriter writer,
            string objectType,
            string objectNameWithSchema,
            IReadOnlyList<string> objectArgumentList,
            IEnumerable<string> headerLinesAfterArguments,
            IEnumerable<string> objectBody,
            Dictionary<string, string> argumentNameMap)
        {
            try
            {
                writer.WriteLine("CREATE OR REPLACE {0} {1} {2}", objectType, objectNameWithSchema, objectArgumentList.Count == 0 ? "()" : "(");

                if (objectArgumentList.Count > 0)
                {
                    var indexEnd = objectArgumentList.Count - 1;

                    var updatedArgument = new StringBuilder();

                    for (var i = 0; i <= indexEnd; i++)
                    {
                        updatedArgument.Clear();

                        var match = mArgumentMatcher.Match(objectArgumentList[i]);

                        if (match.Success)
                        {
                            var argDirection = match.Groups["Direction"].Value;

                            if (string.IsNullOrWhiteSpace(argDirection))
                            {
                                if (!objectType.Equals("FUNCTION", StringComparison.OrdinalIgnoreCase))
                                {
                                    OnWarningEvent("Argument for {0} {1} did not have a direction: {2}",
                                        objectType, objectNameWithSchema, objectArgumentList[i]);
                                }
                            }
                            else if (!argDirection.Equals("IN", StringComparison.OrdinalIgnoreCase))
                            {
                                updatedArgument.Append(argDirection);
                            }

                            // ToDo: Capitalize the argument name based on the capitalization in the input file

                            var argumentName = match.Groups["Name"].Value;
                            string argumentNameToUse;

                            if (argumentNameMap.TryGetValue(argumentName, out var argumentNameCapitalized))
                            {
                                argumentNameToUse = argumentNameCapitalized;
                            }
                            else if (argumentName.Equals("_returnCode", StringComparison.OrdinalIgnoreCase))
                            {
                                argumentNameToUse = "_returnCode";
                            }
                            else
                            {
                                argumentNameToUse = argumentName;
                            }

                            updatedArgument.AppendFormat("{0} {1}", argumentNameToUse, match.Groups["Type"].Value);

                            var defaultValue = match.Groups["Default"].Value;

                            if (!string.IsNullOrWhiteSpace(defaultValue))
                            {
                                updatedArgument.AppendFormat(" {0}", defaultValue.Trim().Replace("DEFAULT ", "= ").Replace("::text", string.Empty));
                            }
                        }
                        else
                        {
                            OnWarningEvent("Argument for {0} {1} did not match the expected format: {2}",
                                objectType, objectNameWithSchema, objectArgumentList[i]);

                            updatedArgument.Append(objectArgumentList[i]);
                        }

                        writer.WriteLine("    {0}{1}", updatedArgument, i < indexEnd ? "," : string.Empty);
                    }

                    writer.WriteLine(")");
                }

                foreach (var headerLine in headerLinesAfterArguments)
                {
                    writer.WriteLine(headerLine);
                }

                foreach (var bodyLine in objectBody)
                {
                    writer.WriteLine(bodyLine);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(ex, "Error in WriteHeaderAndBody for {0} {1}", objectType, objectNameWithSchema);
                return false;
            }
        }
    }
}
