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
        // Ignore Spelling: Postgres, Sql

        private const string COMMA_PLACEHOLDER = "_@_LITERAL_COMMA_@_";

        private readonly ProcedureListUpdaterOptions mOptions;

        /// <summary>
        /// This RegEx matches the argument direction, name, type, and default value (if defined)
        /// </summary>
        private readonly Regex mArgumentMatcher = new("((?<Direction>INOUT|OUT|IN) +)?(?<Name>[^ ]+) +(?<Type>[^ ]+)(?<Default>.*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This RegEx looks for lines with $$ or $_$ or $body$, optionally preceded by "AS "
        /// </summary>
        private readonly Regex mDollarMatcher = new(@"(AS +|^ *)(?<Delimiter>\$[^$]*\$)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This RegEx looks for the column names for functions that return a table
        /// </summary>
        private readonly Regex mReturnedColumnListMatcher = new(@"\bTABLE *\((?<TableColumns>[^)]+)\) *(?<LanguageAndOptions>.*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This RegEx looks for the return type of a function
        /// </summary>
        /// <remarks>
        /// Example matches:
        ///   RETURNS anyelement
        ///   RETURNS boolean
        ///   RETURNS event_trigger
        ///   RETURNS integer
        ///   RETURNS numeric
        ///   RETURNS public.citext
        ///   RETURNS record
        ///   RETURNS SETOF public.pg_stat_statements
        ///   RETURNS SETOF record
        ///   RETURNS smallint
        ///   RETURNS TABLE(category text, value text)
        ///   RETURNS TABLE(dy timestamp with time zone)
        ///   RETURNS text
        ///   RETURNS timestamp without time zone
        ///   RETURNS trigger
        ///   RETURNS xml
        /// </remarks>
        private readonly Regex mReturnsMatcher = new(@"\bRETURNS +(?<ReturnType>timestamp with(out)? time zone|(SETOF )?[^ (]+) *(?<LanguageAndOptions>.*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public ProcedureListUpdater(ProcedureListUpdaterOptions options)
        {
            mOptions = options;
        }

        private bool ExtractArguments(
            string objectType,
            string sourceDescription,
            ICollection<string> headerLines,
            StringBuilder objectHeader,
            ICollection<ArgumentInfo> argumentList)
        {
            argumentList.Clear();

            // Determine the procedure or function argument names and types

            var success = ExtractArgumentsFromHeader(objectType, sourceDescription, objectHeader, out var argumentListMatch);

            if (!success)
                return false;

            if (argumentListMatch.Trim().Length == 0)
                return true;

            // Replace any quoted commas with a placeholder

            var argumentListToParse = ReplaceQuotedCommas(argumentListMatch);

            // Split the argument list on commas

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var argument in argumentListToParse.Split(','))
            {
                // Convert any comma placeholders back to actual commas
                var argumentInfo = new ArgumentInfo(argument.Trim().Replace(COMMA_PLACEHOLDER, ","));

                var nameWithSpace = string.Format("{0} ", argumentInfo.Definition.Replace("public.citext", "citext"));

                // Look for the argument in headerLines
                // If the argument has a comment, include the comment when adding to argumentList
                foreach (var headerLine in headerLines)
                {
                    var argumentIndex = headerLine.IndexOf(nameWithSpace, StringComparison.OrdinalIgnoreCase);

                    if (argumentIndex < 0)
                        continue;

                    var commentIndex = headerLine.IndexOf("--", StringComparison.Ordinal);

                    if (commentIndex < 0)
                        break;

                    argumentInfo.Comment = headerLine.Substring(commentIndex);
                    break;
                }

                argumentList.Add(argumentInfo);
            }

            return true;
        }

        private bool ExtractArgumentsFromHeader(
            string objectType,
            string sourceDescription,
            StringBuilder objectHeader,
            out string argumentListMatch)
        {
            var sourceHeaderText = objectHeader.ToString();

            string headerText;

            if (objectType.Equals("FUNCTION", StringComparison.OrdinalIgnoreCase))
            {
                var returnsIndex = sourceHeaderText.IndexOf("RETURNS ", StringComparison.OrdinalIgnoreCase);

                if (returnsIndex < 0)
                {
                    OnWarningEvent("Header found, but 'RETURNS' not found and thus unable to parse the arguments for {0}: {1}", sourceDescription, objectHeader);
                    argumentListMatch = string.Empty;
                    return false;
                }

                headerText = sourceHeaderText.Substring(0, returnsIndex);
            }
            else
            {
                headerText = sourceHeaderText;
            }

            var openingParentheses = headerText.IndexOf("(", StringComparison.Ordinal);
            var closingParentheses = headerText.LastIndexOf(")", StringComparison.Ordinal);

            if (openingParentheses < 0)
            {
                OnWarningEvent("Header found, but '(' not found and thus cannot parse the arguments for {0}: {1}", sourceDescription, objectHeader);
                argumentListMatch = string.Empty;
                return false;
            }

            if (closingParentheses < 0)
            {
                OnWarningEvent("Header found, but ')' not found and thus cannot parse the arguments for {0}: {1}", sourceDescription, objectHeader);
                argumentListMatch = string.Empty;
                return false;
            }

            if (closingParentheses == openingParentheses + 1)
            {
                argumentListMatch = string.Empty;
            }
            else
            {
                argumentListMatch = headerText.Substring(openingParentheses + 1, closingParentheses - openingParentheses - 1);
            }

            return true;
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

            objectName = nameMatch.Groups["ObjectName"].Value.Trim();
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

                headerLinesAfterArguments.Add(headerTextAfterArguments.Substring(languageIndex, asBodyDelimiterIndex - languageIndex).Trim());
                headerLinesAfterArguments.Add(headerTextAfterArguments.Substring(asBodyDelimiterIndex).Trim());

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
                OnStatusEvent("Writing " + PathUtils.CompactPathString(outputFilePath, 100));

                using var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));
                using var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                var objectNameMatcher = new Regex("CREATE OR REPLACE (?<ObjectType>PROCEDURE|FUNCTION) *(?<ObjectName>[^(]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var currentLineNumber = 0;
                var lastStatusTime = DateTime.UtcNow;

                var objectsProcessed = 0;
                var objectsUpdated = 0;

                // Keys in this dictionary are procedure or function names, values are the number of instances of the object (used for handling overloaded procedures)
                var objectNames = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

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

                    int overloadNumber;

                    if (objectNames.TryGetValue(objectNameWithSchema, out var instanceCount))
                    {
                        instanceCount++;
                        objectNames[objectNameWithSchema] = instanceCount;
                        overloadNumber = instanceCount;
                    }
                    else
                    {
                        instanceCount = 1;
                        objectNames.Add(objectNameWithSchema, instanceCount);
                        overloadNumber = instanceCount;
                    }

                    if (objectNameWithSchema.Equals("public.retry_myemsl_upload"))
                    {
                        Console.WriteLine("Check this code");
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

                    var sourceArgumentList = new List<ArgumentInfo>();

                    var argumentsFound = ExtractArguments(objectType, sourceDescription, sourceHeaderLines, sourceObjectHeader, sourceArgumentList);

                    if (!argumentsFound)
                    {
                        OnWarningEvent("Aborting since ExtractArguments returned false");
                        return false;
                    }

                    var argumentNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    var argumentNumber = 0;

                    foreach (var argument in sourceArgumentList)
                    {
                        argumentNumber++;

                        if (string.IsNullOrWhiteSpace(argument.Definition))
                        {
                            OnWarningEvent("Argument {0} is empty for {1} {2}{3}",
                                argumentNumber + 1, objectType.ToLower(), objectNameWithSchema,
                                argumentNumber == sourceArgumentList.Count ? "; likely the final argument has a trailing comma" : string.Empty);

                            continue;
                        }

                        var match = mArgumentMatcher.Match(argument.Definition);

                        if (match.Success)
                        {
                            var argumentName = match.Groups["Name"].Value;

                            if (argumentNameMap.ContainsKey(argumentName))
                            {
                                OnWarningEvent("Argument name map for {0} {1} already has argument {2}; skipping",
                                    objectType.ToLower(), objectNameWithSchema, argument);
                            }
                            else
                            {
                                argumentNameMap.Add(argumentName, argumentName);
                            }
                        }
                        else
                        {
                            OnWarningEvent("Argument for {0} {1} did not match the expected format: {2}",
                                objectType.ToLower(), objectNameWithSchema, argument);
                        }
                    }

                    if (mOptions.VerboseOutput)
                    {
                        OnStatusEvent("Reading file {0}", PathUtils.CompactPathString(foundFiles[0].Name, 120));
                    }

                    if (DateTime.UtcNow.Subtract(lastStatusTime).TotalSeconds >= 0.2)
                    {
                        ShowUpdateStatus(objectsUpdated, objectsProcessed);
                        lastStatusTime = DateTime.UtcNow;
                    }

                    // Load the object info from the DDL file with the updated version of the procedure or function
                    var sqlFileRead = ReadSqlFile(
                        foundFiles[0],
                        objectType.ToLower(),
                        objectNameWithSchema,
                        objectNameMatcher,
                        overloadNumber,
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

                        objectsProcessed++;
                        continue;
                    }

                    var writeSuccess = WriteHeaderAndBody(
                        writer,
                        objectType,
                        objectNameWithSchema,
                        objectArgumentList,
                        headerLinesAfterArguments,
                        objectBody,
                        argumentNameMap);

                    if (writeSuccess)
                    {
                        objectsProcessed++;
                        objectsUpdated++;
                        continue;
                    }

                    OnWarningEvent("Aborting since WriteHeaderAndBody returned false");
                    return false;
                }

                Console.WriteLine();
                OnStatusEvent("Processed {0} lines in the input file", currentLineNumber);
                ShowUpdateStatus(objectsUpdated, objectsProcessed);

                Console.WriteLine();
                OnStatusEvent("Output file: " + outputFilePath);

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

                    // Do not include comments when appending the data line to objectHeader (since that can cause spurious RegEx matches)

                    objectHeader.AppendFormat(" {0}", RemoveComment(dataLine));

                    var match = mDollarMatcher.Match(dataLine);

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
        /// <param name="overloadNumberToFind">This is typically 1, but if an object is overloaded, this will be 2 when processing the second instance of an object</param>
        /// <param name="argumentList"></param>
        /// <param name="headerLinesAfterArguments">Tracks the text that occurs after the closing parenthesis of the procedure or function's argument list</param>
        /// <param name="objectBody">Procedure or function text between the starting and ending $$</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ReadSqlFile(
            FileSystemInfo inputFile,
            string objectType,
            string objectNameWithSchema,
            Regex objectNameMatcher,
            int overloadNumberToFind,
            out List<ArgumentInfo> argumentList,
            out List<string> headerLinesAfterArguments,
            out List<string> objectBody)
        {
            argumentList = new List<ArgumentInfo>();
            headerLinesAfterArguments = new List<string>();

            // Typically $$, but is sometimes $_$
            var objectBodyDelimiter = string.Empty;

            try
            {
                // Each line of the object header
                var headerLines = new List<string>();

                var overloadNumber = 0;

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

                    overloadNumber++;

                    if (overloadNumberToFind > overloadNumber)
                    {
                        // Found an earlier overload; need to keep processing to find the next overload
                        OnStatusEvent("Looking for overload {0} of {1} {2}", overloadNumberToFind, objectType, objectNameWithSchema);
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

                    var argumentsFound = ExtractArguments(objectType, sourceDescription, headerLines, objectHeader, argumentList);

                    if (!argumentsFound)
                    {
                        return false;
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

                        var returnsIndex = headerLine.IndexOf("RETURNS ", StringComparison.OrdinalIgnoreCase);

                        var parenthesisIndex = returnsIndex > 0
                            ? headerLine.Substring(0, returnsIndex).LastIndexOf(')')
                            : headerLine.LastIndexOf(')');

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
                            headerTextAfterArguments.ToString().Trim(),
                            objectBodyDelimiter,
                            objectType,
                            inputFile,
                            headerLinesAfterArguments);

                        return true;
                    }

                    // Determine the return type of the function
                    // If it is a table, parse the column names and types

                    var returnMatch = mReturnsMatcher.Match(headerTextAfterArguments.ToString());

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

                    var columnListMatch = mReturnedColumnListMatcher.Match(headerTextAfterArguments.ToString());

                    if (!columnListMatch.Success)
                    {
                        OnWarningEvent("Header found, but unable to determine the column list for the table returned by function {0}",
                            objectNameWithSchema);

                        return false;
                    }

                    headerLinesAfterArguments.Add("RETURNS TABLE (");

                    var tableColumns = columnListMatch.Groups["TableColumns"].Value.Split(',').ToList();
                    var indexEnd = tableColumns.Count - 1;

                    for (var i = 0; i <= indexEnd; i++)
                    {
                        var columnNameAndType = tableColumns[i].Replace("public.citext", "citext");

                        headerLinesAfterArguments.Add(string.Format("    {0}{1}", columnNameAndType.Trim(), i < indexEnd ? "," : string.Empty));
                    }

                    headerLinesAfterArguments.Add(")");

                    ParseHeaderLinesAfterArguments(
                        columnListMatch.Groups["LanguageAndOptions"].Value,
                        objectBodyDelimiter,
                        objectType,
                        inputFile,
                        headerLinesAfterArguments);

                    return true;
                }

                if (overloadNumber == 0)
                {
                    OnWarningEvent("Did not find \"CREATE OR REPLACE\" in file {0}", PathUtils.CompactPathString(inputFile.FullName, 120));
                }
                else if (overloadNumberToFind > 1)
                {
                    OnWarningEvent("Found overload {0} but not overload {1} in file {2}", overloadNumber, overloadNumberToFind, PathUtils.CompactPathString(inputFile.FullName, 120));
                }
                else
                {
                    OnWarningEvent("Unreachable code encountered while looking for overload {0} in file {1}", overloadNumberToFind, PathUtils.CompactPathString(inputFile.FullName, 120));
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

        private string RemoveComment(string dataLine)
        {
            var commentIndex = dataLine.IndexOf("--", StringComparison.Ordinal);

            return commentIndex < 0
                ? dataLine
                : dataLine.Substring(0, commentIndex).Trim();
        }

        private string ReplaceQuotedCommas(string argumentList)
        {
            var startIndex = 0;

            var updatedArgumentList = new StringBuilder();

            while (true)
            {
                var quoteIndex = argumentList.Substring(startIndex).IndexOf('\'');

                if (quoteIndex < 0)
                    break;

                quoteIndex += startIndex;

                var nextQuoteIndex = argumentList.Substring(quoteIndex + 1).IndexOf('\'');

                if (nextQuoteIndex < 0)
                    break;

                nextQuoteIndex += quoteIndex + 1;

                if (nextQuoteIndex == quoteIndex + 1)
                {
                    startIndex = nextQuoteIndex + 1;
                    continue;
                }

                var quotedValue = argumentList.Substring(quoteIndex + 1, nextQuoteIndex - quoteIndex - 1);

                var commaIndex = quotedValue.IndexOf(',');

                if (commaIndex < 0)
                {
                    startIndex = nextQuoteIndex + 1;
                    continue;
                }

                if (startIndex < quoteIndex)
                {
                    updatedArgumentList.Append(argumentList, 0, quoteIndex);
                }

                updatedArgumentList.AppendFormat("'{0}'", quotedValue.Replace(",", COMMA_PLACEHOLDER));

                if (nextQuoteIndex < argumentList.Length - 1)
                {
                    updatedArgumentList.Append(argumentList, nextQuoteIndex + 1, argumentList.Length - (nextQuoteIndex + 1));
                }

                var addedCharacterCount = updatedArgumentList.Length - argumentList.Length;

                argumentList = updatedArgumentList.ToString();
                startIndex = nextQuoteIndex + 1 + addedCharacterCount;
            }

            return argumentList;
        }

        private void ShowUpdateStatus(int objectsUpdated, int objectsProcessed, bool processingComplete = false)
        {
            OnStatusEvent("{0,-9} procedures or functions {1} updated using .sql files",
                string.Format("{0}{1} / {2}", objectsUpdated < 99 ? " " : string.Empty, objectsUpdated, objectsProcessed),
                processingComplete ? "were" : "have been");
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
            IReadOnlyList<ArgumentInfo> objectArgumentList,
            IEnumerable<string> headerLinesAfterArguments,
            IEnumerable<string> objectBody,
            IReadOnlyDictionary<string, string> argumentNameMap)
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

                        if (string.IsNullOrWhiteSpace(objectArgumentList[i].Definition))
                        {
                            OnWarningEvent("Argument {0} is empty for {1} {2}",
                                i + 1, objectType.ToLower(), objectNameWithSchema);

                            continue;
                        }

                        var match = mArgumentMatcher.Match(objectArgumentList[i].Definition);

                        if (match.Success)
                        {
                            var argDirection = match.Groups["Direction"].Value;

                            if (string.IsNullOrWhiteSpace(argDirection))
                            {
                                if (!objectType.Equals("FUNCTION", StringComparison.OrdinalIgnoreCase))
                                {
                                    OnWarningEvent("Argument for {0} {1} did not have a direction: {2}",
                                        objectType.ToLower(), objectNameWithSchema, objectArgumentList[i].Definition);
                                }
                            }
                            else if (!argDirection.Equals("IN", StringComparison.OrdinalIgnoreCase))
                            {
                                updatedArgument.AppendFormat("{0} ", argDirection);
                            }

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
                                OnWarningEvent("Source file does not have argument {0} for {1} {2}",
                                    argumentName, objectType.ToLower(), objectNameWithSchema);

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
                                objectType.ToLower(), objectNameWithSchema, objectArgumentList[i]);

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
