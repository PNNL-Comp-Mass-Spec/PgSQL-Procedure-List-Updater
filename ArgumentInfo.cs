namespace PgSqlProcedureListUpdater
{
    internal class ArgumentInfo
    {
        public string Comment { get; set; }

        /// <summary>
        /// Argument direction, name, type, and possible a default value
        /// </summary>
        public string Definition { get; }

        public ArgumentInfo(string name, string comment = "")
        {
            Definition = name;
            Comment = comment;
        }

        public override string ToString()
        {
            return Definition;
        }
    }
}
