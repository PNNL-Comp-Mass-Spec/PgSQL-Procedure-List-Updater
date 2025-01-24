namespace PgSqlProcedureListUpdater
{
    internal class ArgumentInfo
    {
        // Ignore Spelling: Sql
        public string Comment { get; set; }

        /// <summary>
        /// Argument direction, name, type, and possible a default value
        /// </summary>
        public string Definition { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">Argument name</param>
        /// <param name="comment">Comment</param>
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
