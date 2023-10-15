# PgSQL Procedure List Updater

This program processes a PostgreSQL DDL file with `CREATE OR REPLACE PROCEDURE` and
`CREATE OR REPLACE FUNCTION` statements and updates each procedure or function found
using a file created by the DB Schema Export Tool (https://github.com/PNNL-Comp-Mass-Spec/DB-Schema-Export-Tool).

The intended purpose is to update the DDL file with updated versions of the procedures and functions,
scripted from the live database.

## Console Switches

The PgSQL Procedure List Updater is a console application, and must be run from the Windows command prompt.

```
PgSqlProcedureListUpdater.exe
  /I:InputFilePath
  /D:SqlFilesDirectory
  [/O:OutputFilePath]
  [/Verbose|/V]
  [/ParamFile:ParamFileName.conf] [/CreateParamFile]
```

The input file should be a SQL text file with `CREATE OR REPLACE` statements
* Example input file statements:

```PLpgSQL
CREATE OR REPLACE FUNCTION public.days_and_hours_in_date_range
(
    _startDate timestamp = '1/1/2005',
    _endDate timestamp = '1/21/2005',
    _hourInterval int = 6
)
RETURNS TABLE(dy timestamp)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Returns a series of date/time values spaced _hourInterval hours apart
**
**  Auth:   mem
**  Date:   11/07/2007
**          11/29/2007 mem - Fixed bug that started at _startDate + _hourInterval instead of at _startDate
**          06/17/2022 mem - Ported to PostgreSQL
**          10/22/2022 mem - Directly pass value to function argument
**
*****************************************************/
BEGIN
    RETURN QUERY
    SELECT generate_series (_startDate, _endDate, make_interval(hours => _hourInterval));
END
$$;
```

* For each procedure or function found, looks for a corresponding `.sql` file in the directory specified by `/D`
  * If a file is found, replaces the procedure or function definition with the definition in the file

* A new file is created with the updated DDL

* Use [`/V`] to show additional status messages while processing the input file

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov\
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics/
Source code: https://github.com/PNNL-Comp-Mass-Spec/PgSQL-Table-Creator-Helper

## License

Licensed under the 2-Clause BSD License; you may not use this program except
in compliance with the License.  You may obtain a copy of the License at
https://opensource.org/licenses/BSD-2-Clause

Copyright 2023 Battelle Memorial Institute
