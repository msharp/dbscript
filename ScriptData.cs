using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace dbscript
{
    class ScriptData
    {
        private string _table;
        private string _database;
        private int _limit_results;
        private bool _tbl_has_identity;
        private bool _for_fixtures;
        private string _where_clause;

        private const int ROW_LIMIT = 10000;

        public ScriptData(Connection conn, string dbFilesPath, string dbName, string tblName)
        :this(conn, dbFilesPath, dbName, tblName, -1, false, "")
        {}

        public ScriptData(Connection conn, string dbFilesPath, string dbName, string tblName, int limit, bool fixtures, string where)
        {
            Console.WriteLine(String.Format(@"Generating data insert script for: {0}.dbo.{1}", dbName, tblName));

            // generate data insert script as <dbFilePath>\Data\<tblName>.insert.sql
            string dbDataScriptsPath = getDataScriptsPath(dbFilesPath, dbName, fixtures);

            string filename = String.Format(@"{0}\{1}.insert.sql", dbDataScriptsPath, tblName);
            _table = tblName;
            _database = dbName;
            _limit_results = limit;
            _for_fixtures = fixtures;
            _where_clause = where;

            _tbl_has_identity = hasIdentity(conn, dbName, tblName);

            if (_tbl_has_identity == false) Console.WriteLine("table has no identity column !!");            
            ArrayList cols = columns(conn);
            Console.WriteLine("got " + cols.Count + " columns ");

            List<Hashtable> data = tableData(cols, conn);
            Console.WriteLine("got " + data.Count + " rows ");

            // write file
            generateDataScript(filename, cols, data);

            Console.WriteLine("Done ... \n"); 

        }

        private void generateDataScript(string filename, ArrayList cols, List<Hashtable> data)
        { 
            int fileCounter = 0;
            string sqlfile = filename;

            if (data.Count > ROW_LIMIT)
            {
                fileCounter++;
                sqlfile = Regex.Replace(filename, @"insert\.sql", @"insert." + fileCounter.ToString() + ".sql");
            }

            TextWriter tw = getScriptFileForWriting(sqlfile, cols);

            int rowcount = 0;
            foreach (Hashtable row in data)
            {
                int colcount = 0;
                string sql = "SELECT "; 
                foreach (string col in cols)
                {
                    sql += formatForTsqlScript(row[col.ToString()]);
                    colcount++;
                    if (colcount < cols.Count) sql += ",";
                }
                tw.WriteLine(sql);
                tw.Flush();

                rowcount++;
                if (rowcount % ROW_LIMIT == 0)
                {
                    // large data sets need to be separated into multiple insert files
                    // larger insert scripts (millions of rows) throw memory exceptions
                    closeScriptFile(tw);
                    fileCounter++;
                    sqlfile = Regex.Replace(filename, @"insert\.sql", @"insert." + fileCounter.ToString() + ".sql");
                    tw = getScriptFileForWriting(sqlfile, cols, false);
                }
                else if (rowcount < data.Count)
                {
                    tw.WriteLine("UNION ALL"); // if not at end of data set
                }
            }
            closeScriptFile(tw);
        }

        private TextWriter getScriptFileForWriting(string filename, ArrayList cols)
        {
            bool trunc = true;
            if (_for_fixtures == true) trunc = false;
            return getScriptFileForWriting(filename, cols, trunc);
        }

        private TextWriter getScriptFileForWriting(string filename, ArrayList cols, bool withTruncate)
        {
            Console.WriteLine("generating script file: " + filename);
            // write file
            TextWriter tw = new StreamWriter(filename);
            if (withTruncate == true) tw.WriteLine("TRUNCATE TABLE [{0}]", _table); // fixtures don't truncate tables
            if (_tbl_has_identity == true) tw.WriteLine("SET IDENTITY_INSERT [{0}] ON", _table);

            tw.WriteLine("INSERT INTO [{0}] (", _table);
            tw.WriteLine("[" + string.Join("],[", cols.ToArray(typeof(string)) as string[]) + "]");
            tw.WriteLine(")\n");

            return tw;
        }
        private void closeScriptFile(TextWriter tw)
        {
            if (_tbl_has_identity == true) tw.WriteLine("SET IDENTITY_INSERT [{0}] OFF", _table);
            tw.Close();
        }

        private List<Hashtable> tableData(ArrayList cols, Connection conn)
        {

            // limiting results?
            string limit = "";
            string order_by = ""; // if negative number for LIMIT then order descending (by first column)

            if (_limit_results != 0)
            {
                // get top n
                limit = String.Format(@"TOP {0} ", Math.Abs(_limit_results).ToString());
                // ordering
                order_by = "ORDER BY " + cols[0];
                if (_limit_results < 0) order_by += " DESC";
            }

            string command = String.Format(@"SELECT {0}* FROM [{1}].[dbo].[{2}] WITH(NOLOCK) {3} {4}", limit, _database, _table, _where_clause, order_by);

            SqlConnection sqlconn = new SqlConnection(conn.connectionString());
            sqlconn.Open();

            SqlCommand cmd = new SqlCommand(command, sqlconn);
            SqlDataReader rdr = cmd.ExecuteReader();

            List<Hashtable> rows = new List<Hashtable>();

            while (rdr.Read())
            {
                Hashtable row = new Hashtable();
                foreach (string c in cols)
                {
                    row.Add(c, rdr[c]);
                }
                rows.Add(row);
            }
            rdr.Close();

            return rows;
        }

        private ArrayList columns(Connection conn) 
        {
            ArrayList cols = new ArrayList();

            SqlConnection sqlconn = new SqlConnection(conn.connectionString());
            sqlconn.Open();

            SqlCommand cmd = new SqlCommand(_database+".dbo.sp_columns", sqlconn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.Add(new SqlParameter("@table_name", _table));

            // execute the command
            SqlDataReader rdr = cmd.ExecuteReader();

            while (rdr.Read())
            {
                cols.Add(rdr["COLUMN_NAME"]);
            }
            rdr.Close();

            return cols;
        }
        private string dataScript(ArrayList cols, List<Hashtable> data)
        {
            string script = @"INSERT INTO [" + _table + "] (";
            script += string.Join(",", cols.ToArray(typeof(string)) as string[]);
            script += ")\n";

            int rowcount = 0;
            foreach (Hashtable row in data)
            {
                int colcount = 0;
                script += "SELECT "; 
                foreach (string col in cols)
                {
                    script += formatForTsqlScript(row[col.ToString()]);
                    colcount++;
                    if (colcount < cols.Count) script += ",";
                }
                rowcount++;
                // if not at end of data
                if (rowcount < data.Count) script += "\nUNION ALL\n";
            }
            return script;
        }

        public static bool hasIdentity(Connection conn, string dbName, string tblName)
        { 
            //var sql = String.Format(@"SELECT COUNT(*) FROM {0}.SYS.IDENTITY_COLUMNS WHERE OBJECT_NAME(OBJECT_ID) = '{1}'", dbName, tblName);
            var sql = String.Format(@"USE {0} SELECT OBJECTPROPERTY(OBJECT_ID('{1}'), 'TableHasIdentity')", dbName, tblName);
            SqlConnection sqlconn = new SqlConnection(conn.connectionString());
            sqlconn.Open();

            SqlCommand cmd = new SqlCommand(sql,sqlconn);
            if ((int)cmd.ExecuteScalar() == (int)0)
                return false;
            else
                return true;   
        }

        public static string[] getTables(Connection conn, string dbName)
        { 
            ArrayList tbls = new ArrayList();

            SqlConnection sqlconn = new SqlConnection(conn.connectionString());
            sqlconn.Open();

            SqlCommand cmd = new SqlCommand(dbName + ".dbo.sp_tables", sqlconn);
            cmd.CommandType = CommandType.StoredProcedure;

            SqlDataReader rdr = cmd.ExecuteReader();
            while (rdr.Read())
            {
                if (
                    Convert.ToString(rdr["TABLE_TYPE"]) == "TABLE" && 
                    Convert.ToString(rdr["TABLE_OWNER"]) != "sys"
                    ) tbls.Add(rdr["TABLE_NAME"]);
            }
            rdr.Close();

            return tbls.ToArray(typeof(string)) as string[];
        }

        static string getDataScriptsPath(string dbFilesPath, string dbName, bool fixtures)
        {
            string dir = "Data";
            if (fixtures == true) dir = "Fixtures";

            var dbPath = String.Format(@"{0}\{1}", dbFilesPath, dbName);
            var dataScriptsPath = String.Format(@"{0}\{1}", dbPath, dir);
            if (!Directory.Exists(dbFilesPath)) Directory.CreateDirectory(dbFilesPath);
            if (!Directory.Exists(dbPath)) Directory.CreateDirectory(dbPath);
            if (!Directory.Exists(dataScriptsPath)) Directory.CreateDirectory(dataScriptsPath);
            return dataScriptsPath;
        }

        static string formatForTsqlScript(object val)
        {
            Type t = val.GetType();
            string ret = "";

            if (t == typeof(System.Boolean))
            {
                if ((bool)val == true)
                    ret = "1";
                else
                    ret = "0";
            }
            else if (t == typeof(System.Int32) || t == typeof(System.Int64))
            {
                ret = val.ToString();
            }
            else if (t == typeof(System.DBNull))
            {
                ret = "NULL";
            }
            else if (t == typeof(System.DateTime))
            {
                DateTime dt = (DateTime)val;
                ret = "'" + dt.ToString("yyyyMMdd HH':'mm':'ss") + "'";
            }
            else // strings, etc... 
            {
                ret = "'" + val.ToString().Replace("'", "''") + "'";
            }
            return ret;
        }
    }
}
