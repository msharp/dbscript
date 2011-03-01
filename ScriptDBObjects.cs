/* 
 * *******************************************************************
 * Program: ScriptDbObjects.cs
 * Author: Max Sharples
 * Date: 2010-8-12
 * *******************************************************************
 
  This is a tool for creating  DDL scripts from a SQLServer database 
  The DDL scripts are generated within a specific file structure that 
  can be used by dbscript.CreateDatabase to recreate the database.
  
  Example of file structure created by ScriptDbObjects
  
    /Databases (directory in local repository branch)
        /DatabaseName
            /SchemaObjects
                dbname.database.sql
                /Tables
                    schema.tablename.table.sql *
                    /Constraints
                        dbo.tablename.contraintname.chkconst.sql *
                        dbo.tablename.contraintname.defconst.sql *
                    /Indexes
                        dbo.tablename.indexname.index.sql *
                    /Keys
                        dbo.tablename.keyname.pkey.sql *
                    /Triggers
                        dbo.tablename.triggername.trigger.sql *
                /Views
                    dbo.viewname.view.sql *   
                    /Indexes
                        dbo.viewname.indexname.index.sql *
                    /Triggers
                        dbo.viewname.triggername.trigger.sql *
                /Stored Procedures
                    dbo.sp_procedurename.proc.sql *
                /Functions
                    dbo.fn_functionname.function *
     
 */

using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;

namespace dbscript
{
    class ScriptDbObjects
    {
        public ScriptDbObjects(Connection connection, string filePath, string dbName)
        {
           
            // ensure the provided path is available
            if (!Directory.Exists(filePath))
            {
                Console.WriteLine("\nERROR: the Path \"" + filePath + "\" does not exist\n");
                return;
            }
            
            // start
            DateTime began = DateTime.Now;
            int objectCount = 0;

            List<string> databases = new List<string>();
            if (dbName.Length > 0) databases.Add(dbName);

            // if no database specified, script 'em all
            if (databases.Count == 0) databases = getAllDatabases(connection);

            foreach (string db in databases)
            {
                ScriptDB scr = new ScriptDB(db, connection);
                scr.scriptDB(filePath);

                objectCount += scr.objectCount;
            }

            // done
            DateTime ended = DateTime.Now;
            Console.WriteLine("\nProgram began " + began.ToLongTimeString() + ", ended: " + ended.ToLongTimeString());
            Console.WriteLine(objectCount.ToString() + " objects scripted.\n");
        }

        static List<string> getAllDatabases(Connection connstr)
        {
            List<string> dbs = new List<string>();
            string command = "SELECT "
                             + "   name "
                             + "FROM "
                             + "   master.dbo.sysdatabases "
                             + "WHERE "
                             + "   name NOT IN ( 'master', 'model', 'msdb', 'tempdb' ) "
                             + "ORDER BY "
                             + "   name";

            SqlConnection cn = new SqlConnection(connstr.connectionString());
            cn.Open();
            SqlCommand cmd = new SqlCommand(command, cn);

            // issue the query
            SqlDataReader rdr = null;
            try
            {
                rdr = cmd.ExecuteReader();
                if (rdr != null && !rdr.IsClosed)
                {
                    if (rdr.HasRows)
                    {
                        while (rdr.Read())
                        {
                            dbs.Add(rdr["name"].ToString());
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("\nUnable to retrieve list of databases from server \n");
                Console.WriteLine(e);
            }

            return dbs;
        }

    }
}
