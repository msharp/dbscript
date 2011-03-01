/* 
 * *******************************************************************
 * Program: CreateDatabase.cs
 * Author: Max Sharples
 * Date: 2010-8-12
 * *******************************************************************
 
  This is a tool for recreating a SQLServer database from DDL scripts
  The DDL scripts are generated using another utility ScriptDbObjects
  and will be contained in a specific file structure 
  
  Example of file structure created by ScriptDbObjects and expected by CreateDatabase:
  
    /Databases (directory in local repository branch)
        /DatabaseName
            /Data
                insert_static_data.sql (created by ScriptData utility)
            /Fixtures
                insert_arbitrary_testing_data.sql (created by ScriptData or by dev's)
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
 
 
  This program walks the directory tree in a specific order 
  (Tables, Views, Funtions, Stored procedures, Data)
  to execute the scripts to build a fresh database instance

 */

using System;
using System.IO;
using System.Collections.Generic;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;

namespace dbscript
{
    class CreateDatabase
    {
        public List<ScriptExecute> failedScripts = new List<ScriptExecute>();
        public int totalScripts = 0;
        private bool with_fixtures = false;

        public CreateDatabase(Connection conn, string dbPath, string dbName, bool withFixtures)
        {
            with_fixtures = withFixtures;
            createDb(conn, dbPath, dbName);
        }

        public CreateDatabase(Connection conn, string dbPath, string dbName)
        {
            createDb(conn, dbPath, dbName);
        }

        private void createDb(Connection conn, string dbPath, string dbName)
        {
            // get sql files to execute in order of
            // ./SchemaObjects >> Tables, Views, Functions, Procs
            // ./Data & ./Fixtures
            string dbFilePath = dbPath + "\\" + dbName;
            if (isDatabaseDirectory(dbName, dbFilePath))
            {
                Database db = createDB(conn, dbName, dbFilePath);
                if (db == null)
                {
                    Console.WriteLine("\nquitting .... ");
                    return;
                }

                // TODO _ only run the permission atatements (not 'create database')
                // runScripts(dbFilePath + @"\SchemaObjects", db);

                runScripts(dbFilePath + @"\SchemaObjects\Tables", db);
                runScripts(dbFilePath + @"\SchemaObjects\Tables\Keys", db);
                //runScripts(dbFilePath + @"\SchemaObjects\Tables\Keys", db, "*.pkey.sql");
                //runScripts(dbFilePath + @"\SchemaObjects\Tables\Keys", db, "*.fkey.sql");
                //runScripts(dbFilePath + @"\SchemaObjects\Tables\Keys", db, "*.ukey.sql");
                runScripts(dbFilePath + @"\SchemaObjects\Tables\Indexes", db);
                runScripts(dbFilePath + @"\SchemaObjects\Tables\Constraints", db);
                runScripts(dbFilePath + @"\SchemaObjects\Tables\Triggers", db);

                runScripts(dbFilePath + @"\SchemaObjects\Views", db);
                runScripts(dbFilePath + @"\SchemaObjects\Views\Indexes", db);
                runScripts(dbFilePath + @"\SchemaObjects\Views\Triggers", db);

                runScripts(dbFilePath + @"\SchemaObjects\Functions", db);
                runScripts(dbFilePath + @"\SchemaObjects\Stored Procedures", db);

                runScripts(dbFilePath + @"\Data", db);

                retryFailedScripts(db);

                if (with_fixtures == true)
                {
                    Console.WriteLine("\nBuilding test data from fixtures.\n");
                    runScripts(dbFilePath + @"\Fixtures", db);
                    //retryFailedScripts(db);
                }

                Console.WriteLine("\n" + totalScripts + " scripts executed. " + failedScripts.Count + " failed.\n");
            }
            else
            {
                Console.WriteLine(dbFilePath + " doesn't look like is contains database object creation scripts.");
            }
        }

        // **********************************************
        // run in this program with a database object
        private void runScripts(string dir, Database db)
        {
            if (Directory.Exists(dir))
            {
                string[] sqlfiles = Directory.GetFiles(dir);
                runScripts(sqlfiles, db);
            }
        }        
        private void runScripts(string dir, Database db, string glob)
        {
            if (Directory.Exists(dir))
            {
                string[] sqlfiles = Directory.GetFiles(dir, glob);
                runScripts(sqlfiles, db);
            }
        }

        private void runScripts(string[] files, Database db)
        {
            foreach (string sql in files)
            {
                Console.WriteLine(sql);
                totalScripts++;
                try
                {
                    ScriptExecute exec = new ScriptExecute(sql, db);
                    if (exec.success == false) failedScripts.Add(exec);
                }
                catch (Microsoft.SqlServer.Management.Smo.FailedOperationException e)
                {
                    Console.WriteLine(e.InnerException);
                }
            }
        }


        // **********************************************
        // run using the sqlcmd utility
        private void runScripts(string dir, Connection conn, string dbName)
        {
            if (Directory.Exists(dir))
            {
                string[] sqlfiles = Directory.GetFiles(dir);
                runScripts(sqlfiles, conn, dbName);
            }           
        }

        private void runScripts(string[] files, Connection conn, string dbName)
        {
            foreach (string sql in files)
            {
                Console.WriteLine(sql);
                totalScripts++;
                try
                {
                    ScriptExecute exec = new ScriptExecute(sql, dbName, conn);
                    if (exec.success == false)
                    {
                        failedScripts.Add(exec);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.InnerException);
                }
            }
        }



        private void retryFailedScripts(Database db)
        {            
            // rerun any failed scripts to resolve dependencies between views/functions/procs
            int fld = failedScripts.Count / 4; // semi-arbitrary value
            for (int i = 0; i < fld; i++)
            {
                Console.WriteLine("Retrying failed scripts [" + i.ToString() + "] ... ");
                foreach (ScriptExecute exec in failedScripts)
                {
                    Console.WriteLine("Retry: " + exec.sql);
                    exec.execute(db);
                }
                for (int j = 0; j < failedScripts.Count; j++)
                {
                    if (failedScripts[j].success == true) failedScripts.Remove(failedScripts[j]);
                }
            }
            if (failedScripts.Count > 0) alertFailed();
        }

        private void alertFailed()
        {
            Console.WriteLine("\n**************************************************************");
            Console.WriteLine("The following " + failedScripts.Count + " scripts failed. Please review accordingly.");
            Console.WriteLine("*************************************************************\n.");
            foreach (ScriptExecute exec in failedScripts)
            {
                if (exec.success == false)
                {
                    Console.WriteLine("\nScript File ===> \n " + exec.sql);
                    Console.WriteLine("\nException Trace ===> \n " + exec.exception +"\n");

                    Console.WriteLine("\nPress Enter to continue, q to skip alerts, or v to view the contents of the script file.");
                    string nxt = Console.ReadLine();
                    if (nxt.ToLower() == "q") return;
                    if (nxt.ToLower() == "v")
                    {
                        Console.WriteLine("\n" + exec.script + "\n");
                        Console.WriteLine("\nPress Enter to continue.");
                        Console.ReadLine();
                    }
                }
            }
        }

        private Database createDB(Connection conn, string dbName, string dbFilePath)
        {
            Server svr = conn.server();           
            if (svr.Databases.Contains(dbName) == true)
            {   // drop it like it's hot            
                try
                {
                    Console.WriteLine("Database [" + dbName + "] exists. Drop it? Y/n");
                    string ok = Console.ReadLine();
                    if (ok.ToLower() == "y")
                    {
                        //svr.KillDatabase(dbName);
                        Console.WriteLine("Dropping [" + dbName + "]");
                        Database dropme = svr.Databases[dbName];
                        dropme.Drop();
                    }
                    else
                    {
                        return null;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Didn't drop database [" + dbName + "]");
                    Console.WriteLine("Database may be in use. Try this command again. Otherwise, try dropping the database manually using SQL Server Management Studio, then try again.");
                    //Console.WriteLine(e);
                    svr.KillDatabase(dbName);
                    return null;
                }
            }

            // create new db
            Database db = conn.database(dbName);            
            try
            {
                Console.WriteLine("Creating new database [" + dbName + "]");
                db.Create();
            }
            catch (Exception e) // permissions?
            {
                Console.WriteLine("ERROR - create database failed.");
                Console.WriteLine(e.InnerException);
                return null;
            }

            return db;
        }

        public static bool isDatabaseDirectory(string dbName, string dbFilePath)
        {                                   
            // does it contain the <dbName>.database.sql file?
            string targetFile = String.Format(@"{0}\SchemaObjects\{1}.database.sql", dbFilePath, dbName);
            return File.Exists(targetFile);
        }
    }
}
