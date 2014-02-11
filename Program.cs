/* *******************************************************************
 * Program: DBScript utility
 * Author: Max Sharples
 * Date: 2010-8-12
 * 
 * Utility to assist with generating version-controllable SQLServer database schema 
 * generated scripts can be checkin in alongside alongside application code
 * development databses can be rebuilt from previously generated scripts
 * including required data and 'fixtures'
 *
 * Functions:
 *      Script database objects
 *      Create database from scripted objects
 *      Script data from specified table 
 * 
 * *******************************************************************/

using System;
using System.Collections.Generic;
using System.Text;

namespace dbscript
{
    class Program
    {
        const string CMD_SCRIPT_OBJECTS = "scriptdbobjects";
        const string CMD_CREATE_DATABASE = "createdatabase";
        const string CMD_SCRIPT_DATA = "scriptdata";

        static void Main(string[] args)
        {
            /* args expected:
            *  <UtilityName> 
            *  -f <path to database script(s) folder>
            *  -s <server instance>
            *  -u <username>
            *  -p <password>
            *  -d <database name>
            *  -t trusted connection
            *  
            *  [for scripting data]
            *  --table <table name> 
            *  --limit <limit results>
            *  --alltables 
            *  --fixtures [also for building database] 
            *  --where <clause> 
           */
            
            // basic options
            string svr = ""; 
            string usr = "";
            string pwd = "";
            string dbs = "";
            string pth = "";
            bool trustedConnection = false;

            // other options
            string tbl = "";
            int limit_results = 0;
            bool all_tables = false;
            bool with_fixtures = false;
            string where_clause = "";

            // 7+ parameters are required 
            if (args.Length < 8)
            {
                PrintInstructions();
                return;
            }
                  
            // first arg should be the utility required
            string util = args[0];
            if (
                  util.ToLower() != CMD_SCRIPT_OBJECTS &&
                  util.ToLower() != CMD_CREATE_DATABASE &&
                  util.ToLower() != CMD_SCRIPT_DATA
                )
            {
                PrintInstructions();
                return;
            }      

            for (int i = 1; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-s":
                        svr = args[i + 1];
                        break;
                    case "-f":
                        pth = args[i + 1];
                        break;
                    case "-d":
                        dbs = args[i + 1];
                        break;
                    case "-u":
                        usr = args[i + 1];
                        break;
                    case "-p":
                        pwd = args[i + 1];
                        break;
                    case "-t":
                        if (pwd != "" || usr != "") Console.WriteLine(args[i] + "argument only applies if no usrname or password is set");
                        trustedConnection = true;
                        break;
                    case "--fixtures":
                        if (util.ToLower() == CMD_SCRIPT_OBJECTS) Console.WriteLine(args[i] + " argument does not apply to the ScriptDbObjects utility");
                        with_fixtures = true;
                        break;
                    case "--where":
                        if (util.ToLower() != CMD_SCRIPT_OBJECTS || with_fixtures == false) Console.WriteLine(args[i] + " argument only applies to ScriptData utility when creating fixture scripts");
                        where_clause = "WHERE "+args[i + 1];
                        break;

                    case "--table":
                        if (util.ToLower() != CMD_SCRIPT_DATA) Console.WriteLine(args[i] + " argument only applies to ScriptData utility");
                        tbl = args[i + 1];
                        break;
                    case "--limit":
                        if (util.ToLower() != CMD_SCRIPT_DATA) Console.WriteLine(args[i] + " argument only applies to ScriptData utility");
                        try
                        {
                            limit_results = Convert.ToInt32(args[i + 1]);
                        }
                        catch
                        {
                            Console.WriteLine("Invalid value supplied for " + args[i] + " argument");
                        }
                        break;
                    case "--alltables":
                        if (util.ToLower() != CMD_SCRIPT_DATA) Console.WriteLine(args[i] + " argument only applies to ScriptData utility");
                        all_tables = true;
                        break;
                }
            }
            //

            // all of these are required all the time
            if (pwd != "" && trustedConnection || usr != "" && trustedConnection || svr == "" || pth == "")
            {
                PrintInstructions();
                return;
            }

            // if creating a database - it should only be a local instance??
            /*
            if (util.ToLower() == CMD_CREATE_DATABASE && !svr.Contains(@"\SQLEXPRESS"))
            {
                Console.WriteLine("The " + util + " function should only be used on workstation installations of SQLEXPRESS.");
                return;
            }
            */

            // if creating a database or insert script, need the db name
            /*
            if ((util.ToLower() == CMD_CREATE_DATABASE || util.ToLower() == CMD_SCRIPT_DATA) && dbs == "")
            {
                Console.WriteLine("The " + util + " function requires the database name to be specified.");
                PrintInstructions();
                return;
            }
            */

            // if scripting table data, tablename must be provided
            if (util.ToLower() == CMD_SCRIPT_DATA && tbl == "" && all_tables == false)
            {
                Console.WriteLine("The " + util + " function requires the table name to be specified unless --alltables is specified.");
                PrintInstructions();
                return;
            }

            // establish and test db connection
            Connection connection = new Connection(svr, usr, pwd, trustedConnection);
            if (connection.testConnection() != true)
            {
                Console.WriteLine("Database connection could not be established.");
                Console.WriteLine("Enter v to view the connection error message, q to quit.");
                string nxt = Console.ReadLine();
                if (nxt.ToLower() == "v")
                {
                    Console.WriteLine("\n" + connection.connectionError + "\n");
                    Console.ReadLine();
                }
                return;
            }

            // Create Database
            /*
            if (util.ToLower() == CMD_CREATE_DATABASE)
            {
                CreateDatabase db = new CreateDatabase(connection, pth, dbs, with_fixtures);
            }
            */
            // Script Objects
            if (util.ToLower() == CMD_SCRIPT_OBJECTS)
            {
                ScriptDbObjects dbo = new ScriptDbObjects(connection, pth, dbs);
            }

            // Script Data
            if (util.ToLower() == CMD_SCRIPT_DATA)
            {
                string[] tables;
                if (all_tables == true)
                    tables = ScriptData.getTables(connection, dbs);
                else
                    tables = new string[]{tbl};

                foreach (string t in tables)
                {
                    ScriptData sd = new ScriptData(connection, pth, dbs, t, limit_results, with_fixtures, where_clause);
                }
            }
        }

        static void PrintInstructions()
        {
            String instructions = @"
 
DBScripter - DataBase scripting tool.


Usage:
---------------------------------

> dbscript <UtilityName> -f ""<Path>"" -s <ServerName> -u <Username> -p <Password> -d <Database> -t [--table <Table> --alltables --limit --fixtures]

Available utilities are ScriptDbObjects & ScriptData 

The <Root Path> is the starting point for reading/writing all your database creation scripts (probably within a subversion branch).
The <ServerName> is the server instance you are connectiong to. For a local installation of SQLEXPRESS it will be PCNAME\SQLEXPRESS
<Database> is the database you want to script or build. This parameter is optional when using ScriptDbObjects. If it is not supplied the utility will script all databases.      

When scripting databases the user credentials supplied should have sufficient permissions (preferably 'sa' account).

-t option is trusted connection, which can be used instead of username and password.


Options:
---------------------------------

--table <TableName> - provide the table name for scripting data inserts (only applies for ScriptData)

--alltables - script data inserts for all tables in database  (only applies for ScriptData - not recommended)

--limit <integer> - limit the number of rows to include (only applies for ScriptData). A negative integer will reverse-order the results.

--fixtures - execute the insert statements from the /Fixtures directory for including test data in the build (for CreateDatabase command) or for creating test data insert scripts (Fixtures)
 
--where <clause> - filter table with a where clause when creaating insert statements for fixtures

Examples:
---------------------------------

> dbscript ScriptDbObjects -f ""\dev\files\branches\myFeatureBranch\Databases"" -s MYPC\SQLEXPRESS -u sa -p adminpass -d textme

> dbscript ScriptData  -d textme --table GlobalOptions -f ""\dev\files\branches\myFeatureBranch\Databases"" -s MYPC\SQLEXPRESS -u sa -p adminpass


Please report or repair any bugs you encounter.
    
    ";
            Console.WriteLine(instructions);
        }
    }
}
