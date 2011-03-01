using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;

namespace dbscript
{
    class ScriptDB
    {
        public int objectCount = 0;

        private string m_dbPath;
        private string m_database;
        private Connection m_connection;

        public ScriptDB(string dbName, Connection conn)
        {
            m_database = dbName;
            m_connection = conn;
        }

        public void scriptDB(string directory)
        {

            DateTime began = DateTime.Now;

            m_dbPath = generateScriptPath(directory);

            Console.WriteLine("\n**********************************************");
            Console.WriteLine("Scripting [" + m_database + "] to " + m_dbPath);
            Console.WriteLine("**********************************************\n");

            Server srvr = m_connection.server(m_database);
            try
            {
                srvr.Initialize();
                srvr.SetDefaultInitFields(true);
            }
            catch (Exception e)
            {
                Console.WriteLine("\nERROR: Connection to Server " + m_connection.serverName + ", Database " + m_database + " failed\n");
                Console.WriteLine(e);
                return;
            }

            Database db = srvr.Databases[m_database];
            if (db == null)
            {
                Console.WriteLine("\nERROR: Database " + m_database + " does not exist\n");
                return;
            }

            // set up SMO scripting objects
            Urn[] urn = new Urn[1];
            Scripter scrp = new Scripter(srvr);
            
            // set common scipter options
            scrp.Options.IncludeHeaders = false; // don't include header info in version-controlled scripts
            scrp.Options.AppendToFile = false; // fresh file every time
            scrp.Options.AnsiFile = true;
            scrp.Options.ContinueScriptingOnError = true;
            scrp.Options.PrimaryObject = true;
            scrp.Options.SchemaQualify = true;
            scrp.Options.ToFileOnly = true;
            scrp.Options.ConvertUserDefinedDataTypesToBaseType = true;

            // options that make the results more likely to be executable.
            scrp.Options.ExtendedProperties = false; // don't want extra guff - just defailts please
            scrp.Options.NoCollation = true; // don't include collation info - defaults are ok
            scrp.Options.Permissions = false;

            

            // scripting the objects
            scriptDatabase(db, scrp, urn);
            scriptTables(db, scrp, urn);
            scriptViews(db, scrp, urn);
            scriptStoredProcedures(db, scrp, urn);
            scriptUserDefinedFunctions(db, scrp, urn);

            // done!
            DateTime ended = DateTime.Now;

            Console.WriteLine("\n[" + m_database + "] began: " + began.ToLongTimeString() + ", ended: " + ended.ToLongTimeString());
            Console.WriteLine("Number of objects scripted: " + objectCount.ToString());
            Console.WriteLine("\n[" + m_database + "] done....");

        }


        void ScriptIt(Urn[] urn, Scripter scrp, string filename, bool withDrop)
        {
            scrp.Options.ScriptDrops = withDrop;
            scrp.Options.IncludeIfNotExists = withDrop;
            ScriptIt(urn, scrp, filename);
        }
        void ScriptIt(Urn[] urn, Scripter scrp, string filename)
        {
            scrp.Options.FileName = filename;

            try
            {
                scrp.Script(urn);
            }
            catch (Exception e)
            {
                Console.WriteLine("Scripting for this object FAILED for the following reason:");
                Console.WriteLine(e.InnerException);
                Console.WriteLine("");
                return;
            }

            objectCount++;
        }

        /*******************************************************************************
        * Script Database settings
        *******************************************************************************/
        void scriptDatabase(Database db, Scripter scrp, Urn[] urn)
        {
            string filename;
            urn[0] = db.Urn;

            filename = m_dbPath + @"\" + scrub(db.Name) + ".database.sql";
            Console.WriteLine("Database: " + db.Name);

            // script the database
            ScriptIt(urn, scrp, filename);
        }

        /*******************************************************************************
        * Script Tables
        *******************************************************************************/
        void scriptTables(Database db, Scripter scrp, Urn[] urn)
        {
            string filename;
            string tblPath = m_dbPath + @"\Tables";
            Directory.CreateDirectory(tblPath);

            foreach (Table tbl in db.Tables)
            {
                // skip system tables
                if (tbl.IsSystemObject)
                {
                    continue;
                }

                urn[0] = tbl.Urn;

                scrp.Options.DriAll = false;
                scrp.Options.Indexes = false;
                scrp.Options.Triggers = false;
                scrp.Options.NoFileGroup = false;
                scrp.Options.DriForeignKeys = false;  
                scrp.Options.NoTablePartitioningSchemes = false;

                //Script Tables
                filename = tblPath + @"\" + scrub(tbl.Schema) + "." + scrub(tbl.Name) + ".table.sql";
                Console.WriteLine("  Table: " + tbl.Schema + "." + tbl.Name);

                // script the table
                ScriptIt(urn, scrp, filename);

                // permissions
                string command = "EXEC sp_helprotect"
                           + "  @name = '" + tbl.Name + "'"
                           + ", @grantorname = '" + tbl.Schema + "'";
                ScriptPermissions(m_connection.connectionString(), command, filename);


                // Script Table Indexes
                string keyPath = tblPath + @"\Keys";
                Directory.CreateDirectory(keyPath);

                string ndxPath = tblPath + @"\Indexes";
                Directory.CreateDirectory(ndxPath);

                foreach (Index ndx in tbl.Indexes)
                {
                    Console.WriteLine("    Index: " + ndx.Name);
                    urn[0] = ndx.Urn;

                    if (ndx.IndexKeyType.ToString() == "DriUniqueKey")
                    {
                        filename = keyPath + @"\" + scrub(tbl.Schema) + "." + scrub(tbl.Name)
                                 + "." + scrub(ndx.Name) + ".ukey.sql";
                    }
                    else if (ndx.IndexKeyType.ToString() == "DriPrimaryKey")
                    {
                        filename = keyPath + @"\" + scrub(tbl.Schema) + "." + scrub(tbl.Name)
                                 + "." + scrub(ndx.Name) + ".pkey.sql";
                    }
                    else
                    {
                        filename = ndxPath + @"\" + scrub(tbl.Schema) + "." + scrub(tbl.Name)
                                 + "." + scrub(ndx.Name) + ".index.sql";
                    }

                    // script the index
                    ScriptIt(urn, scrp, filename);
                }

                // Script Table Triggers
                string trgPath = tblPath + @"\Triggers";
                Directory.CreateDirectory(trgPath);

                foreach (Trigger trg in tbl.Triggers)
                {
                    Console.WriteLine("    Trigger: " + trg.Name);
                    urn[0] = trg.Urn;

                    filename = trgPath + @"\" + scrub(tbl.Schema) + "." + scrub(tbl.Name)
                             + "." + scrub(trg.Name) + ".trigger.sql";

                    // script the trigger
                    ScriptIt(urn, scrp, filename);
                }

                // Script Check Constraints
                string chkPath = tblPath + @"\Constraints";
                Directory.CreateDirectory(chkPath);

                scrp.Options.DriChecks = true;

                foreach (Check chk in tbl.Checks)
                {
                    Console.WriteLine("    Constraint: " + chk.Name);
                    urn[0] = chk.Urn;

                    filename = chkPath + @"\" + scrub(tbl.Schema) + "." + scrub(tbl.Name)
                             + "." + scrub(chk.Name) + ".chkconst.sql";

                    // script the constraint
                    ScriptIt(urn, scrp, filename);
                }

                // Script Default Constraints
                string defPath = chkPath;

                scrp.Options.DriChecks = false;

                foreach (Column col in tbl.Columns)
                {
                    if (col.DefaultConstraint != null)
                    {
                        Console.WriteLine("    Constraint: " + col.DefaultConstraint.Name);
                        urn[0] = col.DefaultConstraint.Urn;

                        filename = defPath + @"\" + scrub(tbl.Schema) + "." + scrub(tbl.Name)
                                 + "." + scrub(col.DefaultConstraint.Name) + ".defconst.sql";

                        // script the constraint
                        ScriptIt(urn, scrp, filename);
                    }
                }

                // Script Foreign Keys
                scrp.Options.DriForeignKeys = true;
                scrp.Options.SchemaQualifyForeignKeysReferences = true;

                foreach (ForeignKey fk in tbl.ForeignKeys)
                {
                    Console.WriteLine("    Foreign Key: " + fk.Name);
                    urn[0] = fk.Urn;

                    filename = keyPath + @"\" + scrub(tbl.Schema) + "." + scrub(tbl.Name)
                             + "." + scrub(fk.Name) + ".fkey.sql";

                    // script the constraint
                    ScriptIt(urn, scrp, filename);
                }
            }
        }


        /*******************************************************************************
        * Script Views
        *******************************************************************************/
        void scriptViews(Database db, Scripter scrp, Urn[] urn)
        {
            string filename;
            string vwPath = m_dbPath + @"\Views";
            Directory.CreateDirectory(vwPath);

            foreach (View vw in db.Views)
            {
                // skip system views
                if (vw.IsSystemObject)
                {
                    continue;
                }

                urn[0] = vw.Urn;

                scrp.Options.Indexes = false;
                scrp.Options.Triggers = false;
                scrp.Options.ScriptDrops = true; //include the drop statement prior to create
                scrp.Options.ScriptSchema = true;

                filename = vwPath + @"\" + scrub(vw.Schema) + "." + scrub(vw.Name) + ".view.sql";
                Console.WriteLine("  View: " + vw.Schema + "." + vw.Name);

                // script the view
                ScriptIt(urn, scrp, filename, true);
                ScriptIt(urn, scrp, filename, false);

                string command = "EXEC sp_helprotect"
                                  + "  @name = '" + vw.Name + "'"
                                  + ", @grantorname = '" + vw.Schema + "'";


                // Script View Indexes
                string ndxPath = vwPath + @"\Indexes";
                Directory.CreateDirectory(ndxPath);

                foreach (Index ndx in vw.Indexes)
                {
                    Console.WriteLine("    Index: " + ndx.Name);
                    urn[0] = ndx.Urn;

                    filename = ndxPath + @"\" + scrub(vw.Schema) + "." + scrub(vw.Name)
                             + "." + scrub(ndx.Name) + ".index.sql";

                    // script the index
                    ScriptIt(urn, scrp, filename);
                }

                // Script View Triggers
                string trgPath = vwPath + @"\Triggers";
                Directory.CreateDirectory(trgPath);

                foreach (Trigger trg in vw.Triggers)
                {
                    Console.WriteLine("    Trigger: " + trg.Name);
                    urn[0] = trg.Urn;

                    filename = trgPath + @"\" + scrub(vw.Schema) + "." + scrub(vw.Name)
                             + "." + scrub(trg.Name) + ".trigger.sql";

                    // script the trigger with drop statement
                    ScriptIt(urn, scrp, filename);
                }
            }
        }

        /*******************************************************************************
        * Script Stored Procedures
        *******************************************************************************/
        void scriptStoredProcedures(Database db, Scripter scrp, Urn[] urn)
        {
            string filename;
            string procPath = m_dbPath + @"\Stored Procedures";
            Directory.CreateDirectory(procPath);

            scrp.Options.Permissions = true;

            foreach (StoredProcedure proc in db.StoredProcedures)
            {
                // skip system procedures
                if (proc.IsSystemObject)
                {
                    continue;
                }

                urn[0] = proc.Urn;

                filename = procPath + @"\" + scrub(proc.Schema) + "." + scrub(proc.Name) + ".proc.sql";
                Console.WriteLine("  Stored Procedure: " + proc.Schema + "." + proc.Name);

                // script the procedure with drop statement
                ScriptIt(urn, scrp, filename, true);
                ScriptIt(urn, scrp, filename, false);
            }
        }


        /*******************************************************************************
        * Script User Defined Functions
        *******************************************************************************/
        void scriptUserDefinedFunctions(Database db, Scripter scrp, Urn[] urn)
        {
            string filename;
            string funcPath = m_dbPath + @"\Functions";
            Directory.CreateDirectory(funcPath);

            scrp.Options.ScriptSchema = true;

            foreach (UserDefinedFunction func in db.UserDefinedFunctions)
            {
                // skip system functions
                if (func.IsSystemObject)
                {
                    continue;
                }

                urn[0] = func.Urn;

                filename = funcPath + @"\" + scrub(func.Schema) + "." + scrub(func.Name) + ".function.sql";
                Console.WriteLine("  User Defined Function: " + func.Schema + "." + func.Name);

                // script the function with drop statement
                ScriptIt(urn, scrp, filename, true);
                ScriptIt(urn, scrp, filename, false);
            }
        }


        /*******************************************************************************
        * Script XML Schema Collections
        *******************************************************************************/
        void scriptXmlSchemaCollections(Database db, Scripter scrp, Urn[] urn)
        {
            string filename;
            string xmlPath = m_dbPath + @"\XML Schema Collections";
            Directory.CreateDirectory(xmlPath);

            foreach (XmlSchemaCollection xml in db.XmlSchemaCollections)
            {
                urn[0] = xml.Urn;

                filename = xmlPath + @"\" + scrub(xml.Schema) + "." + scrub(xml.Name) + ".xmlschema.sql";
                Console.WriteLine("  XML Schema Collection: " + xml.Schema + "." + xml.Name);

                // script the xml schema collection
                ScriptIt(urn, scrp, filename);
            }
        }


        void ScriptPermissions(string connect, string command, string filename)
        {
            SqlConnection cn = new SqlConnection(connect);
            cn.Open();
            SqlCommand cmd = new SqlCommand(command, cn);

            // issue the query
            SqlDataReader rdr = null;
            try
            {
                rdr = cmd.ExecuteReader();
            }
            catch
            {
                // Some tables/views don't have granted permissions; ignore error
                ;
            }

            // if the query returned any rows, constuct the permissions
            string text = "";
            StringBuilder perms = new StringBuilder(1024);
            if (rdr != null && !rdr.IsClosed)
            {
                if (rdr.HasRows)
                {
                    while (rdr.Read())
                    {
                        if (rdr["ProtectType"].ToString() == "Grant_WGO ")
                        {
                            text = "GRANT";
                        }
                        else
                        {
                            text = rdr["ProtectType"].ToString().ToUpper().Trim();
                        }
                        text += " " + rdr["Action"].ToString().ToUpper().Trim();

                        if (
                                rdr["Column"].ToString().Trim() != "(All)"
                             && rdr["Column"].ToString().Trim() != "(All+New)"
                             && rdr["Column"].ToString().Trim() != "."
                           )
                        {
                            text += " ( [" + rdr["Column"].ToString().Trim() + "] )";
                        }

                        text += " ON [" + rdr["Owner"].ToString().Trim() + "].["
                              + rdr["Object"].ToString().Trim() + "] TO "
                              + rdr["Grantee"].ToString().Trim();

                        if (rdr["ProtectType"].ToString() == "Grant_WGO ")
                        {
                            text += " WITH GRANT OPTION";
                        }

                        perms.AppendLine(text);
                        perms.AppendLine("GO\r\n");
                    }
                    // convert the StringBuilder back to a string for the StreamWriter
                    text = "\r\n" + perms.ToString();

                    FileStream FS = new FileStream(filename, FileMode.Append, FileAccess.Write);
                    StreamWriter SW = new StreamWriter(FS, Encoding.ASCII);

                    // write the permissions to the file
                    try
                    {
                        SW.Write(text);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                    finally
                    {
                        SW.Flush();
                        SW.Dispose();
                        FS.Dispose();
                    }
                }
                cn.Close();
            }
        }

        // TODO _ review this method - are all replaces necessary?
        string scrub(string name)
        {
            // convert irregular characters to underbar, as does 'Data Dude'
            name = Regex.Replace(name, @"[<>;:*/#\|\?\\]", "_");
            // convert additional irregular characters to hyphen
            name = Regex.Replace(name, @"[@!%',{}=\[\]\$\(\)\^]", "-");
            // convert remaining irregular characters to hyphen, just because
            name = Regex.Replace(name, @"[~+\.]", "-");

            return name;
        }

        string generateScriptPath(string directory)
        {
            // srvrPath will be our root directory
            string srvrPath = directory;
            while (srvrPath.EndsWith(@"\"))
            {
                srvrPath = srvrPath.Substring(0, srvrPath.Length - 1);
            }

            // create subdirectory for different server instances
            if (Directory.Exists(srvrPath) == false)
            {
                try
                {
                    Directory.CreateDirectory(srvrPath);
                }
                catch (Exception e)
                {
                    Console.WriteLine("\nERROR: couln't create directory \"" + srvrPath + "\".\n");
                    Console.WriteLine(e);
                }
            }

            var dbPath = String.Format(@"{0}\{1}\SchemaObjects\", srvrPath, m_database);

            if (Directory.Exists(dbPath))
            {
                try
                {   // recursively delete *.sql files - retain other files/dirs (such as .svn)
                    string[] fileList = System.IO.Directory.GetFiles(dbPath, @"*.sql", System.IO.SearchOption.AllDirectories);
                    foreach (string file in fileList)
                    {
                        System.IO.File.Delete(file);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("FAILED to clean existing files from \"" + dbPath + "\"");
                    //Console.WriteLine(e);
                }
            }
            else
            {
                // (re)create the db scripts directory
                Directory.CreateDirectory(dbPath);
            }
            return dbPath;
        }

    }
}
