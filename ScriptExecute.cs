using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;

namespace dbscript
{
    class ScriptExecute
    {   
        public bool success = false;
        public string exception = "";
        public int attempts = 0;
        public string sql = "";
        public string dbName = "";
        public string script = "";
        private bool abort = false;
        
        public ScriptExecute(string sqlFile, string db, Connection conn)
        {
            sql = sqlFile;
            dbName = db;
            execute(conn);
        }

        public void execute(Connection conn)
        {
            // arguments for sqlcmd.exe utility
            var args = String.Format(@" -S {0} -U {1} -P {2} -d {3} -i {4} ", conn.serverName, conn.username, conn.password, dbName, sql);

            try
            {                
                Process p = new Process();
                p.StartInfo.UseShellExecute = false;
                p.StartInfo.CreateNoWindow = true;
                p.StartInfo.RedirectStandardOutput = true;
                p.StartInfo.FileName = "sqlcmd";
                p.StartInfo.Arguments = args;

                p.Start();

                // waiting for exit makes this very slow
                // but not waiting can cause memory overflow
                string output = p.StandardOutput.ReadToEnd();
                p.WaitForExit();
                p.Close();

                success = true;

            }
            catch (Exception e)
            {
                success = false;
                exception = e.ToString();
            }
            finally
            {
                attempts++;
            }

        }

        // other method //
        public ScriptExecute(string sqlFile, Database db)
        {
            sql = sqlFile;
            execute(db);
        }

        public void execute(Database db)
        {
            if (abort == true) return;
            
            StreamReader sr = new StreamReader(sql);
            script = sr.ReadToEnd();

            try
            {
                db.ExecuteNonQuery(script);
                success = true;
            }
            catch (Microsoft.SqlServer.Management.Smo.FailedOperationException e)
            {
                success = false;
                exception = e.InnerException.ToString();
            }
            /*
            catch (OutOfMemoryException e)
            {
                success = false;
                exception = e.ToString();
                abort = true;
                return;
            }
            */
            finally
            {
                if (success == false) Console.WriteLine("!failed...");
                attempts++;
            }
        }
    }
}
