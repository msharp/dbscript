using System;
using System.Collections.Generic;
using System.Text;

using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;

namespace dbscript
{
    public class Connection
    {
        public string serverName;
        public string username;
        public string password;
        public bool connectionOK;
        public string connectionError = "";

        public Connection(string svrName, string usrName, string pssWord)
        {
            serverName = svrName;
            username = usrName;
            password = pssWord;
        }

        public string connectionString()
        {
            string connstr = "Data Source=" + serverName
                           + "; User Id=" + username
                           + "; Password=" + password;
            return connstr;
        }

        public bool testConnection()
        {
            Console.WriteLine("\nUsing connection string: " + connectionString());
            Server srvr = server();
            try
            {
                srvr.Initialize();
                string srvrVersion = srvr.Information.VersionString;
                Console.WriteLine("Connection to Server " + serverName.ToUpper() + " is OK");
                Console.WriteLine("Server Version is " + srvrVersion + "\n");
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("\nERROR: Connection to Server " + serverName.ToUpper() + " failed\n");
                connectionError = e.InnerException.ToString();
                return false;
            }
        }

        public Database database(string dbName)
        {
            Database db = new Database(server(), dbName);
            return db;
        }

        public Server server()
        {
            ServerConnection conn = serverConnection();
            Server srvr = new Server(conn);
            return srvr;
        }

        public Server server(string database)
        {
            ServerConnection conn = serverConnection();
            conn.DatabaseName = database;
            Server srvr = new Server(conn);
            return srvr;
        }

        public ServerConnection serverConnection()
        {
            ServerConnection conn = new ServerConnection();
            conn.LoginSecure = false;
            conn.Login = username;
            conn.Password = password;
            conn.ServerInstance = serverName;
            return conn;
        }
    }
}
