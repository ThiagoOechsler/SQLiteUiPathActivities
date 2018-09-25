using System;
using System.Linq;
using System.Data.SQLite;
using System.Data;
using System.IO;

namespace SQLite
{
    public class SQLiteCore
    {
        public static void ExecuteNonQuery(string databaseFullName, SQLiteConnection conn, string sql)
        {
            if (conn == null && string.IsNullOrEmpty(databaseFullName))
                throw new Exception("You need to inform one of the parameters for the Database");

            if (!string.IsNullOrEmpty(databaseFullName))
            {
                Validate.ValidateConnection(databaseFullName);
                conn = new SQLiteConnection("Data Source=" + databaseFullName + ";Version=3;");
                conn.Open();
            }

            SQLiteCommand command1 = new SQLiteCommand(sql, conn);
            command1.ExecuteNonQuery();

            if (!string.IsNullOrEmpty(databaseFullName))
            {
                conn.Close();
            }
        }

        public static DataTable ExecuteQuery(string databaseFullName, SQLiteConnection conn, string sql)
        {
            if (conn == null && string.IsNullOrEmpty(databaseFullName))
                throw new Exception("You need to inform one of the parameters for the Database");

            if (!string.IsNullOrEmpty(databaseFullName))
            {
                Validate.ValidateConnection(databaseFullName);
                conn = new SQLiteConnection("Data Source=" + databaseFullName + ";Version=3;");
                conn.Open();
            }

            System.Data.DataTable res = new System.Data.DataTable();

            SQLiteDataAdapter da = new SQLiteDataAdapter(sql, conn);
            da.Fill(res);
            
            if (!string.IsNullOrEmpty(databaseFullName))
            {
                conn.Close();
            }

            return res;
        }

        public static SQLiteConnection CreateConnection (string databaseFullName)
        {
            Validate.ValidateConnection(databaseFullName);

            SQLiteConnection conn = new SQLiteConnection("Data Source=" + databaseFullName + ";Version=3;");
            conn.Open();

            return conn;
        }

        public static void InsertDataTable(string databaseFullName, SQLiteConnection conn, DataTable dtb, string table, string pk)
        {
            if (conn == null && string.IsNullOrEmpty(databaseFullName))
                throw new Exception("You need to inform one of the parameters for the Database");

            if (!string.IsNullOrEmpty(databaseFullName))
            {
                Validate.ValidateConnection(databaseFullName);
                conn = new SQLiteConnection("Data Source=" + databaseFullName + ";Version=3;");
                conn.Open();
            }

            //Check if the table exists, if not, the table will be created
            System.Data.DataTable tables = new System.Data.DataTable();
            SQLiteDataAdapter adapterTables = new SQLiteDataAdapter("SELECT name FROM sqlite_master WHERE type = 'table'", conn);
            adapterTables.Fill(tables);
            bool createnewtable = true;
            foreach (DataRow row in tables.Rows)
            {
                if (row[0].ToString().Equals(table))
                {
                    createnewtable = false;
                }
            }

            if (createnewtable)
            {
                string sqlsc = "CREATE TABLE " + table + " ( ";
                for (int i = 0; i < dtb.Columns.Count; i++)
                {
                    sqlsc += "\n" + "[" + dtb.Columns[i].ColumnName + "]";
                    if (dtb.Columns[i].DataType.ToString().Contains("System.Int32"))
                        sqlsc += " int ";
                    else if (dtb.Columns[i].DataType.ToString().Contains("System.Int64"))
                        sqlsc += " bigint ";
                    else if (dtb.Columns[i].DataType.ToString().Contains("System.DateTime"))
                        sqlsc += " datetime ";
                    else if (dtb.Columns[i].DataType.ToString().Contains("System.Decimal"))
                        sqlsc += " decimal ";
                    else if (dtb.Columns[i].DataType.ToString().Contains("System.Double"))
                        sqlsc += " double ";
                    else
                        sqlsc += " nvarchar(500) ";

                    //If primary key is provided, create it 
                    if ((!String.IsNullOrEmpty(pk)) && pk.Equals(dtb.Columns[i].ColumnName, StringComparison.InvariantCultureIgnoreCase))
                    {
                        sqlsc += " NOT NULL PRIMARY KEY ";
                    }

                    sqlsc += ",";
                }
                sqlsc = sqlsc.Substring(0, sqlsc.Length - 1);
                sqlsc += ")";

                SQLiteCommand createtable = new SQLiteCommand(sqlsc, conn);
                createtable.ExecuteNonQuery();
            }

            string cmd = string.Format("SELECT * FROM {0}", table);
            SQLiteDataAdapter da = new SQLiteDataAdapter(cmd, conn);
            SQLiteCommandBuilder builder = new SQLiteCommandBuilder(da);
            da.Update(dtb);

            if (!string.IsNullOrEmpty(databaseFullName))
            {
                conn.Close();
            }
        }
        
    }

    public static class Validate
    {
        public static void ValidateConnection(string fullPath)
        {
            if (!File.Exists(fullPath) || fullPath.IndexOf('.') <= 0 ||
                (fullPath.Split('.').LastOrDefault().ToLower() != "db" &&
                 fullPath.Split('.').LastOrDefault().ToLower() != "sqlite" &&
                 fullPath.Split('.').LastOrDefault().ToLower() != "sqlite3" &&
                 fullPath.Split('.').LastOrDefault().ToLower() != "db3"))
                throw new Exception("The path to the database is invalid. Inform the fully qualified path. The supported databases are: .db, .db3, .sqlite, .sqlite3");

        }

        public static void ValidateDatabaseName(string fullPath)
        {
            string directory = fullPath.Substring(0, fullPath.LastIndexOf('\\'));
            if (!Directory.Exists(directory))
                throw new Exception("The specified folder is invalid");

            if (fullPath.IndexOf('.') <= 0 ||
                (fullPath.Split('.').LastOrDefault().ToLower() != "db" &&
                 fullPath.Split('.').LastOrDefault().ToLower() != "sqlite" &&
                 fullPath.Split('.').LastOrDefault().ToLower() != "sqlite3" &&
                 fullPath.Split('.').LastOrDefault().ToLower() != "db3"))
                throw new Exception("The supported database extensions are: .db, .db3, .sqlite, .sqlite3");

        }
    }
}
