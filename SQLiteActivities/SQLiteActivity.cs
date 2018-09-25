using System;
using System.Linq;
using System.Activities;
using System.ComponentModel;
using System.Data.SQLite;
using System.Data;
using System.IO;
using SQLiteCore = SQLite.SQLiteCore;

namespace SQLite
{
    [Description("Executes a SQL command that has no result dataset.\n(UPDATE, DELETE,CREATE TABLE, ALTER TABLE, etc)")]
    public class ExecuteNonQuery : CodeActivity
    {

        [RequiredArgument]
        [OverloadGroup("DatabasePath")]
        [Category("Database")]
        [Description("String: Full database file path.\n(Use this property if the Connection property is empty)")]
        public InArgument<string> DatabaseFullPath { get; set; }

        [RequiredArgument]
        [OverloadGroup("Connection")]
        [Category("Database")]
        [Description("SQLiteConnection: Connection object from Create Connection activity.\n(Use this property if the Database Path is not provided)")]
        public InArgument<SQLiteConnection> Connection { get; set; }

        [RequiredArgument]
        [Category("SQL")]
        [Description("String: The SQL command")]
        public InArgument<string> SQLCommand { get; set; }

        protected override void Execute(CodeActivityContext context)
        {
            SQLiteCore.ExecuteNonQuery(context.GetValue(this.DatabaseFullPath), context.GetValue(this.Connection), context.GetValue(this.SQLCommand));
        }
    }

    [Description("Executes a SQL command with a resulting DataTable dataset.\n(SELECT)")]
    public class ExecuteQuery : CodeActivity
    {

        [RequiredArgument]
        [OverloadGroup("DatabasePath")]
        [Category("Database")]
        [Description("String: Full database file path.\n(Use this property if the Connection property is empty)")]
        public InArgument<string> DatabaseFullPath { get; set; }

        [RequiredArgument]
        [OverloadGroup("Connection")]
        [Category("Database")]
        [Description("SQLiteConnection: Connection object from Create Connection activity.\n(Use this property if the Database Path is not provided)")]
        public InArgument<SQLiteConnection> Connection { get; set; }

        [RequiredArgument]
        [Category("SQL")]
        [Description("String: The SQL command")]
        public InArgument<string> SQLCommand { get; set; }

        [RequiredArgument]
        [Category("Output")]
        [Description("DataTable: The resulting datatable from the query.")]
        public OutArgument<System.Data.DataTable> QueryResult { get; set; }


        protected override void Execute(CodeActivityContext context)
        {

            this.QueryResult.Set(context, SQLiteCore.ExecuteQuery(context.GetValue(this.DatabaseFullPath), context.GetValue(this.Connection), context.GetValue(this.SQLCommand)));
        }
    }

    [Description("Creates a connection object to be maintained throughout the workflow.")]
    public class CreateConnection : CodeActivity
    {

        [RequiredArgument]
        [Category("Database")]
        [Description("String: Full database file path.")]
        public InArgument<string> DatabaseFullPath { get; set; }

        [RequiredArgument]
        [Category("Connection Object")]
        [Description("SQLiteConnection: The connection object result")]
        public OutArgument<SQLiteConnection> Output { get; set; }

        protected override void Execute(CodeActivityContext context)
        {
            this.Output.Set(context, SQLiteCore.CreateConnection(context.GetValue(this.DatabaseFullPath)));
        }
    }

    [Description("Closes a connection object created from the Create Connection activity.")]
    public class CloseConnection : CodeActivity
    {

        [RequiredArgument]
        [Category("Connection Object")]
        [Description("SQLiteConnection: The connection object to be closed")]
        public InArgument<SQLiteConnection> Connection { get; set; }

        protected override void Execute(CodeActivityContext context)
        {
            SQLiteConnection conn = context.GetValue(this.Connection);

            if (conn == null)
                throw new Exception("The connection object cannot be null");

            conn.Dispose();
        }
    }

    [Description("Creates a new SQLite database file.")]
    public class CreateDatabase : CodeActivity
    {

        [RequiredArgument]
        [Category("Database")]
        [Description("String: Full database file path.")]
        public InArgument<string> DatabaseFullPath { get; set; }

        protected override void Execute(CodeActivityContext context)
        {
            string databaseFullName = context.GetValue(this.DatabaseFullPath);

            Validate.ValidateDatabaseName(databaseFullName);

            SQLiteConnection.CreateFile(databaseFullName);
        }
    }

    [Description("Inserts all data from a DataTable into a SQLite table.\nIf the database table does not exists, it is automatically created.")]
    public class InsertDatatable : CodeActivity
    {

        [RequiredArgument]
        [OverloadGroup("DatabasePath")]
        [Category("Database")]
        [Description("String: Full database file path.\n(Use this property if the Connection property is empty)")]
        public InArgument<string> DatabaseFullPath { get; set; }

        [RequiredArgument]
        [OverloadGroup("Connection")]
        [Category("Database")]
        [Description("SQLiteConnection: Connection object from Create Connection activity.\n(Use this property if the Database Path is not provided)")]
        public InArgument<SQLiteConnection> Connection { get; set; }

        [RequiredArgument]
        [Category("Input data")]
        [Description("DataTable: A datatable to be persisted")]
        public InArgument<System.Data.DataTable> Datatable { get; set; }

        [RequiredArgument]
        [Category("Input data")]
        [Description("String: The database table name.\nIf the table does not exists, it is automatically created.")]
        public InArgument<string> Table { get; set; }
               
        [Category("Primary Key")]
        [Description("String: A datatable column name to be set as Primary Key.\n(Only if the table is created automatically)")]
        public InArgument<string> PrimaryKey { get; set; }

        protected override void Execute(CodeActivityContext context)
        {
            SQLiteCore.InsertDataTable(context.GetValue(this.DatabaseFullPath), context.GetValue(this.Connection), context.GetValue(this.Datatable), context.GetValue(this.Table), context.GetValue(this.PrimaryKey));
        }
    }

    



}
