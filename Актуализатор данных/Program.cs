using Newtonsoft.Json;
using Npgsql;
using System.Data;
using System.Globalization;

namespace Актуализатор_данных
{
    /// <summary>
    /// Helper functions for schema inspection and data comparison.
    /// </summary>
    public class HelpingFunctions
    {
        /// <summary>
        /// Retrieves the list of column names for a given PostgreSQL table,
        /// excluding spatial types (<c>geometry</c> and <c>geography</c>).
        /// </summary>
        /// <param name="connString">PostgreSQL connection string.</param>
        /// <param name="tableName">Fully qualified table name in the form <c>schema.table</c>.</param>
        /// <returns>List of column names (non-spatial).</returns>
        /// <remarks>
        /// Uses <c>information_schema.columns</c>. The method splits the schema and table name
        /// from the input and filters out columns with <c>udt_name</c> in ('geometry','geography').
        /// </remarks>
        public List<string> GetColumns(string connString, string tableName)
        {
            List<string> columns = new List<string>();

            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                string schemaName = tableName.Split('.')[0];
                tableName = tableName.Split('.')[1];
                // Получаем список всех колонок, исключая geometry и geography
                string columnQuery = $"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schemaName}' " +
                    $"AND table_name = '{tableName}' AND udt_name NOT IN ('geometry', 'geography');";

                using (var cmd = new NpgsqlCommand(columnQuery, conn))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(reader.GetString(0));
                    }
                }
                return columns;
            }
        }
        /// <summary>
        /// Compares two <see cref="DataTable"/> instances cell-by-cell and
        /// returns a formatted status log line.
        /// </summary>
        /// <param name="MasterTable">Master (parent) table data.</param>
        /// <param name="SlaveTable">Slave (child) table data.</param>
        /// <param name="MasterTable_name">Master table name (schema.table).</param>
        /// <param name="SlaveTable_name">Slave table name (schema.table).</param>
        /// <param name="MasterDB_name">Master database name (for logging).</param>
        /// <param name="SlaveDB_name">Slave database name (for logging).</param>
        /// <returns>
        /// A string in the form <c>[OK] MasterDB:MasterTable-SlaveDB:SlaveTable</c> or
        /// <c>[Error] MasterDB:MasterTable-SlaveDB:SlaveTable</c>.
        /// </returns>
        /// <remarks>
        /// The method first checks row/column counts. If they differ, it returns an error.
        /// Otherwise, it iterates through all cells (converted to strings) and returns OK only if
        /// all values match.
        /// </remarks>
        public string TableComparer(DataTable MasterTable, DataTable SlaveTable, string MasterTable_name, string SlaveTable_name, string MasterDB_name, string SlaveDB_name)
        {
            string Log = "";
            string status = "";
            int RowCount = -1;
            int ColCount = -1;
            if (MasterTable.Rows.Count == SlaveTable.Rows.Count && MasterTable.Columns.Count == SlaveTable.Columns.Count)
            {
                RowCount = MasterTable.Rows.Count;
                ColCount = MasterTable.Columns.Count;
            }
            else
            {
                status = "Error";
                Log = $"[{status}] {MasterDB_name}:{MasterTable_name}-{SlaveDB_name}:{SlaveTable_name}";
                return Log;
            }
            for (int i = 0; i < RowCount; i++)
            {
                for (int j = 0; j < ColCount; j++)
                {
                    var MD = MasterTable.Rows[i][j].ToString();
                    var SD = SlaveTable.Rows[i][j].ToString();
                    if (MD == SD)
                    {
                        status = "OK";
                    }
                    else
                    {
                        status = "Error";
                        break;
                    }
                }
            }
            Log = $"[{status}] {MasterDB_name}:{MasterTable_name}-{SlaveDB_name}:{SlaveTable_name}";

            return Log;
        }
    }
    /// <summary>
    /// Lightweight wrapper for PostgreSQL operations via Npgsql.
    /// </summary>
    public class NpgsqlDbHelper
    {
        private readonly string _connectionString;
        /// <summary>
        /// Initializes a new instance with the provided connection string.
        /// </summary>
        /// <param name="connectionString">PostgreSQL connection string.</param>
        public NpgsqlDbHelper(string connectionString)
        {
            _connectionString = connectionString;
        }
         /// <summary>
        /// Creates and opens a new <see cref="NpgsqlConnection"/>.
        /// </summary>
        /// <returns>An open <see cref="NpgsqlConnection"/>.</returns>
        public NpgsqlConnection GetConnection()
        {
            NpgsqlConnection conn = new NpgsqlConnection(_connectionString);
            conn.Open();
            return conn;
        }
        /// <summary>
        /// Reads values from a table for the specified columns and returns them as a <see cref="DataTable"/>.
        /// </summary>
        /// <param name="conn">An open <see cref="NpgsqlConnection"/>.</param>
        /// <param name="tableName">Fully qualified table name (schema.table).</param>
        /// <param name="columns">Columns to select (in order).</param>
        /// <returns>A <see cref="DataTable"/> with the selected rows, ordered by the first column.</returns>
        /// <remarks>
        /// The query is generated as <c>SELECT "col1","col2",... FROM schema.table ORDER BY 1</c>.
        /// </remarks>
        public DataTable GetValues(NpgsqlConnection conn, string tableName, List<string> columns)
        {            
            string query = $"SELECT \"{string.Join("\", \"", columns)}\" FROM {tableName} ORDER BY 1";
            using var cmd = new NpgsqlCommand(query, conn);
            using var reader = cmd.ExecuteReader();
            DataTable table = new DataTable();
            table.Load(reader);
            return table;            
        }
    }
    /// <summary>
    /// Child database configuration section (from JSON config).
    /// </summary>
    public class ChildDatabase
    {
        public string ConnectionString { get; set; }
        public string Schema { get; set; }
        public List<string> Tables { get; set; }
    }
    /// <summary>
    /// Parent (master) database configuration section (from JSON config).
    /// </summary>
    public class ParentDatabase
    {
        public string ConnectionString { get; set; }
        public string Schema { get; set; }
    }
    /// <summary>
    /// Root configuration POCO matching <c>appsetting.json</c>.
    /// </summary>
    public class Root
    {
        public ParentDatabase ParentDatabase { get; set; }
        public List<ChildDatabase> ChildDatabases { get; set; }
    }

    internal class Program
    {
        /// <summary>
        /// Entry point: loads JSON configuration, iterates through child databases and tables,
        /// compares them against the parent database, and writes log files with results.
        /// </summary>
        /// <remarks>
        /// Log files are written to <c>./logs/{yyyy}/{MM}/log-{dd.MM.yyyy}.txt</c> and <c>./logs/log-last.txt</c>.
        /// Comparison is performed by reading non-spatial columns, selecting data, and running a
        /// cell-by-cell equality check via <see cref="HelpingFunctions.TableComparer"/>.
        /// </remarks>
        static void Main(string[] args)
        {
            List<string> Logs = new List<string>();
            List<string> tables = new List<string>();
            HelpingFunctions HF = new HelpingFunctions();

            string ProgramPath = Environment.CurrentDirectory;
            if (!Directory.Exists(ProgramPath + "/logs"))
                Directory.CreateDirectory("./logs");
            if (!Directory.Exists(ProgramPath + "/logs/" + DateTime.Now.ToString("yyyy")));
                Directory.CreateDirectory(ProgramPath + "/logs/" + DateTime.Now.ToString("yyyy"));
            if (!Directory.Exists(ProgramPath + "/logs/" + DateTime.Now.ToString("yyyy") + "/" + DateTime.Now.ToString("MM")));
                Directory.CreateDirectory(ProgramPath + "/logs/" + DateTime.Now.ToString("yyyy") + "/" + DateTime.Now.ToString("MM"));

            var ConfigFile = File.ReadAllText("./appsetting.json");
            Root App_Config = JsonConvert.DeserializeObject<Root>(ConfigFile);

            List<string> Master_cols = new List<string>();
            DataTable MasterData;

            foreach (var Child in App_Config.ChildDatabases)
            {
                foreach (var table in Child.Tables)
                {                    
                    string MasterDB_name = App_Config.ParentDatabase.ConnectionString.Split(';')[2].Split('=')[1];
                    string SlaveDB_name = Child.ConnectionString.Split(';')[2].Split('=')[1];

                    string Master_table = App_Config.ParentDatabase.Schema + '.' + table;
                    var Slave_table = Child.Schema + '.' + table;

                    try
                    {
                        //Получем данные родителькой таблицы
                        NpgsqlDbHelper MasterDB = new NpgsqlDbHelper(App_Config.ParentDatabase.ConnectionString);
                        Master_cols = HF.GetColumns(App_Config.ParentDatabase.ConnectionString, Master_table);
                        MasterData = MasterDB.GetValues(MasterDB.GetConnection(), Master_table, Master_cols);

                        //Получаем данные дочерней таблицы
                        NpgsqlDbHelper SlaveDB = new NpgsqlDbHelper(Child.ConnectionString);
                        var Slave_cols = HF.GetColumns(Child.ConnectionString, Slave_table);
                        var SlaveData = SlaveDB.GetValues(SlaveDB.GetConnection(), Slave_table, Slave_cols);

                        Logs.Add(HF.TableComparer(MasterData, SlaveData, Master_table, Slave_table, MasterDB_name, SlaveDB_name));
                    }             
                    catch 
                    {
                        Logs.Add($"[Error - Error connecting to the database or table]  {MasterDB_name}:{Master_table}-{SlaveDB_name}:{Slave_table}");
                    }
                    
                }
            }

            using (StreamWriter writer = new StreamWriter($"./logs/{DateTime.Now.ToString("yyyy")}/{DateTime.Now.ToString("MM")}/log-{DateTime.Now.ToString("dd.MM.yyyy", CultureInfo.GetCultureInfo("ru-RU"))}.txt"))
            {
                foreach (string log in Logs)
                {
                    writer.WriteLine(log); // Запись каждой строки
                }
            }
            using (StreamWriter writer = new StreamWriter($"./logs/log-last.txt"))
            {
                foreach (string log in Logs)
                {
                    writer.WriteLine(log); // Запись каждой строки
                }
            }
        }
    }
}
