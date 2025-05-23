﻿using Newtonsoft.Json;
using Npgsql;
using System.Data;
using System.Globalization;

namespace Актуализатор_данных
{
    public class HelpingFunctions
    {
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
    
    public class NpgsqlDbHelper
    {
        private readonly string _connectionString;
        public NpgsqlDbHelper(string connectionString)
        {
            _connectionString = connectionString;
        }
        public NpgsqlConnection GetConnection()
        {
            NpgsqlConnection conn = new NpgsqlConnection(_connectionString);
            conn.Open();
            return conn;
        }
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
    
    public class ChildDatabase
    {
        public string ConnectionString { get; set; }
        public string Schema { get; set; }
        public List<string> Tables { get; set; }
    }

    public class ParentDatabase
    {
        public string ConnectionString { get; set; }
        public string Schema { get; set; }
    }

    public class Root
    {
        public ParentDatabase ParentDatabase { get; set; }
        public List<ChildDatabase> ChildDatabases { get; set; }
    }

    internal class Program
    {
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
