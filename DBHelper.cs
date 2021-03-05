using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;


/// <summary>
///  SQLServer - Data Acceess Blocker - POC
/// </summary>
namespace DBHelperpoc
{
    public static class DBhelper
    {
        #region "Constants"
        /// <summary>
        /// Default Timeout (seconds)
        /// </summary>
        public const int Timeout_Default = 3600;
        #endregion

        #region "SQLwithParameters"

        /// <summary>
        /// Execute SQL Query with parameters with no return of data or values
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="SQL">SQL Statement</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="TimeOut">Timeout  seconds, with default</param>
        /// <returns>Rows affected</returns>
        public static int ExecuteQueryWithParametersNoReturn(string connectionString, string SQL, List<SqlParameter> parameters, int TimeOut = Timeout_Default)
        {
            System.Data.CommandType CmdType = System.Data.CommandType.Text;
            int iRows = 0;

            using (var conDB = new System.Data.SqlClient.SqlConnection(connectionString))
            {
                conDB.Open();
                using (SqlCommand command = new SqlCommand(SQL, conDB))
                {
                    command.CommandType = CmdType;
                    command.CommandTimeout = TimeOut;
                    if (parameters != null) foreach (var p in parameters) command.Parameters.Add(p);
                    var irows = command.ExecuteNonQuery();
                }
            }
            return iRows;
        }

        /// <summary>
        /// Execute SQL Query with parameters with no return of data or values - Transaction & Rollback
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="SQL">SQL Statement</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="TimeOut">Timeout  seconds, with default</param>
        /// <returns>Rows affected</returns>
        public static int ExecuteQueryNonWithParametersNoReturn(string connectionString, string SQL, List<SqlParameter> parameters, int TimeOut = Timeout_Default)
        {
            System.Data.CommandType CmdType = System.Data.CommandType.Text;
            int iRows = 0;
            SqlCommand command = null;            

            using (var conDB = new System.Data.SqlClient.SqlConnection(connectionString))
            {
                conDB.Open();
                using (SqlTransaction trn = conDB.BeginTransaction())
                {
                    try
                    {
                        using (command = new SqlCommand(SQL, conDB))
                        {
                            command.Transaction = trn;
                            command.CommandType = CmdType;
                            command.CommandTimeout = TimeOut;
                            if (parameters != null) foreach (var p in parameters) command.Parameters.Add(p);
                            iRows = command.ExecuteNonQuery();
                            // End the tansaction 
                            trn.Commit();
                        }
                    }
                    catch (Exception ex)
                    {
                        trn.Rollback();
                        Console.WriteLine(ex);
                        // log4net 
                        DBExceptionManager.Instance.Publish(ex, command ,"Error in command");
                        String expcetionDescrition = DBExceptionManager.Instance.LastException.ToString();
                    }

                }

            }
            return iRows;
        }

        /// <summary>
        /// Execute SQL Query with parameters with a DataTable return
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="SQL">SQL Statement</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="TimeOut">Timeout  seconds, with default</param>
        /// <returns>DataTable</returns>
        public static DataTable ExecuteQueryWithParametersToDataTable(string connectionString, string SQL, List<SqlParameter> parameters, int TimeOut = Timeout_Default)
        {
            System.Data.CommandType CmdType = System.Data.CommandType.Text;
            DataTable dt = null;

            using (var conDB = new System.Data.SqlClient.SqlConnection(connectionString))
            {
                conDB.Open();
                using (SqlCommand command = new SqlCommand(SQL, conDB))
                {
                    command.CommandType = CmdType;
                    command.CommandTimeout = TimeOut;
                    if (parameters != null) foreach (var p in parameters) command.Parameters.Add(p);
                    var rd = command.ExecuteReader();
                    dt = new DataTable();
                    dt.Load(rd);
                }
            }
            return dt;
        }

        /// <summary>
        /// Execute a parameterized SQL Query statement with a single value return
        /// </summary>
        /// <typeparam name="T">Type</typeparam>
        /// <param name="connectionString">Connection String</param>
        /// <param name="SQL"></param>
        /// <param name="parameters"></param>
        /// <param name="TimeOut"></param>
        /// <returns>T</returns>
        public static T ExecuteQueryWithParametersToScaler<T>(string connectionString, string SQL, List<SqlParameter> parameters, int TimeOut = Timeout_Default)
        {
            System.Data.CommandType CmdType = System.Data.CommandType.Text;
            T data = default(T);
            try
            {
                using (var conDB = new System.Data.SqlClient.SqlConnection(connectionString))
                {
                    conDB.Open();
                    using (SqlCommand command = new SqlCommand(SQL, conDB))
                    {
                        command.CommandType = CmdType;
                        command.CommandTimeout = TimeOut;
                        if (parameters != null) foreach (var p in parameters) command.Parameters.Add(p);
                        var rd = command.ExecuteScalar();
                        data = (T)rd;
                    }
                }
                return data;
            }
            catch (SqlException ex)
            {
                DBExceptionManager.Instance.Publish(ex);
                return default(T);
            }
        }

        #endregion


        #region "StoredProcedures"

        /// <summary>
        /// Execute a sql stored procedure that returns rows
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="ProcedureName">SP Name</param>
        /// <param name="parameters">Stored Procedure Arguments</param>
        /// <param name="TimeOut">Timeout  seconds, with default</param>
        /// <returns>Datatable or null</returns>
        public static DataTable ExecuteSPWithDataTable(string connectionString, string ProcedureName, List<SqlParameter> parameters, int TimeOut = Timeout_Default)
        {
            DataTable dt = null;

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (SqlCommand command = new SqlCommand())
                {
                    command.Connection = conn;
                    command.CommandText = ProcedureName;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = TimeOut;
                    if (parameters != null) foreach (var p in parameters) command.Parameters.Add(p);

                    SqlDataAdapter da = new SqlDataAdapter(command);
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    if ((ds != null) && (ds.Tables != null) && (ds.Tables.Count > 0)) dt = ds.Tables[0];
                }
                conn.Close();
            }

            return dt;
        }

        /// <summary>
        /// Executes a SQL Stored Procedure with no rows returned
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="ProcedureName">SP Name</param>
        /// <param name="parameters">Stored Procedure Arguments</param>
        /// <param name="TimeOut">Timeout  seconds, with default</param>
        /// <returns>Rows affected (not always correct)</returns>
        public static int ExecuteSPWithNoReturn(string connectionString, string ProcedureName, List<SqlParameter> parameters, int TimeOut = Timeout_Default)
        {
            int rows = -1;
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (SqlCommand command = new SqlCommand())
                {
                    command.Connection = conn;
                    command.CommandText = ProcedureName;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = TimeOut;
                    if (parameters != null) foreach (var p in parameters) command.Parameters.Add(p);
                    rows = command.ExecuteNonQuery();
                }
                conn.Close();
            }

            return rows;
        }

        /// <summary> 
        /// Execute SQL Stored Procedure With Parameters To Scaler
        /// </summary>
        /// <typeparam name="T">Type</typeparam>
        /// <param name="connectionString">Connection String</param>
        /// <param name="ProcedureName">SP Name</param>
        /// <param name="parameters">Stored Procedure Arguments</param>
        /// <param name="TimeOut">Timeout  seconds, with default</param>
        /// <returns></returns>
        public static T ExecuteSPWithParametersToScaler<T>(string connectionString, string ProcedureName, List<SqlParameter> parameters, int TimeOut = Timeout_Default)
        {
            T data = default(T);

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (SqlCommand command = new SqlCommand())
                {
                    command.Connection = conn;
                    command.CommandText = ProcedureName;
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = TimeOut;
                    if (parameters != null) foreach (var p in parameters) command.Parameters.Add(p);
                    data = (T)command.ExecuteScalar();
                }
                conn.Close();
            }

            return data;
        }


        #endregion


    }
}