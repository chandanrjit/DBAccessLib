using System;
using System.Data.SqlClient;

namespace DBHelperpoc
{
    class DBExceptionManager
    {
        #region Instance Property
        private static DBExceptionManager _Instance;

        public static DBExceptionManager Instance
        {
            get
            {
                if (_Instance == null)
                {
                    _Instance = new DBExceptionManager();
                }

                return _Instance;
            }
            set { _Instance = value; }
        }
        #endregion

        /// <summary>
        /// Get/Set Last Exception Object Created
        /// </summary>
        public Exception LastException { get; set; }

        #region Publish Methods
        public virtual void Publish(Exception ex)
        {
            LastException = ex;

            // TODO: Implement an exception publisher here - No time to do 
            System.Diagnostics.Debug.WriteLine(ex.ToString());
        }

        public virtual void Publish(Exception ex, SqlCommand cmd)
        {
            Publish(ex, cmd, null);
        }

        public virtual void Publish(Exception ex, SqlCommand cmd, string exceptionMsg)
        {
            LastException = ex;

            if (cmd != null)
            {
                LastException = CreateDbException(ex, cmd, null);

                System.Diagnostics.Debug.WriteLine(ex.ToString());
            }
        }
        #endregion

        #region CreateDbException Method
        public virtual DBDataException CreateDbException(Exception ex, SqlCommand cmd, string exceptionMsg)
        {
            DBDataException excep;
            exceptionMsg = string.IsNullOrEmpty(exceptionMsg) ? string.Empty : exceptionMsg + " - ";

            excep = new DBDataException(exceptionMsg + ex.Message, ex)
            {
                ConnectionString = cmd.Connection.ConnectionString,
                Database = cmd.Connection.Database,
                SQL = cmd.CommandText,
                CommandParameters = cmd.Parameters,
                WorkstationId = Environment.MachineName
            };

            return excep;
        }
        #endregion
    }
}
