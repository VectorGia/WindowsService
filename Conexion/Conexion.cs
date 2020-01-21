using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;

namespace WindowsService1.Conexion
{
    public class Conexion
    {

        string cadena = "";
        NpgsqlConnection con;
        OdbcConnection odbcCon;



        public NpgsqlConnection ConnexionDB(string stringConnection)

        {
            con = new NpgsqlConnection(stringConnection);
            return con;
        }

      
        public OdbcConnection ConexionSybaseodbc(string DsnName)

        {
            try
            {

                odbcCon = new OdbcConnection("DSN=" + DsnName);
                // odbcCon = new OdbcConnection("DSN=GIAODBCPRUEBAS"); ////ODBCGIA

                return odbcCon;
            }
            catch (InvalidOperationException ex)
            {
                string error = ex.Message;
                return null;
            }
        }

    }
}
