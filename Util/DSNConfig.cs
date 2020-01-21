using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WindowsService1.Models;
using Microsoft.Win32;
using WindowsService1.Servicio;

namespace WindowsService1.Util
{
    public class DSNConfig
    {
        public DSNConfig()
        {
            //Constructor
        }

        public DSN crearDSN(int id_compania)
        {
            ETL etl = new ETL();
            List<Compania> lstCia = etl.CadenaConexionETL_lst(id_compania);

            //Obtener los datos de la Tab_Compania para crear el DSN
            //ETLDataAccesLayer eTLDataAccesLayer = new ETLDataAccesLayer();
            //List<Compania> lstCia = eTLDataAccesLayer.CadenaConexionETL_lst(id_compania);

            Compania cia = new Compania();
            cia.STR_USUARIO_ETL = lstCia[0].STR_USUARIO_ETL;
            cia.STR_CONTRASENIA_ETL = lstCia[0].STR_CONTRASENIA_ETL;
            cia.STR_HOST_COMPANIA = lstCia[0].STR_HOST_COMPANIA;
            cia.STR_PUERTO_COMPANIA = lstCia[0].STR_PUERTO_COMPANIA;
            cia.STR_BD_COMPANIA = lstCia[0].STR_BD_COMPANIA;
            cia.INT_IDCOMPANIA_P = lstCia[0].INT_IDCOMPANIA_P;

            string ODBC_PATH = string.Empty;
            string driver = string.Empty;
            string DsnNombre = string.Empty;
            string Descri = string.Empty;
            string DireccionDriver = string.Empty;
            bool trustedConnection = false;

            try
            {
                ODBC_PATH = "SOFTWARE\\ODBC\\ODBC.INI\\";
                driver = "SQL Anywhere 12"; //Nombre del Driver
                DsnNombre = cia.STR_NOMBRE_COMPANIA + "_" + cia.INT_IDCOMPANIA_P + "_" + cia.STR_HOST_COMPANIA; //nombre con el que se va identificar el DSN
                Descri = "DNS_Sybase" + DsnNombre;
                DireccionDriver = "C:\\Program Files\\SQL Anywhere 12\\Bin64\\dbodbc12.dll";
                var datasourcesKey = Registry.LocalMachine.CreateSubKey(ODBC_PATH + "ODBC Data Sources");



                if (datasourcesKey == null)
                {
                    throw new Exception("La clave de registro ODBC no existe");
                }




                //// Se crea el DSN en datasourcesKey aunque ya exista 
                datasourcesKey.SetValue(DsnNombre, driver);
                //// Borrado de DSN para Actualizar  datos en base de datos 
                datasourcesKey.DeleteValue(DsnNombre);
                /// Se crea DSN con datos actuales 
                datasourcesKey.SetValue(DsnNombre, driver);


                var dsnKey = Registry.LocalMachine.CreateSubKey(ODBC_PATH + DsnNombre);

                if (dsnKey == null)
                {
                    throw new Exception("No se creó la clave de registro ODBC para DSN");
                }

                dsnKey.SetValue("Database", cia.STR_BD_COMPANIA);
                dsnKey.SetValue("Description", Descri);
                dsnKey.SetValue("Driver", DireccionDriver);
                dsnKey.SetValue("User", cia.STR_USUARIO_ETL);
                dsnKey.SetValue("Host", cia.STR_HOST_COMPANIA + ":" + cia.STR_PUERTO_COMPANIA);
                dsnKey.SetValue("Server", cia.STR_HOST_COMPANIA);
                dsnKey.SetValue("Database", cia.STR_BD_COMPANIA);
                dsnKey.SetValue("username", cia.STR_USUARIO_ETL);
                dsnKey.SetValue("password", cia.STR_CONTRASENIA_ETL);
                dsnKey.SetValue("Trusted_Connection", trustedConnection ? "Yes" : "No");

                DSN dsn = new DSN();
                dsn.creado = true;
                dsn.nombreDSN = DsnNombre;
                return dsn;
                //return 1; //se creo
            }
            catch (Exception ex)
            {
                string error = ex.Message;

                DSN dsn = new DSN();
                dsn.creado = false;
                dsn.nombreDSN = DsnNombre;
                return dsn;
                //return 0; //Nose creo
            }
        }
    }
}
