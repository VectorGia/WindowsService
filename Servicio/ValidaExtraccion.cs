using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsService1.Servicio
{
    public class ValidaExtraccion
    {
        NpgsqlConnection conP = new NpgsqlConnection();
        NpgsqlCommand comP = new NpgsqlCommand();
        char cod = '"';

        public ValidaExtraccion()
        {
            //Constructor
            //conP = new NpgsqlConnection("User ID=postgres;Password=omnisys;Host=192.168.1.78;Port=5433;Database=GIA;Pooling=true;");
            conP = new NpgsqlConnection("User ID=postgres;Password=omnisys;Host=127.0.0.1;Port=5432;Database=GIA;Pooling=true;");
        }

        /// <summary>
        /// Busca en la tabla y recupera todos lo registros de la tabla de extracción programada
        /// </summary>
        /// <returns>Data Table TAB_ETL_PROG</returns> 
        public DataTable FechaExtra()
        {
            string consulta = "SELECT " + cod + "INT_ID_ETL_PROG" + cod + ","
                + cod + "TEXT_FECH_EXTR" + cod + ","
                + cod + "TEXT_HORA_EXTR" + cod + ","
                + cod + "INT_ID_EMPRESA" + cod
                + " FROM " + cod + "TAB_ETL_PROG" + cod;
            //+ " WHERE " + cod + "INT_ID_EMPRESA" + cod + " = " + id_empresa;

            try
            {
                conP.Open();
                comP = new Npgsql.NpgsqlCommand(consulta, conP);
                Npgsql.NpgsqlDataAdapter daP = new Npgsql.NpgsqlDataAdapter(comP);

                DataTable dt = new DataTable();
                daP.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {
                conP.Close();
                string error = ex.Message;
                throw;
            }
            finally
            {
                conP.Close();
            }
        }

        public List<TAB_ETL_PROG> lstParametros()
        {
            List<TAB_ETL_PROG> lstEtlP = new List<TAB_ETL_PROG>();
            DataTable dt = new DataTable();
            dt = FechaExtra();
            foreach (DataRow r in dt.Rows)
            {
                TAB_ETL_PROG ETLPROG = new TAB_ETL_PROG();
                ETLPROG.INT_ID_ETL_PROG = Convert.ToInt32(r["INT_ID_ETL_PROG"]);
                ETLPROG.TEXT_FECH_EXTR = r["TEXT_FECH_EXTR"].ToString();
                ETLPROG.TEXT_HORA_EXTR = r["TEXT_HORA_EXTR"].ToString();
                ETLPROG.INT_ID_EMPRESA = Convert.ToInt32(r["INT_ID_EMPRESA"]);

                lstEtlP.Add(ETLPROG);
            }
            return lstEtlP;
        }

        /// <summary>
        /// Metodo para revisar si ya existe una extracion PREVIA
        /// El tipo de extraccion es Programada
        /// debe recibir el ID de la empresa
        /// </summary>
        /// <returns></returns>
        public DataTable existeExtr()
        {
            TAB_ETL_PROG tab_etl_prog = new TAB_ETL_PROG();
            List<TAB_ETL_PROG> lstPara = lstParametros();

            string consulta = "SELECT 1 as EXISTE, " + cod + "INT_TIPO_EXTRACCION" + cod + ","
                   + cod + "TEXT_FECH_EXTR" + cod + ","
                   + cod + "TEXT_HORA" + cod + ","
                   + cod + "INT_ID_EMPRESA" + cod
                   + " FROM " + cod + "TAB_BALANZA" + cod
                   + " WHERE" + cod + "INT_ID_EMPRESA" + cod + " = " + lstPara[0].INT_ID_EMPRESA //tab_etl_prog.INT_ID_EMPRESA
                   + " AND " + cod + "INT_TIPO_EXTRACCION" + cod +" = 2";
            try
            {
                conP.Open();
                comP = new Npgsql.NpgsqlCommand(consulta, conP);
                Npgsql.NpgsqlDataAdapter daP = new Npgsql.NpgsqlDataAdapter(comP);
                conP.Close();
                DataTable dt = new DataTable();
                daP.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {
                conP.Close();
                string error = ex.Message;
                throw;
            }
            finally
            {
                conP.Close();
            }
        }

        public List<TAB_ETL_PROG> lstExisteExtr()
        {
            List<TAB_ETL_PROG> lstEtlP = new List<TAB_ETL_PROG>();
            DataTable dt = new DataTable();
            dt = existeExtr();
            foreach (DataRow r in dt.Rows)
            {
                TAB_ETL_PROG ETLPROG = new TAB_ETL_PROG();
                ETLPROG.INT_ID_ETL_PROG = Convert.ToInt32(r["INT_ID_ETL_PROG"]);
                ETLPROG.TEXT_FECH_EXTR = r["TEXT_FECH_EXTR"].ToString();
                ETLPROG.TEXT_HORA_EXTR = r["TEXT_HORA_EXTR"].ToString();
                ETLPROG.INT_ID_EMPRESA = Convert.ToInt32(r["INT_ID_EMPRESA"]);
                ETLPROG.EXISTE = Convert.ToInt32(r["EXISTE"]);
                lstEtlP.Add(ETLPROG);
            }
            return lstEtlP;
        }

        /// <summary>
        /// Metodo para obtener fecha y hora del sistema comparar contra los valores de BD
        /// indica si se debe o no realizar la extraccion
        /// </summary>
        /// <returns></returns>
        //public int compararParametros()
        //{
        //    int realizarExtr;
        //    string fechaSistema = DateTime.Now.ToShortDateString();
        //    string hora = DateTime.Now.ToShortTimeString();
        //    TAB_ETL_PROG tab_etl_prog = new TAB_ETL_PROG();
        //    List<TAB_ETL_PROG> lstEtlP = new List<TAB_ETL_PROG>();
        //    lstEtlP = lstParametros();
            
        //    int extrProg = lstEtlP.Count();

        //    for (int i = 0; i < extrProg; i++)
        //    {                
        //        /*Si se requiere comparar fecha - hora se puede agregar en este if*/
        //        if (lstEtlP[i].EXISTE == 1) /*No hace la estracción*/
        //        {
        //             realizarExtr = 1;
        //        }
        //        else
        //        {
        //             realizarExtr = 0;
        //        }
        //        return realizarExtr;
        //    }            
        //}
    }
}
