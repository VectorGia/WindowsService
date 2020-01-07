using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsService1.Servicio
{
    class ETL
    {
        NpgsqlConnection conP = new NpgsqlConnection();
        NpgsqlCommand comP = new NpgsqlCommand();
        char cod = '"';

        SqlConnection conSQLETL = new SqlConnection();
        SqlCommand comSQLETL = new SqlCommand();

        public ETL()
        {
            //Constructor
            conP = new NpgsqlConnection("User ID=postgres;Password=omnisys;Host=192.168.1.78;Port=5432;Database=GIA;Pooling=true;");
        }

        /// <summary>
        /// Metodo para obtener los valores de la cadena de conexion de la empresa
        /// </summary>
        /// <param name="compania"></param>
        /// <returns></returns>
        public DataTable CadenaConexionETL(int id_compania)
        {
            string add = "SELECT " + cod + "INT_IDCOMPANIA_P" + cod + ","
                    + cod + "STR_USUARIO_ETL" + cod + ","
                    + cod + "STR_CONTRASENIA_ETL" + cod + ","
                    + cod + "STR_HOST_COMPANIA" + cod + ","
                    + cod + "STR_PUERTO_COMPANIA" + cod + ","
                    + cod + "STR_BD_COMPANIA" + cod
                    + " FROM " + cod + "CAT_COMPANIA" + cod
                    + " WHERE " + cod + "INT_IDCOMPANIA_P" + cod + " = " + id_compania;
            try
            {
                conP.Open();
                comP = new Npgsql.NpgsqlCommand(add, conP);
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
        }

        /// <summary>
        /// Se convierte el DataTable en una Lista Generica
        /// </summary>
        /// <param name="compania"></param>
        /// <returns>Lista del tipo Compania</returns>
        public List<Compania> CadenaConexionETL_lst(int id_compania)
        {
            List<Compania> lst = new List<Compania>();
            DataTable dt = new DataTable();
            dt = CadenaConexionETL(id_compania);

            foreach (DataRow r in dt.Rows)
            {
                Compania cia = new Compania();
                cia.STR_USUARIO_ETL = r["STR_USUARIO_ETL"].ToString();
                cia.STR_CONTRASENIA_ETL = r["STR_CONTRASENIA_ETL"].ToString();
                cia.STR_HOST_COMPANIA = r["STR_HOST_COMPANIA"].ToString();
                cia.STR_PUERTO_COMPANIA = r["STR_PUERTO_COMPANIA"].ToString();
                cia.STR_BD_COMPANIA = r["STR_BD_COMPANIA"].ToString();
                cia.INT_IDCOMPANIA_P = Convert.ToInt32(r["INT_IDCOMPANIA_P"]);
                lst.Add(cia);
            }
            return lst;
        }

        /// <summary>
        /// Crea la cadena de conexion segun lo guardado en la tabla compania
        /// realiza el select a la tabla Balanza
        /// </summary>
        /// <param name="compania"></param>
        /// <returns>Extracción del SQL</returns>
        public DataTable cadena_conexion_extrac(int id_compania)
        {
            string UserID = string.Empty;
            string Password = string.Empty;
            string Host = string.Empty;
            string Port = string.Empty;
            string DataBase = string.Empty;
            string Cadena = string.Empty;

            List<Compania> lstCadena = new List<Compania>();
            lstCadena = CadenaConexionETL_lst(id_compania);
            UserID = lstCadena[0].STR_USUARIO_ETL;
            Password = lstCadena[0].STR_CONTRASENIA_ETL;
            Host = lstCadena[0].STR_HOST_COMPANIA;
            Port = lstCadena[0].STR_PUERTO_COMPANIA;
            DataBase = lstCadena[0].STR_BD_COMPANIA;

            /*Cadena de Postegres
            Cadena = "USER ID=" + UserID + ";" + "Password=" + Password + ";" + "Host=" + Host + ";" + "Port =" + Port + ";" + "DataBase=" + DataBase + ";" + "Pooling=true;";*/
            //conPETL = new NpgsqlConnection(Cadena);

            /*Cadena de SQL*/
            Cadena = "Data Source =" + Host +","+ Port+";" + "Initial Catalog=" + DataBase + ";" + "Persist Security Info=True;" + "User ID=" + UserID + ";Password=" + Password;
            conSQLETL = new SqlConnection(Cadena);

            try
            {
                string add = "SELECT[INT_IDBALANZA], [VARCHAR_CTA],"
                            + "[VARCHAR_SCTA], [VARCHAR_SSCTA],"
                            + "[INT_YEAR], [DECI_SALINI],"
                            + "[DECI_ENECARGOS], [DECI_ENEABONOS],"
                            + "[DECI_FEBCARGOS], [DECI_FEBABONOS],"
                            + "[DECI_MARCARGOS], [DECI_MARABONOS],"
                            + "[DECI_ABRCARGOS], [DECI_ABRABONOS],"
                            + "[DECI_MAYCARGOS], [DECI_MAYABONOS],"
                            + "[DECI_JUNCARGOS], [DECI_JUNABONOS],"
                            + "[DECI_JULCARGOS], [DECI_JULABONOS],"
                            + "[DECI_AGOCARGOS], [DECI_AGOABONOS],"
                            + "[DECI_SEPCARGOS], [DECI_SEPABONOS],"
                            + "[DECI_OCTCARGOS], [DECI_OCTABONOS],"
                            + "[DECI_NOVCARGOS], [DECI_NOVABONOS],"
                            + "[DECI_DICCARGOS], [DECI_DICABONOS],"
                            + "[INT_CC],"
                            + "[VARCHAR_DESCRIPCION],"
                            + "[VARCHAR_DESCRIPCION2],"
                            + "[DECI_INCLUIR_SUMA]"
                            + " FROM [TAB_BALANZA_SQL]";

                comSQLETL = new SqlCommand(add, conSQLETL);
                SqlDataAdapter da = new SqlDataAdapter(comSQLETL);
                DataTable dt = new DataTable();
                da.Fill(dt);
                return dt;

            }
            catch (Exception ex)
            {
                string error = ex.Message;
                throw;
            }
            finally
            {
                conSQLETL.Close();
            }
        }

        public List<Balanza> lstBalanzaETL(int id_compania)
        {
            List<Balanza> lstBalanza = new List<Balanza>();
            DataTable dt = new DataTable();
            dt = cadena_conexion_extrac(id_compania);
            foreach (DataRow r in dt.Rows)
            {
                Balanza balanza = new Balanza();
                balanza.INT_IDBALANZA = Convert.ToInt32(r["INT_IDBALANZA"]);
                balanza.TEXT_CTA = r["VARCHAR_CTA"].ToString();
                balanza.TEXT_SCTA = r["VARCHAR_SCTA"].ToString();
                balanza.TEXT_SSCTA = r["VARCHAR_SSCTA"].ToString();
                balanza.INT_YEAR = Convert.ToInt32(r["INT_YEAR"]);
                balanza.DECI_SALINI = Convert.ToDouble(r["DECI_SALINI"]);
                balanza.DECI_ENECARGOS = Convert.ToDouble(r["DECI_ENECARGOS"]);
                balanza.DECI_ENEABONOS = Convert.ToDouble(r["DECI_ENEABONOS"]);
                balanza.DECI_FEBCARGOS = Convert.ToDouble(r["DECI_FEBCARGOS"]);
                balanza.DECI_FEBABONOS = Convert.ToDouble(r["DECI_FEBABONOS"]);
                balanza.DECI_MARCARGOS = Convert.ToDouble(r["DECI_MARCARGOS"]);
                balanza.DECI_MARABONOS = Convert.ToDouble(r["DECI_MARABONOS"]);
                balanza.DECI_ABRCARGOS = Convert.ToDouble(r["DECI_ABRCARGOS"]);
                balanza.DECI_ABRABONOS = Convert.ToDouble(r["DECI_ABRABONOS"]);
                balanza.DECI_MAYCARGOS = Convert.ToDouble(r["DECI_ABRABONOS"]);
                balanza.DECI_MAYABONOS = Convert.ToDouble(r["DECI_MAYABONOS"]);
                balanza.DECI_JUNCARGOS = Convert.ToDouble(r["DECI_JUNCARGOS"]);
                balanza.DECI_JUNABONOS = Convert.ToDouble(r["DECI_JUNABONOS"]);
                balanza.DECI_JULCARGOS = Convert.ToDouble(r["DECI_JULCARGOS"]);
                balanza.DECI_JULABONOS = Convert.ToDouble(r["DECI_JULABONOS"]);
                balanza.DECI_AGOCARGOS = Convert.ToDouble(r["DECI_AGOCARGOS"]);
                balanza.DECI_AGOABONOS = Convert.ToDouble(r["DECI_AGOABONOS"]);
                balanza.DECI_SEPCARGOS = Convert.ToDouble(r["DECI_AGOABONOS"]);
                balanza.DECI_SEPABONOS = Convert.ToDouble(r["DECI_SEPABONOS"]);
                balanza.DECI_OCTCARGOS = Convert.ToDouble(r["DECI_OCTCARGOS"]);
                balanza.DECI_OCTABONOS = Convert.ToDouble(r["DECI_OCTABONOS"]);
                balanza.DECI_NOVCARGOS = Convert.ToDouble(r["DECI_NOVCARGOS"]);
                balanza.DECI_NOVABONOS = Convert.ToDouble(r["DECI_NOVABONOS"]);
                balanza.DECI_DICCARGOS = Convert.ToDouble(r["DECI_DICCARGOS"]);
                balanza.DECI_DICABONOS = Convert.ToDouble(r["DECI_DICABONOS"]);
                balanza.INT_CC = Convert.ToInt32(r["INT_CC"]);
                balanza.TEXT_DESCRIPCION = r["VARCHAR_DESCRIPCION"].ToString();
                balanza.TEXT_DESCRIPCION2 = r["VARCHAR_DESCRIPCION2"].ToString();
                balanza.INT_INCLUIR_SUMA = Convert.ToInt32(r["DECI_INCLUIR_SUMA"]);
                lstBalanza.Add(balanza);
            }
            return lstBalanza;
        }

        public int addTAB_BALANZA(int id_compania)
        {
            List<Balanza> lstBala = new List<Balanza>();
            lstBala = lstBalanzaETL(id_compania);

            string addBalanza = "INSERT INTO"
+ cod + "TAB_BALANZA" + cod + "("
//+ cod + "INT_IDBALANZA" + cod + ","
+ cod + "TEXT_CTA" + cod + ","
+ cod + "TEXT_SCTA" + cod + ","
+ cod + "TEXT_SSCTA" + cod + ","
+ cod + "INT_YEAR" + cod + ","
+ cod + "DECI_SALINI" + cod + ","
+ cod + "DECI_ENECARGOS" + cod + ","
+ cod + "DECI_ENEABONOS" + cod + ","
+ cod + "DECI_FEBCARGOS" + cod + ","
+ cod + "DECI_FEBABONOS" + cod + ","
+ cod + "DECI_MARCARGOS" + cod + ","
+ cod + "DECI_MARABONOS" + cod + ","
+ cod + "DECI_ABRCARGOS" + cod + ","
+ cod + "DECI_ABRABONOS" + cod + ","
+ cod + "DECI_MAYCARGOS" + cod + ","
+ cod + "DECI_MAYABONOS" + cod + ","
+ cod + "DECI_JUNCARGOS" + cod + ","
+ cod + "DECI_JUNABONOS" + cod + ","
+ cod + "DECI_JULCARGOS" + cod + ","
+ cod + "DECI_JULABONOS" + cod + ","
+ cod + "DECI_AGOCARGOS" + cod + ","
+ cod + "DECI_AGOABONOS" + cod + ","
+ cod + "DECI_SEPCARGOS" + cod + ","
+ cod + "DECI_SEPABONOS" + cod + ","
+ cod + "DECI_OCTCARGOS" + cod + ","
+ cod + "DECI_OCTABONOS" + cod + ","
+ cod + "DECI_NOVCARGOS" + cod + ","
+ cod + "DECI_NOVABONOS" + cod + ","
+ cod + "DECI_DICCARGOS" + cod + ","
+ cod + "DECI_DICABONOS" + cod + ","
+ cod + "INT_CC" + cod + ","
+ cod + "TEXT_DESCRIPCION" + cod + ","
+ cod + "TEXT_DESCRIPCION2" + cod + ","
+ cod + "INT_INCLUIR_SUMA" + cod + ")"
    + "VALUES "
        //+ "(@INT_IDBALANZA,"
        + "(@TEXT_CTA,"
        + "@TEXT_SCTA,"
        + "@TEXT_SSCTA,"
        + "@INT_YEAR,"
        + "@DECI_SALINI,"
        + "@DECI_ENECARGOS,"
        + "@DECI_ENEABONOS,"
        + "@DECI_FEBCARGOS,"
        + "@DECI_FEBABONOS,"
        + "@DECI_MARCARGOS,"
        + "@DECI_MARABONOS,"
        + "@DECI_ABRCARGOS,"
        + "@DECI_ABRABONOS,"
        + "@DECI_MAYCARGOS,"
        + "@DECI_MAYABONOS,"
        + "@DECI_JUNCARGOS,"
        + "@DECI_JUNABONOS,"
        + "@DECI_JULCARGOS,"
        + "@DECI_JULABONOS,"
        + "@DECI_AGOCARGOS,"
        + "@DECI_AGOABONOS,"
        + "@DECI_SEPCARGOS,"
        + "@DECI_SEPABONOS,"
        + "@DECI_OCTCARGOS,"
        + "@DECI_OCTABONOS,"
        + "@DECI_NOVCARGOS,"
        + "@DECI_NOVABONOS,"
        + "@DECI_DICCARGOS,"
        + "@DECI_DICABONOS,"
        + "@INT_CC,"
        + "@TEXT_DESCRIPCION,"
        + "@TEXT_DESCRIPCION2,"
        + "@INT_INCLUIR_SUMA)";

            try
            {
                {
                    NpgsqlCommand cmd = new NpgsqlCommand(addBalanza, conP);
                    //cmd.Parameters.AddWithValue("@INT_IDBALANZA", NpgsqlTypes.NpgsqlDbType.Integer, lstBala[0].INT_IDBALANZA);
                    cmd.Parameters.AddWithValue("@TEXT_CTA", NpgsqlTypes.NpgsqlDbType.Text, lstBala[0].TEXT_CTA);
                    cmd.Parameters.AddWithValue("@TEXT_SCTA", NpgsqlTypes.NpgsqlDbType.Text, lstBala[0].TEXT_SCTA);
                    cmd.Parameters.AddWithValue("@TEXT_SSCTA", NpgsqlTypes.NpgsqlDbType.Text, lstBala[0].TEXT_SSCTA);
                    cmd.Parameters.AddWithValue("@INT_YEAR", NpgsqlTypes.NpgsqlDbType.Integer, lstBala[0].INT_YEAR);
                    cmd.Parameters.AddWithValue("@DECI_SALINI", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_SALINI);
                    cmd.Parameters.AddWithValue("@DECI_ENECARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_ENECARGOS);
                    cmd.Parameters.AddWithValue("@DECI_ENEABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_ENEABONOS);
                    cmd.Parameters.AddWithValue("@DECI_FEBCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_FEBCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_FEBABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_FEBABONOS);
                    cmd.Parameters.AddWithValue("@DECI_MARCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_MARCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_MARABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_MARABONOS);
                    cmd.Parameters.AddWithValue("@DECI_ABRCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_ABRCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_ABRABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_ABRABONOS);
                    cmd.Parameters.AddWithValue("@DECI_MAYCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_MAYCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_MAYABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_MAYABONOS);
                    cmd.Parameters.AddWithValue("@DECI_JUNCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_JUNCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_JUNABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_JUNABONOS);
                    cmd.Parameters.AddWithValue("@DECI_JULCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_JULCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_JULABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_JULABONOS);
                    cmd.Parameters.AddWithValue("@DECI_AGOCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_AGOCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_AGOABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_AGOABONOS);
                    cmd.Parameters.AddWithValue("@DECI_SEPCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_SEPCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_SEPABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_SEPABONOS);
                    cmd.Parameters.AddWithValue("@DECI_OCTCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_OCTCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_OCTABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_OCTABONOS);
                    cmd.Parameters.AddWithValue("@DECI_NOVCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_NOVCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_NOVABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_NOVABONOS);
                    cmd.Parameters.AddWithValue("@DECI_DICCARGOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_DICCARGOS);
                    cmd.Parameters.AddWithValue("@DECI_DICABONOS", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].DECI_DICABONOS);
                    cmd.Parameters.AddWithValue("@INT_CC", NpgsqlTypes.NpgsqlDbType.Integer, lstBala[0].INT_CC);
                    cmd.Parameters.AddWithValue("@TEXT_DESCRIPCION", NpgsqlTypes.NpgsqlDbType.Text, lstBala[0].TEXT_DESCRIPCION);
                    cmd.Parameters.AddWithValue("@TEXT_DESCRIPCION2", NpgsqlTypes.NpgsqlDbType.Text, lstBala[0].TEXT_DESCRIPCION2);
                    cmd.Parameters.AddWithValue("@INT_INCLUIR_SUMA", NpgsqlTypes.NpgsqlDbType.Double, lstBala[0].INT_INCLUIR_SUMA);
                    //conP.Open();
                    int cantFilaAfect = Convert.ToInt32(cmd.ExecuteNonQuery());
                    conP.Close();
                    return cantFilaAfect;
                }
            }
            catch (Exception ex)
            {
                conP.Close();
                string error = ex.Message;
                throw;
            }
        }
    }
}
