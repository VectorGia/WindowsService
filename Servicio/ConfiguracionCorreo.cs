using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WindowsService1.Models;

namespace WindowsService1.Servicio
{
    public class ConfiguracionCorreo

    {
        NpgsqlConnection con;
        Conexion.Conexion conex = new Conexion.Conexion();
        char cod = '"';

        public ConfiguracionCorreo()
        {
            //con = conex.ConnexionDB("User ID=postgres;Password=omnisys;Host=192.168.1.78;Port=5432;Database=GIA;Pooling=true;");
            con = conex.ConnexionDB();
        }

        public void EnviarCorreo(string cuerpoMensaje, string tituloMensaje)
        {

            string listaDestinatarios = ListaCorreosDestinatarios().TrimEnd(',');
            List<ConfigCorreo> listaConfiguracionCorreo = new List<ConfigCorreo>();
            listaConfiguracionCorreo = GetAllConfigCorreo();

            System.Net.Mail.MailMessage mmsg = new System.Net.Mail.MailMessage();
            mmsg.To.Add(listaDestinatarios.ToString());
            mmsg.Subject = tituloMensaje;//"Correo ejemplo GIA";
            mmsg.SubjectEncoding = System.Text.Encoding.UTF8;
            mmsg.Body = cuerpoMensaje;//"Prueba de correo GIA";
            mmsg.BodyEncoding = System.Text.Encoding.UTF8;
            mmsg.IsBodyHtml = false;
            mmsg.From = new System.Net.Mail.MailAddress(listaConfiguracionCorreo[0].TEXT_FROM);

            System.Net.Mail.SmtpClient cliente = new System.Net.Mail.SmtpClient();

            cliente.Host = listaConfiguracionCorreo[0].TEXT_HOST;
            cliente.Port = listaConfiguracionCorreo[0].INT_PORT;
            cliente.EnableSsl = true;
            cliente.DeliveryMethod = System.Net.Mail.SmtpDeliveryMethod.Network;
            cliente.UseDefaultCredentials = false;
            cliente.Credentials = new System.Net.NetworkCredential(listaConfiguracionCorreo[0].TEXT_FROM, listaConfiguracionCorreo[0].TEXT_PASSWORD);
           
       


            string output = null;
            try
            {
                cliente.Send(mmsg);
            }
            catch (System.Net.Mail.SmtpException ex)
            {
                output = "Error enviando correo electrónico: " + ex.Message;
            }

        }


        public string ListaCorreosDestinatarios()
        {

            string correos = string.Empty;
            List<Usuario> lista = new List<Usuario>();
            lista = GetDestinatariosCorreo();
            foreach (Usuario usuario in lista)
            {
                correos += usuario.STR_EMAIL_USUARIO.ToString() + ",";
            }
            return correos;

        }

        public List<ConfigCorreo> GetAllConfigCorreo()
        {


            string cadena = "SELECT " + cod + "TEXT_FROM" + cod + "," + cod + "TEXT_PASSWORD" + cod + ","
                              + cod + "INT_PORT" + cod + "," + cod + "INT_ID_CORREO" + cod + "," + cod + "TEXT_HOST" + cod + " FROM " + cod + "TAB_CONFIG_CORREO" + cod;

            try
            {
                List<ConfigCorreo> listaConfigCorreo = new List<ConfigCorreo>();
                //using (NpgsqlConnection con = new NpgsqlConnection(connectionString))
                {
                    NpgsqlCommand cmd = new NpgsqlCommand(cadena.Trim(), con);

                    con.Open();
                    NpgsqlDataReader rdr = cmd.ExecuteReader();

                    while (rdr.Read())
                    {

                        ConfigCorreo configCorreo = new ConfigCorreo();
                        configCorreo.INT_ID_CORREO = Convert.ToInt32(rdr["INT_ID_CORREO"]);
                        configCorreo.TEXT_FROM = rdr["TEXT_FROM"].ToString().Trim();
                        configCorreo.TEXT_PASSWORD = rdr["TEXT_PASSWORD"].ToString().Trim();
                        configCorreo.INT_PORT = Convert.ToInt32(rdr["INT_PORT"]);
                        configCorreo.TEXT_HOST = rdr["TEXT_HOST"].ToString().Trim();


                        listaConfigCorreo.Add(configCorreo);
                    }
                    con.Close();
                }
                return listaConfigCorreo;
            }
            catch (Exception ex)
            {
                con.Close();

                throw ex;
            }
        }
        public List<Usuario> GetDestinatariosCorreo()
        {


            string cadena = "SELECT "
                            + cod + "STR_EMAIL_USUARIO" + cod
                            + " FROM "
                            + cod + "TAB_USUARIO" + cod
                            + " WHERE " + cod + "STR_EMAIL_USUARIO" + cod + "!='sin Correo'";

            try
            {
                List<Usuario> listaUsuarioCorreo = new List<Usuario>();
                //using (NpgsqlConnection con = new NpgsqlConnection(connectionString))
                {
                    NpgsqlCommand cmd = new NpgsqlCommand(cadena.Trim(), con);

                    con.Open();
                    NpgsqlDataReader rdr = cmd.ExecuteReader();

                    while (rdr.Read())
                    {

                        Usuario configUsuarioCorreo = new Usuario();
                        configUsuarioCorreo.STR_EMAIL_USUARIO = rdr["STR_EMAIL_USUARIO"].ToString().Trim();




                        listaUsuarioCorreo.Add(configUsuarioCorreo);
                    }
                    con.Close();
                }
                return listaUsuarioCorreo;
            }
            catch
            {
                con.Close();
                throw;
            }
        }
    }
}
