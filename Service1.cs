﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WindowsService1.Servicio;

namespace WindowsService1
{
    public partial class Service1 : ServiceBase
    {
        public static bool continua = false;
        ConfiguracionCorreo configCorreo = new ConfiguracionCorreo();

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            Configuration configuration = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            AppSettingsSection appSettings = configuration.AppSettings;
            string timeToRun = appSettings.Settings["timechecking"].Value;
            int timeTo = Convert.ToInt32(timeToRun);
            continua = true;
            iniciaServicio( timeTo);
        }

        public void iniciaServicio(int timeTo)
        {
            while (continua)
            {
                ETL etl = new ETL();
                ValidaExtraccion valiExtr = new ValidaExtraccion();
                List<TAB_ETL_PROG> lstExtrProg = new List<TAB_ETL_PROG>();
                lstExtrProg = valiExtr.lstExisteExtr();

                List<TAB_ETL_PROG> lstExtrProg1 = new List<TAB_ETL_PROG>();
                lstExtrProg1 = valiExtr.lstParametros();

                //int id_compania = lstExtrProg1[0].INT_ID_EMPRESA;
           
                int idCompania = 0;
                string nombreCompania = "";

                foreach (TAB_ETL_PROG etlProg in lstExtrProg1) {
                    idCompania = etlProg.INT_ID_EMPRESA;
                    List<Compania> lstCompania = etl.CadenaConexionETL_lst(idCompania);
                    if (lstCompania!=null) {
                        if (lstCompania.Count >=1)
                        {
                            nombreCompania = lstCompania[0].STR_NOMBRE_COMPANIA;
                        }
                    }

                    try
                    {
                        Thread.Sleep(timeTo);
                        
                        etl.insertarTabBalanza(idCompania,nombreCompania);
                    }
                    catch (Exception ex)
                    {
                        string error = ex.Message;
                        configCorreo.EnviarCorreo("Estimado Usuario : \n\n  La extracción correspondiente a la compania " + idCompania +"."+ nombreCompania + " se genero incorrectamente \n\n Mensaje de Error: \n " + ex, "ETL Extracción Balanza");
                        throw;
                    }
                }



                //try
                //{
                //    Thread.Sleep(timeTo);
                //    Compania compania = new Compania();

                //    if (lstExtrProg.Count() == 0)
                //    {
                //        //etl.CadenaConexionETL(id_compania);
                //        etl.addTAB_BALANZA(id_compania);
                //        //if (lstExtrProg[0].EXISTE == 0)
                //        //if (lstExtrProg[0].EXISTE == 0)
                //        //{
                //        //    new ETL().addTAB_BALANZA(compania);
                //        //}
                //        //else
                //        //{
                //        //    continue;
                //        //}
                //    }
                //    else
                //    {
                        
                //    }

                    
                //}
                //catch (Exception ex)
                //{
                //    string error = ex.Message;
                //    throw;
                //}
            }
        }

        public void onDebug()
        {
            OnStart(null);
        }
        protected override void OnStop()
        {
            continua = false;
        }
    }
}
