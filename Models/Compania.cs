using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsService1.Servicio
{
    public class Compania
    {
        public Compania()
        {
            //Constructor
        }

        public string STR_NOMBRE_COMPANIA { get; set; }

        public string STR_ABREV_COMPANIA { get; set; }

        public string STR_IDCOMPANIA { get; set; }

        public bool BOOL_ETL_COMPANIA { get; set; }

        public string STR_HOST_COMPANIA { get; set; }

        public string STR_USUARIO_ETL { get; set; }

        public string STR_CONTRASENIA_ETL { get; set; }

        public string STR_PUERTO_COMPANIA { get; set; }

        public string STR_MONEDA_COMPANIA { get; set; }

        public string STR_BD_COMPANIA { get; set; }

        public bool BOOL_ESTATUS_LOGICO_COMPANIA { get; set; }

        public int INT_IDCOMPANIA_P { get; set; }

        public int INT_IDPROYECTO_F { get; set; }

        public int INT_IDCENTROCOSTO_F { get; set; }

        public DateTime FEC_MODIF_COMPANIA { get; set; }
    }
}
