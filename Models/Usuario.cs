using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsService1.Models
{
    public class Usuario
    {

        public string userName { get; set; }

        public string displayName { get; set; }

        public int INT_IDUSUARIO_P { get; set; }

        public string STR_USERNAME_USUARIO { get; set; }

        public string STR_PASSWORD_USUARIO { get; set; }

        public string STR_EMAIL_USUARIO { get; set; }

        public bool BOOL_ESTATUS_LOGICO_USUARIO { get; set; }

        public string STR_PUESTO { get; set; }

        public DateTime FEC_MODIF_USUARIO { get; set; }

        public string STR_NOMBRE_USUARIO { get; set; }

    }
}
