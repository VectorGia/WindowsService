using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsService1.Models
{
    public class ConfigCorreo
    {
        public int INT_ID_CORREO { get; set; }
        public string TEXT_FROM { get; set; }
        public string TEXT_PASSWORD { get; set; }
        public int INT_PORT { get; set; }
        public string TEXT_HOST { get; set; }
    }
}
