using System;
using System.Collections.Generic;
using System.Text;
using CommandLine;
using CommandLine.Text;
using Microsoft.VisualBasic.CompilerServices;

namespace ifis_patbase_impoter
{
    class Options
    {
        [Option('d', "target database", Required = false, Default = "grand_central", HelpText = "the target database to write records to")]
        public string targetDBName { get; set; }

        [Option('s', "server", Required = false, Default = "localhost", HelpText = "the host server")]
        public string server { get; set; }

        [Option('u', "userid", Required = true, HelpText = "username")]
        public string userid { get; set; }

        [Option('p', "password", Required = true, HelpText = "password")]
        public string password { get; set; }

        [Option('v', "verbose", Required = false, Default = false, HelpText = "")]
        public bool verbose { get; set; }

        [Option('x',"xmlPath", Required = true, Default = false, HelpText = "path to xml source")]
        public string xmlPath { get; set; }
    }
}
