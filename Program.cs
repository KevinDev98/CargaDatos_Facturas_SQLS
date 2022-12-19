using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace CargaDatos_Facturas
{
    class Program
    {
        static void Main(string[] args)
        {
            DataTable ConvertCSVtoDataTable(string strFilePath)
            {
                DataTable dt = new DataTable();
                using (StreamReader DataReaderCSV = new StreamReader(strFilePath))
                {
                    string[] headers = DataReaderCSV.ReadLine().Split(',');
                    foreach (string header in headers)
                    {
                        dt.Columns.Add(header);
                    }
                    try
                    {
                        while (!DataReaderCSV.EndOfStream)
                        {
                            string[] rows = DataReaderCSV.ReadLine().Split(',');
                            DataRow dr = dt.NewRow();
                            string data;
                            for (int i = 0; i < headers.Length; i++)
                            {
                                data = rows[i];
                                if (rows[i].Contains(":") & rows[i].Contains("/")) //Elimina los espacios en blanco de las columnas que no son hora
                                {
                                    data = data.Replace(" 00:00", ""); //Elimina caracteres inecesarios
                                    rows[i] = data;
                                }
                                else
                                {
                                    data = data.Substring(0, data.Length - 1);//Elimina el espacio en blanco                                    
                                    rows[i] = data;
                                }
                                dr[i] = rows[i];
                            }
                            dt.Rows.Add(dr);
                        }
                    }
                    catch (Exception EX)
                    {
                        Console.WriteLine(EX.Message);
                    }
                }
                return dt;
            }
            void CleanDataTable(ref DataTable dt, DataTable csv)
            {
                int ColumnasHeader = dt.Columns.Count;
                int columnasCarga = csv.Columns.Count;

                // Validamos que las columnas estén iguales
                if (columnasCarga > ColumnasHeader)
                    Console.WriteLine("El encabezado es incorecto. Le faltan campos");
                else if (columnasCarga < ColumnasHeader)
                    Console.WriteLine("El encabezado es incorrecto. Tiene campos extra");
                // Renombramos las columnas para que coincidan con la tabla de carfa
                for (var i = 0; i <= ColumnasHeader - 1; i++)
                    csv.Columns[i].ColumnName = dt.Columns[i].ColumnName;

                // Eliminamos la primer fila que trae el encabezado del csv
                csv.Rows.RemoveAt(0);
            }
            string connSql = @"Server=localhost;Database=VENTAS_CIUDAD;User Id=sa;Password=inginf98;Connection Timeout=360";

            using (SqlConnection con = new SqlConnection(connSql))
            {
                DataTable dtTabla = new DataTable("VNT_FACTURACIONES");
                SqlDataAdapter da = new SqlDataAdapter();
                con.Open();
                var encabezado = true;

                string ruta = @"C:/Users/Kevin/Documents/KEVIN/PROYECTOS DEV/CARGA DATOS/DATA FILES/CARGA_CSV"; //LEER LA CARPETA CON TODOS LOS ARCHIVOS Y CARGARLA TODA.

                DirectoryInfo di = new DirectoryInfo(@ruta);

                FileInfo[] files = di.GetFiles();
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Abriendo Archivo");
                var lines = File.ReadAllLines(files[1].ToString(), Encoding.Default);
                DataTable DATACSV = ConvertCSVtoDataTable(files[1].ToString());
                String lineEncabezados = @"VNT_ID_FACTURA,CTL_CVE_FKCLIENTE,VNT_CVE_FKCIUDAD,CBR_ESTADO_FACTURA,VNT_MONTO_FACTURA,VNT_IVA_FACTURA,VNT_FECHA_FACTURA";
                DataTable dtencabezado = new DataTable();
                string[] splitencabezado = lineEncabezados.Split(',');
                foreach (string colums in splitencabezado)
                {
                    dtencabezado.Columns.Add(new DataColumn(colums, typeof(string)));
                }

                CleanDataTable(ref dtencabezado, DATACSV);

                SqlBulkCopy fakebulk = new SqlBulkCopy(con);
                fakebulk.BulkCopyTimeout = 600;
                fakebulk.BatchSize = 5000;

                fakebulk.DestinationTableName = "[VENTAS_CIUDAD].[dbo].[VNT_FACTURACIONES]";
                for (var i = 0; i <= DATACSV.Columns.Count - 1; i++) //Realiza mapeo de datos
                {
                    fakebulk.ColumnMappings.Add(DATACSV.Columns[i].ColumnName, DATACSV.Columns[i].ColumnName);
                }
                try
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Comenzando la carga ");
                    fakebulk.WriteToServer(DATACSV); //Carga de datos a Servidor SQL SERVER
                    Console.WriteLine("TERMINA LA CARGA");
                    Console.ReadLine();
                }
                catch (FileNotFoundException)
                {
                    Console.WriteLine("No se ha encontrado el archivo");
                }

                Console.ReadLine();

            }
        }
    }
}
