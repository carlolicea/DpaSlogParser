using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DpaSlogParser
{
    //
    //Program will be the starting point of this console application. It is where DataTables of the GeneralOperations class kick off for each project drive
    public class Program
    {
        static void Main(string[] args)
        {
            //Search 7 days back from the date of running to try to catch any project whose SLOG file was inaccessible 
            int daysBack = -7;
            //The output folder is where the CSV files will be stored
            string CsvOutputFolder = DpaSlogParser.Properties.Settings.Default.CsvOutputFolder;
            //For each office, a DataTable from the GeneralOperations class is created, taking in the parameters for the office and its project drive
            DataTable dpaCollection = GeneralOperations.PerformOperations(daysBack, "DPA", Properties.Settings.Default.DpaDrivePath, "BIM_Slog_Data", Properties.Settings.Default.CsvOutputFolder);            
        }        
    }

    //
    //GeneralOperations is where the actual work is done in the data collection
    public class GeneralOperations
    {
        //The core output of the GeneralOperations is a DataTable for use in writing to the SQL database and the CSV file
        public static DataTable PerformOperations(int daysBack, string officeLocation, string officeDrivePath, string officeDbTableName, string csvSaveDirectory)
        {
            //Get the current date and the date from the number of days back
            DateTime date = DateTime.Now;
            DateTime startDate = DateTime.Now.AddDays(daysBack);
            //Parse the date to use in formatting a date output
            string year = DateTime.Today.Year.ToString();
            string month = DateTime.Today.Month.ToString();
            string day = DateTime.Today.Day.ToString();

            //Prepare a new output log to record the events of the SLOG parsing
            CreateOutputLog LogFile = new CreateOutputLog(officeLocation,startDate,date,officeDrivePath);   
            //Give some feedback in the console by stating which office is being evaluated
            Console.WriteLine("Collecting " + officeLocation + " SLOG Files");
            //Collect the project SLOG files
            List<string> filesToCheck = GetAllRvtProjectSlogs(officeDrivePath, startDate, LogFile);
            //When the file collection is done, give feedback that the SLOG Data collection is proceeding
            Console.WriteLine("Collecting " + officeLocation + " SLOG Data");
            //Pass the list of files to parse to the FillDataTable method and let it parse the SLOG
            DataTable dataTable = FillDataTable(filesToCheck,LogFile.m_slogReadErrors);
            //Create a SQL connection to prepare to write to the database
            SqlConnection sqlConnection = DatabaseOperations.SqlOpenConnection(DatabaseOperations.adminDataSqlConnectionString);
            //Provide feedback that the data is being written to the database
            Console.WriteLine("Writing " + officeLocation + " SLOG Data to SQL Database");
            //Then, write to the database
            DatabaseOperations.SqlWriteDataTable(officeDbTableName, sqlConnection, dataTable, LogFile);
            //When the database is written to, save the DataTable out to a CSV file
            CreateCSVFromDataTable(dataTable, officeLocation + " SLOG FILES " + year + month + day, csvSaveDirectory);
            //Last, create the output log and return the DataTable
            LogFile.SetOutputLogData(officeLocation, startDate, date, officeDrivePath, LogFile.m_slogReadErrors, LogFile.m_newDbEntries, LogFile.m_existingDbEntries, LogFile.m_dbTableName, year, month, day);
            return dataTable;
        }

        //This will collect the SLOG files
        public static List<string> GetAllRvtProjectSlogs(string directoryPath, DateTime date, CreateOutputLog log)
        {
            //Create a new list and array of directories
            List<string> files = new List<string>();
            string[] directories = Directory.GetDirectories(directoryPath);
            //Cycle through the directories
            foreach (string directory in directories)
            {
                //Encapsulating this in a Try/Catch to find the files that could not be accessed and allow it to continue
                try
                {
                    //For each directory, cycle through the files in it that contain .slog in the path
                    List<string> filePaths = Directory.EnumerateFiles(directory, "*.slog", SearchOption.AllDirectories).ToList();
                    foreach (string file in filePaths)
                    {
                        //Then try cycling through those paths and find only the ones that reside in the E1 Revit folder
                        try
                        {
                            if (file.Contains("E1 Revit"))
                            {
                                //The attributes of the file need reset to eliminate any Read Only attributes
                                File.SetAttributes(file, FileAttributes.Normal);
                                //However, the file still needs the Archive attribute, so reset it
                                File.SetAttributes(file, FileAttributes.Archive);
                                //Get the FileInfo of the file and determine if the last write time is newer than the number of days back
                                FileInfo fileInfo = new FileInfo(file);
                                if (fileInfo.LastWriteTime >= date)
                                {
                                    files.Add(file);
                                }
                            }
                        }
                        catch(Exception e)
                        {
                            //If the file could not be accessed, report it in the console and add the exception to the log
                            Console.WriteLine(String.Format("{0} : Exception: {1}",file,e.Message));
                            log.m_slogReadErrors.Add(file);
                            log.m_slogReadErrors.Add("    Exception: " + e.Message);
                            continue;
                        }                        
                    }
                }
                catch { continue; }
            }
            //Return the SLOG files that could be accessed for parsing
            return files;
        }

        //This method is where the SLOG parsing begins
        public static DataTable FillDataTable(List<string> files, List<string> slogReadErrors)
        {
            //Creating a new DataTable with the following columns
            DataTable dt = new DataTable();
            DataColumn sessionIdColumn = dt.Columns.Add("SessionId", typeof(String));
            DataColumn projectNumberColumn = dt.Columns.Add("ProjectNumber", typeof(String));
            DataColumn fileNameColumn = dt.Columns.Add("FileName", typeof(String));
            DataColumn filePathColumn = dt.Columns.Add("FilePath", typeof(String));
            DataColumn userColumn = dt.Columns.Add("UserName", typeof(String));
            DataColumn buildColumn = dt.Columns.Add("Build", typeof(String));
            DataColumn journalColumn = dt.Columns.Add("Journal", typeof(String));
            DataColumn hostColumn = dt.Columns.Add("Host", typeof(String));
            DataColumn serverColumn = dt.Columns.Add("Server", typeof(String));
            DataColumn centralColumn = dt.Columns.Add("Central", typeof(String));
            DataColumn localColumn = dt.Columns.Add("Local", typeof(String));
            DataColumn startColumn = dt.Columns.Add("DateTimeStart", typeof(DateTime));
            DataColumn endColumn = dt.Columns.Add("DateTimeEnd", typeof(DateTime));
            DataColumn durationColumn = dt.Columns.Add("Duration", typeof(Decimal));

            //Cycle through the SLOG files
            foreach (string slogfile in files)
            {                
                //Create a boolean value to use in determining if the SLOG file should be skipped
                bool skip = false;
                string projectNumber = "";
                //The following are three cases where a project number could be found. The project number is needed for the data collection
                Match matchProjectNumber1 = Regex.Match(slogfile, @"[0-9][0-9][0-9][0-9][0-9][0-9].[0-9][0-9]", RegexOptions.IgnoreCase);
                Match matchProjectNumber2 = Regex.Match(slogfile, @"[0-9][0-9][0-9][0-9][0-9][0-9].\w\w", RegexOptions.IgnoreCase);
                Match matchProjectNumber3 = Regex.Match(slogfile, @"[0-9][0-9][0-9][0-9][0-9][0-9]", RegexOptions.IgnoreCase);

                if (matchProjectNumber1.Success)
                {
                    GroupCollection groups = Regex.Match(slogfile, @"[0-9][0-9][0-9][0-9][0-9][0-9].[0-9][0-9]", RegexOptions.IgnoreCase).Groups;
                    projectNumber = matchProjectNumber1.Value;
                }
                else if (matchProjectNumber2.Success)
                {
                    GroupCollection groups = Regex.Match(slogfile, @"[0-9][0-9][0-9][0-9][0-9][0-9].\w\w", RegexOptions.IgnoreCase).Groups;
                    projectNumber = matchProjectNumber2.Value;
                }
                else if (matchProjectNumber3.Success)
                {
                    GroupCollection groups = Regex.Match(slogfile, @"[0-9][0-9][0-9][0-9][0-9][0-9]", RegexOptions.IgnoreCase).Groups;
                    projectNumber = matchProjectNumber3.Value;
                }
                else
                {
                    skip = true;
                }

                //If the project number was found, continue
                if (skip == false)
                {
                    //Creating a dictionary of dictionaryies to store the info from the ParseSlogFile method
                    Dictionary<string, Dictionary<string, object>> values = ParseSlogFile(slogfile, slogReadErrors);

                    //Retrieve the data from the dictionaries where each primary dictionary key is the SessionId, and each sub dictionary's keys contain data about that session
                    foreach (string key in values.Keys)
                    {
                        DataRow row = dt.NewRow();
                        row["SessionId"] = key;
                        row["ProjectNumber"] = projectNumber;
                        row["FileName"] = Path.GetFileNameWithoutExtension(slogfile);
                        row["FilePath"] = slogfile;
                        row["UserName"] = values[key]["UserName"];
                        row["Build"] = values[key]["Build"];
                        row["Journal"] = values[key]["Journal"];
                        row["Host"] = values[key]["Host"];
                        row["Server"] = values[key]["Server"];
                        row["Central"] = values[key]["Central"];
                        row["Local"] = values[key]["Local"];
                        row["DateTimeStart"] = values[key]["DateTimeStart"];
                        row["DateTimeEnd"] = values[key]["DateTimeEnd"];
                        row["Duration"] = values[key]["Duration"];
                        dt.Rows.Add(row);
                    }
                }
            }
            return dt;
        }

        //This method is what parses a SLOG file and obtains a dictionary of dictionaries for use in filling out the DataTable
        public static Dictionary<string, Dictionary<string, object>> ParseSlogFile(string slogFile, List<String> slogReadErrors)
        {
            //The reason why a dictionary is being nested in a dictionary is so each set of data can be retrieved by an index associated with a session ID, which acts as a unique key in the DataTable
            Dictionary<string, Dictionary<string, object>> returnValues = new Dictionary<string, Dictionary<string, object>>();
           try
            {
                //First, create a StreamReader to read each line of the SLOG file and add the line to a list
                List<string> splitDataLines = new List<string>();
                StreamReader streamReader = new StreamReader(slogFile);
                string readLine = streamReader.ReadLine();
                while (readLine != null)
                {
                    splitDataLines.Add(readLine); ;
                    readLine = streamReader.ReadLine();
                }

                //Clean the list by making a new list of lines where the string is not null or a white space
                List<string> cleanSplitDataLines = splitDataLines.Where(l => !String.IsNullOrWhiteSpace(l)).ToList();
                //Now that the list of lines from the SLOG file is clean, step through it
                for (int x = 0; x < cleanSplitDataLines.Count; x++)
                {
                    //Sessions start with $, so find that first to identify a session entry
                    if (cleanSplitDataLines[x].StartsWith("$"))
                    {
                        //Return the session ID by splitting the line string at the first space
                        string keyValue = cleanSplitDataLines[x].Split(Convert.ToChar(" ")).First();
                        //If the dictionary of session IDs does not contain a key for the session ID, continue
                        if (!returnValues.Keys.Contains(keyValue))
                        {
                            //Make a new sub dictionary to assign to the index of the primary dictionary for the session ID key
                            Dictionary<string, object> subDictionary = new Dictionary<string, object>();

                            //From the line where the first entry of the SessionID was found, get the start date, which occurs at the second split where a space is found
                            string timeStartDate = cleanSplitDataLines[x].Split(Convert.ToChar(" "))[1];
                            //Likewise, get teh start time which occurs at the third split where a space is found
                            string timeStartTime = cleanSplitDataLines[x].Split(Convert.ToChar(" "))[2];
                            //Parse the date and time to use in creating a DateTime object
                            int year = Convert.ToInt32(timeStartDate.Split(Convert.ToChar("-"))[0]);
                            int month = Convert.ToInt32(timeStartDate.Split(Convert.ToChar("-"))[1]);
                            int day = Convert.ToInt32(timeStartDate.Split(Convert.ToChar("-"))[2]);
                            int hour = Convert.ToInt32(timeStartTime.Split(Convert.ToChar(":"))[0]);
                            int minute = Convert.ToInt32(timeStartTime.Split(Convert.ToChar(":"))[1]);
                            int second = Convert.ToInt32(timeStartTime.Split(Convert.ToChar(":"))[2].Split(Convert.ToChar("."))[0]);
                            int millisecond = Convert.ToInt32(timeStartTime.Split(Convert.ToChar(":"))[2].Split(Convert.ToChar("."))[1]);
                            //Make the start time from a DateTime object using the parsed date and time
                            DateTime timeStart = new DateTime(year, month, day, hour, minute, second, millisecond);
                            subDictionary["DateTimeStart"] = timeStart;
                            //Until the end time is found, add one hour to the start time
                            DateTime endTime = timeStart.AddHours(1);
                            subDictionary["DateTimeEnd"] = endTime;
                            //Set the duration to 0 minutes
                            subDictionary["Duration"] = 0d;

                            //Define some initial values for the data to collect
                            string user = "";
                            string build = "";
                            string journal = "";
                            string host = "";
                            string server = "";
                            string central = "";
                            string local = "";
                            bool deleteKey = false;

                            //After the first occurance of a session ID value, the next line should begin with "user"
                            Match matchUserName = Regex.Match(cleanSplitDataLines[x + 1], " user");
                            if (matchUserName.Success)
                            {
                                //If the line begins with user, get the username by replacing " and splitting the string at =, where the item after = will be the username
                                user = cleanSplitDataLines[x + 1].Replace("\"", "").Split(Convert.ToChar("="))[1];
                                subDictionary["UserName"] = user;
                            }//If the username cannot be retrieved, delete this key
                            else { deleteKey = true; }

                            //If the username was found, continue
                            if (deleteKey == false)
                            {
                                //After the username line should be the line for the Revit build. Use the same splitting operations to get the value for the build
                                Match matchBuild = Regex.Match(cleanSplitDataLines[x + 2], " build");
                                if (matchBuild.Success)
                                {
                                    build = cleanSplitDataLines[x + 2].Replace("\"", "").Split(Convert.ToChar("="))[1];
                                    subDictionary["Build"] = build;
                                }
                                else { subDictionary["Build"] = ""; }

                                //Following the line with the Revit build should be the location of the journal file
                                Match matchJournal = Regex.Match(cleanSplitDataLines[x + 3], " journal");
                                if (matchJournal.Success)
                                {
                                    journal = cleanSplitDataLines[x + 3].Replace("\"", "").Split(Convert.ToChar("="))[1];
                                    subDictionary["Journal"] = journal;
                                }
                                else { subDictionary["Journal"] = ""; }

                                //After the line with the journal file comes the line with the host server
                                Match matchHost = Regex.Match(cleanSplitDataLines[x + 4], " host");
                                if (matchHost.Success)
                                {
                                    host = cleanSplitDataLines[x + 4].Split(Convert.ToChar(" "))[2].Replace("\"", "");
                                    subDictionary["Host"] = host;
                                }
                                else { subDictionary["Host"] = ""; }

                                //Next is the line with the server name
                                Match matchServer = Regex.Match(cleanSplitDataLines[x + 5], " server");
                                if (matchServer.Success)
                                {
                                    server = cleanSplitDataLines[x + 5].Split(Convert.ToChar(" "))[2].Replace("\"", "");
                                    subDictionary["Server"] = server;
                                }
                                else { subDictionary["Server"] = ""; }

                                //Then is the line with the central file path
                                Match matchCentral = Regex.Match(cleanSplitDataLines[x + 6], " central");
                                if (matchCentral.Success)
                                {
                                    central = cleanSplitDataLines[x + 6].Replace("\"", "").Split(Convert.ToChar("="))[1];
                                    subDictionary["Central"] = central;
                                }
                                else { subDictionary["Central"] = ""; }

                                //Followed by the line with the local file path
                                Match matchLocal = Regex.Match(cleanSplitDataLines[x + 7], " local");
                                if (matchLocal.Success)
                                {
                                    local = cleanSplitDataLines[x + 6].Replace("\"", "").Split(Convert.ToChar("="))[1];
                                    subDictionary["Local"] = local;
                                }
                                else { subDictionary["Local"] = ""; }

                                //The returned primary dictionary key is the sub dictionary of data
                                returnValues[keyValue] = subDictionary;
                            }
                            else { returnValues.Remove(keyValue); }
                        }

                        //If the key for the session ID already exists, then there is no need to get the rest of the data except the last time for the session.
                        //This will rewrite the end time and duration until the session ID value is no longer found in the SLOG file, thus ending with the last write time of the session  
                        else
                        {
                            try
                            {
                                //The parse the date and time from the line so a DateTime end time can be constructed
                                string timeEndDate = cleanSplitDataLines[x].Split(Convert.ToChar(" "))[1];
                                string timeEndTime = cleanSplitDataLines[x].Split(Convert.ToChar(" "))[2];
                                int year = Convert.ToInt32(timeEndDate.Split(Convert.ToChar("-"))[0]);
                                int month = Convert.ToInt32(timeEndDate.Split(Convert.ToChar("-"))[1]);
                                int day = Convert.ToInt32(timeEndDate.Split(Convert.ToChar("-"))[2]);
                                int hour = Convert.ToInt32(timeEndTime.Split(Convert.ToChar(":"))[0]);
                                int minute = Convert.ToInt32(timeEndTime.Split(Convert.ToChar(":"))[1]);
                                int second = Convert.ToInt32(timeEndTime.Split(Convert.ToChar(":"))[2].Split(Convert.ToChar("."))[0]);
                                int millisecond = Convert.ToInt32(timeEndTime.Split(Convert.ToChar(":"))[2].Split(Convert.ToChar("."))[1]);
                                DateTime dateTimeEnd = new DateTime(year, month, day, hour, minute, second, millisecond);

                                //Grab the start time from the dictionary
                                DateTime dateTimeStart = Convert.ToDateTime(returnValues[keyValue]["DateTimeStart"]);
                                //To get the number of hours the session lasted, subtract the end time from the start time and then convert the total hours to a decimal value
                                TimeSpan duration = dateTimeEnd - dateTimeStart;
                                decimal durationHours = Convert.ToDecimal(duration.TotalHours);

                                //Add the values for the end time and duration to the sub dictionary keys for this session ID index
                                returnValues[keyValue]["DateTimeEnd"] = dateTimeEnd;
                                returnValues[keyValue]["Duration"] = durationHours;
                            }
                            catch
                            {
                                //If the end time could not be found, such as in the case of a Revit crash, set the end time to the start time plus 1 hour and the duration to 0 hours
                                DateTime startTime = (DateTime)(returnValues[keyValue]["DateTimeStart"]);
                                DateTime endTime = startTime.AddHours(1);
                                returnValues[keyValue]["DateTimeEnd"] = endTime;
                                returnValues[keyValue]["Duration"] = 0d;
                            }
                        }
                    }                   
                }
            }
            catch (Exception e)
            {
                //If the file could not be read or there was an issue reading the SLOG file, add it to the errors to include in the report
                Console.WriteLine(String.Format("{0} : {1}", Path.GetFileNameWithoutExtension(slogFile), e.Message));
                slogReadErrors.Add(Path.GetFileNameWithoutExtension(slogFile));
                slogReadErrors.Add("    Exception: " + e.Message);

            }                      
            return returnValues;
        }

        //This StringBuilder is used to convert the DataTable to a string for CSV output
        public static StringBuilder BuildCSVStringFromDataTable(DataTable dt)
        {
            //Make a new StringBuilder object
            StringBuilder output = new StringBuilder();
            foreach (DataColumn column in dt.Columns)
            {
                //Parse the data column names, separated by commas for the first line of the CSV
                var item = column.ColumnName;
                output.AppendFormat(string.Concat("\"", item.ToString(), "\"", ","));
            }
            output.AppendLine();

            //Then cycle through the rows and add each of their cell values to the string separated by commas
            foreach (DataRow rows in dt.Rows)
            {
                foreach (DataColumn col in dt.Columns)
                {
                    var item = rows[col];
                    output.AppendFormat(string.Concat("\"", item.ToString(), "\"", ","));
                }
                output.AppendLine();
            }
            return output;
        }

        //This method uses the StringBuilder to build a string from a single DataTable row, which is used later in writing to the SQL database
        public static StringBuilder BuildCSVStringFromDataTableRow(DataTable dt, DataRow row)
        {
            StringBuilder output = new StringBuilder();
            foreach (DataColumn column in dt.Columns)
            {
                var item = row[column].ToString();
                output.AppendFormat(string.Concat("\"", item.ToString(), "\"", ","));

            }
            output.AppendLine();            
            return output;
        }

        //This method makes a CSV file from a DataTable by writing to a file the string obtained from the BuildCSVFromDataTable StringBuilder
        public static void CreateCSVFromDataTable(DataTable dt, string exportName, string exportDirectory)
        {
            string exportPath = exportDirectory + @"\" + exportName + ".csv";
            if (File.Exists(exportPath))
            {
                File.Delete(exportPath);
            }
            StringBuilder sb = GeneralOperations.BuildCSVStringFromDataTable(dt);
            File.WriteAllText(exportPath, sb.ToString());
        }                
    }

    //This class is where the operations pertaining to the SQL database are performed
    public static class DatabaseOperations
    {
        //The following are just for creating the connection string
        private static readonly string integratedSecurity = "False";
        private static readonly string userId = DpaSlogParser.Properties.Settings.Default.SQLServerUser;
        private static readonly string password = DpaSlogParser.Properties.Settings.Default.SQLServerPwd;
        private static readonly string connectTimeout = "3";
        private static readonly string encrypt = "False";
        private static readonly string trustServerCertificate = "True";
        private static readonly string applicationIntent = "ReadWrite";
        private static readonly string multiSubnetFailover = "False";
        private static readonly string dbServer = DpaSlogParser.Properties.Settings.Default.SQLServerName;
        private static readonly string database = DpaSlogParser.Properties.Settings.Default.DpaBimDbName;
        public static string adminDataSqlConnectionString = "Server=" + dbServer +
                                ";Database=" + database +
                                ";Integrated Security=" + integratedSecurity +
                                ";User Id=" + userId +
                                ";Password=" + password +
                                ";Connect Timeout=" + connectTimeout +
                                ";Encrypt=" + encrypt +
                                ";TrustServerCertificate=" + trustServerCertificate +
                                ";ApplicationIntent=" + applicationIntent +
                                ";MultiSubnetFailover=" + multiSubnetFailover;

        //Open the connection to the SQL server
        public static SqlConnection SqlOpenConnection(string connectionString)
        {
            SqlConnection sqlConnection;
            //Try to open the connection
            sqlConnection = new SqlConnection(connectionString);
            try
            {
                sqlConnection.Open();
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return sqlConnection;
        }
        
        //Close the connection to the SQL server
        public static void SqlCloseConnection(SqlConnection sqlConnection)
        {
            try
            {
                sqlConnection.Close();
            }
            catch
            {
                MessageBox.Show("Could not close SQL connection");
            }
        }        
        
        //Write a DataTable to the SQL server
        public static void SqlWriteDataTable(string tableName, SqlConnection sqlConnection, DataTable dataTable, CreateOutputLog log)
        {
            //Collect the list of tables from the SQL server
            DataTable dt = sqlConnection.GetSchema("Tables");
            List<string> existingTables = new List<string>();
            foreach (DataRow row in dt.Rows)
            {
                string existingTableName = (string)row[2];
                existingTables.Add(existingTableName);
            }

            //Check if the SQL server already has a table with the same name
            if (existingTables.Contains(tableName))
            {
                //Record in the log the database table name
                log.m_dbTableName = tableName;
                using (sqlConnection)
                {
                    //Report in the console that the data is going to be copied
                    Console.WriteLine("Copying Data To SQL Table");
                    //Cycle through each row in the DataTable
                    foreach (DataRow row in dataTable.Rows)
                    {                        
                        bool skip = false;
                        //Determing if the table already has a row with the SessionId value. If so, add it to the log as an existing entry and set the skip boolean to true because we don't want duplicate entries 
                        using (SqlCommand command = new SqlCommand("SELECT COUNT (*) FROM " + tableName +" WHERE SessionId LIKE '"+ row["SessionId"]+"'", sqlConnection))
                        {
                            try
                            {
                                Int32 count = Convert.ToInt32(command.ExecuteScalar());
                                if (count > 0)
                                {
                                    skip = true;
                                    StringBuilder sb = GeneralOperations.BuildCSVStringFromDataTableRow(dataTable, row);
                                    log.m_existingDbEntries.Add(sb.ToString());
                                }
                            }
                            catch{ continue; }                            
                        }

                        //Assuming this is a new table entry, continue
                        if (skip == false)
                        {
                            //Specify an entry into the database table by adding the values from the DataTable 
                            using (SqlCommand comm = new SqlCommand("INSERT INTO " + tableName + " (SessionId, ProjectNumber, FileName, FilePath, UserName, Build, Journal, Host, Server, Central, Local, DateTimeStart, DateTimeEnd, Duration) VALUES (@v1, @v2, @v3, @v4, @v5, @v6, @v7, @v8, @v9, @v10, @v11, @v12, @v13, @v14)"))
                            {
                                comm.Connection = sqlConnection;
                                comm.Parameters.AddWithValue("@v1", row["SessionId"]);
                                comm.Parameters.AddWithValue("@v2", row["ProjectNumber"]);
                                comm.Parameters.AddWithValue("@v3", row["FileName"]);
                                comm.Parameters.AddWithValue("@v4", row["FilePath"]);
                                comm.Parameters.AddWithValue("@v5", row["UserName"]);
                                comm.Parameters.AddWithValue("@v6", row["Build"]);
                                comm.Parameters.AddWithValue("@v7", row["Journal"]);
                                comm.Parameters.AddWithValue("@v8", row["Host"]);
                                comm.Parameters.AddWithValue("@v9", row["Server"]);
                                comm.Parameters.AddWithValue("@v10", row["Central"]);
                                comm.Parameters.AddWithValue("@v11", row["Local"]);
                                comm.Parameters.AddWithValue("@v12", row["DateTimeStart"]);
                                comm.Parameters.AddWithValue("@v13", row["DateTimeEnd"]);
                                comm.Parameters.AddWithValue("@v14", row["Duration"]);
                                try
                                {
                                    //Execute the command to insert the row into the database table and add the row to the log for new entries
                                    comm.ExecuteNonQuery();
                                    StringBuilder sb = GeneralOperations.BuildCSVStringFromDataTableRow(dataTable, row);
                                    log.m_newDbEntries.Add(sb.ToString());
                                }
                                catch (SqlException e)
                                {
                                    //If something should fail, write it to the console and pause until acknowledged
                                    Console.WriteLine(e.Message);
                                    Console.ReadLine();
                                }
                            }
                        }                        
                    }
                }
                SqlCloseConnection(sqlConnection);
            }
            //If the database table does not yet exist, create a new table using the SqlBulkCopy
            else
            {
                log.m_dbTableName = tableName;
                Console.WriteLine("Creating New SQL Table");
                try
                {
                    //Create a new SQL table with the specified headers and data types
                    SqlCommand sqlCreateTable = new SqlCommand("CREATE TABLE " + tableName + " (SessionId text, ProjectNumber text, FileName text, FilePath text, UserName text, Build text, Journal text, Host text, Server text, Central text, Local text, DateTimeStart datetime, DateTimeEnd datetime, Duration float)", sqlConnection);
                    sqlCreateTable.ExecuteNonQuery();
                }
                catch(SqlException f)
                {
                    //If something should fail, write it to the console and pause until acknowledged
                    Console.WriteLine(f.Message);
                    Console.ReadLine();
                }

                //Try to simply do a bulk copy to the table
                try
                {
                    SqlBulkCopyOptions options = SqlBulkCopyOptions.Default;

                    Console.WriteLine("Copying Data To SQL Table");
                    using (SqlBulkCopy s = new SqlBulkCopy(sqlConnection, options, null))
                    {
                        //Specify the destination table and map each column from the DataTable to the database table columns
                        s.DestinationTableName = "[" + tableName + "]";
                        foreach (DataColumn appColumn in dataTable.Columns)
                        {
                            s.ColumnMappings.Add(appColumn.ToString(), appColumn.ToString());
                        }
                        s.WriteToServer(dataTable);
                    }
                    //Build a string of the new table and write it to the log as new data entries
                    StringBuilder sb = GeneralOperations.BuildCSVStringFromDataTable(dt);
                    log.m_newDbEntries.Add(sb.ToString());
                }
                catch(SqlException g)
                {
                    //If something should fail, write it to the console and pause until acknowledged
                    Console.WriteLine(g.Message);
                    Console.ReadLine();
                }  
            }
            //Close the connection and write the log of the user actions to the SQL table for tracking data writes
            DatabaseOperations.SqlCloseConnection(sqlConnection);
            SqlLogWriter(tableName);
        }

        //Use the table name to record who wrote to the SQL database
        public static void SqlLogWriter(string writtenTableName)
        {
            try
            {
                //Open the SQL connection and get the existing tables
                SqlConnection sqlConnection = DatabaseOperations.SqlOpenConnection(DatabaseOperations.adminDataSqlConnectionString);
                DataTable dt = sqlConnection.GetSchema("Tables");

                List<string> existingTables = new List<string>();
                foreach (DataRow row in dt.Rows)
                {
                    string tableName = (string)row[2];
                    existingTables.Add(tableName);
                }

                //If the table exists, add the username, table, and DateTime to the table
                if (existingTables.Contains("BARevitTools_SQLWriterLog"))
                {
                    string commandString = "INSERT INTO [BARevitTools_SQLWriterLog] (UserName, TableName, WriteDate) VALUES (@userName, @tableName, @dateTime)";
                    using (SqlCommand sqlInsert = new SqlCommand(commandString, sqlConnection))
                    {
                        sqlInsert.Parameters.AddWithValue("@userName", Environment.UserName);
                        sqlInsert.Parameters.AddWithValue("@tableName", writtenTableName);
                        sqlInsert.Parameters.AddWithValue("@dateTime", DateTime.Now);
                        sqlInsert.ExecuteNonQuery();
                    }
                }
                //Else, make a new table and add the values
                else
                {
                    SqlCommand sqlCreateTable = new SqlCommand("CREATE TABLE BARevitTools_SQLWriterLog (UserName varchar(255), TableName varchar(255), WriteDate datetime)", sqlConnection);
                    sqlCreateTable.ExecuteNonQuery();
                    string commandString = "INSERT INTO [BARevitTools_SQLWriterLog] (UserName, TableName, WriteDate) VALUES (@userName, @tableName, @dateTime)";
                    using (SqlCommand sqlInsert = new SqlCommand(commandString, sqlConnection))
                    {
                        sqlInsert.Parameters.AddWithValue("@userName", Environment.UserName);
                        sqlInsert.Parameters.AddWithValue("@tableName", writtenTableName);
                        sqlInsert.Parameters.AddWithValue("@dateTime", DateTime.Now);
                        sqlInsert.ExecuteNonQuery();
                    }
                }
                //Close the connection
                DatabaseOperations.SqlCloseConnection(sqlConnection);
            }
            catch(Exception e) {Console.WriteLine(e.Message); }
        }
    }

    //This class is responsible for making instances of itself to record the data from the SLOG parsing and database operations
    public class CreateOutputLog
    {
        //Create the objects for storing information
        public List<string> m_slogReadErrors = new List<string>();
        public List<string> m_newDbEntries = new List<string>();
        public List<string> m_existingDbEntries = new List<string>();
        public string m_dbTableName;
        public string m_officeLocation;
        public DateTime m_startDateRange;
        public DateTime m_endDateRange;
        public string m_officeDrive;

        //This constructor makes the instance of the log file when the class is called
        public CreateOutputLog(string officeLocation, DateTime startDateRange, DateTime endDateRange, string officeDrive)
        {
            m_officeLocation = officeLocation;
            m_startDateRange = startDateRange;
            m_endDateRange = endDateRange;
            m_officeDrive = officeDrive;
        }
        //Set the data for the log
        public void SetOutputLogData(string officeLocation, DateTime startDateRange, DateTime endDateRange, string officeDrive, List<string> slogReadErrors, List<string> newDbEntries, List<string> existingDbEntries, string dbTableName, string year, string month, string day)
        {
            //Initiate a new StringBuilder
            StringBuilder sb = new StringBuilder();
            //Add to the StringBuilder the information regarding the office, date range searched, and drive searched
            sb.Append(String.Format("Office: {0}", officeLocation));
            sb.Append(Environment.NewLine);
            sb.Append(String.Format("Date Range: {0} - {1}", startDateRange.ToString(), endDateRange.ToString()));
            sb.Append(Environment.NewLine);
            sb.Append(String.Format("Drive Searched: {0}", officeDrive));
            sb.Append(Environment.NewLine);
            sb.Append(Environment.NewLine);

            //Next, add the errors encountered
            sb.Append("The Following Errors Occurred:");
            sb.Append(Environment.NewLine);
            foreach (string line in slogReadErrors)
            {
                sb.Append(line);
                sb.Append(Environment.NewLine);
            }
            sb.Append(Environment.NewLine);
            sb.Append(Environment.NewLine);

            //Add the new database entries
            sb.Append(String.Format("The Following Entries Were Added To {0}:", dbTableName));
            sb.Append(Environment.NewLine);
            foreach (string line in newDbEntries)
            {
                sb.Append(line);                
            }
            sb.Append(Environment.NewLine);
            sb.Append(Environment.NewLine);

            //Add the existing database entries
            sb.Append(String.Format("The Following Entries Already Existed And Were Skipped In {0}:", dbTableName));
            sb.Append(Environment.NewLine);
            foreach (string line in existingDbEntries)
            {
                sb.Append(line);
            }
            sb.AppendLine();

            //Write the log to a file
            System.IO.StreamWriter file = new StreamWriter(DpaSlogParser.Properties.Settings.Default.CsvOutputFolder + "\\" + officeLocation + " Parser Log " + year + month + day + ".txt");
            file.WriteLine(sb.ToString());
        }
    }
}
