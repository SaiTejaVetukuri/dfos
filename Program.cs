using Microsoft.IdentityModel.Clients.ActiveDirectory;
using MongoDB.Bson;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace AutoCausalL24Generic
{
    class Program
    {
        public static string StartTime;
        public static string EndTime;
        public static DateTime time;
        public static int yesterday;
        public static string shiftName;
        public static string reportingDate;

        static void Main(string[] args)
        {
            var utctime = DateTime.UtcNow;
            string region = "Europe";
            string factory = "Caivano";
            string line = "Line24";
            string bnMachine = "BigDrumL24";

            List<string> machines = new List<string>();

            time = DateTime.Now;
            yesterday = time.Day - 1;
            StartTime = "2019-07-16 04:00:00";
            EndTime = "2019-07-16 12:00:00";
            shiftName = "1";
            reportingDate = "2019-7-16";

            List<string> nodename = new List<string>();

            var ActualData = GetDataFromTSIAsync(region, factory, line, bnMachine, machines, StartTime, EndTime, shiftName, reportingDate, nodename).Result;
        }

        public static async Task<string> GetDataFromTSIAsync(string clusterName, string factory, string line, string bnMachine, List<string> machine, string dateFrom, string dateTo, string shiftName, string reportingDate, List<string> nodename = null)
        {
            var json = "";
            try
            {
                string rd = reportingDate;
                string sn = shiftName;

                DateTime _dateFrom = new DateTime();
                DateTime _dateTo = new DateTime();
                StringBuilder sbMachines = new StringBuilder();
                StringBuilder sbnodename = new StringBuilder();

                string AppClientID = "92f1afe6-7646-43f5-a1e8-b2c0ae7a1a30";
                string ClentSecretID = "MzE2Y2NmOTYtNjlhZi00ZWJlLThjY2QtY2I0MGFiOWY3MmI2=";
                string TenantId = "unilever.onmicrosoft.com";
                string environmentFqdn = "88ed9ca6-ed21-4bc2-83e9-32eab8b9daf5.env.timeseries.azure.com";
                //string ApiVersion = "api-version=2016-12-12";

                if (!string.IsNullOrEmpty(dateFrom))
                {
                    _dateFrom = Convert.ToDateTime(dateFrom);
                }

                if (!string.IsNullOrEmpty(dateTo))
                {
                    _dateTo = Convert.ToDateTime(dateTo);
                }

                string from = _dateFrom.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
                string to = _dateTo.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

                string accessToken = await AcquireAccessTokenAsync(AppClientID, ClentSecretID, TenantId);

                JObject contentInputPayloadEvents = new JObject();

                contentInputPayloadEvents = new JObject(new JProperty("take", /*10000*/10000), new JProperty("searchSpan", new JObject(new JProperty("from", from), new JProperty("to", to))), new JProperty("predicate", new JObject(new JProperty("predicateString", "clustername= '" + clusterName + "' AND factoryname = '" + factory + "' AND linename = '" + line + "'   AND  startwindow <> null  AND state <> null "))));

                HttpWebRequest request = CreateHttpsWebRequest(environmentFqdn, "POST", "events", accessToken, new[] { "timeout=PT20S" });

                await WriteRequestStreamAsync(request, contentInputPayloadEvents);
                var eventsResponse = await GetResponseAsync(request);

                var jsonData = GetEvents(eventsResponse);

                json = jsonData;

                JArray jsonArray = JArray.Parse(json);

                jsonArray = new JArray(jsonArray.OrderBy(obj => (DateTime)obj["startwindow"]));

                //Taking machine names from Timeseries Json data

                List<string> machinesInSeq = new List<string>();
                for (int m = 0; m < jsonArray.Count; m++)
                {
                    dynamic machineData = JObject.Parse(jsonArray[m].ToString());
                    string machineName = machineData.machineid;
                    if ((machinesInSeq.Contains(machineName) == false))
                    {
                        machinesInSeq.Add(machineName);
                    }
                }

                List<JArray> machineArray = new List<JArray>(machinesInSeq.Count);

                //Declaring the dynamic arrays to store the particular machine events
                for (int dL = 0; dL < machinesInSeq.Count; dL++)
                {
                    machineArray.Add(new JArray(dL));
                    machineArray[dL].RemoveAt(0);
                }

                // storing the machine events in particular arrays            

                for (int i = 0; i < machinesInSeq.Count; i++)
                {
                    for (int j = 0; j < jsonArray.Count; j++)
                    {
                        dynamic data = JObject.Parse(jsonArray[j].ToString());
                        string startWindow = data.startwindow;
                        string endWindow = data.endwindow;

                        if (data.machineid == machinesInSeq[i] && startWindow != endWindow)
                        {
                            machineArray[i].Add(data);
                        }
                    }
                }

                for (int i = 0; i < machineArray.Count; i++)
                {
                    machineArray[i] = new JArray(machineArray[i].OrderBy(obj => (DateTime)obj["startwindow"]));
                }

                // combining the states and storing in particular arrays
                for (int i = 0; i < machineArray.Count; i++)
                {
                    int resaltauntCount = 0;
                    string resaltauntStartWindow = "";
                    string resaltauntState = "";

                    JArray machineData = new JArray();

                    for (int j = 0; j < machineArray[i].Count; j++)
                    {
                        dynamic data1 = JObject.Parse(machineArray[i][j].ToString());
                        machineData.Add(data1);
                    }

                    machineArray[i].RemoveAll();

                    for (int k = 0; k < machineData.Count; k++)
                    {
                        dynamic data = JObject.Parse(machineData[k].ToString());
                        string currentState = data.state;

                        if (resaltauntState == currentState)
                        {
                            data.startwindow = resaltauntStartWindow;
                            machineArray[i].RemoveAt(resaltauntCount - 1);
                            machineArray[i].Insert(resaltauntCount - 1, data);
                        }
                        else
                        {
                            resaltauntCount++;
                            machineArray[i].Add(data);
                        }
                        resaltauntState = data.state;
                        resaltauntStartWindow = data.startwindow;
                    }
                    Console.WriteLine(machineArray[i].Count);
                }

                for (int i = 0; i < machineArray.Count; i++)
                {
                    machineArray[i] = new JArray(machineArray[i].OrderBy(obj => (DateTime)obj["startwindow"]));
                }

                JArray bottleneckMachines = new JArray();
                JArray ResultantMachines = new JArray();
                JArray machineDetails = new JArray();
                int count = 0;
                string previousEndTime = "";
                int previousSeq = 0;

                for (int i = 0; i < machinesInSeq.Count; i++)
                {
                    JArray machineData = new JArray();
                    for (int j = 0; j < machineArray[i].Count; j++)
                    {
                        JObject data = JObject.Parse(machineArray[i][j].ToString());
                        machineData.Add(data);
                    }

                    machineArray[i].RemoveAll();

                    for (int k = 0; k < machineData.Count; k++)
                    {
                        dynamic data = JObject.Parse(machineData[k].ToString());

                        DateTime bottleneckmachinestart = Convert.ToDateTime(data.startwindow);
                        var epochbottleneckmachinestart = (bottleneckmachinestart - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
                        DateTime bottleneckmachineend = Convert.ToDateTime(data.endwindow);
                        var epochbottleneckmachineend = (bottleneckmachineend - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
                        var epochdiff = epochbottleneckmachineend - epochbottleneckmachinestart;

                        if (data.machineid == bnMachine && data.state != "RUN")
                        {
                            count++;

                            data.ShiftName = sn;
                            data.ReportingDate = rd;
                            data.Sequence = count;
                            if (data.startwindow == previousEndTime)
                            {
                                data.Sequence = previousSeq;
                                count--;
                            }
                            previousSeq = data.Sequence;
                            previousEndTime = data.endwindow;
                            bottleneckMachines.Add(data);
                            machineDetails.Add(data);
                        }
                        else if (data.machineid != bnMachine)
                        {
                            if (data.machineid == "Case_ErectorL23" && epochdiff > 15)
                            {
                                ResultantMachines.Add(data);
                                machineDetails.Add(data);
                            }
                            else if (data.machineid != "Case_ErectorL23")
                            {
                                ResultantMachines.Add(data);
                                machineDetails.Add(data);
                            }
                        }
                    }
                }

                bottleneckMachines = new JArray(bottleneckMachines.OrderBy(obj => (DateTime)obj["startwindow"]));
                ResultantMachines = new JArray(ResultantMachines.OrderBy(obj => (DateTime)obj["startwindow"]));

                for (int i = 0; i < machineDetails.Count; i++)
                {
                    dynamic data = JObject.Parse(machineDetails[i].ToString());
                    data.Sequence = 0;
                    machineDetails.RemoveAt(i);
                    machineDetails.Insert(i, data);
                }

                machineDetails = new JArray(machineDetails.OrderBy(obj => (DateTime)obj["startwindow"]));

                int previousSequence = 0;

                for (int i = 0; i < bottleneckMachines.Count; i++)
                {
                    JArray NewResultantMachines = new JArray();
                    var epochresultantmachinestart = 0.00000000;
                    var epochresultantmachineend = 0.0000000000;

                    dynamic data = JObject.Parse(bottleneckMachines[i].ToString());
                    DateTime bottleneckmachinestart = Convert.ToDateTime(data.startwindow);
                    var epochbottleneckmachinestart = (bottleneckmachinestart - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
                    DateTime bottleneckmachineend = Convert.ToDateTime(data.endwindow);
                    var epochbottleneckmachineend = (bottleneckmachineend - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
                    int seq = data.Sequence;

                    if (seq != previousSequence)
                    {
                        /*taking events between the 30 mins flowtime*/
                        for (int j = 0; j < ResultantMachines.Count; j++)
                        {
                            data = JObject.Parse(ResultantMachines[j].ToString());
                            DateTime resultantmachinestart = Convert.ToDateTime(data.startwindow);
                            epochresultantmachinestart = (resultantmachinestart - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
                            DateTime resultantmachineend = Convert.ToDateTime(data.endwindow);
                            epochresultantmachineend = (resultantmachineend - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;

                            if ((epochresultantmachinestart >= epochbottleneckmachinestart - 1800) && (epochresultantmachineend <= epochbottleneckmachinestart + 1800))
                            {
                                data.Sequence = seq;
                                data.ShiftName = sn;
                                data.ReportingDate = rd;
                                NewResultantMachines.Add(data);
                            }
                        }
                        NewResultantMachines.Add(bottleneckMachines[i]);

                        NewResultantMachines = new JArray(NewResultantMachines.OrderBy(obj => (DateTime)obj["startwindow"]));

                        /*taking inbetween events based on the bottleneck machine start and end times*/
                        for (int j = 0; j < machinesInSeq.Count; j++)
                        {
                            for (int m = 0; m < NewResultantMachines.Count; m++)
                            {
                                dynamic machineData1 = JObject.Parse(NewResultantMachines[m].ToString());

                                DateTime resultantmachinestart = Convert.ToDateTime(machineData1.startwindow);
                                epochresultantmachinestart = (resultantmachinestart - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
                                DateTime resultantmachineend = Convert.ToDateTime(machineData1.endwindow);
                                epochresultantmachineend = (resultantmachineend - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;

                                for (int k = 0; k < machinesInSeq.Count; k++)
                                {
                                    if (machineData1.machineid == machinesInSeq[j] && machinesInSeq[j] == machinesInSeq[k])
                                    {
                                        if (((epochresultantmachinestart >= epochbottleneckmachinestart) && (epochresultantmachineend <= epochbottleneckmachineend)) || ((epochresultantmachinestart < epochbottleneckmachinestart) && (epochresultantmachineend >= epochbottleneckmachinestart) && (epochresultantmachineend < epochbottleneckmachineend)) || ((epochresultantmachinestart > epochbottleneckmachinestart) && (epochresultantmachinestart < epochbottleneckmachineend) && (epochresultantmachineend >= epochbottleneckmachineend)) || ((epochresultantmachinestart == epochbottleneckmachinestart) && (epochresultantmachineend <= epochbottleneckmachineend) && (epochresultantmachinestart > epochbottleneckmachinestart) && (epochresultantmachineend <= epochbottleneckmachineend)) || ((epochresultantmachinestart <= epochbottleneckmachinestart) && (epochresultantmachineend >= epochbottleneckmachineend)))
                                        {
                                            machineArray[k].Add(machineData1);
                                        }
                                    }
                                }
                            }
                            Console.WriteLine("initial " + "{0}={1}", machinesInSeq[j], machineArray[j].Count);
                            machineArray[j] = new JArray(machineArray[j].OrderBy(obj => (DateTime)obj["startwindow"]));
                        }

                        /*moving to and fro based on the inbetween firstevent index number */
                        for (int l = 0; l < machinesInSeq.Count; l++)
                        {
                            if (machineArray[l].Count > 0 && machinesInSeq[l] != bnMachine)
                            {
                                dynamic resultantData = JObject.Parse(machineArray[l][0].ToString());
                                string resultantDataMachineid = resultantData.machineid;
                                string resultantDataStartWindow = resultantData.startwindow;
                                string resultantDataState = resultantData.state;

                                int indexOfNew_Case_Erector = 0;

                                machineArray[l].RemoveAll();

                                machineArray[l].Add(resultantData);

                                for (int v = 0; v < NewResultantMachines.Count; v++)
                                {
                                    dynamic dataV = JObject.Parse(NewResultantMachines[v].ToString());
                                    string dataVstartwindow = dataV.startwindow;
                                    string dataVmachineid = dataV.machineid;

                                    if (resultantDataStartWindow == dataVstartwindow && dataVmachineid == machinesInSeq[l])
                                    {
                                        indexOfNew_Case_Erector = v;
                                        string dataVv = NewResultantMachines[indexOfNew_Case_Erector].ToString();
                                        break;
                                    }
                                }

                                int index = indexOfNew_Case_Erector;
                                if (machinesInSeq[l] == "Case_ErectorL23")
                                {
                                    while (indexOfNew_Case_Erector - 1 > 0 && resultantDataState != "RUN")
                                    {
                                        dynamic dataV = JObject.Parse(NewResultantMachines[indexOfNew_Case_Erector - 1].ToString());
                                        string dataVstartwindow = dataV.startwindow;
                                        string dataVmachineid = dataV.machineid;
                                        string dataVstate = dataV.state;
                                        if (dataVmachineid == machinesInSeq[l] && dataVstate != "RUN")
                                        {
                                            machineArray[l].Add(dataV);
                                        }
                                        else if (dataVmachineid == machinesInSeq[l] && dataVstate == "RUN")
                                        {
                                            break;
                                        }
                                        indexOfNew_Case_Erector--;
                                    }
                                }

                                else if (machinesInSeq[l] != "Case_ErectorL23")
                                {
                                    while (indexOfNew_Case_Erector - 1 > 0)
                                    {
                                        dynamic dataV = JObject.Parse(NewResultantMachines[indexOfNew_Case_Erector - 1].ToString());
                                        string dataVstartwindow = dataV.startwindow;
                                        string dataVmachineid = dataV.machineid;
                                        string dataVstate = dataV.state;

                                        if (dataVmachineid == machinesInSeq[l] && dataVstate != "RUN")
                                        {
                                            machineArray[l].Add(dataV);
                                        }
                                        else if (dataVmachineid == machinesInSeq[l] && dataVstate == "RUN")
                                        {
                                            break;
                                        }

                                        indexOfNew_Case_Erector--;
                                    }
                                }

                                while (index + 1 < NewResultantMachines.Count && resultantDataState != "RUN")
                                {
                                    dynamic dataV = JObject.Parse(NewResultantMachines[index + 1].ToString());
                                    string dataVstartwindow = dataV.startwindow;
                                    string dataVmachineid = dataV.machineid;
                                    string dataVstate = dataV.state;
                                    if (dataVmachineid == machinesInSeq[l] && dataVstate != "RUN")
                                    {
                                        machineArray[l].Add(dataV);
                                    }
                                    else if (dataVmachineid == machinesInSeq[l] && dataVstate == "RUN")
                                    {
                                        break;
                                    }

                                    index++;
                                }
                            }
                            Console.WriteLine("after " + "{0}={1}", machinesInSeq[l], machineArray[l].Count);
                        }
                        Console.WriteLine("-------------------------------------------------------------");

                        JArray causalMachinesArray = new JArray();

                        for (int m = 0; m < machineArray.Count; m++)
                        {
                            for (int k = 0; k < machineArray[m].Count; k++)
                            {
                                JObject data1 = JObject.Parse(machineArray[m][k].ToString());
                                causalMachinesArray.Add(data1);
                            }
                            machineArray[m].RemoveAll();
                        }

                        causalMachinesArray = new JArray(causalMachinesArray.OrderBy(obj => (DateTime)obj["startwindow"]));

                        JArray finalCausalArray1 = new JArray();

                        List<int> indexOfRepeatedSeq = new List<int>();

                        for (int m = 0; m < causalMachinesArray.Count; m++)
                        {
                            data = JObject.Parse(causalMachinesArray[m].ToString());
                            string startDate = data.startwindow;
                            string endDate = data.endwindow;
                            string machineName = data.machineid;
                            string causalSequence = data.Sequence;
                            for (int l = 0; l < machineDetails.Count; l++)
                            {
                                dynamic machineDetailsData = JObject.Parse(machineDetails[l].ToString());
                                string machineStartDate = machineDetailsData.startwindow;
                                string machineEndDate = machineDetailsData.endwindow;
                                string machineMachineName = machineDetailsData.machineid;
                                if (startDate == machineStartDate && endDate == machineEndDate && machineName == machineMachineName)
                                {
                                    if (machineDetailsData.Sequence == 0)
                                    {
                                        machineDetailsData.Sequence = causalSequence;
                                        machineDetails.RemoveAt(l);
                                        machineDetails.Insert(l, machineDetailsData);
                                    }
                                    else
                                    {
                                        indexOfRepeatedSeq.Add(m);
                                    }
                                }
                            }
                        }
                        int countVal = 0;
                        for (int l = 0; l < indexOfRepeatedSeq.Count; l++)
                        {
                            int indexValue = indexOfRepeatedSeq[l] - countVal;
                            causalMachinesArray.RemoveAt(indexValue);
                            countVal++;
                        }

                        for (int l = 0; l < causalMachinesArray.Count; l++)
                        {
                            dynamic data1 = JObject.Parse(causalMachinesArray[l].ToString());
                            string causalManchineState = data1.state;
                            if (causalManchineState != "RUN")
                            {
                                finalCausalArray1.Add(data1);
                            }
                        }

                        JArray finalCausalArray = new JArray();

                        foreach (JObject jObject in finalCausalArray1)
                        {
                            //Verify if the ID column value exist in the uniqueArray
                            JObject rowObject = finalCausalArray.Children<JObject>().FirstOrDefault(o => o["startwindow"].ToString() == jObject.Property("startwindow").Value.ToString() && o["machineid"].ToString() == jObject.Property("machineid").Value.ToString());

                            //rowObject will be null if these is no match for the value in ID column
                            if (rowObject == null)
                            {
                                finalCausalArray.Add(jObject);
                            }
                        }

                        finalCausalArray = new JArray(finalCausalArray.OrderBy(obj => (DateTime)obj["startwindow"]));
                        string causalMachineStatus = "";

                        dynamic dataFinalCausalArray = JObject.Parse(finalCausalArray[0].ToString());

                        string dataFinalMachine = dataFinalCausalArray.machineid;

                        if (dataFinalMachine == "BigDrumL23")
                        {
                            causalMachineStatus = "Causal";
                        }


                        dataFinalCausalArray.Status = "Causal";
                        finalCausalArray.RemoveAt(0);
                        finalCausalArray.Insert(0, dataFinalCausalArray);

                        finalCausalArray = new JArray(finalCausalArray.OrderBy(obj => (DateTime)obj["startwindow"]));



                        for (int z = 1; z < finalCausalArray.Count; z++)
                        {
                            dynamic dataFinal = JObject.Parse(finalCausalArray[z].ToString());
                            string dataFinalMachine1 = dataFinal.machineid;

                            if (dataFinalMachine1 == bnMachine)
                            {
                                causalMachineStatus = "Resultant";
                            }
                            dataFinal.Status = "Resultant";
                            finalCausalArray.RemoveAt(z);
                            finalCausalArray.Insert(z, dataFinal);
                        }


                        for (int k = i + 1; k < bottleneckMachines.Count; k++)
                        {
                            dynamic bottleneckData = JObject.Parse(bottleneckMachines[k].ToString());
                            int bottleneckSeq = bottleneckData.Sequence;
                            if (seq == bottleneckSeq)
                            {
                                bottleneckData.Status = causalMachineStatus;
                                finalCausalArray.Add(bottleneckData);
                            }
                        }

                        finalCausalArray = new JArray(finalCausalArray.OrderBy(obj => (DateTime)obj["startwindow"]));

                        for (int k = 0; k < machinesInSeq.Count; k++)
                        {
                            for (int j = 0; j < finalCausalArray.Count; j++)
                            {
                                dynamic datanewone = JObject.Parse(finalCausalArray[j].ToString());
                                string machinename = datanewone.machineid;
                                string endtime = datanewone.endwindow;

                                if (machinename == machinesInSeq[k] && machineArray[k].Count > 0)
                                {
                                    dynamic data5 = JObject.Parse(machineArray[k][0].ToString());
                                    data5.endwindow = endtime;

                                    machineArray[k].Insert(0, data5);
                                    machineArray[k].RemoveAt(1);
                                }
                                else if (machinename == machinesInSeq[k] && machineArray[k].Count == 0)
                                {
                                    machineArray[k].Add(datanewone);
                                }
                            }
                        }

                        JArray finalCauaslArraync = new JArray();

                        for (int m = 0; m < machineArray.Count; m++)
                        {
                            for (int k = 0; k < machineArray[m].Count; k++)
                            {
                                JObject data1 = JObject.Parse(machineArray[m][k].ToString());
                                finalCauaslArraync.Add(data1);
                            }
                            machineArray[m].RemoveAll();
                        }

                        finalCauaslArraync = new JArray(finalCauaslArraync.OrderBy(obj => (DateTime)obj["startwindow"]));

                        List<JObject> a1 = new List<JObject>();
                        BsonArray b1 = new BsonArray();

                        for (int k = 0; k < finalCauaslArraync.Count; k++)
                        {
                            b1.Add(BsonDocument.Parse(finalCauaslArraync[k].ToString()));
                        }

                        BsonDocument bson = new BsonDocument()
                        {
                            new BsonElement("autocausal",b1)
                        };

                        //MongoClient dbClient = new MongoClient("mongodb://bnlwe-gs-d-57321-uni-cosmosdb01:U1p5lyDPFicpMcljgm7UJ3hs4tCnOJ5yvtE9JeXczst7EBO2ylgOyykUkQez45RaHyRyajFdJw0FsCE2XgklCA==@bnlwe-gs-d-57321-uni-cosmosdb01.documents.azure.com:10255/?ssl=true&replicaSet=globaldb");
                        //IMongoDatabase db = dbClient.GetDatabase("bnlwe-gs-d-57321-uni-mongodb-01");
                        //var personColl = db.GetCollection<BsonDocument>("autocausal1");

                        //personColl.InsertOne(bson);
                    }
                    previousSequence = seq;
                }
            }
            catch (Exception)
            {
                throw;
            }
            return json;
        }
        public static async Task<string> AcquireAccessTokenAsync(string ApplicationClientId, string ApplicationClientSecret, string Tenant)
        {
            if (ApplicationClientId == "#DUMMY#" || ApplicationClientSecret == "#DUMMY#" || Tenant.StartsWith("#DUMMY#"))
            {
                throw new Exception(
                    $"Use the link {"https://docs.microsoft.com/azure/time-series-insights/time-series-insights-authentication-and-authorization"} to update the values of 'ApplicationClientId', 'ApplicationClientSecret' and 'Tenant'.");
            }

            var authenticationContext = new AuthenticationContext(
                $"https://login.windows.net/{Tenant}",
                TokenCache.DefaultShared);

            AuthenticationResult token = await authenticationContext.AcquireTokenAsync(
               resource: "https://api.timeseries.azure.com/",
                clientCredential: new ClientCredential(
                    clientId: ApplicationClientId
                    , clientSecret: ApplicationClientSecret
                    ));

            return token.AccessToken;
        }
        public static HttpWebRequest CreateHttpsWebRequest(string host_, string method, string path_, string accessToken, string[] queryArgs = null)
        {
            string query = "api-version=2016-12-12";
            if (queryArgs != null && queryArgs.Any())
            {
                query += "&" + String.Join("&", queryArgs);
            }

            Uri uri = new UriBuilder("https", host_)
            {
                Path = path_,
                Query = query
            }.Uri;
            HttpWebRequest request = WebRequest.CreateHttp(uri);
            request.Method = method;
            request.Headers.Add("Authorization", "Bearer " + accessToken);
            return request;
        }
        public static async Task WriteRequestStreamAsync(HttpWebRequest request, JObject inputPayload)
        {
            using (var stream = await request.GetRequestStreamAsync())
            using (var streamWriter = new StreamWriter(stream))
            {
                await streamWriter.WriteAsync(inputPayload.ToString());
                await streamWriter.FlushAsync();
                streamWriter.Close();
            }
        }
        public static async Task<JToken> GetResponseAsync(HttpWebRequest request)
        {
            using (WebResponse webResponse = await request.GetResponseAsync())
            using (var sr = new StreamReader(webResponse.GetResponseStream()))
            {
                string result = await sr.ReadToEndAsync();
                return JsonConvert.DeserializeObject<JToken>(result);
            }
        }
        private static string GetEvents(JToken responseContent)
        {
            // Response content has a list of events under "events" property for HTTP request.
            JArray events = (JArray)responseContent["events"];
            int eventCount = events.Count;
            Console.WriteLine("Acquired {0} events:", eventCount);

            var eventsData = new List<Dictionary<string, object>>();

            var metadata = new List<string>();

            for (int i = 0; i < eventCount; i++)
            {
                JObject currentEvent = (JObject)events[i];

                if (i == 0)
                {
                    var schemaObject = (JObject)currentEvent["schema"];
                    var properties = (JArray)schemaObject["properties"];

                    for (int j = 0; j < properties.Count; j++)
                    {
                        JObject currentProperty = (JObject)properties[j];
                        Console.WriteLine("'{0}': {1}", currentProperty["name"], currentProperty["type"]);

                        metadata.Add(Convert.ToString(currentProperty["name"]));
                    }
                }
                JArray values = (JArray)currentEvent["values"];

                var jsonData = new Dictionary<string, object>();
                jsonData.Add("timestamp", currentEvent["$ts"]);
                var counter = 0;
                foreach (var metadataVal in metadata)
                {
                    if (values.Count > counter)
                    {
                        jsonData.Add(metadataVal, Convert.ToString(values[counter]));
                    }
                    else
                    {
                        Console.WriteLine($"Counter {counter} I is {i} and values count is {values.Count}");
                    }
                    counter++;
                }
                eventsData.Add(jsonData);
            }
            return JsonConvert.SerializeObject(eventsData);
        }
    }
}