using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.ComplexEventProcessing;
using Microsoft.ComplexEventProcessing.Linq;   
using System.Reactive;  //added assembly reference to System.Reactive
using System.Reactive.Linq; //added assembly reference to System.Reactive.Providers

using System.Data.Linq; //added assembly reference
using System.Data.SqlClient;
using System.Runtime.Serialization; //added assembly reference

namespace Seroter.StreamInsight.SqlEnumerable
{
    class Program
    {
        static void Main(string[] args)
        {
            //BasicStreamInsightDemo();

            //CreateSqlEnumerable();

            CreateStreamInsightSqlQuery();
        }

        //made a member variable so that we can check it in different function than one that instantiates it
        private static SqlConnection conn;

        /// <summary>
        /// Return IEnumerable of SQL Server query results
        /// </summary>
        /// <returns></returns>
        private static IEnumerable<ServerEvent> GetEvents()
        {
            //define connection string
            string connString = "Data Source=.;Initial Catalog=DemoDb;Integrated Security=SSPI;";

            //create enumerable to hold results
            IEnumerable<ServerEvent> result;

            //define dataconext object which is used later for translating results to objects
            DataContext dc = new DataContext(connString);
            
            //initiate and open connection
            conn = (SqlConnection)dc.Connection;
            conn.Open();

            //return all events stored in the SQL Server table
            SqlCommand command = new SqlCommand("select ID, ServerName, Level, Timestamp From ServerEvent", conn);
            
            //get the database results and set the connection to close after results are read
            SqlDataReader dataReader = command.ExecuteReader(System.Data.CommandBehavior.CloseConnection);

            //use "translate" to flip the reader stream to an Enumerable of my custom object type
            result = dc.Translate<ServerEvent>(dataReader);
            
            return result;
        }

        /// <summary>
        /// Use StreamInsight v2.1 libraries to get SQL Server results and display in the console
        /// </summary>
        private static void CreateStreamInsightSqlQuery()
        {
            //create embedded streaminsight application
            using (Server server = Server.Create("RSEROTER21"))
            {
                try
                {
                    //create StreamInsight application container
                    Application app = server.CreateApplication("app");

                    //define the (point event) source by creating an enumerable from the GetEvents operation
                    var source = app.DefineEnumerable<ServerEvent>(() => GetEvents()).
                        ToPointStreamable<ServerEvent, ServerEvent>(
                            e => PointEvent.CreateInsert<ServerEvent>(e.Timestamp, e), 
                            AdvanceTimeSettings.StrictlyIncreasingStartTime);
                    
                    //******************************** 
                    //EXAMPLE #1 - passthrough query
                    //********************************
                    //write LINQ query against event stream
                    var query = from ev in source
                                select ev;

                    //create observer as sink and write results to console
                    var sink = app.DefineObserver(() =>
                        Observer.Create<ServerEvent>(x => Console.WriteLine(x.ServerName + ": " + x.Level)));

                    //bind the query to the sink
                    using (IDisposable proc = query.Bind<ServerEvent>(sink).Run("MyProcess"))
                    {
                        Console.WriteLine("Press [Enter] to close the application.");
                        Console.ReadLine();
                    }


                    //******************************** 
                    //EXAMPLE #2 - windowed query
                    //********************************
                    var query2 = from ev in source.TumblingWindow(TimeSpan.FromMinutes(2))
                                 select ev.Count();

                    //create observer as sink and write results to console
                    var sink2 = app.DefineObserver(() =>
                        Observer.Create<long>(x => Console.WriteLine("Count: " + x.ToString())));

                    //bind the query to the sink
                    using (IDisposable proc = query2.Bind<long>(sink2).Run("MyProcess"))
                    {
                        Console.WriteLine("Press [Enter] to close the application.");
                        Console.ReadLine();
                    }


                    //******************************** 
                    //EXAMPLE #3 - groups and window query
                    //********************************
                    var query3 = from ev in source
                                 group ev by ev.Level into levelgroup
                                 from win in levelgroup.TumblingWindow(TimeSpan.FromMinutes(3))
                                 select new EventSummary
                                 {
                                     EventCount = win.Count(),
                                     EventMessage = levelgroup.Key
                                 };

                    //create observer as sink and write results to console
                    var sink3 = app.DefineObserver(() =>
                        Observer.Create<EventSummary>(x => Console.WriteLine("Level: " + x.EventMessage + " /  Count: " + x.EventCount.ToString())));

                    //bind the query to the sink
                    using (IDisposable proc = query3.Bind<EventSummary>(sink3).Run("MyProcess"))
                    {
                        Console.WriteLine("Press [Enter] to close the application.");
                        Console.ReadLine();
                    }
                }
                catch (Exception ex)
                {
                    //write errors
                    Console.WriteLine("Error! " + ex.ToString());
                }
                finally
                {
                    //should always be closed when IEnumerable completes, but just in case
                    if (conn != null && conn.State == System.Data.ConnectionState.Open)
                    {
                        conn.Close();
                    }
                }
            }
                
        }

        private static void BasicStreamInsightDemo()
        {
            using (Server server = Server.Create("RSEROTER21"))
            {
                // Create an app
                Application app = server.CreateApplication("app");
                // Define a simple observable which generates an integer every second
                var source = app.DefineObservable(() =>
                    Observable.Interval(TimeSpan.FromSeconds(1)));
                // Define a sink.
                var sink = app.DefineObserver(() =>
                    Observer.Create<long>(x => Console.WriteLine(x)));
                // Define a query to filter the events
                var query = from e in source
                            where e % 2 == 0
                            select e;
                // Bind the query to the sink and create a runnable process
                using (IDisposable proc = query.Bind(sink).Run("MyProcess"))
                {
                    Console.WriteLine("Press a key to dispose the process...");
                    Console.ReadKey();
                }
            }
        }

        private static void CreateSqlEnumerable()
        {
            string connString = "Data Source=.;Initial Catalog=DemoDb;Integrated Security=SSPI;";
            using (DataContext dc = new DataContext(connString))
            {
                SqlConnection conn = (SqlConnection)dc.Connection;
                conn.Open();
                SqlCommand command = new SqlCommand("select ID, ServerName, Level, Timestamp From ServerEvent", conn);
                SqlDataReader dataReader = command.ExecuteReader();
                var result = dc.Translate<ServerEvent>(dataReader);
                foreach (ServerEvent evt in result)
                {
                    Console.WriteLine("***************");
                    Console.WriteLine(evt.Id);
                    Console.WriteLine(evt.ServerName);
                    Console.WriteLine(evt.Level);
                }
            }

            Console.ReadLine();

        }
    }

    [DataContract]
    public class ServerEvent
    {
        [DataMember]
        public int Id { get; set; }
        [DataMember]
        public string ServerName { get; set; }
        [DataMember]
        public string Level { get; set; }
        [DataMember]
        public DateTime Timestamp { get; set; }
    }

    public class EventSummary
    {
        public long EventCount { get; set; }
        public string EventMessage { get; set; }
    }
}
