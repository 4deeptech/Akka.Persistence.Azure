using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit;
using Akka.Persistence;
using Akka.Persistence.Journal;
using Xunit;
using Akka.Persistence.TestKit.Snapshot;
using Akka.Configuration;


namespace Akka.Persistence.TableStorage.Tests
{
    public partial class TableStorageSnapshotSpec : SnapshotStoreSpec
    {
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka {
                stdout-loglevel = DEBUG
	            loglevel = DEBUG
                loggers = [""Akka.Logger.NLog.NLogLogger,Akka.Logger.NLog""]

                persistence {
                publish-plugin-commands = on
                snapshot-store {
                    plugin = ""akka.persistence.snapshot-store.table-storage""
                    table-storage {
                        class = ""TableStorage.Persistence.TableStorageSnapshotStore, Akka.Persistence.Azure""
                        plugin-dispatcher = ""akka.actor.default-dispatcher""
                        stream-dispatcher = ""akka.persistence.dispatchers.default-stream-dispatcher""
                        table-name = snapshots
                        auto-initialize = on
                        connection-strings = [""UseDevelopmentStorage=true"",
                                  ""UseDevelopmentStorage=true"",
								  ""UseDevelopmentStorage=true"",
                                  ""UseDevelopmentStorage=true"",
								  ""UseDevelopmentStorage=true"",
                                  ""UseDevelopmentStorage=true"",
								  ""UseDevelopmentStorage=true"",
                                  ""UseDevelopmentStorage=true"",
								  ""UseDevelopmentStorage=true"",
                                  ""UseDevelopmentStorage=true"",
								  ""UseDevelopmentStorage=true"",
                                  ""UseDevelopmentStorage=true"",
								  ""UseDevelopmentStorage=true"",
                                  ""UseDevelopmentStorage=true"",
								  ""UseDevelopmentStorage=true"",
                                  ""UseDevelopmentStorage=true""]
                    }
                }
            }
        }    
        ");
        
        public TableStorageSnapshotSpec() : base(SpecConfig,"TableStorageSnapshotSpec") {
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            //cleanup
            TableStorageCleanup.Clean(SpecConfig.GetString("akka.persistence.snapshot-store.table-storage.table-name"),
                SpecConfig.GetStringList("akka.persistence.snapshot-store.table-storage.connection-strings"));
        }
    }

     
}
