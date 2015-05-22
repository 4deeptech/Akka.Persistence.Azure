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
using Akka.Persistence.TestKit.Journal;
using Akka.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;


namespace Akka.Persistence.TableStorage.Tests
{
    public partial class TableStorageJournalSpec : JournalSpec
    {
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka {
                stdout-loglevel = DEBUG
	            loglevel = DEBUG
                loggers = [""Akka.Logger.NLog.NLogLogger,Akka.Logger.NLog""]

                persistence {

                publish-plugin-commands = on
                journal {
                    plugin = ""akka.persistence.journal.table-storage""
                    table-storage {
                        class = ""TableStorage.Persistence.TableStorageJournal, Akka.Persistence.Azure""
                        plugin-dispatcher = ""akka.actor.default-dispatcher""
                        table-name = events
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
        public TableStorageJournalSpec() : base(SpecConfig,"TableStorageJournalSpec") 
        {
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            //cleanup
            TableStorageCleanup.Clean(SpecConfig.GetString("akka.persistence.journal.table-storage.table-name"),
                SpecConfig.GetStringList("akka.persistence.journal.table-storage.connection-strings"));
        }
    }
}
