using Akka.Actor;
using Akka.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Akka.Persistence.Azure
{
    public class TableStorageJournalSettings : JournalSettings
    {
        public const string ConfigPath = "akka.persistence.journal.table-storage";

        /// <summary>
        /// Flag determining in in case of event journal table missing, it should be automatically initialized.
        /// </summary>
        public bool AutoInitialize { get; private set; }

        public TableStorageJournalSettings(Config config)
            : base(config)
        {
            AutoInitialize = config.GetBoolean("auto-initialize");
        }
    }

    public class TableStorageSnapshotSettings : TableSnapshotStoreSettings
    {
        public const string ConfigPath = "akka.persistence.snapshot-store.table-storage";

        /// <summary>
        /// Flag determining in case of snapshot store table missing, it should be automatically initialized.
        /// </summary>
        public bool AutoInitialize { get; private set; }

        public TableStorageSnapshotSettings(Config config)
            : base(config)
        {
            AutoInitialize = config.GetBoolean("auto-initialize");
        }
    }

    public class BlobStorageSnapshotSettings : BlobSnapshotStoreSettings
    {
        public const string ConfigPath = "akka.persistence.snapshot-store.blob-storage";

        /// <summary>
        /// Flag determining in case of snapshot store table missing, it should be automatically initialized.
        /// </summary>
        public bool AutoInitialize { get; private set; }

        public BlobStorageSnapshotSettings(Config config)
            : base(config)
        {
            AutoInitialize = config.GetBoolean("auto-initialize");
        }
    }

    /// <summary>
    /// An actor system extension initializing support for Table Storage persistence layer.
    /// </summary>
    public class AzureStoragePersistenceExtension : IExtension
    {
        /// <summary>
        /// Journal-related settings loaded from HOCON configuration.
        /// </summary>
        public readonly TableStorageJournalSettings TableJournalSettings;

        /// <summary>
        /// Snapshot store related settings loaded from HOCON configuration.
        /// </summary>
        public readonly TableStorageSnapshotSettings TableSnapshotStoreSettings;

        /// <summary>
        /// Blob Storage Snapshot store related settings loaded from HOCON configuration.
        /// </summary>
        public readonly BlobStorageSnapshotSettings BlobSnapshotStoreSettings;

        public AzureStoragePersistenceExtension(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(AzureStoragePersistence.DefaultConfiguration());

            TableJournalSettings = new TableStorageJournalSettings(system.Settings.Config.GetConfig(TableStorageJournalSettings.ConfigPath));
            TableSnapshotStoreSettings = new TableStorageSnapshotSettings(system.Settings.Config.GetConfig(TableStorageSnapshotSettings.ConfigPath));
            BlobSnapshotStoreSettings = new BlobStorageSnapshotSettings(system.Settings.Config.GetConfig(BlobStorageSnapshotSettings.ConfigPath));

            if (TableJournalSettings.AutoInitialize)
            {
                AzureStorageInitializer.CreateJournalTables(TableJournalSettings.ConnectionStrings, TableJournalSettings.TableName);
            }

            if (BlobSnapshotStoreSettings.AutoInitialize)
            {
                AzureStorageInitializer.CreateSnapshotStoreContainer(BlobSnapshotStoreSettings.ConnectionStrings, BlobSnapshotStoreSettings.ContainerName);
            }

            if (TableSnapshotStoreSettings.AutoInitialize)
            {
                AzureStorageInitializer.CreateSnapshotStoreTables(TableSnapshotStoreSettings.ConnectionStrings, TableSnapshotStoreSettings.TableName);
            }
        }
    }

    /// <summary>
    /// Singleton class used to setup Azure Storage backend for akka persistence plugin.
    /// </summary>
    public class AzureStoragePersistence : ExtensionIdProvider<AzureStoragePersistenceExtension>
    {
        public static readonly AzureStoragePersistence Instance = new AzureStoragePersistence();

        /// <summary>
        /// Initializes a Table Storage persistence plugin inside provided <paramref name="actorSystem"/>.
        /// </summary>
        public static void Init(ActorSystem actorSystem)
        {
            Instance.Apply(actorSystem);
        }

        private AzureStoragePersistence() { }

        /// <summary>
        /// Creates an actor system extension for akka persistence Azure Storage support.
        /// </summary>
        /// <param name="system"></param>
        /// <returns></returns>
        public override AzureStoragePersistenceExtension CreateExtension(ExtendedActorSystem system)
        {
            return new AzureStoragePersistenceExtension(system);
        }

        /// <summary>
        /// Returns a default configuration for akka persistence table storage journals and snapshot stores.
        /// </summary>
        /// <returns></returns>
        public static Config DefaultConfiguration()
        {
            return ConfigurationFactory.FromResource<AzureStoragePersistence>("Akka.Persistence.Azure.azure-storage.conf");
        }
    }
}
