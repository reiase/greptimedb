use std::sync::Arc;

use catalog::kvbackend::{DummyKvCacheInvalidator, KvBackendCatalogManager};
use catalog::CatalogManagerRef;
use clap::Parser;
use common_base::Plugins;
use common_config::{kv_store_dir, KvStoreConfig, WalConfig};
use common_meta::kv_backend::KvBackendRef;
use common_procedure::ProcedureManagerRef;
use common_telemetry::info;
use common_telemetry::logging::LoggingOptions;
use datanode::config::{DatanodeOptions, StorageConfig};
use datanode::datanode::{DatanodeBuilder, ProcedureConfig};
use datanode::region_server::RegionServer;
use frontend::frontend::FrontendOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance, StandaloneDatanodeManager};
use frontend::service_config::{
    GrpcOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, PostgresOptions, PromStoreOptions,
};
use serde::{Deserialize, Serialize};
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::Mode;
use snafu::ResultExt;

use crate::error::{
    IllegalConfigSnafu, InitMetadataSnafu, Result, StartDatanodeSnafu, StartFrontendSnafu,
};
use crate::frontend::load_frontend_plugins;
use crate::options::{MixOptions, Options, TopLevelOptions};

// async fn build_frontend(
//     plugins: Arc<Plugins>,
//     datanode_instance: InstanceRef,
// ) -> Result<FeInstance> {
//     let mut frontend_instance = FeInstance::try_new_standalone(datanode_instance.clone())
//         .await
//         .context(StartFrontendSnafu)?;
//     frontend_instance.set_plugins(plugins.clone());
//     Ok(frontend_instance)
// }

/// Build frontend instance in standalone mode
async fn build_frontend(
    plugins: Arc<Plugins>,
    kv_store: KvBackendRef,
    procedure_manager: ProcedureManagerRef,
    catalog_manager: CatalogManagerRef,
    region_server: RegionServer,
) -> Result<FeInstance> {
    let frontend_instance = FeInstance::try_new_standalone(
        kv_store,
        procedure_manager,
        catalog_manager,
        plugins,
        region_server,
    )
    .await
    .context(StartFrontendSnafu)?;
    Ok(frontend_instance)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StandaloneOptions {
    pub mode: Mode,
    pub enable_telemetry: bool,
    pub http_options: HttpOptions,
    pub grpc_options: GrpcOptions,
    pub mysql_options: MysqlOptions,
    pub postgres_options: PostgresOptions,
    pub opentsdb_options: OpentsdbOptions,
    pub influxdb_options: InfluxdbOptions,
    pub prom_store_options: PromStoreOptions,
    pub wal: WalConfig,
    pub storage: StorageConfig,
    pub kv_store: KvStoreConfig,
    pub procedure: ProcedureConfig,
    pub logging: LoggingOptions,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            enable_telemetry: true,
            http_options: HttpOptions::default(),
            grpc_options: GrpcOptions::default(),
            mysql_options: MysqlOptions::default(),
            postgres_options: PostgresOptions::default(),
            opentsdb_options: OpentsdbOptions::default(),
            influxdb_options: InfluxdbOptions::default(),
            prom_store_options: PromStoreOptions::default(),
            wal: WalConfig::default(),
            storage: StorageConfig::default(),
            kv_store: KvStoreConfig::default(),
            procedure: ProcedureConfig::default(),
            logging: LoggingOptions::default(),
        }
    }
}

impl StandaloneOptions {
    fn frontend_options(self) -> FrontendOptions {
        FrontendOptions {
            mode: self.mode,
            http: self.http_options,
            grpc: self.grpc_options,
            mysql: self.mysql_options,
            postgres: self.postgres_options,
            opentsdb: self.opentsdb_options,
            influxdb: self.influxdb_options,
            prom_store: self.prom_store_options,
            meta_client: None,
            logging: self.logging,
            ..Default::default()
        }
    }

    fn datanode_options(self) -> DatanodeOptions {
        DatanodeOptions {
            node_id: Some(0),
            enable_telemetry: self.enable_telemetry,
            wal: self.wal,
            storage: self.storage,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Parser)]
pub struct Standalone {
    #[arg(long)]
    http_addr: Option<String>,
    #[arg(long)]
    rpc_addr: Option<String>,
    #[arg(long)]
    mysql_addr: Option<String>,
    #[arg(long)]
    postgres_addr: Option<String>,
    #[arg(long)]
    opentsdb_addr: Option<String>,
    #[arg(long)]
    influxdb_enable: bool,
    #[arg(short, long)]
    config_file: Option<String>,
    #[arg(short = 'm', long = "memory-catalog")]
    enable_memory_catalog: bool,
    #[arg(long)]
    tls_mode: Option<TlsMode>,
    #[arg(long)]
    tls_cert_path: Option<String>,
    #[arg(long)]
    tls_key_path: Option<String>,
    #[arg(long)]
    user_provider: Option<String>,
}

impl Standalone {
    pub fn load_options(&self, top_level_options: TopLevelOptions) -> Result<Options> {
        let mut opts: StandaloneOptions =
            Options::load_layered_options(self.config_file.as_deref(), "ENGRAM_", None)?;

        opts.mode = Mode::Standalone;

        if let Some(dir) = top_level_options.log_dir {
            opts.logging.dir = dir;
        }

        if top_level_options.log_level.is_some() {
            opts.logging.level = top_level_options.log_level;
        }

        let tls_opts = TlsOption::new(
            self.tls_mode.clone(),
            self.tls_cert_path.clone(),
            self.tls_key_path.clone(),
        );

        if let Some(addr) = &self.http_addr {
            opts.http_options.addr = addr.clone();
        }

        if let Some(addr) = &self.rpc_addr {
            // frontend grpc addr conflict with datanode default grpc addr
            let datanode_grpc_addr = DatanodeOptions::default().rpc_addr;
            if addr.eq(&datanode_grpc_addr) {
                return IllegalConfigSnafu {
                    msg: format!(
                        "gRPC listen address conflicts with datanode reserved gRPC addr: {datanode_grpc_addr}",
                    ),
                }
                .fail();
            }
            opts.grpc_options.addr = addr.clone();
        }

        if let Some(addr) = &self.mysql_addr {
            let mysql_opts = &mut opts.mysql_options;
            mysql_opts.addr = addr.clone();
            mysql_opts.tls = tls_opts.clone();
        }

        if let Some(addr) = &self.postgres_addr {
            let postgres_opts = &mut opts.postgres_options;
            postgres_opts.addr = addr.clone();
            postgres_opts.tls = tls_opts;
        }

        if let Some(addr) = &self.opentsdb_addr {
            let opentsdb_addr = &mut opts.opentsdb_options;
            opentsdb_addr.addr = addr.clone();
        }

        if self.influxdb_enable {
            opts.influxdb_options = InfluxdbOptions { enable: true };
        }

        let kv_store_cfg = opts.kv_store.clone();
        let procedure_cfg = opts.procedure.clone();
        let fe_opts = opts.clone().frontend_options();
        let logging_opts = opts.logging.clone();
        let dn_opts = opts.datanode_options();

        Ok(Options::Standalone(Box::new(MixOptions {
            procedure_cfg,
            kv_store_cfg,
            data_home: dn_opts.storage.data_home.to_string(),
            fe_opts,
            dn_opts,
            logging_opts,
        })))

        // let fe_opts = opts.clone().frontend_options();
        // let logging = opts.logging.clone();
        // let dn_opts = opts.datanode_options();

        // Ok(Options::Standalone(Box::new(MixOptions {
        //     fe_opts,
        //     dn_opts,
        //     logging,
        // })))
    }

    pub async fn execute(self, opts: MixOptions) -> Result<()> {
        let plugins = Arc::new(load_frontend_plugins(&self.user_provider)?);
        let fe_opts = opts.fe_opts;
        let dn_opts = opts.dn_opts;

        info!("Standalone start command: {:#?}", self);
        info!(
            "Standalone frontend options: {:#?}, datanode options: {:#?}",
            fe_opts, dn_opts
        );

        let kv_dir = kv_store_dir(&opts.data_home);
        let (kv_store, procedure_manager) = FeInstance::try_build_standalone_components(
            kv_dir,
            opts.kv_store_cfg,
            opts.procedure_cfg,
        )
        .await
        .context(StartFrontendSnafu)?;

        let mut datanode =
            DatanodeBuilder::new(dn_opts.clone(), Some(kv_store.clone()), plugins.clone())
                .build()
                .await
                .context(StartDatanodeSnafu)?;
        let region_server = datanode.region_server();

        let catalog_manager = KvBackendCatalogManager::new(
            kv_store.clone(),
            Arc::new(DummyKvCacheInvalidator),
            Arc::new(StandaloneDatanodeManager(region_server.clone())),
        );

        catalog_manager
            .table_metadata_manager_ref()
            .init()
            .await
            .context(InitMetadataSnafu)?;
        info!("Datanode instance started");

        let mut frontend = build_frontend(
            plugins,
            kv_store,
            procedure_manager,
            catalog_manager,
            region_server,
        )
        .await?;

        frontend
            .build_servers(&fe_opts)
            .await
            .context(StartFrontendSnafu)?;

        datanode.start().await.context(StartDatanodeSnafu)?;
        frontend.start().await.context(StartFrontendSnafu)?;

        // Ok(Instance { datanode, frontend })

        // let mut frontend = build_frontend(plugins.clone(), datanode.get_instance()).await?;

        // frontend
        //     .build_servers(&fe_opts)
        //     .await
        //     .context(StartFrontendSnafu)?;
        // frontend.start().await.context(StartFrontendSnafu)?;

        Ok(())
    }
}
