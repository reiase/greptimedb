use std::sync::Arc;

use clap::Parser;
use common_base::Plugins;
use common_telemetry::info;
use common_telemetry::logging::LoggingOptions;
use datanode::datanode::{Datanode, DatanodeOptions, ProcedureConfig, StorageConfig, WalConfig};
use datanode::instance::InstanceRef;
use frontend::frontend::FrontendOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance};
use frontend::service_config::{
    GrpcOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, PostgresOptions, PromStoreOptions,
};
use serde::{Deserialize, Serialize};
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::Mode;
use snafu::ResultExt;

use crate::error::{
    IllegalConfigSnafu, Result, ShutdownDatanodeSnafu, ShutdownFrontendSnafu, StartDatanodeSnafu,
    StartFrontendSnafu,
};
use crate::frontend::load_frontend_plugins;
use crate::options::{MixOptions, Options, TopLevelOptions};

async fn build_frontend(
    plugins: Arc<Plugins>,
    datanode_instance: InstanceRef,
) -> Result<FeInstance> {
    let mut frontend_instance = FeInstance::try_new_standalone(datanode_instance.clone())
        .await
        .context(StartFrontendSnafu)?;
    frontend_instance.set_plugins(plugins.clone());
    Ok(frontend_instance)
}

pub struct Instance {
    datanode: Datanode,
    frontend: FeInstance,
}

impl Instance {
    pub async fn start(&mut self) -> Result<()> {
        // Start datanode instance before starting services, to avoid requests come in before internal components are started.
        self.datanode
            .start_instance()
            .await
            .context(StartDatanodeSnafu)?;
        info!("Datanode instance started");

        self.frontend.start().await.context(StartFrontendSnafu)?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.frontend
            .shutdown()
            .await
            .context(ShutdownFrontendSnafu)?;

        self.datanode
            .shutdown_instance()
            .await
            .context(ShutdownDatanodeSnafu)?;
        info!("Datanode instance stopped.");

        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StandaloneOptions {
    pub mode: Mode,
    pub enable_memory_catalog: bool,
    pub enable_telemetry: bool,
    pub http_options: Option<HttpOptions>,
    pub grpc_options: Option<GrpcOptions>,
    pub mysql_options: Option<MysqlOptions>,
    pub postgres_options: Option<PostgresOptions>,
    pub opentsdb_options: Option<OpentsdbOptions>,
    pub influxdb_options: Option<InfluxdbOptions>,
    pub prom_store_options: Option<PromStoreOptions>,
    pub wal: WalConfig,
    pub storage: StorageConfig,
    pub procedure: ProcedureConfig,
    pub logging: LoggingOptions,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            enable_memory_catalog: false,
            enable_telemetry: true,
            http_options: Some(HttpOptions::default()),
            grpc_options: Some(GrpcOptions::default()),
            mysql_options: Some(MysqlOptions::default()),
            postgres_options: Some(PostgresOptions::default()),
            opentsdb_options: Some(OpentsdbOptions::default()),
            influxdb_options: Some(InfluxdbOptions::default()),
            prom_store_options: Some(PromStoreOptions::default()),
            wal: WalConfig::default(),
            storage: StorageConfig::default(),
            procedure: ProcedureConfig::default(),
            logging: LoggingOptions::default(),
        }
    }
}

impl StandaloneOptions {
    fn frontend_options(self) -> FrontendOptions {
        FrontendOptions {
            mode: self.mode,
            http_options: self.http_options,
            grpc_options: self.grpc_options,
            mysql_options: self.mysql_options,
            postgres_options: self.postgres_options,
            opentsdb_options: self.opentsdb_options,
            influxdb_options: self.influxdb_options,
            prom_store_options: self.prom_store_options,
            meta_client_options: None,
            logging: self.logging,
            ..Default::default()
        }
    }

    fn datanode_options(self) -> DatanodeOptions {
        DatanodeOptions {
            enable_memory_catalog: self.enable_memory_catalog,
            enable_telemetry: self.enable_telemetry,
            wal: self.wal,
            storage: self.storage,
            procedure: self.procedure,
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

        opts.enable_memory_catalog = self.enable_memory_catalog;

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
            if let Some(http_opts) = &mut opts.http_options {
                http_opts.addr = addr.clone()
            }
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
            if let Some(grpc_opts) = &mut opts.grpc_options {
                grpc_opts.addr = addr.clone()
            }
        }

        if let Some(addr) = &self.mysql_addr {
            if let Some(mysql_opts) = &mut opts.mysql_options {
                mysql_opts.addr = addr.clone();
                mysql_opts.tls = tls_opts.clone();
            }
        }

        if let Some(addr) = &self.postgres_addr {
            if let Some(postgres_opts) = &mut opts.postgres_options {
                postgres_opts.addr = addr.clone();
                postgres_opts.tls = tls_opts;
            }
        }

        if let Some(addr) = &self.opentsdb_addr {
            if let Some(opentsdb_addr) = &mut opts.opentsdb_options {
                opentsdb_addr.addr = addr.clone();
            }
        }

        if self.influxdb_enable {
            opts.influxdb_options = Some(InfluxdbOptions { enable: true });
        }

        let fe_opts = opts.clone().frontend_options();
        let logging = opts.logging.clone();
        let dn_opts = opts.datanode_options();

        Ok(Options::Standalone(Box::new(MixOptions {
            fe_opts,
            dn_opts,
            logging,
        })))
    }

    pub async fn build(
        self,
        fe_opts: FrontendOptions,
        dn_opts: DatanodeOptions,
    ) -> Result<Instance> {
        let plugins = Arc::new(load_frontend_plugins(&self.user_provider)?);

        info!("Standalone start command: {:#?}", self);
        info!(
            "Standalone frontend options: {:#?}, datanode options: {:#?}",
            fe_opts, dn_opts
        );

        let datanode = Datanode::new(dn_opts.clone(), Default::default())
            .await
            .context(StartDatanodeSnafu)?;

        let mut frontend = build_frontend(plugins.clone(), datanode.get_instance()).await?;

        frontend
            .build_servers(&fe_opts)
            .await
            .context(StartFrontendSnafu)?;

        Ok(Instance { datanode, frontend })
    }
}
