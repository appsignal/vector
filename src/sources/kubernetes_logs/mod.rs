//! This mod implements `kubernetes_logs` source.
//! The scope of this source is to consume the log files that `kubelet` keeps
//! at `/var/log/pods` at the host of the k8s node when `vector` itself is
//! running inside the cluster as a `DaemonSet`.

#![deny(missing_docs)]

use std::{convert::TryInto, path::PathBuf, time::Duration};

use bytes::Bytes;
use chrono::Utc;
use file_source::{
    Checkpointer, FileServer, FileServerShutdown, FingerprintStrategy, Fingerprinter, Line,
    ReadFrom,
};
use futures_util::Stream;
use k8s_openapi::api::core::v1::{Namespace, Node, Pod};
use kube::{
    api::{Api, ListParams},
    config::{self, KubeConfigOptions},
    runtime::{
        reflector::{self},
        watcher,
    },
    Client, Config as ClientConfig,
};
use vector_common::internal_event::{ByteSize, BytesReceived, InternalEventHandle as _, Protocol};
use vector_common::TimeZone;
use vector_config::{configurable_component, NamedComponent};
use vector_core::{config::LegacyKey, transform::TaskTransform, EstimatedJsonEncodedSizeOf};

use crate::{
    config::{
        log_schema, ComponentKey, DataType, GenerateConfig, GlobalOptions, Output, SourceConfig,
        SourceContext,
    },
    event::Event,
    internal_events::{
        FileSourceInternalEventsEmitter, KubernetesLifecycleError,
        KubernetesLogsEventAnnotationError, KubernetesLogsEventNamespaceAnnotationError,
        KubernetesLogsEventNodeAnnotationError, KubernetesLogsEventsReceived,
        KubernetesLogsPodInfo, StreamClosedError,
    },
    kubernetes::{custom_reflector, meta_cache::MetaCache},
    shutdown::ShutdownSignal,
    sources,
    transforms::{FunctionTransform, OutputBuffer},
    SourceSender,
};

mod k8s_paths_provider;
mod lifecycle;
mod namespace_metadata_annotator;
mod node_metadata_annotator;
mod parser;
mod partial_events_merger;
mod path_helpers;
mod pod_metadata_annotator;
mod transform_utils;
mod util;

use self::namespace_metadata_annotator::NamespaceMetadataAnnotator;
use self::node_metadata_annotator::NodeMetadataAnnotator;
use self::parser::Parser;
use self::pod_metadata_annotator::PodMetadataAnnotator;
use codecs::{BytesDeserializer, BytesDeserializerConfig};
use futures::{future::FutureExt, stream::StreamExt};
use k8s_paths_provider::K8sPathsProvider;
use lifecycle::Lifecycle;
use lookup::{owned_value_path, path, PathPrefix};
use value::{kind::Collection, Kind};
use vector_core::config::LogNamespace;

/// The key we use for `file` field.
const FILE_KEY: &str = "file";

/// The `self_node_name` value env var key.
const SELF_NODE_NAME_ENV_KEY: &str = "VECTOR_SELF_NODE_NAME";

/// Configuration for the `kubernetes_logs` source.
#[configurable_component(source("kubernetes_logs"))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Specifies the label selector to filter `Pod`s with, to be used in addition to the built-in `exclude` filter.
    extra_label_selector: String,

    /// Specifies the label selector to filter `Namespace`s with, to be used in addition to the built-in `exclude` filter.
    extra_namespace_label_selector: String,

    /// The `name` of the Kubernetes `Node` that is running.
    ///
    /// Configured to use an environment var by default, to be evaluated to a value provided by Kubernetes at `Pod` deploy time.
    self_node_name: String,

    /// Specifies the field selector to filter `Pod`s with, to be used in addition to the built-in `Node` filter.
    extra_field_selector: String,

    /// Whether or not to automatically merge partial events.
    auto_partial_merge: bool,

    /// The directory used to persist file checkpoint positions.
    ///
    /// By default, the global `data_dir` option is used. Make sure the running user has write permissions to this directory.
    data_dir: Option<PathBuf>,

    #[configurable(derived)]
    #[serde(alias = "annotation_fields")]
    pod_annotation_fields: pod_metadata_annotator::FieldsSpec,

    #[configurable(derived)]
    namespace_annotation_fields: namespace_metadata_annotator::FieldsSpec,

    #[configurable(derived)]
    node_annotation_fields: node_metadata_annotator::FieldsSpec,

    /// A list of glob patterns to exclude from reading the files.
    exclude_paths_glob_patterns: Vec<PathBuf>,

    /// Max amount of bytes to read from a single file before switching over
    /// to the next file.
    /// This allows distributing the reads more or less evenly across
    /// the files.
    max_read_bytes: usize,

    /// The maximum number of bytes a line can contain before being discarded. This protects
    /// against malformed lines or tailing incorrect files.
    max_line_bytes: usize,

    /// How many first lines in a file are used for fingerprinting.
    fingerprint_lines: usize,

    /// This value specifies not exactly the globbing, but interval
    /// between the polling the files to watch from the `paths_provider`.
    /// This is quite efficient, yet might still create some load of the
    /// file system; in addition, it is currently coupled with chechsum dumping
    /// in the underlying file server, so setting it too low may introduce
    /// a significant overhead.
    glob_minimum_cooldown_ms: usize,

    /// Overrides the name of the log field used to add the ingestion timestamp to each event.
    ///
    /// This is useful to compute the latency between important event processing
    /// stages. For example, the time delta between when a log line was written and when it was
    /// processed by the `kubernetes_logs` source.
    ///
    /// By default, the [global `log_schema.timestamp_key` option][global_timestamp_key] is used.
    ///
    /// [global_timestamp_key]: https://vector.dev/docs/reference/configuration/global-options/#log_schema.timestamp_key
    ingestion_timestamp_field: Option<String>,

    /// The default time zone for timestamps without an explicit zone.
    timezone: Option<TimeZone>,

    /// Optional path to a readable kubeconfig file. If not set,
    /// a connection to Kubernetes is made using the in-cluster configuration.
    kube_config_file: Option<PathBuf>,

    /// How long to delay removing entries from our map when we receive a deletion
    /// event from the watched stream.
    delay_deletion_ms: usize,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,
}

impl GenerateConfig for Config {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(&Self {
            self_node_name: default_self_node_name_env_template(),
            auto_partial_merge: true,
            ..Default::default()
        })
        .unwrap()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            extra_label_selector: "".to_string(),
            extra_namespace_label_selector: "".to_string(),
            self_node_name: default_self_node_name_env_template(),
            extra_field_selector: "".to_string(),
            auto_partial_merge: true,
            data_dir: None,
            pod_annotation_fields: pod_metadata_annotator::FieldsSpec::default(),
            namespace_annotation_fields: namespace_metadata_annotator::FieldsSpec::default(),
            node_annotation_fields: node_metadata_annotator::FieldsSpec::default(),
            exclude_paths_glob_patterns: default_path_exclusion(),
            max_read_bytes: default_max_read_bytes(),
            max_line_bytes: default_max_line_bytes(),
            fingerprint_lines: default_fingerprint_lines(),
            glob_minimum_cooldown_ms: default_glob_minimum_cooldown_ms(),
            ingestion_timestamp_field: None,
            timezone: None,
            kube_config_file: None,
            delay_deletion_ms: default_delay_deletion_ms(),
            log_namespace: None,
        }
    }
}

#[async_trait::async_trait]
impl SourceConfig for Config {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);
        let source = Source::new(self, &cx.globals, &cx.key).await?;

        Ok(Box::pin(
            source
                .run(cx.out, cx.shutdown, log_namespace)
                .map(|result| {
                    result.map_err(|error| {
                        error!(message = "Source future failed.", %error);
                    })
                }),
        ))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<Output> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition = BytesDeserializerConfig
            .schema_definition(log_namespace)
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("file"))),
                &owned_value_path!("file"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .container_id
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("container_id"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .container_image
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("container_image"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .container_name
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("container_name"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.namespace_annotation_fields
                    .namespace_labels
                    .path
                    .clone()
                    .map(|x| LegacyKey::Overwrite(x.path)),
                &owned_value_path!("namespace_labels"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.node_annotation_fields
                    .node_labels
                    .path
                    .clone()
                    .map(|x| LegacyKey::Overwrite(x.path)),
                &owned_value_path!("node_labels"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_annotations
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_annotations"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_ip
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_ip"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_ips
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_ips"),
                Kind::array(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_labels
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_labels"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_name
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_name"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_namespace
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_namespace"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_node_name
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_node_name"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_owner
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_owner"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                self.pod_annotation_fields
                    .pod_uid
                    .path
                    .clone()
                    .map(|k| k.path)
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("pod_uid"),
                Kind::bytes().or_undefined(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("stream"))),
                &owned_value_path!("stream"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!(
                    log_schema().timestamp_key()
                ))),
                &owned_value_path!("timestamp"),
                Kind::timestamp(),
                Some("timestamp"),
            )
            .with_standard_vector_source_metadata();

        vec![Output::default(DataType::Log).with_schema_definition(schema_definition)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

#[derive(Clone)]
struct Source {
    client: Client,
    data_dir: PathBuf,
    auto_partial_merge: bool,
    pod_fields_spec: pod_metadata_annotator::FieldsSpec,
    namespace_fields_spec: namespace_metadata_annotator::FieldsSpec,
    node_field_spec: node_metadata_annotator::FieldsSpec,
    field_selector: String,
    label_selector: String,
    namespace_label_selector: String,
    node_selector: String,
    self_node_name: String,
    exclude_paths: Vec<glob::Pattern>,
    max_read_bytes: usize,
    max_line_bytes: usize,
    fingerprint_lines: usize,
    glob_minimum_cooldown: Duration,
    ingestion_timestamp_field: Option<String>,
    delay_deletion: Duration,
}

impl Source {
    async fn new(
        config: &Config,
        globals: &GlobalOptions,
        key: &ComponentKey,
    ) -> crate::Result<Self> {
        let self_node_name = if config.self_node_name.is_empty()
            || config.self_node_name == default_self_node_name_env_template()
        {
            std::env::var(SELF_NODE_NAME_ENV_KEY).map_err(|_| {
                format!(
                    "self_node_name config value or {} env var is not set",
                    SELF_NODE_NAME_ENV_KEY
                )
            })?
        } else {
            config.self_node_name.clone()
        };

        let field_selector = prepare_field_selector(config, self_node_name.as_str())?;
        let label_selector = prepare_label_selector(config.extra_label_selector.as_ref());
        let namespace_label_selector =
            prepare_label_selector(config.extra_namespace_label_selector.as_ref());
        let node_selector = prepare_node_selector(self_node_name.as_str())?;

        // If the user passed a custom Kubeconfig use it, otherwise
        // we attempt to load the local kubec-config, followed by the
        // in-cluster environment variables
        let client_config = match &config.kube_config_file {
            Some(kc) => {
                ClientConfig::from_custom_kubeconfig(
                    config::Kubeconfig::read_from(kc)?,
                    &KubeConfigOptions::default(),
                )
                .await?
            }
            None => ClientConfig::infer().await?,
        };
        let client = Client::try_from(client_config)?;

        let data_dir = globals.resolve_and_make_data_subdir(config.data_dir.as_ref(), key.id())?;

        let exclude_paths = prepare_exclude_paths(config)?;

        let glob_minimum_cooldown =
            Duration::from_millis(config.glob_minimum_cooldown_ms.try_into().expect(
                "unable to convert glob_minimum_cooldown_ms from usize to u64 without data loss",
            ));

        let delay_deletion = Duration::from_millis(
            config
                .delay_deletion_ms
                .try_into()
                .expect("unable to convert delay_deletion_ms from usize to u64 without data loss"),
        );

        Ok(Self {
            client,
            data_dir,
            auto_partial_merge: config.auto_partial_merge,
            pod_fields_spec: config.pod_annotation_fields.clone(),
            namespace_fields_spec: config.namespace_annotation_fields.clone(),
            node_field_spec: config.node_annotation_fields.clone(),
            field_selector,
            label_selector,
            namespace_label_selector,
            node_selector,
            self_node_name,
            exclude_paths,
            max_read_bytes: config.max_read_bytes,
            max_line_bytes: config.max_line_bytes,
            fingerprint_lines: config.fingerprint_lines,
            glob_minimum_cooldown,
            ingestion_timestamp_field: config.ingestion_timestamp_field.clone(),
            delay_deletion,
        })
    }

    async fn run(
        self,
        mut out: SourceSender,
        global_shutdown: ShutdownSignal,
        log_namespace: LogNamespace,
    ) -> crate::Result<()> {
        let Self {
            client,
            data_dir,
            auto_partial_merge,
            pod_fields_spec,
            namespace_fields_spec,
            node_field_spec,
            field_selector,
            label_selector,
            namespace_label_selector,
            node_selector,
            self_node_name,
            exclude_paths,
            max_read_bytes,
            max_line_bytes,
            fingerprint_lines,
            glob_minimum_cooldown,
            ingestion_timestamp_field,
            delay_deletion,
        } = self;

        let mut reflectors = Vec::new();

        let pods = Api::<Pod>::all(client.clone());

        let pod_watcher = watcher(
            pods,
            ListParams {
                field_selector: Some(field_selector),
                label_selector: Some(label_selector),
                ..Default::default()
            },
        );
        let pod_store_w = reflector::store::Writer::default();
        let pod_state = pod_store_w.as_reader();
        let pod_cacher = MetaCache::new();

        reflectors.push(tokio::spawn(custom_reflector(
            pod_store_w,
            pod_cacher,
            pod_watcher,
            delay_deletion,
        )));

        // -----------------------------------------------------------------

        let namespaces = Api::<Namespace>::all(client.clone());
        let ns_watcher = watcher(
            namespaces,
            ListParams {
                label_selector: Some(namespace_label_selector),
                ..Default::default()
            },
        );
        let ns_store_w = reflector::store::Writer::default();
        let ns_state = ns_store_w.as_reader();
        let ns_cacher = MetaCache::new();

        reflectors.push(tokio::spawn(custom_reflector(
            ns_store_w,
            ns_cacher,
            ns_watcher,
            delay_deletion,
        )));

        // -----------------------------------------------------------------

        let nodes = Api::<Node>::all(client);
        let node_watcher = watcher(
            nodes,
            ListParams {
                field_selector: Some(node_selector),
                ..Default::default()
            },
        );
        let node_store_w = reflector::store::Writer::default();
        let node_state = node_store_w.as_reader();
        let node_cacher = MetaCache::new();

        reflectors.push(tokio::spawn(custom_reflector(
            node_store_w,
            node_cacher,
            node_watcher,
            delay_deletion,
        )));

        let paths_provider =
            K8sPathsProvider::new(pod_state.clone(), ns_state.clone(), exclude_paths);
        let annotator = PodMetadataAnnotator::new(pod_state, pod_fields_spec, log_namespace);
        let ns_annotator =
            NamespaceMetadataAnnotator::new(ns_state, namespace_fields_spec, log_namespace);
        let node_annotator = NodeMetadataAnnotator::new(node_state, node_field_spec, log_namespace);

        // TODO: maybe more of the parameters have to be configurable.

        let checkpointer = Checkpointer::new(&data_dir);
        let file_server = FileServer {
            // Use our special paths provider.
            paths_provider,
            // Max amount of bytes to read from a single file before switching
            // over to the next file.
            // This allows distributing the reads more or less evenly across
            // the files.
            max_read_bytes,
            // We want to use checkpoining mechanism, and resume from where we
            // left off.
            ignore_checkpoints: false,
            // Match the default behavior
            read_from: ReadFrom::Beginning,
            // We're now aware of the use cases that would require specifying
            // the starting point in time since when we should collect the logs,
            // so we just disable it. If users ask, we can expose it. There may
            // be other, more sound ways for users considering the use of this
            // option to solve their use case, so take consideration.
            ignore_before: None,
            // The maximum number of bytes a line can contain before being discarded. This
            // protects against malformed lines or tailing incorrect files.
            max_line_bytes,
            // Delimiter bytes that is used to read the file line-by-line
            line_delimiter: Bytes::from("\n"),
            // The directory where to keep the checkpoints.
            data_dir,
            // This value specifies not exactly the globbing, but interval
            // between the polling the files to watch from the `paths_provider`.
            glob_minimum_cooldown,
            // The shape of the log files is well-known in the Kubernetes
            // environment, so we pick the a specially crafted fingerprinter
            // for the log files.
            fingerprinter: Fingerprinter {
                strategy: FingerprintStrategy::FirstLinesChecksum {
                    // Max line length to expect during fingerprinting, see the
                    // explanation above.
                    ignored_header_bytes: 0,
                    lines: fingerprint_lines,
                },
                max_line_length: max_line_bytes,
                ignore_not_found: true,
            },
            // We'd like to consume rotated pod log files first to release our file handle and let
            // the space be reclaimed
            oldest_first: true,
            // We do not remove the log files, `kubelet` is responsible for it.
            remove_after: None,
            // The standard emitter.
            emitter: FileSourceInternalEventsEmitter,
            // A handle to the current tokio runtime
            handle: tokio::runtime::Handle::current(),
        };

        let (file_source_tx, file_source_rx) = futures::channel::mpsc::channel::<Vec<Line>>(2);

        let mut parser = Parser::new(log_namespace);
        let partial_events_merger = Box::new(partial_events_merger::build(
            auto_partial_merge,
            log_namespace,
        ));

        let checkpoints = checkpointer.view();
        let events = file_source_rx.flat_map(futures::stream::iter);
        let bytes_received = register!(BytesReceived::from(Protocol::HTTP));
        let events = events.map(move |line| {
            let byte_size = line.text.len();
            bytes_received.emit(ByteSize(byte_size));

            let mut event = create_event(
                line.text,
                &line.filename,
                ingestion_timestamp_field.as_deref(),
                log_namespace,
            );
            let file_info = annotator.annotate(&mut event, &line.filename);

            emit!(KubernetesLogsEventsReceived {
                file: &line.filename,
                byte_size: event.estimated_json_encoded_size_of(),
                pod_info: file_info.as_ref().map(|info| KubernetesLogsPodInfo {
                    name: info.pod_name.to_owned(),
                    namespace: info.pod_namespace.to_owned(),
                }),
            });

            if file_info.is_none() {
                emit!(KubernetesLogsEventAnnotationError { event: &event });
            } else {
                let namespace = file_info.as_ref().map(|info| info.pod_namespace);

                if let Some(name) = namespace {
                    let ns_info = ns_annotator.annotate(&mut event, name);

                    if ns_info.is_none() {
                        emit!(KubernetesLogsEventNamespaceAnnotationError { event: &event });
                    }
                }

                let node_info = node_annotator.annotate(&mut event, self_node_name.as_str());

                if node_info.is_none() {
                    emit!(KubernetesLogsEventNodeAnnotationError { event: &event });
                }
            }

            checkpoints.update(line.file_id, line.end_offset);
            event
        });
        let events = events.flat_map(move |event| {
            let mut buf = OutputBuffer::with_capacity(1);
            parser.transform(&mut buf, event);
            futures::stream::iter(buf.into_events())
        });
        let (events_count, _) = events.size_hint();

        let mut stream = partial_events_merger.transform(Box::pin(events));
        let event_processing_loop = out.send_event_stream(&mut stream);

        let mut lifecycle = Lifecycle::new();
        {
            let (slot, shutdown) = lifecycle.add();
            let fut = util::run_file_server(file_server, file_source_tx, shutdown, checkpointer)
                .map(|result| match result {
                    Ok(FileServerShutdown) => info!(message = "File server completed gracefully."),
                    Err(error) => emit!(KubernetesLifecycleError {
                        message: "File server exited with an error.",
                        error,
                        count: events_count,
                    }),
                });
            slot.bind(Box::pin(fut));
        }
        {
            let (slot, shutdown) = lifecycle.add();
            let fut = util::complete_with_deadline_on_signal(
                event_processing_loop,
                shutdown,
                Duration::from_secs(30), // more than enough time to propagate
            )
            .map(|result| {
                match result {
                    Ok(Ok(())) => info!(message = "Event processing loop completed gracefully."),
                    Ok(Err(error)) => emit!(StreamClosedError {
                        error,
                        count: events_count
                    }),
                    Err(error) => emit!(KubernetesLifecycleError {
                        error,
                        message: "Event processing loop timed out during the shutdown.",
                        count: events_count,
                    }),
                };
            });
            slot.bind(Box::pin(fut));
        }

        lifecycle.run(global_shutdown).await;
        // Stop Kubernetes object reflectors to avoid their leak on vector reload.
        for reflector in reflectors {
            reflector.abort();
        }
        info!(message = "Done.");
        Ok(())
    }
}

fn create_event(
    line: Bytes,
    file: &str,
    ingestion_timestamp_field: Option<&str>,
    log_namespace: LogNamespace,
) -> Event {
    let deserializer = BytesDeserializer::new();
    let mut log = deserializer.parse_single(line, log_namespace);

    log_namespace.insert_source_metadata(
        Config::NAME,
        &mut log,
        Some(LegacyKey::Overwrite(path!("file"))),
        path!("file"),
        file,
    );

    log_namespace.insert_vector_metadata(
        &mut log,
        path!(log_schema().source_type_key()),
        path!("source_type"),
        Bytes::from(Config::NAME),
    );
    match (log_namespace, ingestion_timestamp_field) {
        // When using LogNamespace::Vector always set the ingest_timestamp.
        (LogNamespace::Vector, _) => {
            log.metadata_mut()
                .value_mut()
                .insert(path!("vector", "ingest_timestamp"), Utc::now());
        }
        // When LogNamespace::Legacy, only set when the `ingestion_timestamp_field` is configured.
        (LogNamespace::Legacy, Some(ingestion_timestamp_field)) => {
            log.try_insert((PathPrefix::Event, ingestion_timestamp_field), Utc::now())
        }
        // The CRI/Docker parsers handle inserting the `log_schema().timestamp_key()` value.
        (LogNamespace::Legacy, None) => (),
    };

    log.into()
}

/// This function returns the default value for `self_node_name` variable
/// as it should be at the generated config file.
fn default_self_node_name_env_template() -> String {
    format!("${{{}}}", SELF_NODE_NAME_ENV_KEY.to_owned())
}

fn default_path_exclusion() -> Vec<PathBuf> {
    vec![PathBuf::from("**/*.gz"), PathBuf::from("**/*.tmp")]
}

const fn default_max_read_bytes() -> usize {
    2048
}

const fn default_max_line_bytes() -> usize {
    // NOTE: The below comment documents an incorrect assumption, see
    // https://github.com/vectordotdev/vector/issues/6967
    //
    // The 16KB is the maximum size of the payload at single line for both
    // docker and CRI log formats.
    // We take a double of that to account for metadata and padding, and to
    // have a power of two rounding. Line splitting is countered at the
    // parsers, see the `partial_events_merger` logic.

    32 * 1024 // 32 KiB
}

const fn default_glob_minimum_cooldown_ms() -> usize {
    60_000
}

const fn default_fingerprint_lines() -> usize {
    1
}

const fn default_delay_deletion_ms() -> usize {
    60_000
}

// This function constructs the patterns we exclude from file watching, created
// from the defaults or user provided configuration.
fn prepare_exclude_paths(config: &Config) -> crate::Result<Vec<glob::Pattern>> {
    let exclude_paths = config
        .exclude_paths_glob_patterns
        .iter()
        .map(|pattern| {
            let pattern = pattern
                .to_str()
                .ok_or("glob pattern is not a valid UTF-8 string")?;
            Ok(glob::Pattern::new(pattern)?)
        })
        .collect::<crate::Result<Vec<_>>>()?;

    info!(
        message = "Excluding matching files.",
        exclude_paths = ?exclude_paths
            .iter()
            .map(glob::Pattern::as_str)
            .collect::<Vec<_>>()
    );

    Ok(exclude_paths)
}

// This function constructs the effective field selector to use, based on
// the specified configuration.
fn prepare_field_selector(config: &Config, self_node_name: &str) -> crate::Result<String> {
    info!(
        message = "Obtained Kubernetes Node name to collect logs for (self).",
        ?self_node_name
    );

    let field_selector = format!("spec.nodeName={}", self_node_name);

    if config.extra_field_selector.is_empty() {
        return Ok(field_selector);
    }

    Ok(format!(
        "{},{}",
        field_selector, config.extra_field_selector
    ))
}

// This function constructs the selector for a node to annotate entries with a node metadata.
fn prepare_node_selector(self_node_name: &str) -> crate::Result<String> {
    Ok(format!("metadata.name={}", self_node_name))
}

// This function constructs the effective label selector to use, based on
// the specified configuration.
fn prepare_label_selector(selector: &str) -> String {
    const BUILT_IN: &str = "vector.dev/exclude!=true";

    if selector.is_empty() {
        return BUILT_IN.to_string();
    }

    format!("{},{}", BUILT_IN, selector)
}

#[cfg(test)]
mod tests {
    use lookup::{owned_value_path, LookupBuf};
    use similar_asserts::assert_eq;
    use value::{kind::Collection, Kind};
    use vector_core::{config::LogNamespace, schema::Definition};

    use crate::config::SourceConfig;

    use super::Config;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<Config>();
    }

    #[test]
    fn prepare_exclude_paths() {
        let cases = vec![
            (
                Config::default(),
                vec![
                    glob::Pattern::new("**/*.gz").unwrap(),
                    glob::Pattern::new("**/*.tmp").unwrap(),
                ],
            ),
            (
                Config {
                    exclude_paths_glob_patterns: vec![std::path::PathBuf::from("**/*.tmp")],
                    ..Default::default()
                },
                vec![glob::Pattern::new("**/*.tmp").unwrap()],
            ),
            (
                Config {
                    exclude_paths_glob_patterns: vec![
                        std::path::PathBuf::from("**/kube-system_*/**"),
                        std::path::PathBuf::from("**/*.gz"),
                        std::path::PathBuf::from("**/*.tmp"),
                    ],
                    ..Default::default()
                },
                vec![
                    glob::Pattern::new("**/kube-system_*/**").unwrap(),
                    glob::Pattern::new("**/*.gz").unwrap(),
                    glob::Pattern::new("**/*.tmp").unwrap(),
                ],
            ),
        ];

        for (input, mut expected) in cases {
            let mut output = super::prepare_exclude_paths(&input).unwrap();
            expected.sort();
            output.sort();
            assert_eq!(expected, output, "expected left, actual right");
        }
    }

    #[test]
    fn prepare_field_selector() {
        let cases = vec![
            // We're not testing `Config::default()` or empty `self_node_name`
            // as passing env vars in the concurrent tests is difficult.
            (
                Config {
                    self_node_name: "qwe".to_owned(),
                    ..Default::default()
                },
                "spec.nodeName=qwe",
            ),
            (
                Config {
                    self_node_name: "qwe".to_owned(),
                    extra_field_selector: "".to_owned(),
                    ..Default::default()
                },
                "spec.nodeName=qwe",
            ),
            (
                Config {
                    self_node_name: "qwe".to_owned(),
                    extra_field_selector: "foo=bar".to_owned(),
                    ..Default::default()
                },
                "spec.nodeName=qwe,foo=bar",
            ),
        ];

        for (input, expected) in cases {
            let output = super::prepare_field_selector(&input, "qwe").unwrap();
            assert_eq!(expected, output, "expected left, actual right");
        }
    }

    #[test]
    fn prepare_label_selector() {
        let cases = vec![
            (
                Config::default().extra_label_selector,
                "vector.dev/exclude!=true",
            ),
            (
                Config::default().extra_namespace_label_selector,
                "vector.dev/exclude!=true",
            ),
            (
                Config {
                    extra_label_selector: "".to_owned(),
                    ..Default::default()
                }
                .extra_label_selector,
                "vector.dev/exclude!=true",
            ),
            (
                Config {
                    extra_namespace_label_selector: "".to_owned(),
                    ..Default::default()
                }
                .extra_namespace_label_selector,
                "vector.dev/exclude!=true",
            ),
            (
                Config {
                    extra_label_selector: "qwe".to_owned(),
                    ..Default::default()
                }
                .extra_label_selector,
                "vector.dev/exclude!=true,qwe",
            ),
            (
                Config {
                    extra_namespace_label_selector: "qwe".to_owned(),
                    ..Default::default()
                }
                .extra_namespace_label_selector,
                "vector.dev/exclude!=true,qwe",
            ),
        ];

        for (input, expected) in cases {
            let output = super::prepare_label_selector(&input);
            assert_eq!(expected, output, "expected left, actual right");
        }
    }

    #[test]
    fn test_output_schema_definition_vector_namespace() {
        let definition = toml::from_str::<Config>("")
            .unwrap()
            .outputs(LogNamespace::Vector)[0]
            .clone()
            .log_schema_definition
            .unwrap();

        assert_eq!(
            definition,
            Definition::new_with_default_metadata(Kind::bytes(), [LogNamespace::Vector])
                .with_metadata_field(&owned_value_path!("kubernetes_logs", "file"), Kind::bytes())
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "container_id"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "container_image"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "container_name"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "namespace_labels"),
                    Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "node_labels"),
                    Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_annotations"),
                    Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_ip"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_ips"),
                    Kind::array(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_labels"),
                    Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_name"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_namespace"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_node_name"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_owner"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "pod_uid"),
                    Kind::bytes().or_undefined(),
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "stream"),
                    Kind::bytes()
                )
                .with_metadata_field(
                    &owned_value_path!("kubernetes_logs", "timestamp"),
                    Kind::timestamp()
                )
                .with_metadata_field(&owned_value_path!("vector", "source_type"), Kind::bytes())
                .with_metadata_field(
                    &owned_value_path!("vector", "ingest_timestamp"),
                    Kind::timestamp()
                )
                .with_meaning(LookupBuf::root(), "message")
        )
    }

    #[test]
    fn test_output_schema_definition_legacy_namespace() {
        let definition = toml::from_str::<Config>("")
            .unwrap()
            .outputs(LogNamespace::Legacy)[0]
            .clone()
            .log_schema_definition
            .unwrap();

        assert_eq!(
            definition,
            Definition::new_with_default_metadata(
                Kind::object(Collection::empty()),
                [LogNamespace::Legacy]
            )
            .with_event_field(&owned_value_path!("file"), Kind::bytes(), None)
            .with_event_field(
                &owned_value_path!("message"),
                Kind::bytes(),
                Some("message")
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "container_id"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "container_image"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "container_name"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "namespace_labels"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "node_labels"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_annotations"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_ip"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_ips"),
                Kind::array(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_labels"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_name"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_namespace"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_node_name"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_owner"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(
                &owned_value_path!("kubernetes", "pod_uid"),
                Kind::bytes().or_undefined(),
                None
            )
            .with_event_field(&owned_value_path!("stream"), Kind::bytes(), None)
            .with_event_field(
                &owned_value_path!("timestamp"),
                Kind::timestamp(),
                Some("timestamp")
            )
            .with_event_field(&owned_value_path!("source_type"), Kind::bytes(), None)
        )
    }
}
