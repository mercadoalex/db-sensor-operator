/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/tls"
	"flag"
	"net/http"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/controller"
	"github.com/mercadoalex/db-sensor-operator/internal/dashboard"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
	"github.com/mercadoalex/db-sensor-operator/internal/events"
	"github.com/mercadoalex/db-sensor-operator/internal/health"
	"github.com/mercadoalex/db-sensor-operator/internal/inspection"
	"github.com/mercadoalex/db-sensor-operator/internal/metrics"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	// inspectionSelector matches the Jobs and pods of the database inspection and nothing else.
	inspectionSelector = labels.SelectorFromSet(labels.Set{inspection.ComponentLabel: inspection.ComponentValue})
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(sensoriov1.AddToScheme(scheme))

	// Register the DebeziumServer GVK as an unstructured type (Step 0 decision:
	// no importable Go module for the Debezium API). This lets
	// SetControllerReference resolve the owner's scheme and the unstructured
	// .Owns watch resolve the child kind.
	scheme.AddKnownTypeWithName(deployer.DebeziumServerGVK, &unstructured.Unstructured{})
	debeziumListGVK := deployer.DebeziumServerGVK
	debeziumListGVK.Kind += "List"
	scheme.AddKnownTypeWithName(debeziumListGVK, &unstructured.UnstructuredList{})

	// Register the Prometheus Operator monitoring/v1 types (ServiceMonitor,
	// PrometheusRule) so the client can create and own them (Task 5 Service/
	// ServiceMonitor; Task 10 PrometheusRule). Unlike Debezium, this API has an
	// importable, version-compatible Go module (Step 0 decision).
	utilruntime.Must(monitoringv1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var webhookPort int
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var operatorHTTPBindAddress string
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "Port the webhook server listens on. "+
		"Defaults to 9443. Set -1 to disable the webhook server.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&operatorHTTPBindAddress, "operator-http-bind-address", ":8080",
		"The address the operator HTTP server binds to. It serves /metrics (always on), "+
			"/dashboard and /api/events (when a sensor enables the dashboard), /cdc (HTTP CDC "+
			"ingestion), and /healthz,/readyz. Distinct from --metrics-bind-address and "+
			"--health-probe-bind-address. Empty disables the server (e.g. for tests).")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
		Port:    webhookPort,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		// Only the inspection's own Jobs and pods are cached. Without this the first read of a pod
		// would start an informer on every pod in the cluster.
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&batchv1.Job{}: {Label: inspectionSelector},
			&corev1.Pod{}:  {Label: inspectionSelector},
		}},
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "49ded652.sensor.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// Build the operator-wide event-pipeline components (DECISION 2 + Task 14/15
	// DECISION F): the Exporter uses its OWN private registry (not
	// controller-runtime's global registry) so the derived debezium_* metrics are
	// served as plain HTTP on the operator server's /metrics (ADR-04, always on),
	// not the manager's kube-RBAC-protected :8443 endpoint. The LagTracker feeds
	// status, and the RingBuffer records recent events for the dashboard. The
	// Pipeline fans each CDC event out to all three.
	exp := metrics.NewExporter(prometheus.NewRegistry())
	lag := health.NewLagTracker()
	buf := dashboard.NewRingBuffer(0, time.Now)
	gate := &dashboard.Gate{}
	pipeline := &events.Pipeline{Exporter: exp, Lag: lag, Recorder: buf}

	if err := (&controller.DatabaseSensorReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		// The inspection Job runs /inspector from this same image. Empty when the operator is not
		// deployed with the chart; an enabled inspection then says so instead of failing.
		InspectorImage:      os.Getenv("OPERATOR_IMAGE"),
		InspectorPullPolicy: corev1.PullPolicy(os.Getenv("OPERATOR_IMAGE_PULL_POLICY")),
		Exporter:            exp,
		Lag:                 lag,
		Dashboard:           gate,
		DashboardBuf:        buf,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "databasesensor")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	// Run the unified operator HTTP server as a manager Runnable (Task 15,
	// DECISION C/E). It mounts /metrics (exp.Handler(), always on), /dashboard and
	// /api/events (gated), /cdc (HTTP CDC ingestion, replacing the former
	// standalone :8081 consumer), and /healthz,/readyz. Added only when a bind
	// address is configured so an explicit empty value (or a test) can disable it;
	// the controller envtest suite never calls main, so it is unaffected.
	if operatorHTTPBindAddress != "" {
		consumer, err := events.NewConsumer(sensoriov1.TransportConfig{Type: "http"})
		if err != nil {
			setupLog.Error(err, "Failed to create CDC consumer")
			os.Exit(1)
		}
		var cdcHandler http.Handler
		if httpc, ok := consumer.(*events.HTTPConsumer); ok {
			cdcHandler = httpc.Handler(pipeline.Handle)
		}
		server := dashboard.NewServer(operatorHTTPBindAddress, exp.Handler(), cdcHandler, buf, gate)
		if err := mgr.Add(server); err != nil {
			setupLog.Error(err, "Failed to add operator HTTP server runnable")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
