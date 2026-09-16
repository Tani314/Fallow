// Command manager runs the Fallow controller.
//
// Everything policy-shaped lives in ReclaimPolicy objects rather than in
// flags here. The flags configure the one thing a CRD cannot: which signals
// count as evidence of idleness, since that depends on what a given cluster
// has instrumented.
package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
	"github.com/Tani314/Fallow/internal/archive"
	"github.com/Tani314/Fallow/internal/controller"
	"github.com/Tani314/Fallow/internal/idle"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilRuntimeMust(clientgoscheme.AddToScheme(scheme))
	utilRuntimeMust(fallowv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		lastActivityWindow   time.Duration
		rolloutQuietWindow   time.Duration
		resyncPeriod         time.Duration
		protectedNamespaces  string
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the health probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Run leader election so only one replica reclaims workloads at a time.")
	flag.DurationVar(&lastActivityWindow, "last-activity-window", 24*time.Hour,
		"How long a workload must go without recorded activity to look idle to the last-activity signal.")
	flag.DurationVar(&rolloutQuietWindow, "rollout-quiet-window", 7*24*time.Hour,
		"How long since the last rollout before a workload stops counting as work in progress.")
	flag.DurationVar(&resyncPeriod, "resync-period", controller.DefaultResyncPeriod,
		"Upper bound on how long the controller goes without re-examining a policy.")
	flag.StringVar(&protectedNamespaces, "protected-namespaces", "",
		"Comma-separated namespaces no policy may touch, on top of kube-* and Fallow's own namespace.")

	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "fallow.dev",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// The signals a cluster can actually supply decide what "idle" means
	// here. Both of these read from the workload itself, so Fallow has no
	// hard dependency on a metrics stack; adding idle.CPUBelow with a
	// metrics.k8s.io-backed MetricsProvider is the usual next step for a
	// cluster that has one.
	detector := idle.NewAllOf(idle.RealClock{},
		idle.LastActivity{Window: lastActivityWindow},
		idle.RolloutQuiet{Window: rolloutQuietWindow},
	)

	reconciler := &controller.ReclaimPolicyReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		Recorder:        mgr.GetEventRecorderFor("fallow"),
		Detector:        detector,
		Archiver:        archive.NewConfigMapArchiver(mgr.GetClient()),
		Clock:           idle.RealClock{},
		ResyncPeriod:    resyncPeriod,
		AlwaysProtected: alwaysProtected(protectedNamespaces),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up the reclaimpolicy controller")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting Fallow",
		"lastActivityWindow", lastActivityWindow,
		"rolloutQuietWindow", rolloutQuietWindow,
		"resyncPeriod", resyncPeriod,
		"protectedNamespaces", reconciler.AlwaysProtected,
	)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited with an error")
		os.Exit(1)
	}
}

// alwaysProtected combines the operator's own namespace with any the cluster
// admin named. Fallow protecting the namespace it runs in is not a nicety:
// a policy that matched it could scale down the controller mid-escalation
// and strand every workload at whatever rung it had reached.
func alwaysProtected(flagValue string) []string {
	protected := []string{}
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		protected = append(protected, ns)
	}
	for _, ns := range strings.Split(flagValue, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			protected = append(protected, ns)
		}
	}
	return protected
}

func utilRuntimeMust(err error) {
	if err != nil {
		setupLog.Error(err, "unable to build scheme")
		os.Exit(1)
	}
}
