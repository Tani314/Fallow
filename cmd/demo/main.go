// Command demo runs Fallow's escalation ladder against an in-memory cluster.
//
// It exists because the interesting behaviour happens over days: a workload
// goes quiet, gets a warning, gets scaled down, and is finally deleted with
// its manifest tucked away. Waiting that out is no way to review a
// controller, so the demo drives a fake clock and a fake API server and
// prints what the controller does at each step -- including the two paths
// that matter most, a workload that comes back to life and one that has to
// be recovered from its archive.
//
// Run it with: go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
	"github.com/Tani314/Fallow/internal/archive"
	"github.com/Tani314/Fallow/internal/controller"
	"github.com/Tani314/Fallow/internal/idle"
)

const policyName = "reclaim-dev"

// demoClock is the simulated wall clock the whole demo runs on.
type demoClock struct{ t time.Time }

func (d *demoClock) Now() time.Time { return d.t }

// scriptedDetector reports idleness per workload so the demo can have one
// workload come back to life while the others stay quiet.
type scriptedDetector struct{ busy map[string]string }

func (s *scriptedDetector) Probe(_ context.Context, d *appsv1.Deployment) (idle.Verdict, error) {
	if why, ok := s.busy[d.Name]; ok {
		return idle.Verdict{Idle: false, Reason: why}, nil
	}
	return idle.Verdict{
		Idle:   true,
		Reason: "no traffic, no rollout, cpu below threshold",
	}, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "demo failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := fallowv1alpha1.AddToScheme(scheme); err != nil {
		return err
	}

	start := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	clock := &demoClock{t: start}
	detector := &scriptedDetector{busy: map[string]string{}}
	recorder := record.NewFakeRecorder(2048)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster()...).
		WithStatusSubresource(&fallowv1alpha1.ReclaimPolicy{}).
		Build()

	archiver := archive.NewConfigMapArchiver(c)
	reconciler := &controller.ReclaimPolicyReconciler{
		Client:       c,
		Scheme:       scheme,
		Recorder:     recorder,
		Detector:     detector,
		Archiver:     archiver,
		Clock:        clock,
		ResyncPeriod: controller.DefaultResyncPeriod,
	}

	d := &demo{ctx: ctx, client: c, clock: clock, start: start, reconciler: reconciler, events: recorder.Events}

	d.banner("The cluster", `A ReclaimPolicy governs namespaces labelled fallow=enabled.
It notifies after 3h idle, scales to zero 21h later, and deletes 48h after that.
team-a is enrolled. prod is not labelled, so nothing in it is ever considered.`)
	d.printState()

	d.step("Hour 0 -- first look", `Everything in team-a is quiet. Fallow records when it first saw
each workload idle and does nothing else. The timestamp goes on the workload, not
in the controller's memory, so a restart changes nothing.`)

	d.advance(3 * time.Hour)
	d.step("Hour 3 -- the notice period begins", `The idle threshold has passed. Fallow annotates each
workload and emits an event. Replica counts are untouched: this rung exists purely to
give an owner the chance to object.`)

	d.advance(21 * time.Hour)
	d.step("Hour 24 -- scaled to zero", `A day of silence later, the workloads are scaled down. The
replica count each one was running at is saved on the object first, which is what makes
this reversible.`)

	detector.busy["data-explorer"] = "someone opened a dashboard: 14 requests in the last minute"
	d.advance(2 * time.Hour)
	d.step("Hour 26 -- someone comes back", `data-explorer served a request. Activity beats every deadline,
so it falls straight back to Active and returns to the exact size it was -- not to one replica,
and not to zero.`)

	d.advance(48 * time.Hour)
	d.step("Hour 74 -- the grace period runs out", `checkout-api never came back. Its manifest is written to a
ConfigMap and then it is deleted. data-explorer is still serving requests, so two days later it is
still Active and still at full size -- it never re-enters the ladder while anyone is using it.`)

	d.banner("Recovering a deleted workload", `The delete stage is only defensible because it is reversible.
The manifest is in a ConfigMap in the same namespace, under the same RBAC -- recovering it needs
nothing that is not already in the cluster.`)

	if err := d.recover("team-a", "checkout-api"); err != nil {
		return err
	}
	d.printState()

	d.banner("What the policy reports", "")
	return d.printStatus()
}

// cluster is the starting state: two idle workloads in an enrolled
// namespace, one opted out by its owner, and one in a namespace that was
// never enrolled at all.
func cluster() []client.Object {
	optedOut := deployment("team-a", "billing-worker", 2)
	optedOut.Annotations = map[string]string{fallowv1alpha1.AnnotationExclude: "true"}

	return []client.Object{
		namespace("team-a", map[string]string{"fallow": "enabled"}),
		namespace("prod", nil),
		deployment("team-a", "checkout-api", 3),
		deployment("team-a", "data-explorer", 2),
		optedOut,
		deployment("prod", "payments-gateway", 8),
		policy(),
	}
}

func policy() *fallowv1alpha1.ReclaimPolicy {
	return &fallowv1alpha1.ReclaimPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: policyName},
		Spec: fallowv1alpha1.ReclaimPolicySpec{
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"fallow": "enabled"}},
			IdleAfter:         metav1.Duration{Duration: 3 * time.Hour},
			Stages: fallowv1alpha1.ReclaimStages{
				Notify: &fallowv1alpha1.NotifyStage{
					Message: "this workload looks idle and Fallow will start reclaiming it",
				},
				ScaleToZero: &fallowv1alpha1.ScaleToZeroStage{After: metav1.Duration{Duration: 21 * time.Hour}},
				Delete:      &fallowv1alpha1.DeleteStage{After: metav1.Duration{Duration: 48 * time.Hour}},
			},
		},
	}
}

func namespace(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func deployment(ns, name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{"app": name}},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: name, Image: "registry.example.com/" + name + ":v1.4.2"},
				}},
			},
		},
	}
}

// demo carries the simulated cluster and prints what happens to it.
type demo struct {
	ctx        context.Context
	client     client.Client
	clock      *demoClock
	start      time.Time
	reconciler *controller.ReclaimPolicyReconciler
	events     chan string
}

func (d *demo) advance(by time.Duration) { d.clock.t = d.clock.t.Add(by) }

// step reconciles until the ladder settles, then reports what changed.
// Reconciling more than once is not a workaround: the controller climbs one
// rung per pass on purpose, and requeues itself for the next one.
func (d *demo) step(title, description string) {
	d.banner(title, description)
	for i := 0; i < 4; i++ {
		res, err := d.reconciler.Reconcile(d.ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: policyName},
		})
		if err != nil {
			fmt.Printf("  reconcile error: %v\n", err)
			break
		}
		if res.RequeueAfter > time.Second {
			break
		}
	}
	d.printEvents()
	d.printState()
}

func (d *demo) banner(title, description string) {
	fmt.Printf("\n\033[1m%s\033[0m\n", title)
	fmt.Println(strings.Repeat("-", len(title)))
	if description != "" {
		fmt.Println(description)
	}
}

// printEvents drains what the controller announced during the last step.
func (d *demo) printEvents() {
	var lines []string
	for {
		select {
		case e := <-d.events:
			// The recorder emits on both the workload and the policy; the
			// policy copy carries the namespace/name prefix, so it is the
			// one worth showing.
			if strings.Contains(e, "team-a/") {
				lines = append(lines, e)
			}
		default:
			if len(lines) == 0 {
				return
			}
			fmt.Println("\n  events:")
			for _, l := range lines {
				fmt.Printf("    %s\n", trimEvent(l))
			}
			return
		}
	}
}

func trimEvent(e string) string {
	for _, prefix := range []string{"Normal ", "Warning "} {
		e = strings.TrimPrefix(e, prefix)
	}
	return e
}

// printState prints the cluster as an operator would see it.
func (d *demo) printState() {
	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  NAMESPACE\tWORKLOAD\tREPLICAS\tSTAGE\tNOTE")

	for _, ns := range []string{"team-a", "prod"} {
		list := &appsv1.DeploymentList{}
		if err := d.client.List(d.ctx, list, client.InNamespace(ns)); err != nil {
			fmt.Fprintf(w, "  %s\t(listing failed: %v)\t\t\t\n", ns, err)
			continue
		}
		for i := range list.Items {
			item := &list.Items[i]
			stage := item.Annotations[fallowv1alpha1.AnnotationStage]
			if stage == "" {
				stage = string(fallowv1alpha1.StageActive)
			}
			fmt.Fprintf(w, "  %s\t%s\t%d\t%s\t%s\n",
				ns, item.Name, replicas(item), stage, note(ns, item))
		}
	}

	for _, gone := range d.deleted() {
		fmt.Fprintf(w, "  %s\t%s\t-\t%s\t%s\n", gone.Namespace, gone.Name, gone.Stage, gone.Reason)
	}
	w.Flush()
	fmt.Printf("\n  simulated time: %s elapsed\n", d.clock.t.Sub(d.start))
}

// deleted returns the workloads that only exist in the policy's status now.
func (d *demo) deleted() []fallowv1alpha1.TargetStatus {
	p := &fallowv1alpha1.ReclaimPolicy{}
	if err := d.client.Get(d.ctx, types.NamespacedName{Name: policyName}, p); err != nil {
		return nil
	}
	var gone []fallowv1alpha1.TargetStatus
	for _, t := range p.Status.Targets {
		if t.Stage != fallowv1alpha1.StageDeleted {
			continue
		}
		err := d.client.Get(d.ctx, types.NamespacedName{Namespace: t.Namespace, Name: t.Name}, &appsv1.Deployment{})
		if apierrors.IsNotFound(err) {
			gone = append(gone, t)
		}
	}
	return gone
}

func note(ns string, d *appsv1.Deployment) string {
	if d.Annotations[fallowv1alpha1.AnnotationExclude] == "true" {
		return "opted out with " + fallowv1alpha1.AnnotationExclude
	}
	if ns == "prod" {
		return "namespace not enrolled"
	}
	if original, ok := d.Annotations[fallowv1alpha1.AnnotationOriginalReplicas]; ok {
		return "will restore to " + original + " replicas"
	}
	if since, ok := d.Annotations[fallowv1alpha1.AnnotationIdleSince]; ok {
		return "idle since " + since
	}
	return ""
}

// recover restores a deleted workload from its archive.
func (d *demo) recover(ns, name string) error {
	archiveName := archive.ArchiveName(name)
	fmt.Printf("\n  $ kubectl -n %s get configmap %s -o jsonpath='{.data.%s}' | kubectl apply -f -\n",
		ns, archiveName, strings.ReplaceAll(archive.KeyManifest, ".", `\.`))

	restored, err := archive.NewConfigMapArchiver(d.client).Restore(d.ctx, ns, archiveName)
	if err != nil {
		return fmt.Errorf("recovering %s/%s: %w", ns, name, err)
	}
	fmt.Printf("  deployment.apps/%s created -- back at %d replicas, running %s\n",
		restored.Name, replicas(restored), restored.Spec.Template.Spec.Containers[0].Image)
	return nil
}

func (d *demo) printStatus() error {
	p := &fallowv1alpha1.ReclaimPolicy{}
	if err := d.client.Get(d.ctx, types.NamespacedName{Name: policyName}, p); err != nil {
		return err
	}

	fmt.Printf("\n  $ kubectl get reclaimpolicy %s\n", policyName)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  NAME\tIDLE AFTER\tNAMESPACES\tWORKLOADS\tRECLAIMED\tDRY RUN")
	fmt.Fprintf(w, "  %s\t%s\t%d\t%d\t%d\t%t\n",
		p.Name, p.Spec.IdleAfter.Duration, p.Status.EnrolledNamespaces,
		p.Status.ObservedWorkloads, p.Status.ReclaimedWorkloads, p.Spec.DryRun)
	w.Flush()

	fmt.Println("\n  status.targets:")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "    WORKLOAD\tSTAGE\tSINCE\tREASON")
	for _, t := range p.Status.Targets {
		since := "-"
		if t.IdleSince != nil {
			since = t.IdleSince.Time.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(tw, "    %s/%s\t%s\t%s\t%s\n", t.Namespace, t.Name, t.Stage, since, t.Reason)
	}
	tw.Flush()

	for _, c := range p.Status.Conditions {
		fmt.Printf("\n  condition %s=%s (%s): %s\n", c.Type, c.Status, c.Reason, c.Message)
	}
	fmt.Println()
	return nil
}

func replicas(d *appsv1.Deployment) int32 {
	if d.Spec.Replicas == nil {
		return 0
	}
	return *d.Spec.Replicas
}
