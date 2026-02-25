/*
Copyright The Kubernetes Authors.

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

package core

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
)

type ResourceFlavorUpdateWatcher interface {
	NotifyResourceFlavorUpdate(oldRF, newRF *kueue.ResourceFlavor)
}

// ResourceFlavorReconciler reconciles a ResourceFlavor object
type ResourceFlavorReconciler struct {
	logName     string
	qManager    *qcache.Manager
	cache       *schdcache.Cache
	client      client.Client
	cqUpdateCh  chan event.GenericEvent
	watchers    []ResourceFlavorUpdateWatcher
	roleTracker *roletracker.RoleTracker
}

var _ reconcile.Reconciler = (*ResourceFlavorReconciler)(nil)
var _ predicate.TypedPredicate[*kueue.ResourceFlavor] = (*ResourceFlavorReconciler)(nil)

func NewResourceFlavorReconciler(
	client client.Client,
	qMgr *qcache.Manager,
	cache *schdcache.Cache,
	roleTracker *roletracker.RoleTracker,
) *ResourceFlavorReconciler {
	return &ResourceFlavorReconciler{
		logName:     "resourceflavor-reconciler",
		cache:       cache,
		client:      client,
		qManager:    qMgr,
		cqUpdateCh:  make(chan event.GenericEvent, updateChBuffer),
		roleTracker: roleTracker,
	}
}

func (r *ResourceFlavorReconciler) logger() logr.Logger {
	return roletracker.WithReplicaRole(ctrl.Log.WithName(r.logName), r.roleTracker)
}

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=resourceflavors,verbs=get;list;watch;update;delete
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=resourceflavors/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=resourceflavors/status,verbs=get;update;patch

func (r *ResourceFlavorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var flavor kueue.ResourceFlavor
	if err := r.client.Get(ctx, req.NamespacedName, &flavor); err != nil {
		// we'll ignore not-found errors, since there is nothing to do.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log := ctrl.LoggerFrom(ctx)
	log.V(2).Info("Reconcile ResourceFlavor")

	if flavor.DeletionTimestamp.IsZero() {
		// Although we'll add the finalizer via webhook mutation now, this is still useful
		// as a fallback.
		if controllerutil.AddFinalizer(&flavor, kueue.ResourceInUseFinalizerName) {
			if err := r.client.Update(ctx, &flavor); err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
			log.V(5).Info("Added finalizer")
		}
		nodes, err := r.GetNodes(ctx, &flavor)
		if err != nil {
			log.Error(err, "Failed to get nodes for ResourceFlavor")
			return ctrl.Result{}, err
		}

		occupiedCPU, err := r.GetCPUOccupied(ctx, nodes)
		if err != nil {
			log.Error(err, "Failed to calculate occupied CPU")
			return ctrl.Result{}, err
		}

		totalCPU := r.GetCPUTotal(ctx, nodes)

		// Update Status
		flavor.Status.NodeNumber = int32(len(nodes))
		flavor.Status.CPUTotal = totalCPU.String()
		flavor.Status.CPUOccupied = occupiedCPU.String()
		if err := r.client.Status().Update(ctx, &flavor); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update ResourceFlavor status: %w", err)
		}
		return ctrl.Result{}, nil
	} else {
		if controllerutil.ContainsFinalizer(&flavor, kueue.ResourceInUseFinalizerName) {
			if cqs := r.cache.ClusterQueuesUsingFlavor(kueue.ResourceFlavorReference(flavor.Name)); len(cqs) != 0 {
				log.V(3).Info("resourceFlavor is still in use", "ClusterQueues", cqs)
				// We avoid to return error here to prevent backoff requeue, which is passive and wasteful.
				// Instead, we drive the removal of finalizer by ClusterQueue Update/Delete events
				// when resourceFlavor is no longer in use.
				return ctrl.Result{}, nil
			}

			controllerutil.RemoveFinalizer(&flavor, kueue.ResourceInUseFinalizerName)
			if err := r.client.Update(ctx, &flavor); err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
			log.V(5).Info("Removed finalizer")
		}
	}

	return ctrl.Result{}, nil
}

// mapPodToFlavors triggers a reconciliation for all ResourceFlavors when a Pod changes.
func (r *ResourceFlavorReconciler) mapPodToFlavors(ctx context.Context, obj client.Object) []reconcile.Request {
	var flavors kueue.ResourceFlavorList
	if err := r.client.List(ctx, &flavors); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, f := range flavors.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: f.Name},
		})
	}
	return requests
}

// GetNodes returns all nodes tied to the ResourceFlavor.
// If flavor.NodeLabels is empty, it returns all nodes in the cluster.
func (r *ResourceFlavorReconciler) GetNodes(ctx context.Context, flavor *kueue.ResourceFlavor) ([]corev1.Node, error) {
	var nodeList corev1.NodeList
	var listOpts []client.ListOption

	// If the flavor has node labels, use them to filter the nodes
	if len(flavor.Spec.NodeLabels) > 0 {
		listOpts = append(listOpts, client.MatchingLabels(flavor.Spec.NodeLabels))
	}

	if err := r.client.List(ctx, &nodeList, listOpts...); err != nil {
		return nil, err
	}
	return nodeList.Items, nil
}

// GetCPUTotal calculates the total allocatable CPU of all nodes tied to the flavor
func (r *ResourceFlavorReconciler) GetCPUTotal(ctx context.Context, nodes []corev1.Node) resource.Quantity {
	totalCPU := resource.NewMilliQuantity(0, resource.DecimalSI)
	for _, node := range nodes {
		if cpu := node.Status.Allocatable.Cpu(); cpu != nil {
			totalCPU.Add(*cpu)
		}
	}
	return *totalCPU
}

// GetCPUOccupied calculates the total CPU requested by Running Pods on the flavor's nodes
func (r *ResourceFlavorReconciler) GetCPUOccupied(ctx context.Context, nodes []corev1.Node) (resource.Quantity, error) {
	totalOccupied := resource.NewMilliQuantity(0, resource.DecimalSI)

	// Create a fast lookup map for our nodes
	nodeMap := make(map[string]bool)
	for _, n := range nodes {
		nodeMap[n.Name] = true
	}

	// List all pods
	var podList corev1.PodList
	if err := r.client.List(ctx, &podList); err != nil {
		return *totalOccupied, err
	}

	for _, pod := range podList.Items {
		// Only count running pods that are scheduled on the nodes belonging to this flavor
		if pod.Status.Phase == corev1.PodRunning && nodeMap[pod.Spec.NodeName] {
			for _, container := range pod.Spec.Containers {
				if req := container.Resources.Requests.Cpu(); req != nil {
					totalOccupied.Add(*req)
				}
			}
		}
	}
	return *totalOccupied, nil
}

func (r *ResourceFlavorReconciler) AddUpdateWatcher(watchers ...ResourceFlavorUpdateWatcher) {
	r.watchers = append(r.watchers, watchers...)
}

func (r *ResourceFlavorReconciler) notifyWatchers(oldRF, newRF *kueue.ResourceFlavor) {
	for _, w := range r.watchers {
		w.NotifyResourceFlavorUpdate(oldRF, newRF)
	}
}

func (r *ResourceFlavorReconciler) Create(e event.TypedCreateEvent[*kueue.ResourceFlavor]) bool {
	defer r.notifyWatchers(nil, e.Object)

	log := r.logger().WithValues("resourceFlavor", klog.KObj(e.Object))
	log.V(2).Info("ResourceFlavor create event")

	// As long as one clusterQueue becomes active,
	// we should inform clusterQueue controller to broadcast the event.
	if cqNames := r.cache.AddOrUpdateResourceFlavor(log, e.Object.DeepCopy()); len(cqNames) > 0 {
		qcache.QueueInadmissibleWorkloads(context.Background(), r.qManager, cqNames)
		// If at least one CQ becomes active, then those CQs should now get evaluated by the scheduler;
		// note that the workloads in those CQs are not necessarily "inadmissible", and hence we trigger a
		// broadcast here in all cases.
		r.qManager.Broadcast()
	}
	return true
}

func (r *ResourceFlavorReconciler) Delete(e event.TypedDeleteEvent[*kueue.ResourceFlavor]) bool {
	defer r.notifyWatchers(e.Object, nil)

	log := r.logger().WithValues("resourceFlavor", klog.KObj(e.Object))
	log.V(2).Info("ResourceFlavor delete event")

	if cqNames := r.cache.DeleteResourceFlavor(log, e.Object); len(cqNames) > 0 {
		qcache.QueueInadmissibleWorkloads(context.Background(), r.qManager, cqNames)
	}
	return false
}

func (r *ResourceFlavorReconciler) Update(e event.TypedUpdateEvent[*kueue.ResourceFlavor]) bool {
	defer r.notifyWatchers(e.ObjectOld, e.ObjectNew)

	log := r.logger().WithValues("resourceFlavor", klog.KObj(e.ObjectNew))
	log.V(2).Info("ResourceFlavor update event")

	if !e.ObjectNew.DeletionTimestamp.IsZero() {
		return true
	}

	if cqNames := r.cache.AddOrUpdateResourceFlavor(log, e.ObjectNew.DeepCopy()); len(cqNames) > 0 {
		qcache.QueueInadmissibleWorkloads(context.Background(), r.qManager, cqNames)
	}
	return false
}

func (r *ResourceFlavorReconciler) Generic(e event.TypedGenericEvent[*kueue.ResourceFlavor]) bool {
	r.logger().V(3).Info("Got ResourceFlavor generic event", "resourceFlavor", klog.KObj(e.Object))
	return true
}

// NotifyClusterQueueUpdate will listen for the update/delete events of clusterQueues to help
// verifying whether resourceFlavors are no longer in use by clusterQueues. There are mainly
// two reasons for this, 1) a clusterQueue is deleted 2) a clusterQueue is updated with
// the resourceFlavors in use.
func (r *ResourceFlavorReconciler) NotifyClusterQueueUpdate(oldCQ, newCQ *kueue.ClusterQueue) {
	// if oldCQ is nil, it's a create event.
	if oldCQ == nil {
		return
	}

	// if newCQ is nil, it's a delete event.
	if newCQ == nil {
		r.cqUpdateCh <- event.GenericEvent{Object: oldCQ}
		return
	}

	oldFlavors := resourceFlavors(oldCQ)
	newFlavors := resourceFlavors(newCQ)
	if !oldFlavors.Equal(newFlavors) {
		r.cqUpdateCh <- event.GenericEvent{Object: oldCQ}
	}
}

// cqHandler signals the controller to reconcile the resourceFlavor
// associated to the clusterQueue in the event.
// Since the events come from a channel Source, only the Generic handler will
// receive events.
type cqHandler struct {
	cache *schdcache.Cache
}

func (h *cqHandler) Create(context.Context, event.CreateEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *cqHandler) Update(context.Context, event.UpdateEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *cqHandler) Delete(context.Context, event.DeleteEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

// Generic accepts update/delete events from clusterQueue via channel.
// For update events, we only check the old obj to see whether old resourceFlavors
// are still in use since new resourceFlavors are always in use.
// For delete events, we check the original obj since new obj is nil.
func (h *cqHandler) Generic(_ context.Context, e event.GenericEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	cq := e.Object.(*kueue.ClusterQueue)
	if cq.Name == "" {
		return
	}

	for _, rg := range cq.Spec.ResourceGroups {
		for _, flavor := range rg.Flavors {
			if cqs := h.cache.ClusterQueuesUsingFlavor(flavor.Name); len(cqs) == 0 {
				req := reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name: string(flavor.Name),
					},
				}
				q.Add(req)
			}
		}
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ResourceFlavorReconciler) SetupWithManager(mgr ctrl.Manager, cfg *config.Configuration) error {
	h := cqHandler{
		cache: r.cache,
	}
	return builder.TypedControllerManagedBy[reconcile.Request](mgr).
		Named("resourceflavor_controller").
		WatchesRawSource(source.TypedKind(
			mgr.GetCache(),
			&kueue.ResourceFlavor{},
			&handler.TypedEnqueueRequestForObject[*kueue.ResourceFlavor]{},
			r,
		)).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.mapPodToFlavors),
		).
		WithOptions(controller.Options{
			NeedLeaderElection:      ptr.To(false),
			MaxConcurrentReconciles: mgr.GetControllerOptions().GroupKindConcurrency[kueue.GroupVersion.WithKind("ResourceFlavor").GroupKind().String()],
			LogConstructor:          roletracker.NewLogConstructor(r.roleTracker, "resourceflavor-reconciler"),
		}).
		WatchesRawSource(source.Channel(r.cqUpdateCh, &h)).
		Complete(WithLeadingManager(mgr, r, &kueue.ResourceFlavor{}, cfg))
}

func resourceFlavors(cq *kueue.ClusterQueue) sets.Set[kueue.ResourceFlavorReference] {
	flavors := sets.New[kueue.ResourceFlavorReference]()
	for _, rg := range cq.Spec.ResourceGroups {
		for _, flavor := range rg.Flavors {
			flavors.Insert(flavor.Name)
		}
	}
	return flavors
}
