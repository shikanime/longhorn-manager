package controller

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	controllerAgentName = "Longhorn Kubernetes Pod Controller"
)

type KubernetesPodController struct {
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	pLister   listerv1.PodLister
	pvLister  listerv1.PersistentVolumeLister
	pvcLister listerv1.PersistentVolumeClaimLister

	pStoreSynced   cache.InformerSynced
	pvStoreSynced  cache.InformerSynced
	pvcStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface
}

func NewKubernetesPodController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubePodInformer coreinformers.PodInformer,
	kubePersistentVolumeInformer coreinformers.PersistentVolumeInformer,
	kubePersistentVolumeClaimInformer coreinformers.PersistentVolumeClaimInformer,
	kubeClient clientset.Interface,
	controllerID string) *KubernetesPodController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	kc := &KubernetesPodController{
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: controllerAgentName}),

		pLister:   kubePodInformer.Lister(),
		pvLister:  kubePersistentVolumeInformer.Lister(),
		pvcLister: kubePersistentVolumeClaimInformer.Lister(),

		pStoreSynced:   kubePodInformer.Informer().HasSynced,
		pvStoreSynced:  kubePersistentVolumeInformer.Informer().HasSynced,
		pvcStoreSynced: kubePersistentVolumeClaimInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(EnhancedDefaultControllerRateLimiter(), "longhorn-kubernetes-pod"),
	}

	kubePodInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			kc.enqueuePodChange(pod)
		},
		UpdateFunc: func(old, cur interface{}) {
			curPod := cur.(*v1.Pod)
			kc.enqueuePodChange(curPod)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			kc.enqueuePodChange(pod)
		},
	})

	return kc
}

func (kc *KubernetesPodController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer kc.queue.ShutDown()

	logrus.Infof("Start %v", controllerAgentName)
	defer logrus.Infof("Shutting down %v", controllerAgentName)

	if !controller.WaitForCacheSync(controllerAgentName, stopCh, kc.pStoreSynced, kc.pvStoreSynced, kc.pvcStoreSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(kc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (kc *KubernetesPodController) worker() {
	for kc.processNextWorkItem() {
	}
}

func (kc *KubernetesPodController) processNextWorkItem() bool {
	key, quit := kc.queue.Get()
	if quit {
		return false
	}
	defer kc.queue.Done(key)
	err := kc.syncHandler(key.(string))
	kc.handleErr(err, key)
	return true
}

func (kc *KubernetesPodController) handleErr(err error, key interface{}) {
	if err == nil {
		kc.queue.Forget(key)
		return
	}

	if kc.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("%v: Error syncing Longhorn kubernetes pod %v: %v", controllerAgentName, key, err)
		kc.queue.AddRateLimited(key)
		return
	}

	logrus.Warnf("%v: Dropping Longhorn kubernetes pod %v out of the queue: %v", controllerAgentName, key, err)
	kc.queue.Forget(key)
	utilruntime.HandleError(err)
}

func (kc *KubernetesPodController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync %v", controllerAgentName, key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	pod, err := kc.pLister.Pods(namespace).Get(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "Error getting Pod: %s", name)
	}
	nodeID := pod.Spec.NodeName
	if err := kc.handlePodDeletionIfNodeDown(pod, nodeID, namespace); err != nil {
		return err
	}

	return nil
}

// handlePodDeletionIfNodeDown determines whether we are allowed to forcefully delete a pod
// from a failed node based on the users chosen NodeDownPodDeletionPolicy.
// This is necessary because Kubernetes never forcefully deletes pods on a down node,
// the pods are stuck in terminating state forever and Longhorn volumes are not released.
// We provide an option for users to help them automatically force delete terminating pods
// of StatefulSet/Deployment on the downed node. By force deleting, k8s will detach Longhorn volumes
// and spin up replacement pods on a new node.
//
// Force delete a pod when all of the below conditions are meet:
// 1. NodeDownPodDeletionPolicy is different than DoNothing
// 2. pod belongs to a StatefulSet/Deployment depend on NodeDownPodDeletionPolicy
// 3. node containing the pod is down
// 4. the pod is terminating and the DeletionTimestamp has passed.
// 5. pod has a PV with provisioner driver.longhorn.io
func (kc *KubernetesPodController) handlePodDeletionIfNodeDown(pod *v1.Pod, nodeID string, namespace string) error {
	deletionPolicy := types.NodeDownPodDeletionPolicyDoNothing
	if deletionSetting, err := kc.ds.GetSettingValueExisted(types.SettingNameNodeDownPodDeletionPolicy); err == nil {
		deletionPolicy = types.NodeDownPodDeletionPolicy(deletionSetting)
	}

	shouldDelete := (deletionPolicy == types.NodeDownPodDeletionPolicyDeleteStatefulSetPod && isOwnedByStatefulSet(pod)) ||
		(deletionPolicy == types.NodeDownPodDeletionPolicyDeleteDeploymentPod && isOwnedByDeployment(pod)) ||
		(deletionPolicy == types.NodeDownPodDeletionPolicyDeleteBothStatefulsetAndDeploymentPod && (isOwnedByStatefulSet(pod) || isOwnedByDeployment(pod)))

	if !shouldDelete {
		return nil
	}

	isNodeDown, err := kc.ds.IsNodeDownOrDeleted(nodeID)
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate Node %v for pod %v in handlePodDeletionIfNodeDown", nodeID, pod.Name)
	}
	if !isNodeDown {
		return nil
	}

	if pod.DeletionTimestamp == nil || pod.DeletionTimestamp.After(time.Now()) {
		return nil
	}

	gracePeriod := int64(0)
	err = kc.kubeClient.CoreV1().Pods(namespace).Delete(pod.Name, &metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to forcefully delete Pod %v on the downed Node %v in handlePodDeletionIfNodeDown", pod.Name, nodeID)
	}
	logrus.Infof("%v: Forcefully deleted pod %v on downed node %v", controllerAgentName, pod.Name, nodeID)

	return nil
}

func isOwnedByStatefulSet(pod *v1.Pod) bool {
	if ownerRef := metav1.GetControllerOf(pod); ownerRef != nil {
		return ownerRef.Kind == types.KubernetesStatefulSet
	}
	return false
}

func isOwnedByDeployment(pod *v1.Pod) bool {
	if ownerRef := metav1.GetControllerOf(pod); ownerRef != nil {
		return ownerRef.Kind == types.KubernetesReplicaSet
	}
	return false
}

// enqueuePodChange determines if the pod requires processing based on whether the pod has a PV created by us (driver.longhorn.io)
func (kc *KubernetesPodController) enqueuePodChange(pod *v1.Pod) {
	key, err := controller.KeyFunc(pod)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", pod, err))
		return
	}

	for _, v := range pod.Spec.Volumes {
		if v.VolumeSource.PersistentVolumeClaim == nil {
			continue
		}

		pvc, err := kc.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(v.VolumeSource.PersistentVolumeClaim.ClaimName)
		if datastore.ErrorIsNotFound(err) {
			continue
		}
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", pvc, err))
			return
		}

		pv, err := kc.getAssociatedPersistentVolume(pvc)
		if datastore.ErrorIsNotFound(err) {
			continue
		}
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error getting Persistent Volume for PVC: %v", pvc))
			return
		}

		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == types.LonghornDriverName {
			kc.queue.AddRateLimited(key)
			break
		}

	}
}

func (kc *KubernetesPodController) getAssociatedPersistentVolume(pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolume, error) {
	pvName := pvc.Spec.VolumeName
	return kc.pvLister.Get(pvName)
}