/*
Copyright 2019 The Volcano Authors.

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

package helpers

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/apis"
	"volcano.sh/volcano/pkg/scheduler/api"
)

const (
	// PodNameFmt pod name format
	PodNameFmt = "%s-%s-%d"
	// persistentVolumeClaimFmt represents persistent volume claim name format
	persistentVolumeClaimFmt = "%s-pvc-%s"
	// OutOfSyncKey 是一个 Pod annotation key，表示该 Pod 已经和当前 Job 控制器期望状态不同步，应该被重启或删除重建。
	//
	// 当 Pod 被打上该 annotation 后，Job Controller 会认为它是“旧版本 Pod”或“失效 Pod”。
	// 对于带有该 annotation 的 Pod，Volcano Job Controller 会忽略它产生的一些 vcjob 事件，
	// 例如：
	//   - PodFailed
	//   - PodEvicted
	//
	// 这样做的目的：
	//   1. 避免旧 Pod 的失败、驱逐等事件影响当前 Job 状态机判断；
	//   2. 避免 Job 在重启、重试、缩容、删除 Pod 的过程中被旧事件反复触发；
	//   3. 防止 controller 因旧 Pod 事件进入重复重启或错误状态转换；
	//   4. 明确标记该 Pod 已经不再代表当前 Job 的有效运行状态。
	//
	// 典型使用场景：
	//   - killJob / killPods 删除 Pod 前，会先给 Pod patch 这个 annotation；
	//   - syncJob 发现某些 Pod 带有该 annotation 时，会把它们加入 podToDelete；
	//   - 事件处理逻辑遇到带有该 annotation 的 Pod 时，会忽略相关失败事件。
	//
	// annotation 示例：
	//   metadata:
	//     annotations:
	//       volcano.sh/controller-out-of-sync: "true"
	//
	// 简单理解：
	//   这个 key 就是告诉 controller：
	//   “这个 Pod 已经过期了，不要再根据它的事件影响 Job 状态，它应该被清理或重建。”
	OutOfSyncKey = "volcano.sh/controller-out-of-sync"
)

// GetPodIndexUnderTask returns task Index.
func GetPodIndexUnderTask(pod *v1.Pod) string {
	num := strings.Split(pod.Name, "-")
	if len(num) >= 3 {
		return num[len(num)-1]
	}

	return ""
}

// CompareTask by pod index
func CompareTask(lv, rv *api.TaskInfo) bool {
	lStr := GetPodIndexUnderTask(lv.Pod)
	rStr := GetPodIndexUnderTask(rv.Pod)
	lIndex, lErr := strconv.Atoi(lStr)
	rIndex, rErr := strconv.Atoi(rStr)
	if lErr != nil || rErr != nil || lIndex == rIndex {
		if lv.Pod.CreationTimestamp.Equal(&rv.Pod.CreationTimestamp) {
			return lv.UID < rv.UID
		}
		return lv.Pod.CreationTimestamp.Before(&rv.Pod.CreationTimestamp)
	}
	if lIndex > rIndex {
		return false
	}
	return true
}

// GetTaskKey returns task key/name
func GetTaskKey(pod *v1.Pod) string {
	if pod.Annotations == nil || pod.Annotations[batch.TaskSpecKey] == "" {
		return batch.DefaultTaskSpec
	}
	return pod.Annotations[batch.TaskSpecKey]
}

// GetTaskSpec returns task spec
func GetTaskSpec(job *batch.Job, taskName string) (batch.TaskSpec, bool) {
	for _, ts := range job.Spec.Tasks {
		if ts.Name == taskName {
			return ts, true
		}
	}
	return batch.TaskSpec{}, false
}

// MakeDomainName creates task domain name
func MakeDomainName(ts batch.TaskSpec, job *batch.Job, index int) string {
	hostName := ts.Template.Spec.Hostname
	subdomain := ts.Template.Spec.Subdomain
	if len(hostName) == 0 {
		hostName = MakePodName(job.Name, ts.Name, index)
	}
	if len(subdomain) == 0 {
		subdomain = job.Name
	}
	return hostName + "." + subdomain
}

// MakePodName creates pod name.
func MakePodName(jobName string, taskName string, index int) string {
	return fmt.Sprintf(PodNameFmt, jobName, taskName, index)
}

// GenRandomStr generate random str with specified length l.
func GenRandomStr(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	var result []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

// GenPVCName generates pvc name with job name.
func GenPVCName(jobName string) string {
	return fmt.Sprintf(persistentVolumeClaimFmt, jobName, GenRandomStr(12))
}

// GetJobKeyByReq gets the key for the job request.
func GetJobKeyByReq(req *apis.Request) string {
	return fmt.Sprintf("%s/%s", req.Namespace, req.JobName)
}

// GetTaskIndexUnderJob return index of the task in the job.
func GetTaskIndexUnderJob(taskName string, job *batch.Job) int {
	for index, task := range job.Spec.Tasks {
		if task.Name == taskName {
			return index
		}
	}
	return -1
}

// GetPodsNameUnderTask return names of all pods in the task.
func GetPodsNameUnderTask(taskName string, job *batch.Job) []string {
	var res []string
	for _, task := range job.Spec.Tasks {
		if task.Name == taskName {
			for index := 0; index < int(task.Replicas); index++ {
				res = append(res, MakePodName(job.Name, taskName, index))
			}
			break
		}
	}
	return res
}

// GetTaskIndexOfPod reads the task index from pod.Labels[batch.TaskIndex].
// This function is used by plugins to determine the task index of a pod within a job.
func GetTaskIndexOfPod(pod *v1.Pod) (int, error) {
	// Read task index from pod.Labels[batch.TaskIndex]
	taskIndexStr, exists := pod.Labels[batch.TaskIndex]
	if !exists {
		return -1, fmt.Errorf("pod %v doesn't have %v label", pod.Name, batch.TaskIndex)
	}
	taskIndex, err := strconv.Atoi(taskIndexStr)
	if err != nil {
		return -1, fmt.Errorf("failed to parse task index for pod %v: %v", pod.Name, err)
	}
	return taskIndex, nil
}

// GetTaskReplicasUnderJob return replicas of the task in the job.
func GetTaskReplicasUnderJob(taskName string, job *batch.Job) int32 {
	for _, task := range job.Spec.Tasks {
		if task.Name == taskName {
			return task.Replicas
		}
	}
	return 0
}

// IsOutOfSyncPod checks whether the pod is marked as out-of-sync.
func IsOutOfSyncPod(pod *v1.Pod) bool {
	if pod.Annotations == nil {
		return false
	}
	_, exists := pod.Annotations[OutOfSyncKey]
	return exists
}

// OutOfSyncJSONPatch generates a JSON patch to mark the pod as out-of-sync with the given reason.
func OutOfSyncJSONPatch() []byte {
	return []byte(fmt.Sprintf(`[{"op":"add","path":"/metadata/annotations/%s","value":"true"}]`,
		escapeJSONPointer(OutOfSyncKey)))
}

// escapeJSONPointer escapes a string for use in a JSON Pointer.
// See RFC 6901 for details: https://datatracker.ietf.org/doc/html/rfc6901
func escapeJSONPointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}
