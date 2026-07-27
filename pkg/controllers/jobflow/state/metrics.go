/*
Copyright 2025 The Volcano Authors.

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

package state

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"volcano.sh/volcano/pkg/controllers/util"
)

// Prometheus 指标定义
// 用于监控 JobFlow 的成功/失败状态变化，按 namespace 维度统计
var (
	// jobflowSucceedPhaseCount JobFlow 成功状态计数器
	// 当 JobFlow 从 Running 转为 Succeed 时递增
	jobflowSucceedPhaseCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: util.VolcanoSubSystemName,
			Name:      "jobflow_succeed_phase_count",
			Help:      "Number of jobflow succeed phase",
		}, []string{"jobflow_namespace"},
	)

	// jobflowFailedPhaseCount JobFlow 失败状态计数器
	// 当 JobFlow 从 Pending 或 Running 转为 Failed 时递增
	jobflowFailedPhaseCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: util.VolcanoSubSystemName,
			Name:      "jobflow_failed_phase_count",
			Help:      "Number of jobflow failed phase",
		}, []string{"jobflow_namespace"},
	)
)

// UpdateJobFlowSucceed 递增指定 namespace 的 JobFlow 成功计数器
// 在 runningState.Execute 中 JobFlow 转为 Succeed 时调用
func UpdateJobFlowSucceed(namespace string) {
	jobflowSucceedPhaseCount.WithLabelValues(namespace).Inc()
}

// UpdateJobFlowFailed 递增指定 namespace 的 JobFlow 失败计数器
// 在 pendingState.Execute 中 JobFlow 转为 Failed 时调用
func UpdateJobFlowFailed(namespace string) {
	jobflowFailedPhaseCount.WithLabelValues(namespace).Inc()
}

// DeleteJobFlowMetrics 删除指定 namespace 的 JobFlow 指标数据
// 用于清理已删除 namespace 的指标，避免指标泄漏
func DeleteJobFlowMetrics(namespace string) {
	jobflowSucceedPhaseCount.DeleteLabelValues(namespace)
	jobflowFailedPhaseCount.DeleteLabelValues(namespace)
}
