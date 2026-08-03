/*
Copyright 2021 The Kubernetes Authors.

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

package volumebinding

import (
	"math"

	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
)

// classResourceMap 按 StorageClass 名称聚合存储资源信息。
// key 为 StorageClass 名称，value 为该 StorageClass 对应的请求容量与可用容量。
type classResourceMap map[string]*StorageResource

// volumeCapacityScorer 是基于存储类资源信息计算节点得分的函数类型。
// 入参 classResourceMap 包含节点上各 StorageClass 的请求/可用容量，
// 返回值为该节点的最终评分（0~MaxNodeScore）。
type volumeCapacityScorer func(classResourceMap) int64

// buildScorerFunction 根据评分函数形状(FunctionShape）构建一个 volumeCapacityScorer。
//
// 评分逻辑说明：
//   - 首先基于传入的 scoringFunctionShape（若干"利用率-分数"采样点），
//     通过 helper.BuildBrokenLinearFunction 构造一条分段线性函数 rawScoringFunction；
//   - 对每个 StorageClass，计算其存储利用率 = requested / capacity * 100，
//     再代入 rawScoringFunction 得到该 StorageClass 的分数；
//   - 最终节点得分 = 所有 StorageClass 分数的算术平均值。
//
// 边界处理：
//   - capacity 为 0 或 requested 超过 capacity 时，按 100% 利用率打分（通常为最低分）；
//   - 没有任何 StorageClass 资源时，节点得分为 0。
//
// 在 alpha 阶段，所有 StorageClass 权重相同，均为 1。
func buildScorerFunction(scoringFunctionShape helper.FunctionShape) volumeCapacityScorer {
	// 第一步：根据采样点构建分段线性函数。
	// helper.BuildBrokenLinearFunction 会将相邻采样点用直线连接，
	// 输入利用率(0~100)，输出对应分数。
	rawScoringFunction := helper.BuildBrokenLinearFunction(scoringFunctionShape)

	// f 是单个 StorageClass 的评分函数。
	// 入参：requested 表示该 StorageClass 上 Pod 请求的存储总量，capacity 表示可用容量。
	f := func(requested, capacity int64) int64 {
		if capacity == 0 || requested > capacity {
			// 容量为 0 或请求量超过容量时，按满利用率（100%）打分，
			// 通常对应配置 shape 中的最低分（惩罚高利用率节点）。
			return rawScoringFunction(maxUtilization)
		}

		// 正常情况：计算利用率百分比并代入分段线性函数。
		// requested * 100 / capacity 即为利用率（0~100）。
		return rawScoringFunction(requested * maxUtilization / capacity)
	}

	// 返回最终的节点评分函数。
	// 该函数遍历节点上所有 StorageClass 的资源信息，分别打分后取平均值作为节点最终得分。
	return func(classResources classResourceMap) int64 {
		var nodeScore int64
		// alpha 阶段所有 StorageClass 权重相同，权重大小等于 StorageClass 个数。
		weightSum := len(classResources)
		if weightSum == 0 {
			// 没有任何存储类资源，直接返回 0 分。
			return 0
		}
		for _, resource := range classResources {
			// 对每个 StorageClass 计算单独得分并累加。
			classScore := f(resource.Requested, resource.Capacity)
			nodeScore += classScore
		}
		// 取所有 StorageClass 得分的算术平均值作为节点最终得分。
		// 使用 math.Round 做四舍五入，避免整数除法截断导致分数偏差。
		return int64(math.Round(float64(nodeScore) / float64(weightSum)))
	}
}
