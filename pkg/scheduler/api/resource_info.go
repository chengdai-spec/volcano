/*
Copyright 2017 The Kubernetes Authors.
Copyright 2017-2024 The Volcano Authors.

Modifications made by Volcano authors:
- Enhanced resource operations with comprehensive comparison and calculation functions
- Added resource dimension defaults and improved scalar resource handling

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

package api

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/scheduler/util"

	"volcano.sh/volcano/pkg/scheduler/util/assert"
)

const (
	// GPUResourceName GPU 资源名称。
	// 这里需要遵循 NVIDIA k8s-device-plugin 中使用的资源名约定。
	GPUResourceName = "nvidia.com/gpu"
)

const (
	// minResource 最小资源阈值。
	// 小于该值的资源通常可视为 0，也用作浮点比较时的误差容忍值。
	minResource float64 = 0.1
)

// DimensionDefaultValue 表示“某个资源维度未定义时”的默认值策略。
type DimensionDefaultValue int

const (
	// Zero 表示：未定义的资源维度按 0 处理。
	Zero DimensionDefaultValue = 0
	// Infinity 表示：未定义的资源维度按无穷大处理。
	// 这里内部用 -1 表示该语义。
	Infinity DimensionDefaultValue = -1
)

// Resource 结构体定义了所有资源类型。
type Resource struct {
	// MilliCPU 表示 CPU，单位为毫核（millicore）。
	MilliCPU float64

	// Memory 表示内存，通常为字节数。
	Memory float64

	// ScalarResources 表示标量资源，如 GPU、pods、ephemeral-storage、hugepages 等。
	ScalarResources map[v1.ResourceName]float64

	// MaxTaskNum 仅用于 predicates（谓词检查）；
	// 不应在其他资源运算（如 Add）中作为普通资源处理。
	MaxTaskNum int
}

// EmptyResource 创建并返回一个空资源对象。
func EmptyResource() *Resource {
	return &Resource{}
}

// InfiniteResource 创建并返回一个“无限资源”对象。
func InfiniteResource() *Resource {
	return &Resource{
		MilliCPU:   math.MaxFloat64,
		Memory:     math.MaxFloat64,
		MaxTaskNum: math.MaxInt,
	}
}

// NewResource 根据 Kubernetes 的 ResourceList 创建一个新的 Resource 对象。
func NewResource(rl v1.ResourceList) *Resource {
	r := EmptyResource()
	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			// CPU 按毫核存储
			r.MilliCPU += float64(rQuant.MilliValue())
		case v1.ResourceMemory:
			// Memory 按整数值（通常为字节）存储
			r.Memory += float64(rQuant.Value())
		case v1.ResourcePods:
			// pods 资源既用于 MaxTaskNum，也存入 scalar 中
			r.MaxTaskNum += int(rQuant.Value())
			r.AddScalar(rName, float64(rQuant.Value()))
		case v1.ResourceEphemeralStorage:
			// 临时存储按 MilliValue 存储
			r.AddScalar(rName, float64(rQuant.MilliValue()))
		default:
			// count/xxx 这类 quota 跳过
			if IsCountQuota(rName) {
				continue
			}
			// 注意：当转换回 k8s resource 时，除了 /1000 之外，还需要保留格式。
			if v1helper.IsScalarResourceName(rName) {
				ignore := false
				IgnoredDevicesList.Range(func(_ int, val string) bool {
					if rName.String() == val {
						ignore = true
						return false
					}
					return true
				})
				if !ignore {
					r.AddScalar(rName, float64(rQuant.MilliValue()))
				} else {
					klog.V(4).Infof("Ignoring resource %s", rName.String())
				}
			}
		}
	}
	return r
}

// ResFloat642Quantity 将 float64 类型资源值转换为 k8s 的 resource.Quantity。
func ResFloat642Quantity(resName v1.ResourceName, quantity float64) resource.Quantity {
	var resQuantity *resource.Quantity
	switch resName {
	case v1.ResourceCPU:
		// CPU 使用毫核格式
		resQuantity = resource.NewMilliQuantity(int64(quantity), resource.DecimalSI)
	default:
		// 其他资源使用普通数量格式
		resQuantity = resource.NewQuantity(int64(quantity), resource.BinarySI)
	}

	return *resQuantity
}

// ResQuantity2Float64 将 k8s 的 resource.Quantity 转换为 float64。
func ResQuantity2Float64(resName v1.ResourceName, quantity resource.Quantity) float64 {
	var resQuantity float64
	switch resName {
	case v1.ResourceCPU:
		// CPU 取 MilliValue
		resQuantity = float64(quantity.MilliValue())
	default:
		// 其他资源取 Value
		resQuantity = float64(quantity.Value())
	}

	return resQuantity
}

// Clone 深拷贝当前 Resource 对象。
func (r *Resource) Clone() *Resource {
	clone := &Resource{
		MilliCPU:   r.MilliCPU,
		Memory:     r.Memory,
		MaxTaskNum: r.MaxTaskNum,
	}

	if r.ScalarResources != nil {
		clone.ScalarResources = make(map[v1.ResourceName]float64)
		for k, v := range r.ScalarResources {
			clone.ScalarResources[k] = v
		}
	}

	return clone
}

// String 返回资源详情的字符串表示。
func (r *Resource) String() string {
	str := fmt.Sprintf("cpu %0.2f, memory %0.2f", r.MilliCPU, r.Memory)

	// 对 scalar 资源名排序，保证字符串输出稳定一致
	var resourceNames []string
	for rName := range r.ScalarResources {
		resourceNames = append(resourceNames, string(rName))
	}
	sort.Strings(resourceNames)

	for _, rName := range resourceNames {
		str = fmt.Sprintf("%s, %s %0.2f", str, rName, r.ScalarResources[v1.ResourceName(rName)])
	}
	return str
}

// ResourceNames 返回所有非零资源类型名称。
func (r *Resource) ResourceNames() ResourceNameList {
	resNames := ResourceNameList{}

	if r.MilliCPU >= minResource {
		resNames = append(resNames, v1.ResourceCPU)
	}

	if r.Memory >= minResource {
		resNames = append(resNames, v1.ResourceMemory)
	}

	for rName, rMount := range r.ScalarResources {
		if rMount >= minResource {
			resNames = append(resNames, rName)
		}
	}

	return resNames
}

// Get 根据资源名称返回对应资源值。
func (r *Resource) Get(rn v1.ResourceName) float64 {
	switch rn {
	case v1.ResourceCPU:
		return r.MilliCPU
	case v1.ResourceMemory:
		return r.Memory
	default:
		if r.ScalarResources == nil {
			return 0
		}
		return r.ScalarResources[rn]
	}
}

// 忽略检查 "pods" 资源。
// 目前所有 pod 都会请求一个 "pods" 资源，因此通常没必要额外检查它。
var ignoredScalarResources = sets.NewString(string(v1.ResourcePods))

// IsIgnoredScalarResource 判断一个 scalar 资源是否应被忽略。
func IsIgnoredScalarResource(name v1.ResourceName) bool {
	return ignoredScalarResources.Has(string(name))
}

// FilteredIgnoredScalarResources 返回一个新的 ResourceNameList，去掉所有被忽略的资源。
func (r ResourceNameList) FilteredIgnoredScalarResources() ResourceNameList {
	filtered := ResourceNameList{}
	for _, name := range r {
		if !ignoredScalarResources.Has(string(name)) {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

// IsEmpty 判断资源对象是否为空。
// 如果任意一种非忽略资源不小于最小阈值，则返回 false，否则返回 true。
func (r *Resource) IsEmpty() bool {
	if !(r.MilliCPU < minResource && r.Memory < minResource) {
		return false
	}

	for rName, rQuant := range r.ScalarResources {
		if IsIgnoredScalarResource(rName) {
			continue
		}
		if rQuant >= minResource {
			return false
		}
	}

	return true
}

// IsZero 判断指定资源维度是否为 0（小于 minResource 即视为 0）。
func (r *Resource) IsZero(rn v1.ResourceName) bool {
	switch rn {
	case v1.ResourceCPU:
		return r.MilliCPU < minResource
	case v1.ResourceMemory:
		return r.Memory < minResource
	default:
		if r.ScalarResources == nil {
			return true
		}

		_, found := r.ScalarResources[rn]
		assert.Assertf(found, "unknown resource %s", rn)

		return r.ScalarResources[rn] < minResource
	}
}

// Add 将 rr 加到当前资源对象 r 上。
func (r *Resource) Add(rr *Resource) *Resource {
	r.MilliCPU += rr.MilliCPU
	r.Memory += rr.Memory

	for rName, rQuant := range rr.ScalarResources {
		if r.ScalarResources == nil {
			r.ScalarResources = map[v1.ResourceName]float64{}
		}
		r.ScalarResources[rName] += rQuant
	}

	return r
}

// Sub 从当前资源对象 r 中减去 rr，并进行断言检查。
// 若资源不足，会触发断言失败。
func (r *Resource) Sub(rr *Resource) *Resource {
	assert.Assertf(rr.LessEqual(r, Zero), "resource is not sufficient to do operation: <%v> sub <%v>", r, rr)
	return r.sub(rr)
}

// SubWithoutAssert 从当前资源对象 r 中减去 rr，但不做断言。
// 如果资源不足允许出现负数，只会打印错误日志。
func (r *Resource) SubWithoutAssert(rr *Resource) *Resource {
	ok, resources := rr.LessEqualWithResourcesName(r, Zero)
	if !ok {
		klog.Errorf("resources <%v> are not sufficient to do operation: <%v> sub <%v>", resources, r, rr)
	}
	return r.sub(rr)
}

// sub 真正执行资源相减的内部函数。
func (r *Resource) sub(rr *Resource) *Resource {
	r.MilliCPU -= rr.MilliCPU
	r.Memory -= rr.Memory

	if r.ScalarResources == nil {
		return r
	}
	for rrName, rrQuant := range rr.ScalarResources {
		r.ScalarResources[rrName] -= rrQuant
	}

	return r
}

// Multi 将资源对象按给定比例进行缩放。
func (r *Resource) Multi(ratio float64) *Resource {
	r.MilliCPU *= ratio
	r.Memory *= ratio
	for rName, rQuant := range r.ScalarResources {
		r.ScalarResources[rName] = rQuant * ratio
	}
	return r
}

// SetMaxResource 与 rr 逐维比较，并将当前资源设置为每一维的最大值。
func (r *Resource) SetMaxResource(rr *Resource) {
	if r == nil || rr == nil {
		return
	}

	if rr.MilliCPU > r.MilliCPU {
		r.MilliCPU = rr.MilliCPU
	}
	if rr.Memory > r.Memory {
		r.Memory = rr.Memory
	}

	for rrName, rrQuant := range rr.ScalarResources {
		if r.ScalarResources == nil {
			r.ScalarResources = make(map[v1.ResourceName]float64)
			for k, v := range rr.ScalarResources {
				r.ScalarResources[k] = v
			}
			return
		}
		_, ok := r.ScalarResources[rrName]
		if !ok || rrQuant > r.ScalarResources[rrName] {
			r.ScalarResources[rrName] = rrQuant
		}
	}
}

// FitDelta 计算资源剩余差值。
// 当前对象 r 一般表示“可用资源”，rr 表示“请求资源”。
// 若某一维结果小于 0，则表示该维资源不足。
func (r *Resource) FitDelta(rr *Resource) *Resource {
	if rr.MilliCPU > 0 {
		r.MilliCPU -= rr.MilliCPU + minResource
	}

	if rr.Memory > 0 {
		r.Memory -= rr.Memory + minResource
	}

	if r.ScalarResources == nil {
		r.ScalarResources = make(map[v1.ResourceName]float64)
	}

	for rrName, rrQuant := range rr.ScalarResources {
		if rrQuant > 0 {
			_, ok := r.ScalarResources[rrName]
			if !ok {
				r.ScalarResources[rrName] = 0
			}
			r.ScalarResources[rrName] -= rrQuant + minResource
		}
	}

	return r
}

// Less 判断 r 是否在所有维度上都严格小于 rr。
// 否则返回 false。
// defaultValue 用于处理 ScalarResources 中未定义维度的默认值，取值只能是 Zero 或 Infinity。
func (r *Resource) Less(rr *Resource, defaultValue DimensionDefaultValue) bool {
	lessFunc := func(l, r float64) bool {
		return l < r
	}

	if !lessFunc(r.MilliCPU, rr.MilliCPU) {
		return false
	}
	if !lessFunc(r.Memory, rr.Memory) {
		return false
	}

	if defaultValue == Infinity {
		for name := range rr.ScalarResources {
			if _, ok := r.ScalarResources[name]; !ok {
				return false
			}
		}
	}

	for resourceName, leftValue := range r.ScalarResources {
		rightValue, ok := rr.ScalarResources[resourceName]
		if !ok && defaultValue == Infinity {
			continue
		}

		if !lessFunc(leftValue, rightValue) {
			return false
		}
	}
	return true
}

// LessEqual 判断 r 是否在所有维度上都小于等于 rr。
// 否则返回 false。
// defaultValue 用于处理 ScalarResources 中未定义维度的默认值，取值只能是 Zero 或 Infinity。
func (r *Resource) LessEqual(rr *Resource, defaultValue DimensionDefaultValue) bool {
	lessEqualFunc := func(l, r, diff float64) bool {
		if l < r || math.Abs(l-r) < diff {
			return true
		}
		return false
	}

	if !lessEqualFunc(r.MilliCPU, rr.MilliCPU, minResource) {
		return false
	}
	if !lessEqualFunc(r.Memory, rr.Memory, minResource) {
		return false
	}

	if defaultValue == Infinity {
		for name := range rr.ScalarResources {
			if _, ok := r.ScalarResources[name]; !ok {
				return false
			}
		}
	}

	for resourceName, leftValue := range r.ScalarResources {
		rightValue, ok := rr.ScalarResources[resourceName]
		if !ok && defaultValue == Infinity {
			continue
		}

		if !lessEqualFunc(leftValue, rightValue, minResource) {
			return false
		}
	}
	return true
}

// LessEqualWithDimensionAndResourcesName 只比较 req 中指定的资源维度。
// 返回 false 时，还会附带返回不足的资源名称列表。
// 如果 req 为 nil，则等价于 r.LessEqualWithResourcesName(rr, Zero)。
func (r *Resource) LessEqualWithDimensionAndResourcesName(rr *Resource, req *Resource) (bool, []string) {
	resources := []string{}
	if r == nil {
		return true, []string{}
	}
	if rr == nil {
		for _, name := range r.ResourceNames() {
			resources = append(resources, string(name))
		}
		return false, resources
	}
	if req == nil {
		return r.LessEqualWithResourcesName(rr, Zero)
	}

	if req.MilliCPU > 0 && r.MilliCPU > rr.MilliCPU {
		resources = append(resources, "cpu")
	}
	if req.Memory > 0 && r.Memory > rr.Memory {
		resources = append(resources, "memory")
	}

	// 如果 r.scalar 为 nil，则无论 rr.scalar 如何，r 都可以视作 <= rr
	if r.ScalarResources == nil {
		if len(resources) > 0 {
			return false, resources
		}
		return true, resources
	}

	for name, quant := range req.ScalarResources {
		if IsIgnoredScalarResource(name) {
			continue
		}
		rQuant := r.ScalarResources[name]
		rrQuant := rr.ScalarResources[name]
		if quant > 0 && rQuant > rrQuant {
			resources = append(resources, string(name))
		}
	}

	if len(resources) > 0 {
		return false, resources
	}
	return true, resources
}

// LessEqualWithResourcesName 判断 r 是否在所有维度上都小于等于 rr。
// 若不满足，返回 false 以及不足的资源名称列表。
// defaultValue 用于处理未定义 scalar 维度的默认值。
// 该函数与 LessEqual 逻辑相同，只是多返回资源名称列表，未来可能会与 LessEqual 合并。
func (r *Resource) LessEqualWithResourcesName(rr *Resource, defaultValue DimensionDefaultValue) (bool, []string) {
	resources := []string{}
	lessEqualFunc := func(l, r, diff float64) bool {
		if l < r || math.Abs(l-r) < diff {
			return true
		}
		return false
	}

	if !lessEqualFunc(r.MilliCPU, rr.MilliCPU, minResource) {
		resources = append(resources, "cpu")
	}
	if !lessEqualFunc(r.Memory, rr.Memory, minResource) {
		resources = append(resources, "memory")
	}

	for resourceName, leftValue := range r.ScalarResources {
		rightValue, ok := rr.ScalarResources[resourceName]
		if !ok && defaultValue == Infinity {
			continue
		}

		if !lessEqualFunc(leftValue, rightValue, minResource) {
			resources = append(resources, string(resourceName))
		}
	}
	if len(resources) > 0 {
		return false, resources
	}
	return true, resources
}

// LessPartly 判断是否存在任意一个维度，使得 r 在该维度上小于 rr。
// 只要存在一个维度满足条件，就返回 true；否则返回 false。
// defaultValue 用于处理未定义 scalar 维度的默认值。
func (r *Resource) LessPartly(rr *Resource, defaultValue DimensionDefaultValue) bool {
	lessFunc := func(l, r float64) bool {
		return l < r
	}

	if lessFunc(r.MilliCPU, rr.MilliCPU) || lessFunc(r.Memory, rr.Memory) {
		return true
	}

	if defaultValue == Zero {
		for name := range rr.ScalarResources {
			if _, ok := r.ScalarResources[name]; !ok {
				return true
			}
		}
	}

	for resourceName, leftValue := range r.ScalarResources {
		rightValue, ok := rr.ScalarResources[resourceName]
		if !ok && defaultValue == Infinity {
			return true
		}

		if lessFunc(leftValue, rightValue) {
			return true
		}
	}
	return false
}

// LessEqualPartly 判断是否存在任意一个维度，使得 r 在该维度上小于等于 rr。
// 只要存在一个维度满足条件，就返回 true；否则返回 false。
// defaultValue 用于处理未定义 scalar 维度的默认值。
func (r *Resource) LessEqualPartly(rr *Resource, defaultValue DimensionDefaultValue) bool {
	lessEqualFunc := func(l, r, diff float64) bool {
		if l < r || math.Abs(l-r) < diff {
			return true
		}
		return false
	}

	if lessEqualFunc(r.MilliCPU, rr.MilliCPU, minResource) || lessEqualFunc(r.Memory, rr.Memory, minResource) {
		return true
	}

	if defaultValue == Zero {
		for name := range rr.ScalarResources {
			if _, ok := r.ScalarResources[name]; !ok {
				return true
			}
		}
	}

	for resourceName, leftValue := range r.ScalarResources {
		rightValue, ok := rr.ScalarResources[resourceName]
		if !ok && defaultValue == Infinity {
			return true
		}

		if lessEqualFunc(leftValue, rightValue, minResource) {
			return true
		}
	}
	return false
}

// LessEqualPartlyWithDimension 在 req 指定的维度中，判断是否存在任意一个维度使得 r <= rr。
// 返回值为：
// 1. 是否存在满足条件的维度
// 2. 满足条件的资源名称列表
// 如果 req 为 nil，则返回 false 和空列表。
func (r *Resource) LessEqualPartlyWithDimension(rr *Resource, req *Resource) (bool, []string) {
	lessEqualFunc := func(l, r, diff float64) bool {
		return l < r || math.Abs(l-r) < diff
	}

	resources := []string{}
	found := false

	if req == nil {
		return false, resources
	}
	// CPU
	if req.MilliCPU > 0 {
		if lessEqualFunc(r.MilliCPU, rr.MilliCPU, minResource) {
			resources = append(resources, "cpu")
			found = true
		}
	}
	// Memory
	if req.Memory > 0 {
		if lessEqualFunc(r.Memory, rr.Memory, minResource) {
			resources = append(resources, "memory")
			found = true
		}
	}
	// Scalar resources
	for name, quant := range req.ScalarResources {
		if IsIgnoredScalarResource(name) {
			continue
		}
		if quant > 0 && lessEqualFunc(r.Get(name), rr.Get(name), minResource) {
			resources = append(resources, string(name))
			found = true
		}
	}
	return found, resources
}

// LessEqualPartlyWithDimensionZeroFiltered 会先过滤 req 中那些在 r 和 rr 中都为 0（或 nil）的维度，
// 然后再调用 LessEqualPartlyWithDimension 进行比较。
// 这在抢占场景中很有用：
// 如果某个维度虽然存在于 req 中，但当前资源和比较对象都没有使用该维度，则无需比较它。
// 返回值为：
// 1. 是否存在满足 r <= rr 的维度
// 2. 满足条件的资源名称列表
// 如果 req 为 nil，则返回 false 和空列表。
func (r *Resource) LessEqualPartlyWithDimensionZeroFiltered(rr *Resource, req *Resource) (bool, []string) {
	if req == nil {
		return false, []string{}
	}
	filteredReq := &Resource{}

	// CPU
	if req.MilliCPU > 0 && !(r.MilliCPU < minResource && rr.MilliCPU < minResource) {
		filteredReq.MilliCPU = req.MilliCPU
	}
	// Memory
	if req.Memory > 0 && !(r.Memory < minResource && rr.Memory < minResource) {
		filteredReq.Memory = req.Memory
	}
	// Scalar resources
	if req.ScalarResources != nil {
		filteredReq.ScalarResources = make(map[v1.ResourceName]float64)
		for name, quant := range req.ScalarResources {
			rQuant := r.Get(name)
			rrQuant := rr.Get(name)
			if quant > 0 && !(rQuant < minResource && rrQuant < minResource) {
				filteredReq.ScalarResources[name] = quant
			}
		}
	}

	return r.LessEqualPartlyWithDimension(rr, filteredReq)
}

// Equal 判断 r 与 rr 是否在所有维度上都相等（允许 minResource 范围内误差）。
// defaultValue 用于处理未定义 scalar 维度的默认值。
func (r *Resource) Equal(rr *Resource, defaultValue DimensionDefaultValue) bool {
	equalFunc := func(l, r, diff float64) bool {
		return l == r || math.Abs(l-r) < diff
	}

	if !equalFunc(r.MilliCPU, rr.MilliCPU, minResource) || !equalFunc(r.Memory, rr.Memory, minResource) {
		return false
	}

	for resourceName, leftValue := range r.ScalarResources {
		rightValue := rr.ScalarResources[resourceName]
		if !equalFunc(leftValue, rightValue, minResource) {
			return false
		}
	}
	return true
}

// GreaterPartly 判断是否存在任意一个维度，使得 r 在该维度上大于 rr。
// 返回：
// 1. 是否存在超出的维度
// 2. 超出的资源名称列表
func (r *Resource) GreaterPartly(rr *Resource, defaultValue DimensionDefaultValue) (bool, []string) {
	ok, resources := r.LessEqualWithResourcesName(rr, defaultValue)
	return !ok, resources
}

// GreaterPartlyWithDimension 在 req 指定的维度中，判断是否存在任意一个维度使得 r > rr。
// 返回：
// 1. 是否存在超出的维度
// 2. 超出的资源名称列表
//
// 该函数与 GreaterPartlyWithRelevantDimensions 的主要区别是：
// 后者会过滤掉 rr 中值为 0 或未定义的“无关维度”；
// 而本函数只要 req 指定了某维度，就会直接比较。
//
// @param rr 用于比较的目标 Resource；如果 rr 为 nil，则视为 EmptyResource()。
// @param req 需要比较的资源维度集合。
// 如果 req 为 nil，则返回 false 和空列表。
func (r *Resource) GreaterPartlyWithDimension(rr *Resource, req *Resource) (bool, []string) {
	greaterFunc := func(l, r float64) bool {
		return l > r
	}

	resources := []string{}

	if req == nil {
		return false, resources
	}
	if rr == nil {
		rr = EmptyResource()
	}

	// CPU
	if req.MilliCPU > 0 {
		if greaterFunc(r.MilliCPU, rr.MilliCPU) {
			resources = append(resources, "cpu")
		}
	}
	// Memory
	if req.Memory > 0 {
		if greaterFunc(r.Memory, rr.Memory) {
			resources = append(resources, "memory")
		}
	}
	// Scalar resources
	for name, quant := range req.ScalarResources {
		if IsIgnoredScalarResource(name) {
			continue
		}
		if quant > 0 && greaterFunc(r.Get(name), rr.Get(name)) {
			resources = append(resources, string(name))
		}
	}
	return len(resources) > 0, resources
}

// GreaterPartlyWithRelevantDimensions 在 req 指定的维度中比较 r 和 rr，
// 但会忽略 rr 中“不相关”的维度：
// 1. 对 CPU/Memory，如果 rr 对应维度为 0 或小于 minResource，则忽略
// 2. 对 scalar 资源，只有该维度同时存在于 req 和 rr 中时才比较
//
// 这在 reclaim（资源回收）场景中非常有用：
// 只检查 r 是否在 rr 实际拥有的资源维度上超量，而不是对所有 req 维度盲目比较。
//
// @param rr 用于比较的目标 Resource；如果 rr 为 nil，则视为 EmptyResource()。
// @param req 需要比较的资源维度集合。
// 如果 req 为 nil，则返回 false 和空列表。
func (r *Resource) GreaterPartlyWithRelevantDimensions(rr *Resource, req *Resource) (bool, []string) {
	if req == nil {
		return false, []string{}
	}
	if rr == nil {
		rr = EmptyResource()
	}
	filteredReq := &Resource{}

	// CPU：仅当 rr 的 CPU 有定义且大于 minResource 时才参与比较
	if req.MilliCPU > 0 && !(rr.MilliCPU < minResource) {
		filteredReq.MilliCPU = req.MilliCPU
	}

	// Memory：仅当 rr 的 Memory 有定义且大于 minResource 时才参与比较
	if req.Memory > 0 && !(rr.Memory < minResource) {
		filteredReq.Memory = req.Memory
	}

	// Scalar resources：只保留 req 中定义且 rr 也存在的维度
	if req.ScalarResources != nil {
		filteredReq.ScalarResources = make(map[v1.ResourceName]float64)
		for name, quant := range req.ScalarResources {
			_, ok := rr.ScalarResources[name]
			if quant > 0 && ok {
				filteredReq.ScalarResources[name] = quant
			}
		}
	}

	return r.GreaterPartlyWithDimension(rr, filteredReq)
}

// Diff 计算两个资源对象的差异。
// 返回两个 Resource：
// 1. increasedVal：左边 r 比右边 rr 多出来的部分
// 2. decreasedVal：右边 rr 比左边 r 多出来的部分
//
// 注意：如果 defaultValue 为 Infinity，则未定义维度的差值可能被视为 Infinity（内部用 -1 标记）。
func (r *Resource) Diff(rr *Resource, defaultValue DimensionDefaultValue) (*Resource, *Resource) {
	leftRes := r.Clone()
	rightRes := rr.Clone()
	increasedVal := EmptyResource()
	decreasedVal := EmptyResource()

	// 先把双方缺失的 scalar 维度按 defaultValue 补齐
	r.setDefaultValue(leftRes, rightRes, defaultValue)

	if leftRes.MilliCPU > rightRes.MilliCPU {
		increasedVal.MilliCPU = leftRes.MilliCPU - rightRes.MilliCPU
	} else {
		decreasedVal.MilliCPU = rightRes.MilliCPU - leftRes.MilliCPU
	}

	if leftRes.Memory > rightRes.Memory {
		increasedVal.Memory = leftRes.Memory - rightRes.Memory
	} else {
		decreasedVal.Memory = rightRes.Memory - leftRes.Memory
	}

	increasedVal.ScalarResources = make(map[v1.ResourceName]float64)
	decreasedVal.ScalarResources = make(map[v1.ResourceName]float64)
	for lName, lQuant := range leftRes.ScalarResources {
		rQuant := rightRes.ScalarResources[lName]
		if lQuant == float64(Infinity) {
			increasedVal.ScalarResources[lName] = lQuant
			continue
		}
		if rQuant == float64(Infinity) {
			decreasedVal.ScalarResources[lName] = rQuant
			continue
		}
		if lQuant > rQuant {
			increasedVal.ScalarResources[lName] = lQuant - rQuant
		} else {
			decreasedVal.ScalarResources[lName] = rQuant - lQuant
		}
	}

	return increasedVal, decreasedVal
}

// AddScalar 给某个 scalar 资源增加数量。
func (r *Resource) AddScalar(name v1.ResourceName, quantity float64) {
	r.SetScalar(name, r.ScalarResources[name]+quantity)
}

// SetScalar 直接设置某个 scalar 资源的值。
func (r *Resource) SetScalar(name v1.ResourceName, quantity float64) {
	// 延迟初始化 scalar 资源 map
	if r.ScalarResources == nil {
		r.ScalarResources = map[v1.ResourceName]float64{}
	}
	r.ScalarResources[name] = quantity
}

// MinDimensionResource 用 rr 逐维更新 r，使 r 的每个维度都取较小值。
// 例：
// r  = <cpu 2000, memory 4047845376, hugepages-2Mi 0, hugepages-1Gi 0>
// rr = <cpu 3000, memory 1000>
// 返回后 r = <cpu 2000, memory 1000, hugepages-2Mi 0, hugepages-1Gi 0>
//
// defaultValue 用于处理 rr 中缺失 scalar 维度时的默认策略：
// - Infinity：缺失维度不处理
// - Zero：缺失维度置 0
func (r *Resource) MinDimensionResource(rr *Resource, defaultValue DimensionDefaultValue) *Resource {
	if rr.MilliCPU < r.MilliCPU {
		r.MilliCPU = rr.MilliCPU
	}
	if rr.Memory < r.Memory {
		r.Memory = rr.Memory
	}

	if r.ScalarResources == nil {
		return r
	}

	if rr.ScalarResources == nil {
		if defaultValue == Infinity {
			return r
		}

		for name := range r.ScalarResources {
			r.ScalarResources[name] = 0
		}
		return r
	}

	for name, quant := range r.ScalarResources {
		rQuant, ok := rr.ScalarResources[name]
		if ok {
			r.ScalarResources[name] = math.Min(quant, rQuant)
		} else {
			if defaultValue == Infinity {
				continue
			}

			r.ScalarResources[name] = 0
		}
	}
	return r
}

// setDefaultValue 为左右两个 Resource 中未定义的 scalar 维度设置默认值。
// defaultValue 只能是 Zero 或 Infinity。
func (r *Resource) setDefaultValue(leftResource, rightResource *Resource, defaultValue DimensionDefaultValue) {
	if leftResource.ScalarResources == nil {
		leftResource.ScalarResources = map[v1.ResourceName]float64{}
	}
	if rightResource.ScalarResources == nil {
		rightResource.ScalarResources = map[v1.ResourceName]float64{}
	}

	// 补齐 rightResource 中缺失的维度
	for resourceName := range leftResource.ScalarResources {
		_, ok := rightResource.ScalarResources[resourceName]
		if !ok {
			rightResource.ScalarResources[resourceName] = float64(defaultValue)
		}
	}

	// 补齐 leftResource 中缺失的维度
	for resourceName := range rightResource.ScalarResources {
		_, ok := leftResource.ScalarResources[resourceName]
		if !ok {
			leftResource.ScalarResources[resourceName] = float64(defaultValue)
		}
	}
}

// ParseResourceList 将给定配置 map 解析为 Kubernetes 的 ResourceList。
// 若解析失败则返回错误。
func ParseResourceList(m map[string]string) (v1.ResourceList, error) {
	if len(m) == 0 {
		return nil, nil
	}

	rl := make(v1.ResourceList)
	for k, v := range m {
		switch v1.ResourceName(k) {
		// 当前仅支持 CPU、memory、ephemeral-storage
		case v1.ResourceCPU, v1.ResourceMemory, v1.ResourceEphemeralStorage:
			q, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, err
			}
			if q.Sign() == -1 {
				return nil, fmt.Errorf("resource quantity for %q cannot be negative: %v", k, v)
			}
			rl[v1.ResourceName(k)] = q
		default:
			return nil, fmt.Errorf("cannot reserve %q resource", k)
		}
	}
	return rl, nil
}

// GetMinResource 返回最小资源阈值。
func GetMinResource() float64 {
	return minResource
}

// ResourceNameList 定义资源名称列表类型。
type ResourceNameList []v1.ResourceName

// Contains 判断 rr 是否是 r 的子集（即 rr 中每个资源名都存在于 r 中）。
func (r ResourceNameList) Contains(rr ResourceNameList) bool {
	for _, rrName := range ([]v1.ResourceName)(rr) {
		isResourceExist := slices.Contains(([]v1.ResourceName)(r), rrName)
		if !isResourceExist {
			return false
		}
	}
	return true
}

// IsCountQuota 判断资源名是否为 count/ 前缀的 quota 资源。
func IsCountQuota(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), "count/")
}

// Intersection 返回两个 Resource 中都存在且非零的资源名称集合。
func Intersection(r1, r2 *Resource) ResourceNameList {
	intersection := ResourceNameList{}
	r1Names := r1.ResourceNames()
	r2Names := r2.ResourceNames()

	nameSet := map[v1.ResourceName]struct{}{}
	for _, name := range r1Names {
		nameSet[name] = struct{}{}
	}
	for _, name := range r2Names {
		if _, exists := nameSet[name]; exists {
			// r1Names 和 r2Names 是由 ResourceNames() 生成的，已经过滤掉零资源
			intersection = append(intersection, name)
		}
	}
	return intersection
}

// IntersectionWithIgnoredScalarResources 返回两个 Resource 中都存在且非零的资源名称集合，
// 同时忽略 ignoredScalarResources 中指定的 scalar 资源。
func IntersectionWithIgnoredScalarResources(r1, r2 *Resource) ResourceNameList {
	intersection := Intersection(r1, r2)
	return intersection.FilteredIgnoredScalarResources()
}

// ExceededPart 返回 left 中超出 right 的那一部分资源。
func ExceededPart(left, right *Resource) *Resource {
	if right == nil {
		return left
	}

	if left == nil {
		return EmptyResource()
	}

	diff, _ := left.Diff(right, Zero)
	return diff
}
