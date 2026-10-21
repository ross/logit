// Command datadog-sketch writes the Datadog OTLP translator's own sketch for every histogram case
// in the Datadog sketch differential corpus (testdata/differential/datadog-sketch/README.md).
//
// Usage: datadog-sketch -out <dir>
//
// Each case is a delta OTLP data point, an explicit-bounds histogram or an exponential histogram,
// put through the translator the Collector's datadogexporter runs (`NewDefaultTranslator` with its
// default options, so `histograms.mode: distributions`) and read back from the `ConsumeSketch` call
// it makes. A point that yields no sketch is recorded with the reason the translator gives: the
// error `MapMetrics` returns, or the message it logs when it drops the point.
//
// Writes <out>/cases/<name>.json for every case, <out>/primitives.json with the Go `math` results
// the conversion depends on, and <out>/manifest.json naming the toolchain and module versions.
//
// Generation fails, writing nothing, on a CPU without AVX and FMA: on amd64, Go's `math.Exp` takes a
// different code path without them (`math/exp_amd64.s`), and the corpus records the FMA one.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"

	"github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/attributes"
	"github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics"
	"github.com/DataDog/datadog-agent/pkg/util/quantile"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/sys/cpu"
)

// The modules whose code the conversion runs, read from the build info for the manifest.
var pinnedModules = []string{
	"github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics",
	"github.com/DataDog/datadog-agent/pkg/util/quantile",
	"github.com/DataDog/sketches-go",
	"go.opentelemetry.io/collector/pdata",
}

// num is a float64 written as Go's shortest round-tripping form ("+Inf", "-Inf", and "NaN"
// included), so the reader recovers the exact bits.
type num float64

func (n num) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatFloat(float64(n), 'g', -1, 64))
}

func optNum(v *float64) *num {
	if v == nil {
		return nil
	}
	n := num(*v)
	return &n
}

func f(v float64) *float64 { return &v }

// explicitCase is an OTLP `HistogramDataPoint`: `counts` has one more entry than `bounds`, or none.
// `count` is the sum of `counts`, as logit's `Histogram` carries no count of its own.
type explicitCase struct {
	name     string
	bounds   []float64
	counts   []uint64
	sum      *float64
	min, max *float64
}

type buckets struct {
	Offset int32    `json:"offset"`
	Counts []uint64 `json:"counts"`
}

// expCase is an OTLP `ExponentialHistogramDataPoint`.
type expCase struct {
	name      string
	scale     int32
	zeroCount uint64
	positive  buckets
	negative  buckets
	count     uint64
	sum       *float64
	min, max  *float64
}

type explicitInput struct {
	Bounds []num    `json:"bounds"`
	Counts []uint64 `json:"counts"`
	Count  uint64   `json:"count"`
	Sum    *num     `json:"sum"`
	Min    *num     `json:"min"`
	Max    *num     `json:"max"`
}

type expInput struct {
	Scale     int32   `json:"scale"`
	ZeroCount uint64  `json:"zero_count"`
	Positive  buckets `json:"positive"`
	Negative  buckets `json:"negative"`
	Count     uint64  `json:"count"`
	Sum       *num    `json:"sum"`
	Min       *num    `json:"min"`
	Max       *num    `json:"max"`
}

type sketchOut struct {
	K   []int32  `json:"k"`
	N   []uint32 `json:"n"`
	Cnt int64    `json:"cnt"`
	Sum num      `json:"sum"`
	Min num      `json:"min"`
	Max num      `json:"max"`
	Avg num      `json:"avg"`
}

type reading struct {
	Name  string      `json:"name"`
	Kind  string      `json:"kind"`
	Input interface{} `json:"input"`
	// Outcome is `sketch`, or why there is none: `empty`, `too_many_observations`,
	// `non_monotonic_bounds`, or `unconvertible`.
	Outcome string     `json:"outcome"`
	Detail  string     `json:"detail,omitempty"`
	Sketch  *sketchOut `json:"sketch,omitempty"`
}

type consumer struct{ sketches []*quantile.Sketch }

func (c *consumer) ConsumeTimeSeries(context.Context, *metrics.Dimensions, metrics.DataType, uint64, int64, float64) {
}
func (c *consumer) ConsumeSketch(_ context.Context, _ *metrics.Dimensions, _ uint64, _ int64, s *quantile.Sketch) {
	c.sketches = append(c.sketches, s)
}
func (c *consumer) ConsumeExplicitBoundHistogram(context.Context, *metrics.Dimensions, pmetric.HistogramDataPointSlice) {
}
func (c *consumer) ConsumeExponentialHistogram(context.Context, *metrics.Dimensions, pmetric.ExponentialHistogramDataPointSlice) {
}

const timestamp = pcommon.Timestamp(1_700_000_000_000_000_000)

// translate runs one metric through a fresh translator, so no case sees another's state.
func translate(build func(pmetric.Metric)) (string, string, *sketchOut) {
	core, logs := observer.New(zapcore.DebugLevel)
	set := componenttest.NewNopTelemetrySettings()
	set.Logger = zap.New(core)
	attrs, err := attributes.NewTranslator(set)
	if err != nil {
		fail(err)
	}
	tr, err := metrics.NewDefaultTranslator(set, attrs)
	if err != nil {
		fail(err)
	}
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("case")
	build(m)
	c := &consumer{}
	_, err = tr.MapMetrics(context.Background(), md, c, nil)
	if err != nil {
		if strings.Contains(err.Error(), quantile.ErrNonMonotonicBoundaries) {
			return "non_monotonic_bounds", err.Error(), nil
		}
		fail(fmt.Errorf("unclassified MapMetrics error: %w", err))
	}
	switch len(c.sketches) {
	case 0:
	case 1:
		s := c.sketches[0]
		k, n := s.Cols()
		if k == nil {
			k, n = []int32{}, []uint32{}
		}
		cnt, min, max, sum, avg := s.BasicStats()
		return "sketch", "", &sketchOut{K: k, N: n, Cnt: cnt, Sum: num(sum), Min: num(min), Max: num(max), Avg: num(avg)}
	default:
		fail(errors.New("more than one sketch from one data point"))
	}
	for _, e := range logs.All() {
		msg := e.Message
		for _, field := range e.Context {
			if field.Key == "error" {
				msg += ": " + field.Interface.(error).Error()
			}
		}
		switch {
		case strings.HasPrefix(e.Message, "Dropped histogram data point: too many observations"):
			return "too_many_observations", msg, nil
		case strings.Contains(msg, "couldn't compute maximum of ddsketch"):
			// GetMaxValue fails only on a sketch holding nothing.
			return "empty", msg, nil
		case strings.HasPrefix(e.Message, "Failed to convert"):
			return "unconvertible", msg, nil
		}
	}
	// An explicit histogram whose buckets insert nothing: `Agent.Finish` returns nil, silently.
	return "empty", "", nil
}

func runExplicit(c explicitCase) reading {
	var count uint64
	for _, n := range c.counts {
		count += n
	}
	outcome, detail, sketch := translate(func(m pmetric.Metric) {
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		dp := h.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetCount(count)
		dp.ExplicitBounds().FromRaw(c.bounds)
		dp.BucketCounts().FromRaw(c.counts)
		if c.sum != nil {
			dp.SetSum(*c.sum)
		}
		if c.min != nil {
			dp.SetMin(*c.min)
		}
		if c.max != nil {
			dp.SetMax(*c.max)
		}
	})
	bounds := make([]num, len(c.bounds))
	for i, b := range c.bounds {
		bounds[i] = num(b)
	}
	counts := c.counts
	if counts == nil {
		counts = []uint64{}
	}
	return reading{
		Name: c.name, Kind: "explicit",
		Input:   explicitInput{Bounds: bounds, Counts: counts, Count: count, Sum: optNum(c.sum), Min: optNum(c.min), Max: optNum(c.max)},
		Outcome: outcome, Detail: detail, Sketch: sketch,
	}
}

func runExp(c expCase) reading {
	outcome, detail, sketch := translate(func(m pmetric.Metric) {
		h := m.SetEmptyExponentialHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		dp := h.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetScale(c.scale)
		dp.SetZeroCount(c.zeroCount)
		dp.Positive().SetOffset(c.positive.Offset)
		dp.Positive().BucketCounts().FromRaw(c.positive.Counts)
		dp.Negative().SetOffset(c.negative.Offset)
		dp.Negative().BucketCounts().FromRaw(c.negative.Counts)
		dp.SetCount(c.count)
		if c.sum != nil {
			dp.SetSum(*c.sum)
		}
		if c.min != nil {
			dp.SetMin(*c.min)
		}
		if c.max != nil {
			dp.SetMax(*c.max)
		}
	})
	pos, neg := c.positive, c.negative
	if pos.Counts == nil {
		pos.Counts = []uint64{}
	}
	if neg.Counts == nil {
		neg.Counts = []uint64{}
	}
	return reading{
		Name: c.name, Kind: "exponential",
		Input: expInput{Scale: c.scale, ZeroCount: c.zeroCount, Positive: pos, Negative: neg,
			Count: c.count, Sum: optNum(c.sum), Min: optNum(c.min), Max: optNum(c.max)},
		Outcome: outcome, Detail: detail, Sketch: sketch,
	}
}

// splitmix64, so the seeded cases depend on no library's generator.
type rng struct{ s uint64 }

func (r *rng) next() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
func (r *rng) intn(n int) int        { return int(r.next() % uint64(n)) }
func (r *rng) unit() float64         { return float64(r.next()>>11) / (1 << 53) }
func (r *rng) chance(p float64) bool { return r.unit() < p }

// sketchCap is `quantile.Default().MaxCount()`, the most observations one sketch may hold.
var sketchCap = uint64(quantile.Default().MaxCount())

func explicitCases() []explicitCase {
	powers := func(from, to, step int) []float64 {
		var out []float64
		for e := from; e <= to; e += step {
			out = append(out, math.Pow(10, float64(e)))
		}
		return out
	}
	repeat := func(n int, v uint64) []uint64 {
		out := make([]uint64, n)
		for i := range out {
			out[i] = v
		}
		return out
	}
	cases := []explicitCase{
		{name: "no-buckets", sum: f(0)},
		{name: "no-buckets-min-max", sum: f(12), min: f(1), max: f(9)},
		{name: "all-zero-counts", bounds: []float64{1, 2, 5}, counts: []uint64{0, 0, 0, 0}, sum: f(0)},
		{name: "single-bucket", counts: []uint64{7}, sum: f(21)},
		{name: "single-bucket-min-max", counts: []uint64{7}, sum: f(21), min: f(0.5), max: f(9)},
		{name: "single-bucket-one", counts: []uint64{1}, sum: f(3.25)},
		{name: "typical", bounds: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 1000}, counts: []uint64{0, 3, 8, 21, 30, 12, 9, 4, 2, 1, 0}, sum: f(3011.5), min: f(1.5), max: f(780)},
		{name: "typical-no-min-max", bounds: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 1000}, counts: []uint64{0, 3, 8, 21, 30, 12, 9, 4, 2, 1, 0}, sum: f(3011.5)},
		{name: "open-first", bounds: []float64{1, 2, 5, 10}, counts: []uint64{40, 0, 0, 0, 0}, sum: f(20)},
		{name: "open-last", bounds: []float64{1, 2, 5, 10}, counts: []uint64{0, 0, 0, 0, 40}, sum: f(800)},
		{name: "open-both", bounds: []float64{1, 2, 5, 10}, counts: []uint64{6, 1, 2, 3, 9}, sum: f(250)},
		{name: "open-both-min-max", bounds: []float64{1, 2, 5, 10}, counts: []uint64{6, 1, 2, 3, 9}, sum: f(250), min: f(0.25), max: f(40)},
		{name: "open-last-max-below-bound", bounds: []float64{10}, counts: []uint64{0, 3}, sum: f(15), max: f(5)},
		{name: "open-first-min-above-bound", bounds: []float64{10}, counts: []uint64{3, 0}, sum: f(45), min: f(15)},
		{name: "zero-lower", bounds: []float64{0, 5, 10}, counts: []uint64{0, 7, 3, 0}, sum: f(40)},
		{name: "zero-lower-only", bounds: []float64{0, 5}, counts: []uint64{0, 12, 0}, sum: f(30)},
		{name: "zero-upper", bounds: []float64{-5, 0, 5}, counts: []uint64{0, 6, 4, 0}, sum: f(-3)},
		{name: "zero-bound-only", bounds: []float64{0}, counts: []uint64{4, 5}, sum: f(10)},
		{name: "negative-bounds", bounds: []float64{-100, -10, -5, -1}, counts: []uint64{2, 7, 11, 5, 3}, sum: f(-200)},
		{name: "negative-to-positive", bounds: []float64{-10, -5, -1, 0, 1, 5, 10}, counts: []uint64{1, 4, 9, 20, 18, 7, 3, 1}, sum: f(0.5), min: f(-12), max: f(11)},
		{name: "huge-counts", bounds: []float64{1, 10, 100}, counts: []uint64{100000, 70000, 1 << 20, 65535}, sum: f(1e8)},
		{name: "one-bucket-over-uint16", bounds: []float64{3}, counts: []uint64{0, 200000}, sum: f(1e6)},
		{name: "point-mass-over-uint16", bounds: []float64{4, 4.0000001}, counts: []uint64{0, 131071, 0}, sum: f(524284)},
		{name: "at-observation-cap", bounds: []float64{1, 2}, counts: []uint64{0, sketchCap, 0}, sum: f(4e8)},
		{name: "over-observation-cap", bounds: []float64{1, 2}, counts: []uint64{0, sketchCap, 1}, sum: f(4e8)},
		{name: "over-observation-cap-one-bucket", bounds: []float64{1, 2}, counts: []uint64{0, sketchCap + 1, 0}, sum: f(4e8)},
		{name: "non-monotonic", bounds: []float64{5, 1}, counts: []uint64{0, 3, 0}, sum: f(9)},
		{name: "non-monotonic-after-cap", bounds: []float64{1, 2, 0.5}, counts: []uint64{0, sketchCap + 1, 1, 0}, sum: f(9)},
		{name: "non-monotonic-same-key", bounds: []float64{1, 0.9999999}, counts: []uint64{0, 3, 0}, sum: f(3)},
		{name: "sum-nan", bounds: []float64{1, 10, 100}, counts: []uint64{2, 30, 50, 4}, sum: f(math.NaN())},
		{name: "sum-inf", bounds: []float64{1, 10, 100}, counts: []uint64{2, 30, 50, 4}, sum: f(math.Inf(1)), min: f(0.5), max: f(150)},
		{name: "sum-unset", bounds: []float64{1, 10, 100}, counts: []uint64{2, 30, 50, 4}},
		{name: "huge-finite-bounds", bounds: []float64{1e300, 1e301}, counts: []uint64{0, 5, 0}, sum: f(5e300)},
		{name: "huge-finite-open-last", bounds: []float64{1e300}, counts: []uint64{0, 5}, sum: f(5e300)},
		{name: "tiny-bounds", bounds: []float64{1e-12, 1e-10}, counts: []uint64{0, 9, 0}, sum: f(1e-10)},
		{name: "straddle-agent-min", bounds: []float64{1e-10, 1e-8}, counts: []uint64{0, 1000, 0}, sum: f(5e-6)},
		{name: "min-max-inverted", bounds: []float64{1, 2}, counts: []uint64{0, 4, 0}, sum: f(6), min: f(1.9), max: f(1.1)},
		{name: "min-only", bounds: []float64{1, 2, 4}, counts: []uint64{0, 4, 4, 0}, sum: f(18), min: f(1.2)},
		{name: "max-only", bounds: []float64{1, 2, 4}, counts: []uint64{0, 4, 4, 0}, sum: f(18), max: f(3.7)},
		{name: "remainder-1000", bounds: []float64{1, 1000}, counts: []uint64{0, 1000, 0}, sum: f(5e5)},
		{name: "remainder-3", bounds: []float64{1, 1000}, counts: []uint64{0, 3, 0}, sum: f(1500)},
		{name: "remainder-narrow", bounds: []float64{100, 103}, counts: []uint64{0, 1001, 0}, sum: f(101500)},
		{name: "fine-buckets", bounds: []float64{1, 1.01, 1.02, 1.03, 1.04, 1.05, 1.06}, counts: []uint64{0, 5, 6, 7, 8, 9, 10, 0}, sum: f(46.5)},
		{name: "trim-left", bounds: powers(0, 210, 10), counts: append([]uint64{0}, repeat(22, 1_000_000)...), sum: f(1e215)},
		{name: "trim-left-overflow", bounds: powers(-9, 201, 10), counts: append([]uint64{60000}, append(repeat(21, 1_000_000), 70000)...), sum: f(1e215)},
		{name: "decades", bounds: powers(-3, 6, 1), counts: []uint64{1, 3, 10, 30, 100, 300, 1000, 300, 100, 30, 10}, sum: f(1e6)},
	}

	r := &rng{s: 0x5eed0001}
	for i := 0; i < 40; i++ {
		n := 1 + r.intn(12)
		start := (r.unit() - 0.3) * math.Pow(10, float64(r.intn(5)))
		if r.chance(0.2) {
			start = 0
		}
		bounds := make([]float64, n)
		v := start
		for j := range bounds {
			bounds[j] = v
			v += r.unit() * math.Pow(10, float64(r.intn(4)))
			if v == bounds[j] {
				v = math.Nextafter(v, math.Inf(1))
			}
		}
		counts := make([]uint64, n+1)
		for j := range counts {
			switch {
			case r.chance(0.3):
			case r.chance(0.1):
				counts[j] = uint64(r.intn(300000))
			default:
				counts[j] = uint64(r.intn(2000))
			}
		}
		c := explicitCase{name: fmt.Sprintf("seeded-%02d", i), bounds: bounds, counts: counts}
		if !r.chance(0.1) {
			c.sum = f((r.unit() - 0.2) * 1e5)
		}
		if r.chance(0.5) {
			c.min = f(bounds[0] - r.unit())
		}
		if r.chance(0.5) {
			c.max = f(bounds[n-1] + r.unit()*10)
		}
		cases = append(cases, c)
	}
	return cases
}

func expCases() []expCase {
	cases := []expCase{
		{name: "empty", sum: f(0)},
		{name: "zero-only", zeroCount: 5, count: 5, sum: f(0)},
		{name: "zero-only-scale-min", scale: -10, zeroCount: 5, count: 5, sum: f(0)},
		{name: "one-observation", positive: buckets{3, []uint64{1}}, count: 1, sum: f(9.5)},
		{name: "one-observation-negative", negative: buckets{3, []uint64{1}}, count: 1, sum: f(-9.5)},
		{name: "one-observation-min-max", positive: buckets{3, []uint64{1}}, count: 1, sum: f(9.5), min: f(9.5), max: f(9.5)},
		{name: "count-one-buckets-three", positive: buckets{0, []uint64{1, 1, 1}}, count: 1, sum: f(4)},
		{name: "count-three-bucket-one", positive: buckets{2, []uint64{1}}, count: 3, sum: f(15)},
		{name: "positive-and-negative", scale: 2, zeroCount: 3, positive: buckets{-4, []uint64{1, 0, 5, 9, 2}}, negative: buckets{1, []uint64{4, 4, 0, 1}}, count: 29, sum: f(-3.5), min: f(-2.4), max: f(1.7)},
		{name: "negative-only", scale: 1, negative: buckets{-2, []uint64{3, 8, 1}}, count: 12, sum: f(-8)},
		{name: "counts-over-uint16", scale: 3, positive: buckets{10, []uint64{70000, 200000, 65535, 65536}}, count: 401071, sum: f(1.2e6)},
		{name: "at-observation-cap", positive: buckets{1, []uint64{sketchCap}}, count: sketchCap, sum: f(1e9)},
		{name: "over-observation-cap", zeroCount: 1, positive: buckets{1, []uint64{sketchCap}}, count: sketchCap + 1, sum: f(1e9)},
		{name: "tiny-values", scale: 0, positive: buckets{-40, []uint64{3, 4}}, count: 7, sum: f(1e-11)},
		{name: "straddle-agent-min", scale: 0, positive: buckets{-32, []uint64{3, 4, 5, 6}}, count: 18, sum: f(1e-8)},
		{name: "index-too-high", scale: 0, positive: buckets{800, []uint64{2}}, count: 2, sum: f(1e241)},
		{name: "boundary-overflow", scale: 0, positive: buckets{1023, []uint64{2}}, count: 2, sum: f(1e308)},
		{name: "sum-nan", scale: 4, positive: buckets{0, []uint64{5, 6, 7}}, count: 18, sum: f(math.NaN())},
		{name: "sum-unset", scale: 4, positive: buckets{0, []uint64{5, 6, 7}}, count: 18},
		{name: "min-max-clamp", scale: 4, positive: buckets{16, []uint64{5, 6, 7}}, count: 18, sum: f(40), min: f(2.5), max: f(2.1)},
		{name: "min-only", scale: 4, positive: buckets{16, []uint64{5, 6, 7}}, count: 18, sum: f(40), min: f(2.01)},
		{name: "max-only", scale: 4, positive: buckets{16, []uint64{5, 6, 7}}, count: 18, sum: f(40), max: f(2.3)},
		{name: "min-above-buckets", scale: 4, positive: buckets{16, []uint64{5, 6, 7}}, count: 18, sum: f(40), min: f(50)},
		{name: "max-below-buckets", scale: 4, positive: buckets{16, []uint64{5, 6, 7}}, count: 18, sum: f(40), max: f(0.5)},
	}
	for s := int32(-10); s <= 20; s++ {
		// Index 0 starts at 1; scale -10's next boundary is 2^1024, which overflows.
		cases = append(cases, expCase{name: fmt.Sprintf("scale-%d-at-one", s), scale: s,
			positive: buckets{0, []uint64{3, 0, 2}}, count: 5, sum: f(5)})
		// About 1000, wherever the scale puts it.
		offset := int32(math.Round(10 * math.Pow(2, float64(s))))
		cases = append(cases, expCase{name: fmt.Sprintf("scale-%d-near-1000", s), scale: s,
			positive: buckets{offset, []uint64{1, 2, 3, 4, 5, 6, 7, 8}},
			negative: buckets{offset - 3, []uint64{2, 0, 2}}, zeroCount: 1, count: 41, sum: f(25000)})
	}

	r := &rng{s: 0x5eed0002}
	for i := 0; i < 40; i++ {
		scale := int32(r.intn(16)) - 3
		c := expCase{name: fmt.Sprintf("seeded-%02d", i), scale: scale}
		span := int(10 * math.Pow(2, float64(scale)))
		mk := func() buckets {
			b := buckets{Offset: int32(r.intn(2*span+1) - span)}
			n := 1 + r.intn(20)
			b.Counts = make([]uint64, n)
			for j := range b.Counts {
				switch {
				case r.chance(0.3):
				case r.chance(0.1):
					b.Counts[j] = uint64(r.intn(300000))
				default:
					b.Counts[j] = uint64(r.intn(3000))
				}
				c.count += b.Counts[j]
			}
			return b
		}
		c.positive = mk()
		if r.chance(0.4) {
			c.negative = mk()
		}
		if r.chance(0.3) {
			c.zeroCount = uint64(r.intn(100))
			c.count += c.zeroCount
		}
		if !r.chance(0.1) {
			c.sum = f((r.unit() - 0.2) * 1e5)
		}
		if r.chance(0.5) {
			c.min = f(-r.unit() * 10)
		}
		if r.chance(0.5) {
			c.max = f(r.unit() * 1e4)
		}
		cases = append(cases, c)
	}
	return cases
}

type primitive struct {
	Fn   string `json:"fn"`
	Args []num  `json:"args"`
	Out  num    `json:"out"`
}

// primitives records the Go `math` results the conversion is built from, on the inputs it uses
// them on: Log of every kind of value a bound can be, Exp across its range, and the two Pow shapes,
// an integer power of the Agent's gamma and an exponential histogram's base.
func primitives() []primitive {
	var out []primitive
	add := func(fn string, args []float64, v float64) {
		a := make([]num, len(args))
		for i, x := range args {
			a[i] = num(x)
		}
		out = append(out, primitive{Fn: fn, Args: a, Out: num(v)})
	}
	add("log1p", []float64{2.0 / 128}, math.Log1p(2.0/128))
	logs := []float64{1e-9, 1e-300, 5e-324, 2.2250738585072014e-308, 1e-310, 0.5, 0.7071067811865476, 1, 1.015625, 2, 3, 10, 1000, 1e10, 1e300, math.MaxFloat64, math.Inf(1), 0}
	r := &rng{s: 0x5eed0003}
	for i := 0; i < 200; i++ {
		logs = append(logs, math.Ldexp(1+r.unit(), r.intn(2100)-1074))
	}
	// log_amd64.s branches on f1 <= Sqrt2/2 where log.go branches on f1 < Sqrt2/2.
	for e := -1070; e <= 1020; e += 10 {
		logs = append(logs, math.Ldexp(math.Sqrt2/2, e))
	}
	for _, v := range logs {
		add("log", []float64{v}, math.Log(v))
	}
	exps := []float64{0, 1, -1, 0.5, 709.78, 709.7827128933841, -708.4, -744, -745.2, -746, 1e-20, -1e-20}
	for i := 0; i < 300; i++ {
		exps = append(exps, (r.unit()-0.5)*1490)
	}
	for i := 0; i < 100; i++ {
		exps = append(exps, (r.unit()-0.5)*4)
	}
	for _, v := range exps {
		add("exp", []float64{v}, math.Exp(v))
	}
	for k := -1400; k <= 32000; k += 37 {
		add("pow", []float64{1.015625, float64(k)}, math.Pow(1.015625, float64(k)))
	}
	for s := -12; s <= 24; s++ {
		y := math.Pow(2, float64(-s))
		add("pow", []float64{2, float64(-s)}, y)
		add("pow", []float64{2, y}, math.Pow(2, y))
	}
	return out
}

var collapseArray = regexp.MustCompile(`\[\s+([^\[\]{}]*?)\s+\]`)
var whitespace = regexp.MustCompile(`\s*\n\s*`)

// encode indents objects and keeps arrays of scalars on one line.
func encode(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fail(err)
	}
	b = collapseArray.ReplaceAllFunc(b, func(m []byte) []byte {
		inner := collapseArray.FindSubmatch(m)[1]
		return []byte("[" + whitespace.ReplaceAllString(string(inner), " ") + "]")
	})
	return append(b, '\n')
}

func main() {
	out := flag.String("out", "", "the corpus directory to write")
	flag.Parse()
	if *out == "" {
		flag.Usage()
		os.Exit(2)
	}
	if runtime.GOARCH != "amd64" || !(cpu.X86.HasAVX && cpu.X86.HasFMA) {
		fail(errors.New("needs amd64 with AVX and FMA, the math.Exp path the corpus records"))
	}
	versions := map[string]string{}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, d := range info.Deps {
			for _, m := range pinnedModules {
				if d.Path == m {
					v := d.Version
					if d.Replace != nil {
						v = d.Replace.Version
					}
					versions[m] = v
				}
			}
		}
	}
	for _, m := range pinnedModules {
		if versions[m] == "" {
			fail(fmt.Errorf("no build info for %s", m))
		}
	}
	fmt.Println(runtime.Version())

	files := map[string][]byte{}
	seen := map[string]bool{}
	for _, c := range explicitCases() {
		name := "explicit-" + c.name
		if seen[name] {
			fail(fmt.Errorf("duplicate case %s", name))
		}
		seen[name] = true
		files[filepath.Join("cases", name+".json")] = encode(runExplicit(c))
	}
	for _, c := range expCases() {
		name := "exponential-" + c.name
		if seen[name] {
			fail(fmt.Errorf("duplicate case %s", name))
		}
		seen[name] = true
		files[filepath.Join("cases", name+".json")] = encode(runExp(c))
	}
	var lines []string
	for _, p := range primitives() {
		b, err := json.Marshal(p)
		if err != nil {
			fail(err)
		}
		lines = append(lines, "  "+string(b))
	}
	files["primitives.json"] = []byte("[\n" + strings.Join(lines, ",\n") + "\n]\n")
	type module struct {
		Path    string `json:"path"`
		Version string `json:"version"`
	}
	manifest := struct {
		Go      string   `json:"go"`
		Arch    string   `json:"arch"`
		Modules []module `json:"modules"`
	}{Go: runtime.Version(), Arch: runtime.GOARCH + " (AVX, FMA)"}
	for _, m := range pinnedModules {
		manifest.Modules = append(manifest.Modules, module{m, versions[m]})
	}
	files["manifest.json"] = encode(manifest)

	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		p := filepath.Join(*out, n)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			fail(err)
		}
		if err := os.WriteFile(p, files[n], 0o644); err != nil {
			fail(err)
		}
	}
	fmt.Printf("wrote %d files\n", len(names))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "datadog-sketch:", err)
	os.Exit(1)
}
