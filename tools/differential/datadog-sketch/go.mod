module datadog-sketch

go 1.26.0

require (
	github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/attributes v0.84.4
	github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics v0.84.4
	github.com/DataDog/datadog-agent/pkg/util/quantile v0.84.4
	go.opentelemetry.io/collector/component/componenttest v0.163.0
	go.opentelemetry.io/collector/pdata v1.69.0
	go.uber.org/zap v1.28.0
	golang.org/x/sys v0.49.0
)

require (
	github.com/DataDog/datadog-agent/pkg/proto v0.84.4 // indirect
	github.com/DataDog/datadog-agent/pkg/trace/log v0.84.4 // indirect
	github.com/DataDog/datadog-agent/pkg/trace/traceutil v0.84.4 // indirect
	github.com/DataDog/sketches-go v1.4.8 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/tinylib/msgp v1.6.5 // indirect
	github.com/twmb/murmur3 v1.2.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/component v1.69.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.69.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/DataDog/datadog-agent/pkg/util/quantile => github.com/DataDog/datadog-agent/pkg/util/quantile v0.84.0-devel.0.20261020233841-45132452b855

replace github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics => github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics v0.84.0-devel.0.20261020233841-45132452b855

replace github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/attributes => github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/attributes v0.84.0-devel.0.20261020233841-45132452b855

replace github.com/DataDog/datadog-agent/pkg/proto => github.com/DataDog/datadog-agent/pkg/proto v0.84.0-devel.0.20261020233841-45132452b855

replace github.com/DataDog/datadog-agent/pkg/trace/log => github.com/DataDog/datadog-agent/pkg/trace/log v0.84.0-devel.0.20261020233841-45132452b855

replace github.com/DataDog/datadog-agent/pkg/trace/traceutil => github.com/DataDog/datadog-agent/pkg/trace/traceutil v0.84.0-devel.0.20261020233841-45132452b855
