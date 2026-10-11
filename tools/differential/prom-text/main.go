// Command prom-text writes Prometheus's own reading of every exposition body in the Prometheus
// text differential corpus (testdata/differential/prometheus-text/README.md).
//
// Usage: prom-text -cases <dir> -recorded <dir> -out <dir>
//
// Each body goes through textparse.New, the parser Prometheus's scrape loop builds, with the scrape
// loop's default ParserOptions: no fallback protocol, `_created` series kept as series, no
// type-and-unit labels, and no classic-to-native histogram conversion. The reading is written to
// <out>/<source>/<stem>.json, where <source> is `cases` or `prometheus-scrape`.
//
// Generation fails, writing nothing, when a case lacks its `.headers` or `.expect.json`, a file in
// either directory belongs to no body, or a recorded body fails to parse.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
)

// The module this program pins, read from the build info so the printed version is the one linked.
const prometheusModule = "github.com/prometheus/prometheus"

// fallbackContentType is what `fallback_scrape_protocol: PrometheusText0.0.4` sets: the reading a
// scrape gets when its Content-Type selects no parser.
const fallbackContentType = "text/plain"

type body struct {
	source string // `cases` or `prometheus-scrape`
	stem   string
	path   string // relative to testdata/, for the reading's `input.path`
	data   []byte
	// contentType is nil when the sidecar has no content-type line.
	contentType *string
}

func main() {
	cases := flag.String("cases", "", "the hand-built cases directory")
	recorded := flag.String("recorded", "", "the recorded scrape bodies directory")
	out := flag.String("out", "", "the reference directory to write")
	flag.Parse()
	if *cases == "" || *recorded == "" || *out == "" {
		flag.Usage()
		os.Exit(2)
	}
	fmt.Println(runtime.Version())
	fmt.Println(prometheusModule, moduleVersion())

	bodies, err := collect(*cases, *recorded)
	if err != nil {
		fail(err)
	}
	readings := make(map[string][]byte, len(bodies))
	for _, b := range bodies {
		reading := read(b)
		if b.source == "prometheus-scrape" {
			if p, ok := reading["parser"].(string); !ok || reading["error"] != nil {
				fail(fmt.Errorf("%s: a recorded body must parse, got parser %v error %v",
					b.path, reading["parser"], reading["error"]))
			} else if p == "" {
				fail(fmt.Errorf("%s: no parser", b.path))
			}
		}
		encoded, err := encode(reading)
		if err != nil {
			fail(fmt.Errorf("%s: %w", b.path, err))
		}
		readings[filepath.Join(b.source, b.stem+".json")] = encoded
	}
	names := make([]string, 0, len(readings))
	for name := range readings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		path := filepath.Join(*out, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fail(err)
		}
		if err := os.WriteFile(path, readings[name], 0o644); err != nil {
			fail(err)
		}
	}
	fmt.Printf("wrote %d readings\n", len(names))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "prom-text:", err)
	os.Exit(1)
}

func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		fail(errors.New("no build info"))
	}
	for _, dep := range info.Deps {
		if dep.Path == prometheusModule {
			return dep.Version
		}
	}
	fail(errors.New("the prometheus module is not linked"))
	return ""
}

// collect reads both directories. A case is `<stem>.txt` or `<stem>.om` beside `<stem>.headers`
// and `<stem>.expect.json`; a recorded body is `<stem>.body` beside `<stem>.headers`.
func collect(casesDir, recordedDir string) ([]body, error) {
	var bodies []body
	caseFiles, err := names(casesDir)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, name := range caseFiles {
		ext := filepath.Ext(name)
		if ext != ".txt" && ext != ".om" {
			continue
		}
		stem := strings.TrimSuffix(name, ext)
		b, err := load(casesDir, "cases", stem, name, "differential/prometheus-text/cases/"+name)
		if err != nil {
			return nil, err
		}
		if _, err := os.Stat(filepath.Join(casesDir, stem+".expect.json")); err != nil {
			return nil, fmt.Errorf("case %s has no .expect.json", stem)
		}
		if seen[stem] {
			return nil, fmt.Errorf("case %s has two bodies", stem)
		}
		seen[stem] = true
		bodies = append(bodies, b)
	}
	for _, name := range caseFiles {
		stem := name
		for _, suffix := range []string{".expect.json", ".headers", ".txt", ".om"} {
			if strings.HasSuffix(name, suffix) {
				stem = strings.TrimSuffix(name, suffix)
				break
			}
		}
		if name == "README.md" {
			continue
		}
		if !seen[stem] {
			return nil, fmt.Errorf("cases/%s belongs to no case", name)
		}
	}

	recordedFiles, err := names(recordedDir)
	if err != nil {
		return nil, err
	}
	seenRecorded := map[string]bool{}
	for _, name := range recordedFiles {
		if filepath.Ext(name) != ".body" {
			continue
		}
		stem := strings.TrimSuffix(name, ".body")
		b, err := load(recordedDir, "prometheus-scrape", stem, name,
			"interop/prometheus-scrape/"+name)
		if err != nil {
			return nil, err
		}
		seenRecorded[stem] = true
		bodies = append(bodies, b)
	}
	for _, name := range recordedFiles {
		if name == "README.md" {
			continue
		}
		stem := strings.TrimSuffix(strings.TrimSuffix(name, ".body"), ".headers")
		if !seenRecorded[stem] {
			return nil, fmt.Errorf("prometheus-scrape/%s belongs to no body", name)
		}
	}
	if len(seen) == 0 || len(seenRecorded) == 0 {
		return nil, fmt.Errorf("found %d cases and %d recorded bodies; are the directories mounted?",
			len(seen), len(seenRecorded))
	}
	return bodies, nil
}

func names(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

func load(dir, source, stem, name, path string) (body, error) {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return body{}, err
	}
	headers, err := os.ReadFile(filepath.Join(dir, stem+".headers"))
	if err != nil {
		return body{}, fmt.Errorf("%s has no .headers: %w", path, err)
	}
	return body{source: source, stem: stem, path: path, data: data,
		contentType: contentType(headers)}, nil
}

// contentType is the value of the sidecar's first `content-type:` line, matched case-insensitively
// and trimmed of surrounding spaces and tabs, as an HTTP client reads a header.
func contentType(headers []byte) *string {
	scanner := bufio.NewScanner(bytes.NewReader(headers))
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), ":")
		if ok && strings.EqualFold(name, "content-type") {
			v := strings.Trim(value, " \t")
			return &v
		}
	}
	return nil
}

func read(b body) map[string]any {
	reading := map[string]any{
		"input": map[string]any{
			"path":   b.path,
			"len":    len(b.data),
			"crc32c": fmt.Sprintf("0x%08x", crc32.Checksum(b.data, crc32.MakeTable(crc32.Castagnoli))),
		},
	}
	ct := ""
	if b.contentType != nil {
		ct = *b.contentType
		reading["content_type"] = ct
	} else {
		reading["content_type"] = nil
	}

	// The scrape loop's own call, with every option at its default. A nil parser fails the scrape;
	// the reading then records the error and goes on with what the fallback protocol would read.
	parser, err := textparse.New(b.data, ct, labels.NewSymbolTable(), textparse.ParserOptions{})
	if parser == nil {
		chosen := map[string]any{"error": errText(err)}
		parser, err = textparse.New(b.data, ct, labels.NewSymbolTable(),
			textparse.ParserOptions{FallbackContentType: fallbackContentType})
		if parser == nil {
			fail(fmt.Errorf("%s: the fallback gave no parser: %v", b.path, err))
		}
		chosen["fallback"] = kind(parser)
		reading["parser"] = chosen
	} else {
		if err != nil {
			fail(fmt.Errorf("%s: a parser with an error and no fallback: %v", b.path, err))
		}
		reading["parser"] = kind(parser)
	}

	entries := []any{}
	reading["error"] = nil
	for {
		entry, err := parser.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			reading["error"] = map[string]any{"after_entry": len(entries), "message": err.Error()}
			break
		}
		switch entry {
		case textparse.EntryType:
			name, typ := parser.Type()
			entries = append(entries, meta("type", name, []byte(typ)))
		case textparse.EntryHelp:
			name, help := parser.Help()
			entries = append(entries, meta("help", name, help))
		case textparse.EntryUnit:
			name, unit := parser.Unit()
			entries = append(entries, meta("unit", name, unit))
		case textparse.EntryComment:
		case textparse.EntrySeries:
			entries = append(entries, series(parser))
		default:
			fail(fmt.Errorf("%s: entry %d has no reading", b.path, entry))
		}
	}
	reading["entries"] = entries
	return reading
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func kind(p textparse.Parser) string {
	switch p.(type) {
	case *textparse.PromParser:
		return "prom"
	case *textparse.OpenMetricsParser:
		return "openmetrics"
	default:
		return fmt.Sprintf("%T", p)
	}
}

func meta(k string, name, value []byte) map[string]any {
	return map[string]any{"kind": k, "name": string(name), "value": string(value)}
}

func series(p textparse.Parser) map[string]any {
	_, ts, v := p.Series()
	var lset labels.Labels
	p.Labels(&lset)
	entry := map[string]any{
		"kind":   "series",
		"name":   lset.Get(labels.MetricName),
		"labels": pairs(lset),
		"value":  bits(v),
	}
	if ts != nil {
		entry["ts_ms"] = *ts
	}
	var e exemplar.Exemplar
	if p.Exemplar(&e) {
		ex := map[string]any{"labels": pairs(e.Labels), "value": bits(e.Value)}
		if e.HasTs {
			ex["ts_ms"] = e.Ts
		}
		entry["exemplar"] = ex
		var second exemplar.Exemplar
		if p.Exemplar(&second) {
			fail(errors.New("a second exemplar on one sample, which the reading has no field for"))
		}
	}
	return entry
}

// pairs is every label but the metric name, sorted by name (labels.Labels is already sorted).
func pairs(lset labels.Labels) [][2]string {
	out := [][2]string{}
	lset.Range(func(l labels.Label) {
		if l.Name != labels.MetricName {
			out = append(out, [2]string{l.Name, l.Value})
		}
	})
	return out
}

func bits(v float64) string {
	return fmt.Sprintf("0x%016x", math.Float64bits(v))
}

// encode is the deterministic JSON form: maps sort their keys, one space of indent, no HTML
// escaping, and a trailing newline.
func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
