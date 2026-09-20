# The `interop` producer: replay every recorded corpus under testdata/interop/ at the listener
# that decoded it, measure the result with `shape`, and check the statsd half against numbers
# derived independently from the same bytes.
#
# Sourced by script/shape-survey, which discovers this file by glob -- everything specific to this
# producer lives here and in tools/shape-survey/configs/interop.yaml, and nothing about it is
# mentioned in lib.sh or the dispatcher. See tools/shape-survey/README.md's "Adding a producer".
#
# First, and free (docs/plans/data-shape-survey.md's W2): these captures already exist, already
# came from real third-party producers, and cost nothing but a container to replay. They are also
# the survey's **acceptance test** -- testdata/interop/statsd/README.md's lines-per-datagram and
# tags-per-line figures were counted by somebody else, so `shape` either reproduces them or it is
# not measuring what this survey thinks it is (tools/shape-survey/check_interop.py).
#
# What this does NOT claim: a recorded corpus is a handful of messages caught from a producer's
# first few seconds, so it says a great deal about *shape* (how a client packs a datagram, how wide
# a line is, which carriers a decoder stamps) and nothing at all about volume, mix, or steady-state
# behaviour. In docs/plans/data-shape-survey.md's grading, these rows are Measured/Default.

# Which corpus goes where. One line per replay: <protocol> <target> <globs...>
#
# The syslog corpus is split by transport rather than replayed wholesale -- `rsyslog-tcp-000.raw`
# is a captured TCP *connection stream* with its own RFC 6587 non-transparent framing, and sending
# it as a datagram would measure this script's mistake rather than rsyslog's behaviour. The same
# reasoning splits graphite's plaintext connection from its pickle frames.
survey_interop_replays() {
    cat <<'EOF'
udp|logit:8125|/corpus/statsd/*.raw
udp|logit:5514|/corpus/syslog/logger-*.raw /corpus/syslog/python-syslog-handler-*.raw /corpus/syslog/rsyslog-000.raw
tcp|logit:601|/corpus/syslog/rsyslog-tcp-*.raw
udp|logit:25826|/corpus/collectd/*.raw
tcp|logit:2003|/corpus/graphite/write-graphite-*.raw
tcp|logit:2004|/corpus/graphite/graphite-pickle-*.raw
http|http://logit:9201/api/v1/write|/corpus/prometheus/*.bin
json|http://logit:4318/v1/logs|/corpus/otlp/logs.json
json|http://logit:4318/v1/metrics|/corpus/otlp/metrics.json
json|http://logit:4318/v1/traces|/corpus/otlp/traces.json
EOF
}

survey_interop() {
    local run_dir config corpus
    run_dir="$(survey_out_dir interop)"
    config="${ROOT}/tools/shape-survey/configs/interop.yaml"
    corpus="${ROOT}/testdata/interop"

    # The representativeness line summarize.py turns into summary.md's banner. These captures are
    # real third-party producers (rsyslog, collectd, a real Prometheus, Datadog's and the
    # plain-statsd client, the OpenTelemetry Collector) -- but each was recorded driving a small
    # synthetic workload for a few seconds, so what they are evidence *about* is grammar, carriers
    # and a client's packing decisions, not what mix or volume of traffic a real deployment sends.
    survey_provenance interop \
        "recorded real producers running synthetic workloads -- grammar/packing evidence, not traffic mix"
    {
        echo "corpus: testdata/interop (this repo, at the SHA above)"
        echo "corpus last recorded by: script/record-fixtures -- see each subdirectory's README.md"
        echo "corpus contents:"
        (cd "${corpus}" && du -sb ./*/ | sed 's/^/  /')
    } >>"${run_dir}/provenance.txt"

    start_logit interop "${config}"

    local spec proto target files
    while IFS='|' read -r proto target files; do
        [ -n "${proto}" ] || continue
        echo "-- replaying ${files} -> ${proto} ${target}"
        case "${proto}" in
        udp | tcp)
            # shellcheck disable=SC2086 -- `files` is a deliberate multi-glob word list
            survey_python "replay" \
                --network "${SURVEY_NET}" -v "${corpus}:/corpus:ro,z" -- \
                python3 /tools/replay.py --proto "${proto}" \
                --host "${target%%:*}" --port "${target##*:}" --files ${files}
            ;;
        http)
            # shellcheck disable=SC2086
            survey_python "replay" \
                --network "${SURVEY_NET}" -v "${corpus}:/corpus:ro,z" -- \
                python3 /tools/replay.py --proto http --url "${target}" --files ${files}
            ;;
        json)
            # shellcheck disable=SC2086
            survey_python "replay" \
                --network "${SURVEY_NET}" -v "${corpus}:/corpus:ro,z" -- \
                python3 /tools/replay.py --proto http --json-lines --url "${target}" --files ${files}
            ;;
        *)
            survey_fail "interop: unknown replay protocol '${proto}'"
            ;;
        esac
    done < <(survey_interop_replays)

    # A settle before SIGTERM: a listener's own batch-assembly timer is 100ms and the pipeline is
    # asynchronous end to end, so stopping the instant the last POST returns would race the last
    # batch into `shape`. Three seconds is two orders of magnitude of headroom on a corpus this
    # size; the flush that actually produces the capture is the one SIGTERM triggers.
    sleep 3
    stop_logit

    # The acceptance test. The producer fails if this fails -- a summary built on an instrument
    # that cannot reproduce a counted corpus is worse than no summary, because it looks like data.
    echo "-- check_interop.py (statsd corpus, independently derived)"
    survey_python "check" -v "${corpus}:/corpus:ro,z" -- \
        python3 /tools/check_interop.py --corpus /corpus/statsd --shape-log /out/shape.log ||
        survey_fail "interop: shape did not reproduce testdata/interop/statsd/ -- STOP and report;" \
            "do not adjust the check"

    survey_summarize
}
