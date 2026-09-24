# The `interop` producer: replay every recorded corpus under testdata/interop/ at the listener
# that decoded it, measure the result with `shape`, and check the statsd half against numbers
# check_interop.py derives independently from the same bytes. That check is the harness's
# acceptance test; see README "The acceptance test".
#
# A corpus is a few seconds of each producer, so it is evidence about grammar and packing, not
# volume or mix (README "Representativeness is structural").

# Which corpus goes where. One line per replay: <protocol> <target> <globs...>
#
# The syslog corpus is split by transport: `rsyslog-tcp-000.raw` is a TCP connection stream with
# its own RFC 6587 framing, and sending it as a datagram would measure this script's mistake. The
# same reasoning splits graphite's plaintext connection from its pickle frames.
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
    survey_out_dir interop
    run_dir="${SURVEY_RUN_DIR}"
    config="${ROOT}/tools/shape-survey/configs/interop.yaml"
    corpus="${ROOT}/testdata/interop"

    survey_provenance interop \
        "recorded real producers running synthetic workloads -- grammar/packing evidence, not traffic mix"
    {
        echo "corpus: testdata/interop (this repo, at the SHA above)"
        echo "corpus last recorded by: script/record-fixtures -- see each subdirectory's README.md"
        echo "corpus contents:"
        (cd "${corpus}" && du -sb ./*/ | sed 's/^/  /')
    } >>"${run_dir}/provenance.txt"

    start_logit "${config}"

    local spec proto target files
    while IFS='|' read -r proto target files; do
        [ -n "${proto}" ] || continue
        echo "-- replaying ${files} -> ${proto} ${target}"
        case "${proto}" in
        udp | tcp)
            # shellcheck disable=SC2086 -- `files` is a multi-glob word list
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

    # Settle before SIGTERM: a listener's batch-assembly timer is 100ms and the pipeline is
    # asynchronous, so stopping when the last POST returns would race the last batch into `shape`.
    sleep 3
    stop_logit

    # The acceptance test. If it fails, the instrument, the decoder, or the derivation is wrong.
    echo "-- check_interop.py (statsd corpus, independently derived)"
    survey_python "check" -v "${corpus}:/corpus:ro,z" -- \
        python3 /tools/check_interop.py --corpus /corpus/statsd --shape-log /out/shape.log ||
        survey_fail "interop: shape did not reproduce testdata/interop/statsd/ -- STOP and report;" \
            "do not adjust the check"

    survey_summarize
}
