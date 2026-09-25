// Randomized 10-300ms response delay for the internal upstream block (127.0.0.1:8081,
// nginx.conf), so nginx.request_time/nginx.upstream_response_time show a real distribution
// instead of near-uniform sub-millisecond values -- a DdSketch's percentiles are a much more
// convincing proof point at p50/p90/p99 when the underlying latencies actually vary. See njs's
// own "delaying a response" documentation example, which this mirrors.
function delay(r) {
    var ms = Math.floor(Math.random() * 290) + 10; // 10-300ms, uniform
    setTimeout(() => {
        r.return(200, "ok\n");
    }, ms);
}

export default { delay };
