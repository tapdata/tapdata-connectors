# Stream Load redirect diagnostics

A 307/308 is a routing response, never a successful load. Both HTTP client factories
wait up to 30 seconds for `100-continue`, allow at most five redirects, preserve PUT,
headers and label, and replay a snapshot of the completed batch if a FE/proxy has
already requested the body. The snapshot adds one batch-sized heap allocation per
active loader and is released after the request. No extra network retry is added;
existing connector retry handling still owns failed loads.

PUT responses 301/302/303 are surfaced as failures to avoid method conversion.
Missing/invalid Location, circular redirects and redirect limits fail explicitly.
Target URI interpretation and scheme routing are left to HttpClient, preserving
its compatibility with underscore authorities, user-info and fragments, and
HTTPS-to-HTTP FE/BE routing. The connector adds no target-URI or downgrade veto.
A BE redirect is expected to cross hosts, so the explicit
Doris Authorization header is preserved, as before. Existing TLS trust policy is
unchanged.

Search for `Stream load failed` or `Stream load recovered after body replay` in WARN
logs. Each includes a label, elapsed time, configured timeouts and redirect limit,
body write count, accepted socket-stream bytes and a chronological list of hops:

- `request`: elapsed milliseconds, target origin and method.
- `bodyStart`: elapsed time and milliseconds since that hop's request began.
- `response`: elapsed time, numeric status, peer IP/port, Location origin and body counters.
- `redirect`: resolved target origin and boolean flags `javaHostMissing`,
  `hasUserInfo`, `hasFragment`, `httpsToHttp`. Flags describe the resolved URI;
  HttpClient may already have normalized it. They never block a request.

Origins use HttpClient's host extraction, so underscore hostnames remain visible
instead of being logged as `invalid`.

A FE 307 with `bodyWrites=0` means no body was sent to that FE. A 307 with a body
write followed by another body write means the batch was replayed. On a first hop,
bodyStart near the 30-second wait suggests the continue wait expired; a much earlier
bodyStart suggests an early `100 Continue`. These timings are evidence, not proof
of which component emitted the interim response; correlate the label and timestamps
with FE/LB logs or a network trace. Socket-stream byte counts do not prove BE receipt
or transaction commit. Successful ordinary loads retain summaries at DEBUG.

The trace records neither Authorization nor arbitrary headers, URL user-info,
paths, queries, fragments, response bodies or row data. A closed connection may
require using the peer captured at request time. Failure summaries include the execution phase, a bounded cause-type chain and safe
protocol reason codes, even when `ClientProtocolException.getMessage()` is null.
Original exception causes are preserved. Arbitrary exception messages are omitted
because they can contain full redirect URLs or data. Reasons include
`missing_location`, `invalid_redirect_location`,
`circular_redirect`, `maximum_redirects_exceeded` and `duplicate_content_length`.
The outer load exception also carries the label, summary and hop trace.
Cause types distinguish protocol/redirect failures from DNS, connection and read failures;
the last Location origin identifies the destination when connection setup fails.

Regression tests use local raw HTTP sockets, including early 100 followed by 307,
a delayed FE response beyond the old three-second limit, cross-host authorization
and label preservation, 308, rejected 302, missing Location, redirect loops
and maximum redirects. Compatibility cases complete HTTP uploads through underscore
hostnames (using a test DNS resolver), user-info and fragment URLs. HTTPS-to-HTTP
compatibility is checked at redirect-strategy level. Both client factories are exercised over HTTP; these tests
do not exercise a deployed Doris cluster, a real proxy or a TLS handshake.
