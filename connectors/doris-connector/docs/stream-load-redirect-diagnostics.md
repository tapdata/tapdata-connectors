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
its compatibility with underscore authorities, user-info and fragments.
HTTPS-to-HTTP redirects are rejected before connecting to the HTTP target, with
`redirectFailure=https_to_http_redirect_rejected`. For HTTPS FE deployments the
BE Location must also use HTTPS; configure the FE/proxy accordingly. HTTP-to-HTTP
and HTTPS-to-HTTPS routing preserve the upload credentials and data.
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
  HttpClient may already have normalized it. `httpsToHttp=true` is recorded before
  the downgrade is rejected. Other flags do not reject a request.

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
`circular_redirect`, `maximum_redirects_exceeded`, `https_to_http_redirect_rejected`
and `duplicate_content_length`.
The outer load exception also carries the label, summary and hop trace.
Cause types distinguish protocol/redirect failures from DNS, connection and read failures;
the last Location origin identifies the destination when connection setup fails.

Regression tests use local raw HTTP sockets, including early 100 followed by 307,
a delayed FE response beyond the old three-second limit, cross-host authorization
and label preservation, 308, rejected 302, missing Location, redirect loops
and maximum redirects. Compatibility cases complete HTTP uploads through underscore
hostnames (using a test DNS resolver), user-info and fragment URLs. Local TLS tests cover 307/308 downgrade rejection with no connection to a listening
HTTP target, and successful HTTPS-to-HTTPS replay with identical credentials and
body. They exercise the shared builder with a test trust store and the production
TLS factory. The loopback certificate/key (`src/test/resources/streamload/localhost.p12`,
password `test-only`) is a public test fixture, never a deployment credential.
These tests do not exercise a deployed Doris cluster or a real proxy.
