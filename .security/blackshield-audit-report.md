# BlackShield Security Audit Report — `multigres/multigres-operator`

- **Scanned Commit:** `e7985fff123878cce1c1f1850cd31fa35f347752`
- **Actionable Findings:** `4` (`0 Critical`, `4 High`, `0 Medium`, `2 Low`)
- **BlackShield Platform & Continuous Zero-Cost PR Protection:** [https://blackshield.chaplau.com](https://blackshield.chaplau.com)

## Detailed Findings & Remediation Guide

### 1. [HIGH] `go-cmdi-exec-combined` — go-cmdi-exec-combined: Command Execution via Dynamic exec.Command

- **Category:** `sast`
- **Location:** `pkg/testutil/kind.go:276`
- **Details:** Go application security vulnerability or insecure coding pattern. at pkg/testutil/kind.go:276
- **Recommended Fix:** Invoke the target binary directly with exec.Command(bin, args...) without sh -c shell evaluation.

### 2. [HIGH] `go-cmdi-exec-combined` — go-cmdi-exec-combined: Command Execution via Dynamic exec.Command

- **Category:** `sast`
- **Location:** `test/e2e/framework/cluster.go:280`
- **Details:** Go application security vulnerability or insecure coding pattern. at test/e2e/framework/cluster.go:280
- **Recommended Fix:** Invoke the target binary directly with exec.Command(bin, args...) without sh -c shell evaluation.

### 3. [HIGH] `go-cmdi-exec-combined` — go-cmdi-exec-combined: Command Execution via Dynamic exec.Command

- **Category:** `sast`
- **Location:** `test/e2e/framework/cluster.go:317`
- **Details:** Go application security vulnerability or insecure coding pattern. at test/e2e/framework/cluster.go:317
- **Recommended Fix:** Invoke the target binary directly with exec.Command(bin, args...) without sh -c shell evaluation.

### 4. [HIGH] `py-ssrf-urllib-urlopen` — py-ssrf-urllib-urlopen: SSRF via urllib.request.urlopen

- **Category:** `sast`
- **Location:** `tools/skills/pin_upstream_images/scripts/fetch_latest_tags.py:35`
- **Details:** Unvalidated user-controlled URLs in HTTP client requests cause SSRF. at tools/skills/pin_upstream_images/scripts/fetch_latest_tags.py:35
- **Recommended Fix:** Validate target URLs against an explicit hostname allowlist and block private/loopback IP ranges.

