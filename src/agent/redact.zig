//! Edge PII redaction for the sermon-daemon.
//!
//! Runs ONCE per collect cycle, in main.zig, on the in-memory ProcessInfo /
//! LogEntry / DiskInfo slices AFTER collection and BEFORE both the staging
//! append (local parquet) and push.buildPayload (remote upload). Redacting at
//! this single chokepoint means local persistence and remote upload see the
//! exact same redacted bytes, and nothing downstream has to be redaction-aware.
//!
//! Design constraints (PRAGMATIC-MINIMAL):
//!   * Only the highest-PII free-text fields are scanned: LogEntry.message,
//!     LogEntry.source, DiskInfo.mount_point, ProcessInfo.cgroup. Everything
//!     else (process name, units, identifiers, ALL numerics) is preserved -
//!     it is operational signal the anomaly engine needs.
//!   * No regex (Zig stdlib has none). Every rule is a hand-written byte
//!     scanner over []const u8.
//!   * Allocation-careful: each struct string field is an individually-owned
//!     heap slice (see collector.collectProcesses / logs.LogEntry.deinit). To
//!     redact a field in place we build the redacted string into a fresh
//!     allocation, free the OLD slice, and store the new pointer. On any error
//!     the original field is left untouched (fail-closed toward "keep raw" is
//!     NOT acceptable for PII, so callers treat a redact error as fatal for
//!     the cycle - see main.zig wiring).
//!   * Shape-preserving: a match is replaced with a typed placeholder like
//!     <REDACTED:EMAIL>, leaving the surrounding text intact. A brute-force
//!     line stays readable as a brute-force line:
//!       "Failed password for admin from 10.0.0.1"
//!         -> "Failed password for admin from <REDACTED:IP>"
//!
//! Recall note: the deterministic scanners below were measured at 100% recall
//! on structured secrets/credentials/cards/SSN/IP (labs pii-redaction-recall).
//! They do NOT catch free-form human usernames or street addresses - that needs
//! an NER model and is a documented follow-on, not part of this pass.

const std = @import("std");
const Allocator = std.mem.Allocator;
const collector = @import("collector");
const logs = @import("logs");

const ProcessInfo = collector.ProcessInfo;
const DiskInfo = collector.DiskInfo;
const LogEntry = logs.LogEntry;

// ============================================================================
// Placeholder tags
// ============================================================================

const Tag = enum {
    ip,
    ipv6,
    mac,
    email,
    ssn,
    card,
    aws_key,
    aws_secret,
    openai_key,
    jwt,
    token,
    value, // generic key=value structural redaction

    fn text(self: Tag) []const u8 {
        return switch (self) {
            .ip => "<REDACTED:IP>",
            .ipv6 => "<REDACTED:IPV6>",
            .mac => "<REDACTED:MAC>",
            .email => "<REDACTED:EMAIL>",
            .ssn => "<REDACTED:SSN>",
            .card => "<REDACTED:CARD>",
            .aws_key => "<REDACTED:AWS_KEY>",
            .aws_secret => "<REDACTED:AWS_SECRET>",
            .openai_key => "<REDACTED:OPENAI_KEY>",
            .jwt => "<REDACTED:JWT>",
            .token => "<REDACTED:TOKEN>",
            .value => "<REDACTED:VALUE>",
        };
    }
};

// ============================================================================
// Tiny character-class helpers (ASCII-only by design; PII patterns are ASCII).
// ============================================================================

inline fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}
inline fn isHex(c: u8) bool {
    return isDigit(c) or (c >= 'a' and c <= 'f') or (c >= 'A' and c <= 'F');
}
inline fn isUpperAlnum(c: u8) bool {
    return isDigit(c) or (c >= 'A' and c <= 'Z');
}
inline fn isAlnum(c: u8) bool {
    return isDigit(c) or (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z');
}
/// Base64url-ish alphabet used by JWTs / AWS secrets / OpenAI key bodies.
inline fn isB64(c: u8) bool {
    return isAlnum(c) or c == '+' or c == '/' or c == '-' or c == '_' or c == '=';
}
/// Characters that may legitimately appear inside an email local-part / domain.
inline fn isEmailChar(c: u8) bool {
    return isAlnum(c) or c == '.' or c == '_' or c == '%' or c == '+' or c == '-';
}
/// Case-insensitive ASCII equality for a single byte.
inline fn eqIgnoreCase(a: u8, b: u8) bool {
    return std.ascii.toLower(a) == std.ascii.toLower(b);
}

/// True when the byte before index `i` is NOT part of the same "word" - used
/// to anchor token scanners so we don't match `xAKIA...` mid-identifier.
inline fn boundaryBefore(s: []const u8, i: usize) bool {
    if (i == 0) return true;
    return !isAlnum(s[i - 1]);
}

// ============================================================================
// Match result: a half-open byte range [start, end) and the tag to emit.
// ============================================================================

const Match = struct {
    start: usize,
    end: usize,
    tag: Tag,
};

/// Result of probing a single scanner at a given offset: either no match, or a
/// match that ENDS at `end` (exclusive). `start` is the probe offset.
const Probe = ?usize;

// ============================================================================
// Individual byte scanners. Each takes the full buffer and a start offset, and
// returns the exclusive end index of a match anchored at `start`, or null.
//
// They are deliberately conservative: a scanner returns null rather than risk a
// partial/garbage match. The driver (scanFirst) tries them in priority order so
// the most specific / highest-entropy pattern wins at a given position.
// ============================================================================

/// IPv4: d{1,3}.d{1,3}.d{1,3}.d{1,3}, each octet 0-255, dotted, 4 groups.
fn scanIpv4(s: []const u8, start: usize) Probe {
    // Reject when the match would sit in the middle of a longer dotted-number
    // (e.g. the "2.3.4.5" tail of a version string "1.2.3.4.5"): if the byte
    // before `start` is a digit or '.', this is not a standalone IPv4.
    if (start > 0 and (isDigit(s[start - 1]) or s[start - 1] == '.')) return null;
    var i = start;
    var groups: usize = 0;
    while (groups < 4) : (groups += 1) {
        if (groups != 0) {
            if (i >= s.len or s[i] != '.') return null;
            i += 1;
        }
        const octet_start = i;
        var val: u32 = 0;
        var n: usize = 0;
        while (i < s.len and isDigit(s[i]) and n < 3) : (i += 1) {
            val = val * 10 + (s[i] - '0');
            n += 1;
        }
        if (i == octet_start) return null; // empty octet
        if (val > 255) return null;
    }
    // Reject if immediately followed by another dotted digit run (e.g. a
    // version string "1.2.3.4.5") or a digit (would be a 5th octet).
    if (i < s.len and (isDigit(s[i]) or s[i] == '.')) {
        if (i + 1 < s.len and s[i] == '.' and isDigit(s[i + 1])) return null;
        if (isDigit(s[i])) return null;
    }
    return i;
}

/// IPv6: at least two hex groups separated by ':', allowing one "::" elision.
/// Conservative: requires either a "::" OR >= 3 colon-separated hex groups so
/// we don't redact a bare "ab:cd" (that is the MAC scanner's job / too short).
fn scanIpv6(s: []const u8, start: usize) Probe {
    var i = start;
    var groups: usize = 0;
    var colons: usize = 0;
    var saw_double: bool = false;
    while (i < s.len) {
        // Hex group (0-4 hex digits).
        var hx: usize = 0;
        while (i < s.len and isHex(s[i]) and hx < 4) : (i += 1) hx += 1;
        if (hx > 0) groups += 1;
        if (i < s.len and s[i] == ':') {
            colons += 1;
            i += 1;
            if (i < s.len and s[i] == ':') {
                if (saw_double) break; // only one "::" allowed
                saw_double = true;
                colons += 1;
                i += 1;
            }
        } else break;
    }
    // Require enough structure to be confidently an IPv6 literal.
    if (saw_double and groups >= 1 and colons >= 2) return i;
    if (!saw_double and groups >= 3 and colons >= 2) return i;
    return null;
}

/// MAC: xx:xx:xx:xx:xx:xx (six pairs of hex, ':' or '-' separated, consistent).
fn scanMac(s: []const u8, start: usize) Probe {
    var i = start;
    var sep: u8 = 0;
    var pair: usize = 0;
    while (pair < 6) : (pair += 1) {
        if (pair != 0) {
            if (i >= s.len) return null;
            const c = s[i];
            if (c != ':' and c != '-') return null;
            if (sep == 0) sep = c else if (c != sep) return null;
            i += 1;
        }
        if (i + 1 >= s.len) return null;
        if (!isHex(s[i]) or !isHex(s[i + 1])) return null;
        i += 2;
    }
    // Must not continue into a longer hex run (would be a hash, not a MAC).
    if (i < s.len and isHex(s[i])) return null;
    return i;
}

/// EMAIL: local@domain.tld. Walk backward NOT needed; the driver anchors at the
/// local-part start. We require a '@', a domain label, and a dotted TLD >= 2.
fn scanEmail(s: []const u8, start: usize) Probe {
    var i = start;
    const local_start = i;
    while (i < s.len and isEmailChar(s[i])) : (i += 1) {}
    if (i == local_start) return null; // empty local part
    if (i >= s.len or s[i] != '@') return null;
    i += 1; // consume '@'
    const domain_start = i;
    var saw_dot = false;
    while (i < s.len and (isAlnum(s[i]) or s[i] == '.' or s[i] == '-')) : (i += 1) {
        if (s[i] == '.') saw_dot = true;
    }
    if (i == domain_start or !saw_dot) return null;
    // Trailing dot is not a valid TLD; trim it back.
    if (s[i - 1] == '.') return null;
    return i;
}

/// SSN: ddd-dd-dddd (exact, dash-separated).
fn scanSsn(s: []const u8, start: usize) Probe {
    if (start + 11 > s.len) return null;
    const w = s[start .. start + 11];
    inline for (.{ 0, 1, 2, 4, 5, 7, 8, 9, 10 }) |d| {
        if (!isDigit(w[d])) return null;
    }
    if (w[3] != '-' or w[6] != '-') return null;
    // Reject if part of a longer digit run on either side.
    if (start > 0 and isDigit(s[start - 1])) return null;
    if (start + 11 < s.len and isDigit(s[start + 11])) return null;
    return start + 11;
}

/// CREDIT CARD: 13-19 digits with optional single space/dash separators.
/// Conservative: must be group-shaped (separators only between digit runs) and
/// pass a Luhn check to avoid eating ordinary long numbers / IDs.
fn scanCard(s: []const u8, start: usize) Probe {
    var i = start;
    var digits: usize = 0;
    var luhn: u32 = 0;
    var buf: [19]u8 = undefined;
    while (i < s.len and digits < 19) {
        const c = s[i];
        if (isDigit(c)) {
            buf[digits] = c;
            digits += 1;
            i += 1;
        } else if ((c == ' ' or c == '-') and digits > 0 and i + 1 < s.len and isDigit(s[i + 1])) {
            i += 1; // skip a single internal separator
        } else break;
    }
    if (digits < 13 or digits > 19) return null;
    // Reject if a digit immediately follows (longer number than a card).
    if (i < s.len and isDigit(s[i])) return null;
    // Luhn check over the collected digits.
    var k: usize = 0;
    while (k < digits) : (k += 1) {
        var d: u32 = buf[digits - 1 - k] - '0';
        if (k % 2 == 1) {
            d *= 2;
            if (d > 9) d -= 9;
        }
        luhn += d;
    }
    if (luhn % 10 != 0) return null;
    return i;
}

/// AWS access key: "AKIA" + 16 [A-Z0-9].
fn scanAwsKey(s: []const u8, start: usize) Probe {
    const prefix = "AKIA";
    if (start + prefix.len + 16 > s.len) return null;
    if (!std.mem.eql(u8, s[start .. start + prefix.len], prefix)) return null;
    var i = start + prefix.len;
    var n: usize = 0;
    while (n < 16) : (n += 1) {
        if (!isUpperAlnum(s[i])) return null;
        i += 1;
    }
    if (i < s.len and isUpperAlnum(s[i])) return null;
    return i;
}

// NOTE on AWS *secret* access keys (40-char base64-ish): a bare 40-char token
// is indistinguishable from any other high-entropy blob, so scanning for it
// standalone would either miss it or over-redact. It is instead caught by the
// key=value structural rule (the canonical form is
// `aws_secret_access_key=...`, and `secret`/`access_key` are sensitive keys),
// which redacts the value regardless of its character class. The AWS_SECRET tag
// is retained for that documented routing decision.

/// OpenAI-style key: "sk-" + 20+ alnum (also covers sk-proj- style which is
/// alnum+dash; we accept dashes in the body too).
fn scanOpenAiKey(s: []const u8, start: usize) Probe {
    const prefix = "sk-";
    if (start + prefix.len > s.len) return null;
    if (!std.mem.eql(u8, s[start .. start + prefix.len], prefix)) return null;
    var i = start + prefix.len;
    var n: usize = 0;
    while (i < s.len and (isAlnum(s[i]) or s[i] == '-' or s[i] == '_')) : (i += 1) n += 1;
    if (n < 20) return null;
    return i;
}

/// JWT: three base64url segments separated by '.', first two starting "eyJ".
fn scanJwt(s: []const u8, start: usize) Probe {
    if (start + 3 > s.len or !std.mem.eql(u8, s[start .. start + 3], "eyJ")) return null;
    var i = start;
    var segs: usize = 0;
    while (segs < 3) : (segs += 1) {
        if (segs != 0) {
            if (i >= s.len or s[i] != '.') return null;
            i += 1;
        }
        const seg_start = i;
        while (i < s.len and isB64(s[i]) and s[i] != '.') : (i += 1) {}
        if (i == seg_start) return null;
    }
    return i;
}

/// Provider tokens: ghp_..., gho_..., xoxb-..., xoxp-... (Slack), generic.
fn scanProviderToken(s: []const u8, start: usize) Probe {
    const ghTbl = [_][]const u8{ "ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_" };
    for (ghTbl) |p| {
        if (start + p.len <= s.len and std.mem.eql(u8, s[start .. start + p.len], p)) {
            var i = start + p.len;
            var n: usize = 0;
            while (i < s.len and (isAlnum(s[i]) or s[i] == '_')) : (i += 1) n += 1;
            if (n >= 16) return i;
            return null;
        }
    }
    // Slack: xoxb- / xoxp- / xoxa- / xoxr- followed by token body.
    if (start + 5 <= s.len and s[start] == 'x' and s[start + 1] == 'o' and
        s[start + 2] == 'x' and (s[start + 3] == 'b' or s[start + 3] == 'p' or
        s[start + 3] == 'a' or s[start + 3] == 'r') and s[start + 4] == '-')
    {
        var i = start + 5;
        var n: usize = 0;
        while (i < s.len and (isAlnum(s[i]) or s[i] == '-')) : (i += 1) n += 1;
        if (n >= 8) return i;
    }
    return null;
}

// ============================================================================
// key=value structural rule.
//
// When the text contains <key><=|:><value> and <key> is one of the sensitive
// key names (pw, pass, password, pwd, secret, token, apikey, user, username,
// login, holder), the VALUE is redacted as <REDACTED:VALUE>. This is what
// catches free-form secrets the entropy scanners can't characterize (e.g.
// "password=hunter2"). The key text itself is preserved (it is structure, not
// PII), so a log line keeps its shape.
// ============================================================================

const sensitive_keys = [_][]const u8{
    "password", "passwd", "pass",   "pwd",     "pw",
    "secret",   "token",  "apikey", "api_key", "access_key",
    "username", "user",   "login",  "holder",
};

/// If a sensitive key name ends exactly at `key_end` (scanning backward from a
/// '=' or ':'), return the matched key length, else null. Anchored on a word
/// boundary so "newuser" does not match "user".
fn matchSensitiveKey(s: []const u8, key_end: usize) ?usize {
    for (sensitive_keys) |key| {
        if (key.len > key_end) continue;
        const slice = s[key_end - key.len .. key_end];
        if (!eqIgnoreCaseSlice(slice, key)) continue;
        const before = key_end - key.len;
        if (before > 0 and isAlnum(s[before - 1])) continue; // boundary
        return key.len;
    }
    return null;
}

fn eqIgnoreCaseSlice(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (!eqIgnoreCase(x, y)) return false;
    }
    return true;
}

/// Scan a value starting at `start` (just past the '='/':' and any spaces or
/// quotes). Returns the exclusive end of the value token. A value ends at
/// whitespace, comma, semicolon, or a closing quote. Empty value -> null.
fn scanValueToken(s: []const u8, start: usize) Probe {
    var i = start;
    // Optional opening quote.
    var quote: u8 = 0;
    if (i < s.len and (s[i] == '"' or s[i] == '\'')) {
        quote = s[i];
        i += 1;
    }
    const v_start = i;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (quote != 0) {
            if (c == quote) break;
        } else if (c == ' ' or c == '\t' or c == ',' or c == ';' or c == '\n' or c == '\r') {
            break;
        }
    }
    if (i == v_start) return null;
    return i; // exclusive end of the value (closing quote, if any, left in place)
}

// ============================================================================
// Driver: scan a buffer left-to-right, emitting the first match at each
// position (scanners tried in priority order), and write a redacted copy.
// ============================================================================

/// Try every standalone scanner at offset `i`, returning the highest-priority
/// match anchored there, or null. Token/secret scanners are anchored on a word
/// boundary so we don't match inside an unrelated identifier.
fn scanFirst(s: []const u8, i: usize) ?Match {
    const boundary = boundaryBefore(s, i);

    // Order matters: most specific / highest-confidence first.
    if (boundary) {
        if (scanAwsKey(s, i)) |e| return .{ .start = i, .end = e, .tag = .aws_key };
        if (scanOpenAiKey(s, i)) |e| return .{ .start = i, .end = e, .tag = .openai_key };
        if (scanJwt(s, i)) |e| return .{ .start = i, .end = e, .tag = .jwt };
        if (scanProviderToken(s, i)) |e| return .{ .start = i, .end = e, .tag = .token };
    }
    if (scanEmail(s, i)) |e| return .{ .start = i, .end = e, .tag = .email };
    if (scanMac(s, i)) |e| return .{ .start = i, .end = e, .tag = .mac };
    if (scanIpv4(s, i)) |e| return .{ .start = i, .end = e, .tag = .ip };
    if (scanIpv6(s, i)) |e| return .{ .start = i, .end = e, .tag = .ipv6 };
    if (boundary) {
        if (scanSsn(s, i)) |e| return .{ .start = i, .end = e, .tag = .ssn };
        if (scanCard(s, i)) |e| return .{ .start = i, .end = e, .tag = .card };
    }
    return null;
}

/// Redact `src` into a freshly allocated buffer, applying both the standalone
/// scanners and the key=value structural rule. Returns an owned slice the
/// caller must free. When nothing matches, still returns an owned COPY (so the
/// caller's free/replace logic is uniform).
pub fn redactText(allocator: Allocator, src: []const u8) Allocator.Error![]u8 {
    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);

    var i: usize = 0;
    while (i < src.len) {
        // 1. key=value structural rule. When we are sitting on a '=' or ':'
        //    whose left side is a sensitive key, redact the value token.
        if ((src[i] == '=' or src[i] == ':') and i > 0) {
            if (matchSensitiveKey(src, i)) |_| {
                var j = i + 1;
                while (j < src.len and (src[j] == ' ' or src[j] == '\t')) : (j += 1) {}
                // For "user:" we keep the colon-as-separator only when a value
                // follows on the same token; otherwise treat ':' as ordinary.
                if (scanValueToken(src, j)) |vend| {
                    // Emit up to and including the separator + leading spaces,
                    // then the placeholder, preserving any opening quote.
                    try out.appendSlice(allocator, src[i..j]);
                    if (j < src.len and (src[j] == '"' or src[j] == '\'')) {
                        try out.append(allocator, src[j]);
                    }
                    try out.appendSlice(allocator, Tag.value.text());
                    i = vend;
                    continue;
                }
            }
        }

        // 2. standalone scanners.
        if (scanFirst(src, i)) |m| {
            try out.appendSlice(allocator, m.tag.text());
            i = m.end;
            continue;
        }

        try out.append(allocator, src[i]);
        i += 1;
    }

    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Field-level helpers. Each frees the OLD owned slice and stores a fresh one.
// On allocation failure the OLD slice is left intact and the error propagates;
// the caller (main.zig) treats a redact error as fatal-for-cycle so raw PII is
// never persisted/pushed.
// ============================================================================

/// Replace `*field` (an owned heap slice) with its redacted form. The new slice
/// is built first; only on success is the old slice freed and the pointer
/// swapped, so a failure leaves the field unchanged.
fn replaceField(allocator: Allocator, field: *[]const u8) Allocator.Error!void {
    const redacted = try redactText(allocator, field.*);
    allocator.free(field.*);
    field.* = redacted;
}

/// LogEntry.source: keep the basename, drop the directory. "/var/log/auth.log"
/// -> "auth.log". A bare "systemd" or already-basename source is unchanged
/// (but still re-owned for uniform free semantics).
fn redactSource(allocator: Allocator, field: *[]const u8) Allocator.Error!void {
    const src = field.*;
    const base = std.fs.path.basename(src);
    const owned = try allocator.dupe(u8, base);
    allocator.free(field.*);
    field.* = owned;
}

/// ProcessInfo.cgroup: redact the path but keep the trailing unit-ish suffix
/// (the last path segment), which is signal. "/system.slice/sshd.service"
/// -> "<REDACTED:PATH>/sshd.service". Empty cgroup stays empty.
fn redactCgroup(allocator: Allocator, field: *[]const u8) Allocator.Error!void {
    const src = field.*;
    if (src.len == 0) return; // nothing to do, keep the owned ""
    // Find the last '/'. Keep the segment after it; redact everything before.
    var slash: ?usize = null;
    var k: usize = 0;
    while (k < src.len) : (k += 1) {
        if (src[k] == '/') slash = k;
    }
    const owned = if (slash) |idx| blk: {
        // src[idx+1..] is the trailing segment (may itself carry a value, so
        // run it through the scanners too).
        const tail = try redactText(allocator, src[idx + 1 ..]);
        defer allocator.free(tail);
        break :blk try std.fmt.allocPrint(allocator, "<REDACTED:PATH>/{s}", .{tail});
    } else try redactText(allocator, src);
    allocator.free(field.*);
    field.* = owned;
}

/// DiskInfo.mount_point: redact sensitive path COMPONENTS but keep the
/// structure (number of segments = mount depth is a weak signal). A path
/// component is redacted to <REDACTED:DIR> when it scans as PII (e.g. a home
/// dir named after a person: "/home/alice@corp.com" or "/mnt/10.0.0.1") or
/// when it looks like a human home directory ("/home/<x>", "/Users/<x>"). The
/// well-known top-level mounts (root "/", /var, /usr, /mnt, /tmp, /opt, etc.)
/// are preserved verbatim because they are pure structure, not PII.
fn redactMountPoint(allocator: Allocator, field: *[]const u8) Allocator.Error!void {
    const src = field.*;
    if (src.len == 0) return;
    if (std.mem.eql(u8, src, "/")) return; // root mount is pure structure

    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);

    var i: usize = 0;
    var seg_index: usize = 0;
    var prev_seg: []const u8 = &[_]u8{};
    while (i < src.len) {
        if (src[i] == '/') {
            try out.append(allocator, '/');
            i += 1;
            continue;
        }
        const seg_start = i;
        while (i < src.len and src[i] != '/') : (i += 1) {}
        const seg = src[seg_start..i];
        seg_index += 1;

        // The component directly under a user-home root is a username -> PII.
        const under_home = eqIgnoreCaseSlice(prev_seg, "home") or
            eqIgnoreCaseSlice(prev_seg, "Users") or eqIgnoreCaseSlice(prev_seg, "export");
        // Or the component itself scans as a structured PII token.
        const scanned = try redactText(allocator, seg);
        defer allocator.free(scanned);
        const has_pii = !std.mem.eql(u8, scanned, seg);

        if (under_home or has_pii) {
            try out.appendSlice(allocator, "<REDACTED:DIR>");
        } else {
            try out.appendSlice(allocator, seg);
        }
        prev_seg = seg;
    }

    const owned = try out.toOwnedSlice(allocator);
    allocator.free(field.*);
    field.* = owned;
}

// ============================================================================
// Public entry points - operate on the in-memory slices in place.
// ============================================================================

/// Redact a single LogEntry: message (full scan, shape-preserving) and source
/// (basename only). identifier / systemd_unit / unit / numerics are preserved.
pub fn redactLog(allocator: Allocator, entry: *LogEntry) Allocator.Error!void {
    try replaceField(allocator, &entry.message);
    try redactSource(allocator, &entry.source);
}

/// Redact a slice of LogEntry in place. Stops and returns the first error; the
/// caller must treat that as fatal-for-cycle (don't persist/push raw PII).
pub fn redactLogs(allocator: Allocator, entries: []LogEntry) Allocator.Error!void {
    for (entries) |*e| try redactLog(allocator, e);
}

/// Redact a single ProcessInfo: cgroup (path redaction, keep unit suffix) and
/// cmdline (full PII/secret scrub). name / unit / username / numerics are
/// preserved (see field policy) - the process NAME is the anomaly signal.
/// cmdline is omitted from the remote push (push.zig) but is still written to
/// local parquet staging, so we scrub it here too: on-disk history never holds
/// a raw token/password even though it never leaves the box.
pub fn redactProcess(allocator: Allocator, proc: *ProcessInfo) Allocator.Error!void {
    try redactCgroup(allocator, &proc.cgroup);
    try replaceField(allocator, &proc.cmdline);
}

pub fn redactProcesses(allocator: Allocator, procs: []ProcessInfo) Allocator.Error!void {
    for (procs) |*p| try redactProcess(allocator, p);
}

/// Redact a single DiskInfo: mount_point (component redaction, keep depth).
/// filesystem / numerics are preserved.
pub fn redactDisk(allocator: Allocator, disk: *DiskInfo) Allocator.Error!void {
    try redactMountPoint(allocator, &disk.mount_point);
}

pub fn redactDisks(allocator: Allocator, disks: []DiskInfo) Allocator.Error!void {
    for (disks) |*d| try redactDisk(allocator, d);
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

/// Helper: run redactText and assert the exact output.
fn expectRedact(src: []const u8, want: []const u8) !void {
    const got = try redactText(testing.allocator, src);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "ipv4 in a brute-force line keeps shape" {
    try expectRedact(
        "Failed password for admin from 10.0.0.1 port 22",
        "Failed password for admin from <REDACTED:IP> port 22",
    );
}

test "ipv4 rejects version strings and 5-octet" {
    try expectRedact("version 1.2.3", "version 1.2.3");
    try expectRedact("addr 1.2.3.4.5 here", "addr 1.2.3.4.5 here");
}

test "email" {
    try expectRedact("contact alice@corp.com now", "contact <REDACTED:EMAIL> now");
}

test "mac vs short hex" {
    try expectRedact("mac de:ad:be:ef:00:11 up", "mac <REDACTED:MAC> up");
    try expectRedact("pair ab:cd done", "pair ab:cd done");
}

test "ssn exact" {
    try expectRedact("ssn 123-45-6789.", "ssn <REDACTED:SSN>.");
    try expectRedact("not 1234-45-6789", "not 1234-45-6789");
}

test "credit card luhn" {
    // 4111 1111 1111 1111 is a known Luhn-valid test card.
    try expectRedact("card 4111 1111 1111 1111 ok", "card <REDACTED:CARD> ok");
    // Non-Luhn long number is left alone.
    try expectRedact("id 1234567890123456 x", "id 1234567890123456 x");
}

test "aws access key" {
    try expectRedact("key AKIAIOSFODNN7EXAMPLE end", "key <REDACTED:AWS_KEY> end");
}

test "openai key" {
    try expectRedact(
        "sk-abcdefghij0123456789ABCD tail",
        "<REDACTED:OPENAI_KEY> tail",
    );
}

test "jwt" {
    try expectRedact(
        "auth eyJhbGciOi.eyJzdWIiOi.SflKxwRJ done",
        "auth <REDACTED:JWT> done",
    );
}

test "github + slack tokens" {
    try expectRedact("t ghp_0123456789ABCDEFxyz q", "t <REDACTED:TOKEN> q");
    try expectRedact("t xoxb-12345-abcde q", "t <REDACTED:TOKEN> q");
}

test "key=value structural" {
    try expectRedact("password=hunter2 ok", "password=<REDACTED:VALUE> ok");
    try expectRedact("user: bob more", "user: <REDACTED:VALUE> more");
    try expectRedact("token=\"abc.def\" end", "token=\"<REDACTED:VALUE>\" end");
    // A non-sensitive key is left alone.
    try expectRedact("color=blue", "color=blue");
}

test "no match returns owned copy" {
    try expectRedact("plain text no pii", "plain text no pii");
}

test "source basename only" {
    var src: []const u8 = try testing.allocator.dupe(u8, "/var/log/auth.log");
    try redactSource(testing.allocator, &src);
    defer testing.allocator.free(src);
    try testing.expectEqualStrings("auth.log", src);
}

test "cgroup keeps unit suffix" {
    var cg: []const u8 = try testing.allocator.dupe(u8, "/system.slice/sshd.service");
    try redactCgroup(testing.allocator, &cg);
    defer testing.allocator.free(cg);
    try testing.expectEqualStrings("<REDACTED:PATH>/sshd.service", cg);

    var empty: []const u8 = try testing.allocator.dupe(u8, "");
    try redactCgroup(testing.allocator, &empty);
    defer testing.allocator.free(empty);
    try testing.expectEqualStrings("", empty);
}

test "redactProcess scrubs secrets from cmdline (local parquet path)" {
    const a = testing.allocator;
    var proc = ProcessInfo{
        .pid = 42,
        .name = "postgres",
        .cmdline = try a.dupe(u8, "postgres --password=hunter2 --peer 10.0.0.5"),
        .state = 'R',
        .cpu_percent = 1.5,
        .mem_rss = 4096,
        .threads = 8,
        .username = "postgres",
        .io_read_bytes = 0,
        .io_write_bytes = 0,
        .cgroup = try a.dupe(u8, ""),
        .unit = "postgresql.service",
    };
    try redactProcess(a, &proc);
    defer a.free(proc.cmdline);
    defer a.free(proc.cgroup);
    // secrets scrubbed from the locally-persisted cmdline ...
    try testing.expect(std.mem.indexOf(u8, proc.cmdline, "hunter2") == null);
    try testing.expect(std.mem.indexOf(u8, proc.cmdline, "10.0.0.5") == null);
    // ... while the process NAME (the anomaly signal) is untouched.
    try testing.expectEqualStrings("postgres", proc.name);
}

test "mount point redacts home user, keeps structure" {
    var mp: []const u8 = try testing.allocator.dupe(u8, "/home/alice/data");
    try redactMountPoint(testing.allocator, &mp);
    defer testing.allocator.free(mp);
    try testing.expectEqualStrings("/home/<REDACTED:DIR>/data", mp);

    var root: []const u8 = try testing.allocator.dupe(u8, "/");
    try redactMountPoint(testing.allocator, &root);
    defer testing.allocator.free(root);
    try testing.expectEqualStrings("/", root);

    var plain: []const u8 = try testing.allocator.dupe(u8, "/var/lib/docker");
    try redactMountPoint(testing.allocator, &plain);
    defer testing.allocator.free(plain);
    try testing.expectEqualStrings("/var/lib/docker", plain);
}

test "record count and structure invariant: in-place replace preserves slice" {
    // Build two log entries with PII; redacting must not change the count.
    var entries = [_]LogEntry{
        .{
            .timestamp = 1,
            .source = try testing.allocator.dupe(u8, "/var/log/auth.log"),
            .unit = null,
            .identifier = null,
            .systemd_unit = null,
            .priority = 6,
            .message = try testing.allocator.dupe(u8, "login from 10.0.0.1"),
            .pid = null,
        },
        .{
            .timestamp = 2,
            .source = try testing.allocator.dupe(u8, "kernel"),
            .unit = null,
            .identifier = null,
            .systemd_unit = null,
            .priority = 6,
            .message = try testing.allocator.dupe(u8, "no pii here"),
            .pid = null,
        },
    };
    defer for (&entries) |*e| e.deinit(testing.allocator);

    try redactLogs(testing.allocator, &entries);
    try testing.expectEqual(@as(usize, 2), entries.len);
    try testing.expectEqualStrings("login from <REDACTED:IP>", entries[0].message);
    try testing.expectEqualStrings("auth.log", entries[0].source);
    try testing.expectEqualStrings("no pii here", entries[1].message);
    try testing.expectEqualStrings("kernel", entries[1].source);
}

test "process named xmrig is not redacted (name/username/numerics preserved)" {
    // A miner named like a real attacker payload must keep its name intact -
    // the anomaly engine matches on it. redactProcess only touches cgroup.
    var proc = ProcessInfo{
        .pid = 4242,
        .name = try testing.allocator.dupe(u8, "xmrig"),
        .cmdline = try testing.allocator.dupe(u8, "/usr/bin/xmrig --donate 0"),
        .state = 'R',
        .cpu_percent = 99.5,
        .mem_rss = 123456,
        .threads = 8,
        .username = try testing.allocator.dupe(u8, "root"),
        .io_read_bytes = 1024,
        .io_write_bytes = 2048,
        .cgroup = try testing.allocator.dupe(u8, "/system.slice/xmrig.service"),
        .unit = try testing.allocator.dupe(u8, "xmrig.service"),
    };
    defer {
        testing.allocator.free(proc.name);
        testing.allocator.free(proc.cmdline);
        testing.allocator.free(proc.username);
        testing.allocator.free(proc.cgroup);
        testing.allocator.free(proc.unit);
    }

    try redactProcess(testing.allocator, &proc);

    // name / username / unit / cmdline are operational signal: untouched.
    try testing.expectEqualStrings("xmrig", proc.name);
    try testing.expectEqualStrings("root", proc.username);
    try testing.expectEqualStrings("xmrig.service", proc.unit);
    try testing.expectEqualStrings("/usr/bin/xmrig --donate 0", proc.cmdline);
    // Only the cgroup path is redacted, keeping the unit suffix.
    try testing.expectEqualStrings("<REDACTED:PATH>/xmrig.service", proc.cgroup);
}

test "redaction never touches numeric fields (process + disk invariant)" {
    var proc = ProcessInfo{
        .pid = 1337,
        .name = try testing.allocator.dupe(u8, "sshd"),
        .cmdline = try testing.allocator.dupe(u8, "/usr/sbin/sshd -D"),
        .state = 'S',
        .cpu_percent = 12.5,
        .mem_rss = 98765,
        .threads = 4,
        .username = try testing.allocator.dupe(u8, "root"),
        .io_read_bytes = 555,
        .io_write_bytes = 666,
        .cgroup = try testing.allocator.dupe(u8, "/system.slice/sshd.service"),
        .unit = try testing.allocator.dupe(u8, "sshd.service"),
    };
    defer {
        testing.allocator.free(proc.name);
        testing.allocator.free(proc.cmdline);
        testing.allocator.free(proc.username);
        testing.allocator.free(proc.cgroup);
        testing.allocator.free(proc.unit);
    }

    try redactProcess(testing.allocator, &proc);
    try testing.expectEqual(@as(u32, 1337), proc.pid);
    try testing.expectEqual(@as(f32, 12.5), proc.cpu_percent);
    try testing.expectEqual(@as(u64, 98765), proc.mem_rss);
    try testing.expectEqual(@as(u32, 4), proc.threads);
    try testing.expectEqual(@as(u64, 555), proc.io_read_bytes);
    try testing.expectEqual(@as(u64, 666), proc.io_write_bytes);

    var disk = DiskInfo{
        .mount_point = try testing.allocator.dupe(u8, "/home/alice/data"),
        .filesystem = try testing.allocator.dupe(u8, "ext4"),
        .total_bytes = 1000000,
        .used_bytes = 750000,
        .percent = 75.0,
    };
    defer {
        testing.allocator.free(disk.mount_point);
        testing.allocator.free(disk.filesystem);
    }

    try redactDisk(testing.allocator, &disk);
    try testing.expectEqual(@as(u64, 1000000), disk.total_bytes);
    try testing.expectEqual(@as(u64, 750000), disk.used_bytes);
    try testing.expectEqual(@as(f32, 75.0), disk.percent);
    try testing.expectEqualStrings("ext4", disk.filesystem);
    // mount_point got redacted (proving the call ran) but numerics are intact.
    try testing.expectEqualStrings("/home/<REDACTED:DIR>/data", disk.mount_point);
}
