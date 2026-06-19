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
const ner = @import("ner");

const ProcessInfo = collector.ProcessInfo;
const DiskInfo = collector.DiskInfo;
const LogEntry = logs.LogEntry;

/// Optional NER backend. The interface is pure (ner.zig links nothing); the
/// concrete model-backed backend lives in ner_pf.zig and is constructed in
/// main.zig. A null `Ner` means deterministic-scanners-only - the daemon
/// degrades to that whenever NER is disabled or the model is absent, and never
/// emits raw PII as a result.
const Ner = ner.Ner;
const Kind = ner.Kind;

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
    // NER-derived tags. These come ONLY from the model backend (ner.Kind),
    // never from a byte scanner, so the deterministic recall guarantee is
    // unaffected. They are merged into the same emit pass as the scanner tags.
    person,
    address,
    phone,
    url,
    date,
    account_number,
    secret,

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
            .person => "<REDACTED:PERSON>",
            .address => "<REDACTED:ADDRESS>",
            .phone => "<REDACTED:PHONE>",
            .url => "<REDACTED:URL>",
            .date => "<REDACTED:DATE>",
            .account_number => "<REDACTED:ACCOUNT>",
            .secret => "<REDACTED:SECRET>",
        };
    }
};

/// Map a model entity Kind onto the placeholder Tag emitted in its place. This
/// is the only crossing from the NER taxonomy to the redaction taxonomy.
fn tagForKind(kind: Kind) Tag {
    return switch (kind) {
        .person => .person,
        .address => .address,
        .email => .email,
        .phone => .phone,
        .url => .url,
        .date => .date,
        .account_number => .account_number,
        .secret => .secret,
        .other => .value, // unknown class -> generic redaction (never raw)
    };
}

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
    var saw_hex_letter: bool = false;
    while (i < s.len) {
        // Hex group (0-4 hex digits).
        var hx: usize = 0;
        while (i < s.len and isHex(s[i]) and hx < 4) : (i += 1) {
            if (!isDigit(s[i])) saw_hex_letter = true;
            hx += 1;
        }
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
    // Require enough structure to be confidently an IPv6 literal. A "::" elision
    // is unambiguous. Without one, demand a hex LETTER (a-f) somewhere so a plain
    // decimal "12:34:56" timestamp is not mistaken for an address (real IPv6
    // almost always carries a hex letter or a "::"; the rare all-decimal literal
    // is an acceptable miss vs. destroying every timestamp's signal).
    if (saw_double and groups >= 1 and colons >= 2) return i;
    if (!saw_double and groups >= 3 and colons >= 2 and saw_hex_letter) return i;
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

/// If a sensitive key name precedes the separator at `sep` (a '=' or ':'),
/// return the matched key length, else null. The key may be separated from the
/// separator by whitespace and/or a closing quote, so `password = x`,
/// `"password":"x"`, and `"token" : "x"` all anchor on the key. Anchored on a
/// word boundary so "newuser" does not match "user".
fn matchSensitiveKey(s: []const u8, sep: usize) ?usize {
    // Walk back over spaces/tabs, then an optional single closing quote, to find
    // where the key token actually ends.
    var key_end = sep;
    while (key_end > 0 and (s[key_end - 1] == ' ' or s[key_end - 1] == '\t')) key_end -= 1;
    if (key_end > 0 and (s[key_end - 1] == '"' or s[key_end - 1] == '\'')) key_end -= 1;
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
// Merged NER + deterministic redaction.
//
// The deterministic scanners (above) are the recall floor: measured 100% on
// structured secrets, and the daemon must NEVER regress that. The optional NER
// model adds free-form PII the scanners can't characterize (human names, street
// addresses). To compose them in ONE pass we:
//   1. collect every deterministic match as a span (origin = .deterministic),
//   2. collect every NER span as a span (origin = .ner),
//   3. merge into a sorted, non-overlapping list with a fixed tie-break:
//        - on overlap, the DETERMINISTIC span wins (protects structured-secret
//          recall: e.g. a loose NER address span that swallows the start of an
//          AKIA key must not suppress the deterministic AWS_KEY redaction),
//        - among same-origin overlaps, the LONGEST span wins, then earliest
//          start, so coverage is maximal and deterministic.
//   4. emit: copy unmatched bytes verbatim, replace each merged span with its
//      tag placeholder.
// A null ner reduces this to exactly the deterministic-only output, byte for
// byte, so degradation is provably lossless.
// ============================================================================

const Origin = enum { deterministic, ner };

/// A span tagged with where it came from, used only inside the merge. Carries
/// the same [start,end) + Tag as the emitter needs, plus origin/score for the
/// tie-break.
const TaggedSpan = struct {
    start: usize,
    end: usize,
    tag: Tag,
    origin: Origin,
    score: f32,
};

/// Walk `src` exactly like redactText does, but instead of emitting, append a
/// TaggedSpan for each deterministic match (standalone scanner OR key=value
/// value-portion). The spans are produced left-to-right and non-overlapping by
/// construction (the walk advances past each match), matching redactText's
/// emit order precisely.
fn collectDeterministic(
    allocator: Allocator,
    src: []const u8,
    list: *std.ArrayList(TaggedSpan),
) Allocator.Error!void {
    var i: usize = 0;
    while (i < src.len) {
        // 1. key=value structural rule (same condition as redactText).
        if ((src[i] == '=' or src[i] == ':') and i > 0) {
            if (matchSensitiveKey(src, i)) |_| {
                var j = i + 1;
                while (j < src.len and (src[j] == ' ' or src[j] == '\t')) : (j += 1) {}
                if (scanValueToken(src, j)) |vend| {
                    // The placeholder replaces only the value token (after the
                    // separator, spaces, and an optional opening quote), exactly
                    // mirroring redactText. So the span starts past the quote.
                    var v_start = j;
                    if (j < src.len and (src[j] == '"' or src[j] == '\'')) v_start = j + 1;
                    try list.append(allocator, .{
                        .start = v_start,
                        .end = vend,
                        .tag = .value,
                        .origin = .deterministic,
                        .score = 1.0,
                    });
                    i = vend;
                    continue;
                }
            }
        }

        // 2. standalone scanners.
        if (scanFirst(src, i)) |m| {
            try list.append(allocator, .{
                .start = m.start,
                .end = m.end,
                .tag = m.tag,
                .origin = .deterministic,
                .score = 1.0,
            });
            i = m.end;
            continue;
        }

        i += 1;
    }
}

/// Two half-open ranges overlap iff each starts before the other ends.
inline fn overlaps(a_start: usize, a_end: usize, b_start: usize, b_end: usize) bool {
    return a_start < b_end and b_start < a_end;
}

/// Order spans for the final emit / NER-NER tie-break: earliest start first;
/// on equal start the LONGEST wins (maximal coverage), then by tag so the order
/// is fully deterministic.
fn lessThanSpan(_: void, a: TaggedSpan, b: TaggedSpan) bool {
    if (a.start != b.start) return a.start < b.start;
    const a_len = a.end - a.start;
    const b_len = b.end - b.start;
    if (a_len != b_len) return a_len > b_len; // longer first
    return @intFromEnum(a.tag) < @intFromEnum(b.tag);
}

/// Append the sub-ranges of `cand` NOT already covered by a span in `accepted`,
/// tagged as `cand`. This CLIPS a (possibly over-extended) NER span around the
/// deterministic recall floor (and already-accepted NER spans) instead of
/// dropping it whole. Dropping a colliding NER span leaks the PII it covers
/// OUTSIDE the overlap - e.g. a real-model address span that over-extends to
/// swallow the start of an AKIA key would, if dropped, leave the street address
/// in cleartext while only the key got redacted. Clipping keeps both.
fn clipAndAppend(
    allocator: Allocator,
    cand: TaggedSpan,
    accepted: *std.ArrayList(TaggedSpan),
) Allocator.Error!void {
    // Snapshot the spans overlapping `cand`, sorted by start, so the appends
    // below (which grow `accepted`) don't disturb this walk.
    var blockers = std.ArrayList(TaggedSpan){};
    defer blockers.deinit(allocator);
    for (accepted.items) |a| {
        if (overlaps(cand.start, cand.end, a.start, a.end)) try blockers.append(allocator, a);
    }
    std.mem.sort(TaggedSpan, blockers.items, {}, lessThanSpan);

    var cursor = cand.start;
    for (blockers.items) |b| {
        if (b.start > cursor) {
            const seg_end = @min(b.start, cand.end);
            if (seg_end > cursor) try accepted.append(allocator, .{
                .start = cursor,
                .end = seg_end,
                .tag = cand.tag,
                .origin = cand.origin,
                .score = cand.score,
            });
        }
        if (b.end > cursor) cursor = b.end;
        if (cursor >= cand.end) return;
    }
    if (cursor < cand.end) try accepted.append(allocator, .{
        .start = cursor,
        .end = cand.end,
        .tag = cand.tag,
        .origin = cand.origin,
        .score = cand.score,
    });
}

/// Redact `src` into a fresh owned buffer using the deterministic scanners AND,
/// when `maybe_ner` is non-null, the NER model's spans, merged in one pass.
/// `threshold` gates model spans by confidence. When `maybe_ner` is null this
/// is byte-identical to redactText(src).
///
/// Merge rule (DETERMINISTIC-WINS, structural - not sort-order-dependent):
///   1. Every deterministic span is kept. They are non-overlapping among
///      themselves by construction (collectDeterministic walks left-to-right
///      and advances past each match), and they are the 100%-recall floor the
///      daemon must never regress. A loose NER span that overlaps a structured
///      secret (e.g. an over-extended address span swallowing the start of an
///      AKIA key) must NOT suppress that secret's redaction.
///   2. Each NER span is CLIPPED around the deterministic spans (and
///      already-accepted NER spans); its non-overlapping residual sub-ranges are
///      kept (clipAndAppend). The deterministic span keeps its exact range + tag
///      over the overlap; the rest of a loose NER span is STILL redacted - so an
///      over-extended address span that bleeds into an AKIA key redacts BOTH the
///      address part AND (deterministically) the key, with no leak. Among
///      mutually-overlapping NER spans the longest is processed first.
/// This makes "deterministic wins the overlap, NER fills the gaps" a structural
/// property of the merge rather than a fragile consequence of sort order.
pub fn redactTextMerged(
    allocator: Allocator,
    src: []const u8,
    maybe_ner: ?Ner,
    threshold: f32,
) ner.Error![]u8 {
    // Deterministic spans: the recall floor, kept unconditionally.
    var det = std.ArrayList(TaggedSpan){};
    defer det.deinit(allocator);
    try collectDeterministic(allocator, src, &det);

    // Final accepted span list starts as a copy of the deterministic spans.
    var accepted = std.ArrayList(TaggedSpan){};
    defer accepted.deinit(allocator);
    try accepted.appendSlice(allocator, det.items);

    if (maybe_ner) |n| {
        // A model inference failure must NOT lose the deterministic spans:
        // degrade to deterministic-only for this field rather than dropping it.
        if (n.classifySpans(allocator, src, threshold)) |ner_spans| {
            defer allocator.free(ner_spans);

            // Sort NER spans longest-first-at-start so the longest of a set of
            // mutually-overlapping NER spans is the one we get to accept.
            var ner_list = std.ArrayList(TaggedSpan){};
            defer ner_list.deinit(allocator);
            for (ner_spans) |s| {
                if (s.end <= s.start or s.end > src.len) continue;
                try ner_list.append(allocator, .{
                    .start = s.start,
                    .end = s.end,
                    .tag = tagForKind(s.kind),
                    .origin = .ner,
                    .score = s.score,
                });
            }
            std.mem.sort(TaggedSpan, ner_list.items, {}, lessThanSpan);

            // Clip each NER candidate around the already-accepted spans
            // (deterministic floor + accepted NER) and keep the residual
            // sub-ranges, rather than dropping a colliding span whole (which
            // would leak the PII the span covers OUTSIDE the overlap).
            for (ner_list.items) |cand| {
                try clipAndAppend(allocator, cand, &accepted);
            }
        } else |_| {
            // fall through with deterministic spans only.
        }
    }

    // Emit in left-to-right order. accepted is now fully non-overlapping.
    std.mem.sort(TaggedSpan, accepted.items, {}, lessThanSpan);

    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);
    var cursor: usize = 0;
    for (accepted.items) |s| {
        if (s.start < cursor) continue; // defensive: never re-emit overlapped bytes
        if (s.start > cursor) try out.appendSlice(allocator, src[cursor..s.start]);
        try out.appendSlice(allocator, s.tag.text());
        cursor = s.end;
    }
    if (cursor < src.len) try out.appendSlice(allocator, src[cursor..]);
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Field-level helpers. Each frees the OLD owned slice and stores a fresh one.
// On allocation failure the OLD slice is left intact and the error propagates;
// the caller (main.zig) treats a redact error as fatal-for-cycle so raw PII is
// never persisted/pushed.
// ============================================================================

/// Default model confidence threshold for free-text fields. Below this, NER
/// spans are dropped. Conservative enough to keep precision but low enough to
/// catch the names/addresses the deterministic scanners miss.
const default_ner_threshold: f32 = 0.5;

/// Replace `*field` (an owned heap slice) with its redacted form, running the
/// deterministic scanners and (when `maybe_ner` is non-null) the NER model,
/// merged in one pass. The new slice is built first; only on success is the old
/// slice freed and the pointer swapped, so a failure leaves the field
/// unchanged. A null `maybe_ner` is byte-identical to the deterministic-only
/// path, so this is the single helper for every free-text field.
fn replaceField(allocator: Allocator, field: *[]const u8, maybe_ner: ?Ner) ner.Error!void {
    const redacted = try redactTextMerged(allocator, field.*, maybe_ner, default_ner_threshold);
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

/// Redact a single LogEntry: message (full scan + optional NER, shape-
/// preserving) and source (basename only). identifier / systemd_unit / unit /
/// numerics are preserved. `maybe_ner` null => deterministic-only (never raw).
pub fn redactLog(allocator: Allocator, entry: *LogEntry, maybe_ner: ?Ner) ner.Error!void {
    try replaceField(allocator, &entry.message, maybe_ner);
    try redactSource(allocator, &entry.source);
}

/// Redact a slice of LogEntry in place. Stops and returns the first error; the
/// caller must treat that as fatal-for-cycle (don't persist/push raw PII).
pub fn redactLogs(allocator: Allocator, entries: []LogEntry, maybe_ner: ?Ner) ner.Error!void {
    for (entries) |*e| try redactLog(allocator, e, maybe_ner);
}

/// Redact a single ProcessInfo: cgroup (path redaction, keep unit suffix) and
/// cmdline (full PII/secret scrub + optional NER). name / unit / username /
/// numerics are preserved (see field policy) - the process NAME is the anomaly
/// signal. cmdline is omitted from the remote push (push.zig) but is still
/// written to local parquet staging, so we scrub it here too: on-disk history
/// never holds a raw token/password even though it never leaves the box.
/// `maybe_ner` null => deterministic-only.
pub fn redactProcess(allocator: Allocator, proc: *ProcessInfo, maybe_ner: ?Ner) ner.Error!void {
    try redactCgroup(allocator, &proc.cgroup);
    try replaceField(allocator, &proc.cmdline, maybe_ner);
}

pub fn redactProcesses(allocator: Allocator, procs: []ProcessInfo, maybe_ner: ?Ner) ner.Error!void {
    for (procs) |*p| try redactProcess(allocator, p, maybe_ner);
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

test "ipv6 redacts real literals but not decimal timestamps" {
    // A "::" elision is unambiguous IPv6.
    try expectRedact("from fe80::1 ok", "from <REDACTED:IPV6> ok");
    // Hex letters present -> confidently an address even without "::".
    try expectRedact("addr 2001:db8:dead here", "addr <REDACTED:IPV6> here");
    // Plain decimal "HH:MM:SS" timestamps must keep their signal (no hex letter,
    // no "::") - previously mis-redacted as IPv6.
    try expectRedact("at 12:34:56 done", "at 12:34:56 done");
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

test "key=value tolerates spaces and quotes before the separator" {
    // JSON-shaped logs: closing quote sits between key and ':'.
    try expectRedact("{\"password\":\"hunter2\"}", "{\"password\":\"<REDACTED:VALUE>\"}");
    try expectRedact("\"token\": \"abc\" end", "\"token\": \"<REDACTED:VALUE>\" end");
    // Spaced config style: whitespace around '='.
    try expectRedact("password = hunter2 ok", "password = <REDACTED:VALUE> ok");
    // Boundary still holds: a longer word ending in a key name does not match.
    try expectRedact("mypassword = keep", "mypassword = keep");
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
    try redactProcess(a, &proc, null);
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

    try redactLogs(testing.allocator, &entries, null);
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

    try redactProcess(testing.allocator, &proc, null);

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

    try redactProcess(testing.allocator, &proc, null);
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

// ============================================================================
// NER merge tests. These use a STUB backend implementing the same ner.Ner
// vtable (no model, no .so), so they run in CI unconditionally and lock the
// tie-break rules that protect deterministic-secret recall. The model-backed
// end-to-end path is exercised separately in ner_pf.zig (gated on the model).
// ============================================================================

/// Test-only NER backend returning a fixed span set. Mirrors the production
/// backend's contract: caller-owned []Span, offsets clamped to text bounds.
const StubNer = struct {
    spans: []const ner.Span,

    fn classifySpans(
        ctx: *anyopaque,
        allocator: Allocator,
        text: []const u8,
        threshold: f32,
    ) ner.Error![]ner.Span {
        const self: *StubNer = @ptrCast(@alignCast(ctx));
        var list = std.ArrayList(ner.Span){};
        errdefer list.deinit(allocator);
        for (self.spans) |s| {
            if (s.score < threshold) continue;
            const start = @min(s.start, text.len);
            const end = @min(s.end, text.len);
            if (end <= start) continue;
            try list.append(allocator, .{ .start = start, .end = end, .kind = s.kind, .score = s.score });
        }
        return list.toOwnedSlice(allocator);
    }

    fn deinitFn(_: *anyopaque) void {}

    const vtable = Ner.VTable{ .classifySpans = classifySpans, .deinit = deinitFn };

    fn asNer(self: *StubNer) Ner {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

/// A backend whose classifySpans always fails, to prove a model-inference error
/// degrades to deterministic-only (does NOT drop the field).
const FailingNer = struct {
    fn classifySpans(_: *anyopaque, _: Allocator, _: []const u8, _: f32) ner.Error![]ner.Span {
        return ner.Error.Inference;
    }
    fn deinitFn(_: *anyopaque) void {}
    const vtable = Ner.VTable{ .classifySpans = classifySpans, .deinit = deinitFn };
    var instance: u8 = 0;
    fn asNer() Ner {
        return .{ .ctx = @ptrCast(&instance), .vtable = &vtable };
    }
};

fn expectMerged(src: []const u8, maybe_ner: ?Ner, want: []const u8) !void {
    const got = try redactTextMerged(testing.allocator, src, maybe_ner, 0.5);
    defer testing.allocator.free(got);
    try testing.expectEqualStrings(want, got);
}

test "merged: null ner is byte-identical to deterministic-only" {
    // Every existing deterministic case must pass unchanged through the merged
    // path when no model is present (the degradation guarantee).
    try expectMerged("login from 10.0.0.1", null, "login from <REDACTED:IP>");
    try expectMerged("password=hunter2 ok", null, "password=<REDACTED:VALUE> ok");
    try expectMerged("plain text no pii", null, "plain text no pii");
    try expectMerged("token=\"abc.def\" end", null, "token=\"<REDACTED:VALUE>\" end");
}

test "merged END-TO-END: NER (person+address) AND scanners (email+AKIA) compose" {
    // The canonical regression: the model contributes person + address spans,
    // the deterministic scanners contribute the email + AWS key, all in ONE
    // merged pass. This runs in CI always (stub backend, no model).
    const text = "Contact John Doe at jdoe@example.com, 742 Evergreen Terrace, key AKIAIOSFODNN7EXAMPLE";
    var stub = StubNer{
        .spans = &[_]ner.Span{
            .{ .start = 8, .end = 16, .kind = .person, .score = 1.0 }, // "John Doe"
            .{ .start = 38, .end = 59, .kind = .address, .score = 0.9 }, // "742 Evergreen Terrace"
        },
    };
    try expectMerged(
        text,
        stub.asNer(),
        "Contact <REDACTED:PERSON> at <REDACTED:EMAIL>, <REDACTED:ADDRESS>, key <REDACTED:AWS_KEY>",
    );
}

test "merged: deterministic EMAIL beats an overlapping NER span on the same range" {
    // A model span covering the same bytes as the email must NOT replace the
    // deterministic EMAIL tag - deterministic wins the overlap.
    const text = "mail bob@corp.com end";
    var stub = StubNer{
        .spans = &[_]ner.Span{
            .{ .start = 5, .end = 17, .kind = .person, .score = 0.99 }, // overlaps the email
        },
    };
    try expectMerged(text, stub.asNer(), "mail <REDACTED:EMAIL> end");
}

test "merged: a loose NER address overlapping a deterministic AWS_KEY redacts BOTH (no leak)" {
    // Mirrors the real model behavior observed in the lib smoke test: the
    // address span over-extends to swallow the START of the AKIA key. The merge
    // must (1) keep the deterministic AWS_KEY redaction (recall floor), AND
    // (2) still redact the address part - by CLIPPING the loose NER span around
    // the key, not dropping it whole. Dropping it leaks "742 Evergreen Terrace".
    const text = "742 Evergreen Terrace, key AKIAIOSFODNN7EXAMPLE";
    // NER address [0,40) over-extends into the key (which starts at 27).
    var stub = StubNer{ .spans = &[_]ner.Span{
        .{ .start = 0, .end = 40, .kind = .address, .score = 0.7 },
    } };
    const got = try redactTextMerged(testing.allocator, text, stub.asNer(), 0.5);
    defer testing.allocator.free(got);
    // (1) the AKIA key is redacted deterministically ...
    try testing.expect(std.mem.indexOf(u8, got, "AKIAIOSFODNN7EXAMPLE") == null);
    try testing.expect(std.mem.indexOf(u8, got, "<REDACTED:AWS_KEY>") != null);
    // (2) ... and the address no longer leaks (the residual is still redacted).
    try testing.expect(std.mem.indexOf(u8, got, "Evergreen") == null);
    try testing.expect(std.mem.indexOf(u8, got, "742") == null);
    try testing.expect(std.mem.indexOf(u8, got, "<REDACTED:ADDRESS>") != null);
}

test "merged: overlapping NER spans both redact - the residual is clipped, not dropped" {
    // Two NER spans partially overlap. The longer is emitted; the shorter's
    // NON-overlapping residual is STILL redacted (clip, don't drop). The model
    // claimed those bytes are PII, so dropping the residual would leak them -
    // the same bug class as the deterministic-overlap case.
    const text = "aaaaaaaaaa"; // 10 bytes, no deterministic matches
    var stub = StubNer{
        .spans = &[_]ner.Span{
            .{ .start = 0, .end = 8, .kind = .person, .score = 0.9 }, // person [0,8)
            .{ .start = 4, .end = 10, .kind = .address, .score = 0.9 }, // overlaps, extends to 10
        },
    };
    // person [0,8) -> PERSON; the address residual [8,10) -> ADDRESS. Nothing leaks.
    try expectMerged(text, stub.asNer(), "<REDACTED:PERSON><REDACTED:ADDRESS>");
}

test "merged: adjacent (touching, non-overlapping) NER spans both emit" {
    const text = "aaaabbbb"; // 8 bytes
    var stub = StubNer{
        .spans = &[_]ner.Span{
            .{ .start = 0, .end = 4, .kind = .person, .score = 0.9 },
            .{ .start = 4, .end = 8, .kind = .address, .score = 0.9 }, // touches at 4
        },
    };
    try expectMerged(text, stub.asNer(), "<REDACTED:PERSON><REDACTED:ADDRESS>");
}

test "merged: pure-NER person span with no deterministic match emits PERSON" {
    const text = "hello John Doe goodbye";
    var stub = StubNer{ .spans = &[_]ner.Span{
        .{ .start = 6, .end = 14, .kind = .person, .score = 0.95 },
    } };
    try expectMerged(text, stub.asNer(), "hello <REDACTED:PERSON> goodbye");
}

test "merged: failing NER backend degrades to deterministic-only (field not dropped)" {
    // A model inference error must fall back to the deterministic spans for that
    // field, NOT drop the whole field or return raw.
    const text = "login from 10.0.0.1 password=hunter2";
    try expectMerged(text, FailingNer.asNer(), "login from <REDACTED:IP> password=<REDACTED:VALUE>");
}

test "merged: redactProcess with a stub NER scrubs cmdline person + secret" {
    const a = testing.allocator;
    var stub = StubNer{
        .spans = &[_]ner.Span{
            .{ .start = 12, .end = 20, .kind = .person, .score = 0.95 }, // "John Doe"
        },
    };
    var proc = ProcessInfo{
        .pid = 7,
        .name = "svc",
        .cmdline = try a.dupe(u8, "--operator John Doe --password=hunter2"),
        .state = 'R',
        .cpu_percent = 0.0,
        .mem_rss = 0,
        .threads = 1,
        .username = "root",
        .io_read_bytes = 0,
        .io_write_bytes = 0,
        .cgroup = try a.dupe(u8, ""),
        .unit = "svc.service",
    };
    try redactProcess(a, &proc, stub.asNer());
    defer a.free(proc.cmdline);
    defer a.free(proc.cgroup);
    try testing.expect(std.mem.indexOf(u8, proc.cmdline, "John Doe") == null);
    try testing.expect(std.mem.indexOf(u8, proc.cmdline, "hunter2") == null);
    try testing.expect(std.mem.indexOf(u8, proc.cmdline, "<REDACTED:PERSON>") != null);
    try testing.expect(std.mem.indexOf(u8, proc.cmdline, "<REDACTED:VALUE>") != null);
}
