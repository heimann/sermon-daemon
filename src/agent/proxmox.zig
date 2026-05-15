// Proxmox runtime detection and inventory collection.
//
// V1 slice (per commissions/proxmox-modification-plan.md items 1+2):
//   1. detectRuntime(): returns whether this daemon is on a Proxmox host,
//      inside a Proxmox LXC, or neither. Reads /proc/self/cgroup for the
//      container case and /etc/pve + `pveversion` for the host case.
//   2. collectInventory(): on a Proxmox host, shells out to
//      `pvesh get /cluster/resources --type vm --output-format json` and
//      returns the (vmid, name, node, type, status, maxmem, maxcpu, uptime)
//      tuples for every LXC container and QEMU VM in the cluster.
//
// Tags are intentionally NOT shipped in V1 per the dossier's privacy posture:
// PVE tags routinely carry secret-shaped strings (API tokens, customer
// names) that need a redaction filter we have not designed yet. Defer to a
// follow-up PR.
//
// Per-CT cgroup metrics, per-PID container attribution, journal-per-CT, and
// VM internals are all explicit follow-ups (mod plan items 3-7).

const std = @import("std");
const Allocator = std.mem.Allocator;
const ChildProcess = std.process.Child;

// Cap free-text fields (name, version) to a reasonable length so a hostile
// or buggy tag value can't blow up the payload. 256 bytes matches the
// dossier's recommended cap.
const max_text_len: usize = 256;

// Wall-clock cap for shelling out to pveversion / pvesh, applied as a single
// deadline that spans the entire read loop (not per-poll-iteration). A wedged
// Corosync or hung /etc/pve FUSE mount is a real Proxmox failure mode
// (split-brain, quorum loss); without this the main collection loop blocks
// indefinitely. A pvesh that trickles one byte every 4.9s would defeat a
// per-poll timeout, so we capture the deadline once before the loop and
// recompute the remaining budget each iteration.
const pve_shell_timeout_ms: i64 = 5000;

pub const PveShellError = error{
    PveShellTimeout,
    PveShellSpawnFailed,
    PveShellExitFailed,
    PveShellOutputTooLarge,
};

pub const Runtime = union(enum) {
    not_proxmox,
    /// Daemon is running on a Proxmox VE host. node = cluster node name
    /// (hypervisor hostname); runtime_version is the `pve-manager/...` line
    /// from pveversion.
    host: HostInfo,
    /// Daemon is running inside an LXC container managed by Proxmox.
    container: ContainerInfo,

    pub fn deinit(self: *Runtime, allocator: Allocator) void {
        switch (self.*) {
            .not_proxmox => {},
            .host => |*h| {
                allocator.free(h.node);
                allocator.free(h.runtime_version);
            },
            .container => {},
        }
    }
};

pub const HostInfo = struct {
    node: []const u8,
    runtime_version: []const u8,
};

pub const ContainerInfo = struct {
    vmid: u32,
};

pub const ContainerEntry = struct {
    vmid: u32,
    name: []const u8,
    node: []const u8,
    /// "lxc" or "qemu" - free-form so a future Proxmox release adding a
    /// new guest type doesn't trip us up.
    type: []const u8,
    /// "running" | "stopped" | "paused" | etc. Free-form for the same
    /// reason.
    status: []const u8,
    maxmem: u64,
    maxcpu: f64,
    /// 0 when not reported (e.g. stopped guest).
    uptime: u64,

    pub fn deinit(self: *ContainerEntry, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.node);
        allocator.free(self.type);
        allocator.free(self.status);
    }
};

pub fn freeContainers(allocator: Allocator, entries: []ContainerEntry) void {
    for (entries) |*entry| entry.deinit(allocator);
    allocator.free(entries);
}

// ============================================================================
// Detection
// ============================================================================

/// Detect whether this daemon is on a Proxmox host, inside an LXC, or neither.
/// On the host case the returned struct owns allocated strings; the caller
/// must call `Runtime.deinit` when done.
pub fn detectRuntime(allocator: Allocator) Runtime {
    // Container case takes precedence: if /proc/self/cgroup says we're inside
    // an LXC, we are, regardless of what /etc/pve looks like (e.g. nested
    // container scenarios where /etc/pve is bind-mounted in).
    if (detectContainer()) |vmid_opt| {
        if (vmid_opt) |vmid| {
            return .{ .container = .{ .vmid = vmid } };
        }
    } else |_| {
        // Couldn't read cgroup; fall through and check host case.
    }

    if (detectHost(allocator)) |host_info| {
        if (host_info) |info| {
            return .{ .host = info };
        }
    } else |_| {
        // Couldn't determine host info; fall through.
    }

    return .not_proxmox;
}

fn detectContainer() !?u32 {
    const file = std.fs.openFileAbsolute("/proc/self/cgroup", .{}) catch return null;
    defer file.close();

    var buf: [4096]u8 = undefined;
    const n = file.readAll(&buf) catch return null;
    return parseCgroupVmid(buf[0..n]);
}

/// Parse /proc/self/cgroup output to find an LXC vmid prefix.
///
/// PVE 8/9 (cgroup v2) writes a single line: `0::/lxc/<vmid>/...`
/// PVE 7 (legacy) used `/lxc.payload.<vmid>/...` in v1 hierarchies.
/// We handle both.
///
/// Important: the marker must anchor to the start of the cgroup path field,
/// not match anywhere in the line. `std.mem.indexOf(u8, line, "/lxc/")`
/// would also match a host admin's custom path like
/// `/system.slice/my-lxc/foo`. The cgroup format is `<id>:<ctrls>:<path>`
/// (v1) or `0::<path>` (v2), and `<path>` always starts with `/`, so a
/// leading `:` before the marker reliably identifies the path field.
pub fn parseCgroupVmid(content: []const u8) ?u32 {
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        // Anchor on the colon that precedes the path field. This catches
        // both `0::/lxc/...` (v2) and `12:devices:/lxc.payload....` (v1)
        // while rejecting `/system.slice/my-lxc/foo`.
        const marker_v2 = ":/lxc/";
        const marker_v1 = ":/lxc.payload.";
        if (std.mem.indexOf(u8, line, marker_v2)) |idx| {
            const after = line[idx + marker_v2.len ..];
            if (parseLeadingDigits(after)) |vmid| return vmid;
        }
        if (std.mem.indexOf(u8, line, marker_v1)) |idx| {
            const after = line[idx + marker_v1.len ..];
            if (parseLeadingDigits(after)) |vmid| return vmid;
        }
    }
    return null;
}

fn parseLeadingDigits(s: []const u8) ?u32 {
    var end: usize = 0;
    while (end < s.len and s[end] >= '0' and s[end] <= '9') : (end += 1) {}
    if (end == 0) return null;
    return std.fmt.parseInt(u32, s[0..end], 10) catch null;
}

fn detectHost(allocator: Allocator) !?HostInfo {
    // /etc/pve only exists on a Proxmox host (FUSE-mounted Corosync DB).
    var dir = std.fs.openDirAbsolute("/etc/pve", .{}) catch return null;
    dir.close();

    const node = try readHostname(allocator);
    errdefer allocator.free(node);

    const version = try readPveVersion(allocator);
    errdefer allocator.free(version);

    return HostInfo{ .node = node, .runtime_version = version };
}

fn readHostname(allocator: Allocator) ![]u8 {
    const file = std.fs.openFileAbsolute("/etc/hostname", .{}) catch {
        return allocator.dupe(u8, "unknown");
    };
    defer file.close();

    var buf: [256]u8 = undefined;
    const n = file.readAll(&buf) catch {
        return allocator.dupe(u8, "unknown");
    };
    const trimmed = std.mem.trim(u8, buf[0..n], " \n\t\r");
    if (trimmed.len == 0) return allocator.dupe(u8, "unknown");
    const capped = trimmed[0..@min(trimmed.len, max_text_len)];
    return allocator.dupe(u8, capped);
}

fn readPveVersion(allocator: Allocator) ![]u8 {
    // Run `pveversion` and parse the first line, e.g. "pve-manager/9.1.1/...".
    // If pveversion isn't installed or hangs (we're not actually on a PVE
    // host even though /etc/pve exists, or Corosync is wedged) fall back to
    // "unknown" rather than blocking the daemon's startup.
    var stdout_buf: [1024]u8 = undefined;
    const n = runPveShell(
        allocator,
        &[_][]const u8{"pveversion"},
        &stdout_buf,
    ) catch return allocator.dupe(u8, "unknown");

    const first_line_end = std.mem.indexOfScalar(u8, stdout_buf[0..n], '\n') orelse n;
    const trimmed = std.mem.trim(u8, stdout_buf[0..first_line_end], " \n\t\r");
    if (trimmed.len == 0) return allocator.dupe(u8, "unknown");
    const capped = trimmed[0..@min(trimmed.len, max_text_len)];
    return allocator.dupe(u8, capped);
}

/// Spawn `argv` and read stdout into `out_buf` with a wall-clock timeout
/// covering the entire read loop. Returns the number of bytes read on
/// EOF/exit; on deadline expiry (or read error with the child still alive)
/// the child is killed and PveShellTimeout is returned. Stderr is discarded.
///
/// This is the synchronous-poll idiom used to keep the agent's main loop
/// responsive when pvesh / pveversion gets stuck on a wedged Corosync ring
/// or hung /etc/pve FUSE mount. Compare logs.zig's O_NONBLOCK approach
/// (also picked to avoid blocking on subprocess I/O).
///
/// The deadline is captured once before the loop and the remaining budget is
/// recomputed each iteration; a child that trickles a byte every 4.9s won't
/// defeat the timeout the way a per-poll-iteration deadline would.
fn runPveShell(allocator: Allocator, argv: []const []const u8, out_buf: []u8) !usize {
    var child = ChildProcess.init(argv, allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Ignore;

    child.spawn() catch return error.PveShellSpawnFailed;

    const out = child.stdout orelse {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
        return error.PveShellSpawnFailed;
    };

    var written: usize = 0;
    var fds = [_]std.posix.pollfd{.{
        .fd = out.handle,
        .events = std.posix.POLL.IN,
        .revents = 0,
    }};

    // True when the child may still be alive and must be killed before wait()
    // (deadline expired, poll error, or read error). Renamed from
    // deadline_reached because read errors take the same cleanup path.
    var should_kill = false;
    const deadline_ms = std.time.milliTimestamp() + pve_shell_timeout_ms;
    while (true) {
        const remaining = @max(@as(i64, 0), deadline_ms - std.time.milliTimestamp());
        if (remaining == 0) {
            should_kill = true;
            break;
        }
        const ready = std.posix.poll(&fds, @intCast(remaining)) catch {
            should_kill = true;
            break;
        };
        if (ready == 0) {
            should_kill = true;
            break;
        }
        if (written >= out_buf.len) break;
        // POLLHUP without POLLIN can still indicate readable EOF on Linux
        // pipes; try to read either way and let the read handle EOF.
        const n = out.read(out_buf[written..]) catch {
            // Genuine read error: child may still be alive, so route through
            // kill+wait rather than falling into the bare wait() below.
            should_kill = true;
            break;
        };
        if (n == 0) break;
        written += n;
        fds[0].revents = 0;
    }

    if (should_kill) {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
        return error.PveShellTimeout;
    }

    const term = child.wait() catch return error.PveShellExitFailed;
    switch (term) {
        .Exited => |code| if (code != 0) return error.PveShellExitFailed,
        else => return error.PveShellExitFailed,
    }
    return written;
}

// ============================================================================
// Inventory (collectInventory + parsePveshJson)
// ============================================================================

/// Collect the cluster's container/VM inventory by shelling out to
/// `pvesh get /cluster/resources --type vm --output-format json`. Caller
/// owns the returned slice; free with `freeContainers`.
///
/// Only call this when the runtime is `host` - on non-Proxmox hosts pvesh
/// won't exist and the call will error.
pub fn collectInventory(allocator: Allocator) ![]ContainerEntry {
    const argv = [_][]const u8{
        "pvesh",
        "get",
        "/cluster/resources",
        "--type",
        "vm",
        "--output-format",
        "json",
    };

    // pvesh's output for a small cluster is small (a few KB per guest);
    // 1 MB is enough headroom for hundreds of guests.
    const max_output: usize = 1024 * 1024;

    var stdout = std.ArrayList(u8){};
    defer stdout.deinit(allocator);
    try stdout.ensureTotalCapacity(allocator, 16 * 1024);

    try runPveShellStreaming(allocator, &argv, &stdout, max_output);

    return parsePveshJson(allocator, stdout.items);
}

/// Same wall-clock-timeout idiom as runPveShell (single deadline spanning the
/// whole loop, recomputed each iteration), but streams into a growable buffer
/// (capped at max_output) for arbitrarily-large inventory output.
fn runPveShellStreaming(
    allocator: Allocator,
    argv: []const []const u8,
    out: *std.ArrayList(u8),
    max_output: usize,
) !void {
    var child = ChildProcess.init(argv, allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Ignore;

    child.spawn() catch return error.PveShellSpawnFailed;

    const stdout_pipe = child.stdout orelse {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
        return error.PveShellSpawnFailed;
    };

    var fds = [_]std.posix.pollfd{.{
        .fd = stdout_pipe.handle,
        .events = std.posix.POLL.IN,
        .revents = 0,
    }};

    var read_buf: [4096]u8 = undefined;
    // True when the child may still be alive and must be killed before wait()
    // (deadline expired, poll error, or read error).
    var should_kill = false;
    var too_large = false;
    const deadline_ms = std.time.milliTimestamp() + pve_shell_timeout_ms;

    while (true) {
        const remaining = @max(@as(i64, 0), deadline_ms - std.time.milliTimestamp());
        if (remaining == 0) {
            should_kill = true;
            break;
        }
        const ready = std.posix.poll(&fds, @intCast(remaining)) catch {
            should_kill = true;
            break;
        };
        if (ready == 0) {
            should_kill = true;
            break;
        }
        const n = stdout_pipe.read(&read_buf) catch {
            // Genuine read error: child may still be alive, route through
            // kill+wait below.
            should_kill = true;
            break;
        };
        if (n == 0) break;
        if (out.items.len + n > max_output) {
            too_large = true;
            break;
        }
        try out.appendSlice(allocator, read_buf[0..n]);
        fds[0].revents = 0;
    }

    if (should_kill or too_large) {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
        if (too_large) return error.PveShellOutputTooLarge;
        return error.PveShellTimeout;
    }

    const term = child.wait() catch return error.PveShellExitFailed;
    switch (term) {
        .Exited => |code| if (code != 0) return error.PveShellExitFailed,
        else => return error.PveShellExitFailed,
    }
}

/// Parse the output of `pvesh get /cluster/resources --type vm --output-format json`.
/// Exposed for tests; no I/O.
pub fn parsePveshJson(allocator: Allocator, json_bytes: []const u8) ![]ContainerEntry {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const arr = switch (parsed.value) {
        .array => |a| a,
        else => return error.UnexpectedJsonShape,
    };

    var out = std.ArrayList(ContainerEntry){};
    errdefer {
        for (out.items) |*e| e.deinit(allocator);
        out.deinit(allocator);
    }

    for (arr.items) |row| {
        const obj = switch (row) {
            .object => |o| o,
            else => continue,
        };

        // pvesh /cluster/resources --type vm rows include both lxc/qemu
        // guests and (depending on PVE version) the node/storage entries
        // when --type isn't honored. Skip anything that isn't lxc/qemu.
        const type_str = jsonString(obj.get("type")) orelse continue;
        if (!std.mem.eql(u8, type_str, "lxc") and !std.mem.eql(u8, type_str, "qemu")) continue;

        const vmid_val = obj.get("vmid") orelse continue;
        const vmid: u32 = switch (vmid_val) {
            .integer => |i| if (i < 0 or i > std.math.maxInt(u32)) continue else @intCast(i),
            .float => |f| if (f < 0 or f > @as(f64, @floatFromInt(std.math.maxInt(u32)))) continue else @intFromFloat(f),
            else => continue,
        };

        const node_str = jsonString(obj.get("node")) orelse continue;
        const name_str = jsonString(obj.get("name")) orelse type_str; // some rows have no name (templates)
        const status_str = jsonString(obj.get("status")) orelse "unknown";

        const maxmem: u64 = jsonU64(obj.get("maxmem")) orelse 0;
        const maxcpu: f64 = jsonF64(obj.get("maxcpu")) orelse 0;
        const uptime: u64 = jsonU64(obj.get("uptime")) orelse 0;

        // Dup each owned string field into a local with its own errdefer
        // BEFORE assembling the struct. The previous shape registered a
        // single `errdefer entry.deinit(...)` after all four `try dup(...)`
        // calls, which leaks any earlier dups if a later one OOMs (only
        // already-appended entries are covered by the outer errdefer).
        const name_owned = try dupCapped(allocator, name_str);
        errdefer allocator.free(name_owned);
        const node_owned = try dupCapped(allocator, node_str);
        errdefer allocator.free(node_owned);
        const type_owned = try dupCapped(allocator, type_str);
        errdefer allocator.free(type_owned);
        const status_owned = try dupCapped(allocator, status_str);
        errdefer allocator.free(status_owned);

        try out.append(allocator, .{
            .vmid = vmid,
            .name = name_owned,
            .node = node_owned,
            .type = type_owned,
            .status = status_owned,
            .maxmem = maxmem,
            .maxcpu = maxcpu,
            .uptime = uptime,
        });
    }

    return out.toOwnedSlice(allocator);
}

fn dupCapped(allocator: Allocator, s: []const u8) ![]u8 {
    const capped = s[0..@min(s.len, max_text_len)];
    return allocator.dupe(u8, capped);
}

// ============================================================================
// Per-container metrics (cgroup v2)
// ============================================================================
//
// V1 minimum: cpu_pct, mem_current, mem_max. PSI/OOM/IO/PIDs deferred to a
// follow-up - the dashboard panel only needs CPU + memory to surface "this
// container is hot." The richer signals are valuable but easier to add once
// John tells us which questions he's actually asking.

pub const ContainerMetrics = struct {
    vmid: u32,
    /// CPU% over the interval since the previous sample. NaN if no prior
    /// sample exists for this vmid (first cycle after CT start, or daemon
    /// restart). Computed from cpu.stat:usage_usec deltas vs wall-clock.
    cpu_pct: f64,
    /// Bytes currently used (memory.current).
    mem_current: u64,
    /// Memory limit in bytes (memory.max). null when set to "max" - common
    /// for unprivileged CTs without a cgroup memory limit.
    mem_max: ?u64,
};

/// Holds the previous sample for each running container so cpu_pct can be
/// computed as a delta. Keyed by vmid; entries for stopped/removed CTs age
/// out naturally because we only insert when a container shows up in the
/// current cycle.
pub const ContainerMetricsState = struct {
    prev: std.AutoHashMap(u32, PrevSample),

    const PrevSample = struct {
        cpu_usage_usec: u64,
        wall_ns: i128,
    };

    pub fn init(allocator: Allocator) ContainerMetricsState {
        return .{ .prev = std.AutoHashMap(u32, PrevSample).init(allocator) };
    }

    pub fn deinit(self: *ContainerMetricsState) void {
        self.prev.deinit();
    }
};

/// Read per-container metrics for every running container in `containers`.
/// Stopped containers contribute no row (their cgroup files don't exist).
/// Caller owns the returned slice.
pub fn collectContainerMetrics(
    allocator: Allocator,
    state: *ContainerMetricsState,
    containers: []const ContainerEntry,
) ![]ContainerMetrics {
    var out = std.ArrayList(ContainerMetrics){};
    errdefer out.deinit(allocator);
    try out.ensureTotalCapacity(allocator, containers.len);

    const now_ns = std.time.nanoTimestamp();

    for (containers) |entry| {
        if (!std.mem.eql(u8, entry.status, "running")) continue;

        var path_buf: [128]u8 = undefined;

        const cpu_usec = readCgroupCpuUsageUsec(&path_buf, entry.vmid) catch continue;
        const mem_current = readCgroupSingleU64(&path_buf, entry.vmid, "memory.current") catch continue;
        const mem_max = readCgroupMemoryMax(&path_buf, entry.vmid) catch null;

        const cpu_pct: f64 = if (state.prev.get(entry.vmid)) |prev| blk: {
            const usec_delta = if (cpu_usec >= prev.cpu_usage_usec)
                cpu_usec - prev.cpu_usage_usec
            else
                0; // counter wrap or restart - treat as no delta this cycle
            const ns_delta = now_ns - prev.wall_ns;
            if (ns_delta <= 0) break :blk std.math.nan(f64);
            // usec_delta / (ns_delta/1000) gives CPU-usec per wall-usec, *100
            // for percent. A single fully-pinned core reads as 100%; a
            // 4-vCPU CT pinned across all cores reads as 400%.
            const wall_usec: f64 = @as(f64, @floatFromInt(ns_delta)) / 1000.0;
            break :blk @as(f64, @floatFromInt(usec_delta)) / wall_usec * 100.0;
        } else std.math.nan(f64);

        try state.prev.put(entry.vmid, .{
            .cpu_usage_usec = cpu_usec,
            .wall_ns = now_ns,
        });

        try out.append(allocator, .{
            .vmid = entry.vmid,
            .cpu_pct = cpu_pct,
            .mem_current = mem_current,
            .mem_max = mem_max,
        });
    }

    return out.toOwnedSlice(allocator);
}

/// Read /sys/fs/cgroup/lxc/<vmid>/cpu.stat and parse the `usage_usec` line.
/// Cgroup v2 file format: `key value\n` per line.
fn readCgroupCpuUsageUsec(path_buf: []u8, vmid: u32) !u64 {
    const path = try std.fmt.bufPrint(path_buf, "/sys/fs/cgroup/lxc/{d}/cpu.stat", .{vmid});
    const file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();

    var content_buf: [4096]u8 = undefined;
    const n = try file.readAll(&content_buf);

    var lines = std.mem.tokenizeScalar(u8, content_buf[0..n], '\n');
    while (lines.next()) |line| {
        var parts = std.mem.tokenizeScalar(u8, line, ' ');
        const key = parts.next() orelse continue;
        if (!std.mem.eql(u8, key, "usage_usec")) continue;
        const val_str = parts.next() orelse continue;
        return std.fmt.parseInt(u64, val_str, 10) catch continue;
    }
    return error.UsageUsecNotFound;
}

/// Read a cgroup file containing a single u64 (e.g. memory.current).
fn readCgroupSingleU64(path_buf: []u8, vmid: u32, leaf: []const u8) !u64 {
    const path = try std.fmt.bufPrint(path_buf, "/sys/fs/cgroup/lxc/{d}/{s}", .{ vmid, leaf });
    const file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();

    var content_buf: [64]u8 = undefined;
    const n = try file.readAll(&content_buf);
    const trimmed = std.mem.trim(u8, content_buf[0..n], " \n\t\r");
    return std.fmt.parseInt(u64, trimmed, 10);
}

/// memory.max is either a u64 byte count or the literal "max" (unlimited,
/// the default for unprivileged LXCs without a memory limit). Return null
/// for "max" so the dashboard can render "no limit" rather than U64_MAX.
fn readCgroupMemoryMax(path_buf: []u8, vmid: u32) !?u64 {
    const path = try std.fmt.bufPrint(path_buf, "/sys/fs/cgroup/lxc/{d}/memory.max", .{vmid});
    const file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();

    var content_buf: [64]u8 = undefined;
    const n = try file.readAll(&content_buf);
    const trimmed = std.mem.trim(u8, content_buf[0..n], " \n\t\r");
    if (std.mem.eql(u8, trimmed, "max")) return null;
    return try std.fmt.parseInt(u64, trimmed, 10);
}

fn jsonString(v: ?std.json.Value) ?[]const u8 {
    const val = v orelse return null;
    return switch (val) {
        .string => |s| s,
        else => null,
    };
}

fn jsonU64(v: ?std.json.Value) ?u64 {
    const val = v orelse return null;
    return switch (val) {
        .integer => |i| if (i < 0) 0 else @intCast(i),
        .float => |f| if (f < 0) 0 else @intFromFloat(f),
        else => null,
    };
}

fn jsonF64(v: ?std.json.Value) ?f64 {
    const val = v orelse return null;
    return switch (val) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => null,
    };
}

// ============================================================================
// Tests
// ============================================================================
//
// These are pure unit tests over canned strings. No real Proxmox host is
// touched; integration tests against a real PVE instance are deferred to
// future empirical work (see commissions/proxmox-empirical-deltas.md).

test "parseCgroupVmid: PVE 8/9 v2 single-line format" {
    const input = "0::/lxc/100/ns/init.scope\n";
    try std.testing.expectEqual(@as(?u32, 100), parseCgroupVmid(input));
}

test "parseCgroupVmid: PVE 7 legacy lxc.payload format" {
    const input = "12:devices:/lxc.payload.205/system.slice\n";
    try std.testing.expectEqual(@as(?u32, 205), parseCgroupVmid(input));
}

test "parseCgroupVmid: host-side cgroup (no lxc marker) returns null" {
    const input = "0::/system.slice/sermon-agent.service\n";
    try std.testing.expectEqual(@as(?u32, null), parseCgroupVmid(input));
}

test "parseCgroupVmid: empty input returns null" {
    try std.testing.expectEqual(@as(?u32, null), parseCgroupVmid(""));
}

test "parseCgroupVmid: vmid embedded in deeper path still parses" {
    const input = "0::/lxc/9999/foo/bar/baz\n";
    try std.testing.expectEqual(@as(?u32, 9999), parseCgroupVmid(input));
}

test "parsePveshJson: typical aniara-shaped response (5 lxc + 1 qemu)" {
    const allocator = std.testing.allocator;
    const json =
        \\[
        \\  {"type":"lxc","vmid":100,"name":"docker-host","node":"aniara","status":"running","maxmem":8589934592,"maxcpu":4,"uptime":12345},
        \\  {"type":"lxc","vmid":101,"name":"web","node":"aniara","status":"running","maxmem":2147483648,"maxcpu":2,"uptime":99},
        \\  {"type":"lxc","vmid":102,"name":"db","node":"aniara","status":"stopped","maxmem":1073741824,"maxcpu":1},
        \\  {"type":"qemu","vmid":200,"name":"win-vm","node":"aniara","status":"running","maxmem":17179869184,"maxcpu":8,"uptime":42},
        \\  {"type":"storage","storage":"local","node":"aniara"}
        \\]
    ;

    const entries = try parsePveshJson(allocator, json);
    defer freeContainers(allocator, entries);

    try std.testing.expectEqual(@as(usize, 4), entries.len);

    try std.testing.expectEqual(@as(u32, 100), entries[0].vmid);
    try std.testing.expectEqualStrings("docker-host", entries[0].name);
    try std.testing.expectEqualStrings("aniara", entries[0].node);
    try std.testing.expectEqualStrings("lxc", entries[0].type);
    try std.testing.expectEqualStrings("running", entries[0].status);
    try std.testing.expectEqual(@as(u64, 8589934592), entries[0].maxmem);
    try std.testing.expectEqual(@as(f64, 4), entries[0].maxcpu);
    try std.testing.expectEqual(@as(u64, 12345), entries[0].uptime);

    try std.testing.expectEqual(@as(u32, 102), entries[2].vmid);
    try std.testing.expectEqualStrings("stopped", entries[2].status);
    try std.testing.expectEqual(@as(u64, 0), entries[2].uptime); // missing in JSON

    try std.testing.expectEqualStrings("qemu", entries[3].type);
}

test "parsePveshJson: empty array yields no entries" {
    const allocator = std.testing.allocator;
    const entries = try parsePveshJson(allocator, "[]");
    defer freeContainers(allocator, entries);
    try std.testing.expectEqual(@as(usize, 0), entries.len);
}

test "parsePveshJson: skips rows without vmid" {
    const allocator = std.testing.allocator;
    const json =
        \\[
        \\  {"type":"lxc","name":"orphan","node":"aniara","status":"running"},
        \\  {"type":"lxc","vmid":300,"name":"good","node":"aniara","status":"running","maxmem":1024,"maxcpu":1}
        \\]
    ;
    const entries = try parsePveshJson(allocator, json);
    defer freeContainers(allocator, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u32, 300), entries[0].vmid);
}

test "parsePveshJson: caps over-long name field at 256 bytes" {
    const allocator = std.testing.allocator;
    var long_buf: [600]u8 = undefined;
    @memset(&long_buf, 'A');

    const json = try std.fmt.allocPrint(
        allocator,
        \\[{{"type":"lxc","vmid":1,"name":"{s}","node":"n","status":"running","maxmem":1,"maxcpu":1}}]
    ,
        .{long_buf},
    );
    defer allocator.free(json);

    const entries = try parsePveshJson(allocator, json);
    defer freeContainers(allocator, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(usize, 256), entries[0].name.len);
}

// ---- Adversarial inputs (added in #116 review followups) ----
//
// Defensive coverage against the kinds of malformed payloads pvesh could
// produce after a future schema bump or a partial socket read; these are
// the cheap pure-function cases.

test "parsePveshJson: truncated input mid-array errors cleanly (no panic, no leak)" {
    const allocator = std.testing.allocator;
    const json =
        \\[{"type":"lxc","vmid":100,"name":"x","node":"n","status":"running"
    ;
    // We don't pin to a specific JSON parser error variant; the contract is
    // that we error rather than panic and that the testing allocator
    // doesn't flag a leak.
    const result = parsePveshJson(allocator, json);
    try std.testing.expect(std.meta.isError(result));
}

test "parsePveshJson: vmid as a string is skipped (not coerced)" {
    const allocator = std.testing.allocator;
    const json =
        \\[
        \\  {"type":"lxc","vmid":"100","name":"x","node":"n","status":"running","maxmem":1,"maxcpu":1},
        \\  {"type":"lxc","vmid":101,"name":"good","node":"n","status":"running","maxmem":1,"maxcpu":1}
        \\]
    ;
    const entries = try parsePveshJson(allocator, json);
    defer freeContainers(allocator, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u32, 101), entries[0].vmid);
}

test "parsePveshJson: vmid exceeding u32 max is skipped" {
    const allocator = std.testing.allocator;
    const json =
        \\[
        \\  {"type":"lxc","vmid":99999999999,"name":"x","node":"n","status":"running","maxmem":1,"maxcpu":1},
        \\  {"type":"lxc","vmid":42,"name":"ok","node":"n","status":"running","maxmem":1,"maxcpu":1}
        \\]
    ;
    const entries = try parsePveshJson(allocator, json);
    defer freeContainers(allocator, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u32, 42), entries[0].vmid);
}

test "parsePveshJson: top-level non-array errors" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.UnexpectedJsonShape,
        parsePveshJson(allocator, "{\"oops\":true}"),
    );
}

test "parsePveshJson: null in optional fields falls back to defaults" {
    const allocator = std.testing.allocator;
    const json =
        \\[
        \\  {"type":"lxc","vmid":1,"name":null,"node":"n","status":null,"maxmem":null,"maxcpu":null,"uptime":null}
        \\]
    ;
    const entries = try parsePveshJson(allocator, json);
    defer freeContainers(allocator, entries);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    // name falls back to type_str when missing or non-string; status falls back to "unknown"
    try std.testing.expectEqualStrings("lxc", entries[0].name);
    try std.testing.expectEqualStrings("unknown", entries[0].status);
    try std.testing.expectEqual(@as(u64, 0), entries[0].maxmem);
    try std.testing.expectEqual(@as(f64, 0), entries[0].maxcpu);
    try std.testing.expectEqual(@as(u64, 0), entries[0].uptime);
}

test "parseCgroupVmid: rejects /lxc/ embedded in a host admin's cgroup path" {
    // Without the `:/lxc/` anchor this would falsely report vmid=42 because
    // `/system.slice/my-lxc/42/...` contains the substring "/lxc/42/".
    const input = "0::/system.slice/my-lxc/42/init.scope\n";
    try std.testing.expectEqual(@as(?u32, null), parseCgroupVmid(input));
}

test "parseCgroupVmid: rejects /lxc.payload. embedded mid-path" {
    const input = "12:devices:/system.slice/foo-lxc.payload.99/system.slice\n";
    try std.testing.expectEqual(@as(?u32, null), parseCgroupVmid(input));
}
