const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const version = b.option([]const u8, "version", "Sermon Daemon version") orelse "dev";

    const options = b.addOptions();
    options.addOption([]const u8, "version", version);

    // ── Shared modules for cross-imports ──
    const collector_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/collector.zig"),
        .target = target,
        .optimize = optimize,
    });

    const logs_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/logs.zig"),
        .target = target,
        .optimize = optimize,
    });

    const rules_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/rules.zig"),
        .target = target,
        .optimize = optimize,
    });
    rules_mod.addImport("logs", logs_mod);

    const proc_self_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/proc_self.zig"),
        .target = target,
        .optimize = optimize,
    });

    const proxmox_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/proxmox.zig"),
        .target = target,
        .optimize = optimize,
    });

    const push_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/push.zig"),
        .target = target,
        .optimize = optimize,
    });
    push_mod.addImport("collector", collector_mod);
    push_mod.addImport("logs", logs_mod);
    push_mod.addImport("rules", rules_mod);
    push_mod.addImport("proc_self", proc_self_mod);
    push_mod.addImport("proxmox", proxmox_mod);
    push_mod.addOptions("build_options", options);

    const storage_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/storage.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    storage_mod.addImport("collector", collector_mod);
    storage_mod.addImport("logs", logs_mod);
    storage_mod.addImport("proxmox", proxmox_mod);
    storage_mod.addIncludePath(b.path("lib"));
    storage_mod.addLibraryPath(b.path("lib"));
    storage_mod.linkSystemLibrary("duckdb", .{});

    // ── sermon-agent (daemon) ──
    const agent_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    agent_mod.addImport("collector", collector_mod);
    agent_mod.addImport("logs", logs_mod);
    agent_mod.addImport("rules", rules_mod);
    agent_mod.addImport("proc_self", proc_self_mod);
    agent_mod.addImport("proxmox", proxmox_mod);
    agent_mod.addImport("push", push_mod);
    agent_mod.addImport("storage", storage_mod);
    agent_mod.addIncludePath(b.path("lib"));
    agent_mod.addLibraryPath(b.path("lib"));
    agent_mod.linkSystemLibrary("duckdb", .{});
    agent_mod.addRPathSpecial("$ORIGIN/../lib");

    const agent = b.addExecutable(.{
        .name = "sermon-agent",
        .root_module = agent_mod,
    });
    b.installArtifact(agent);

    // ── sermon (CLI) ──
    const cli_mod = b.createModule(.{
        .root_source_file = b.path("src/cli/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    cli_mod.addImport("collector", collector_mod);
    cli_mod.addImport("logs", logs_mod);
    cli_mod.addImport("storage", storage_mod);
    cli_mod.addIncludePath(b.path("lib"));
    cli_mod.addLibraryPath(b.path("lib"));
    cli_mod.linkSystemLibrary("duckdb", .{});
    cli_mod.addRPathSpecial("$ORIGIN/../lib");

    const cli = b.addExecutable(.{
        .name = "sermon",
        .root_module = cli_mod,
    });
    b.installArtifact(cli);

    // ── Named build steps ──
    const agent_step = b.step("agent", "Build the agent daemon");
    agent_step.dependOn(&agent.step);

    const cli_step = b.step("cli", "Build the CLI");
    cli_step.dependOn(&cli.step);

    // ── Tests ──
    const storage_test_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/storage.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    storage_test_mod.addImport("collector", collector_mod);
    storage_test_mod.addImport("logs", logs_mod);
    storage_test_mod.addImport("proxmox", proxmox_mod);
    storage_test_mod.addIncludePath(b.path("lib"));
    storage_test_mod.addLibraryPath(b.path("lib"));
    storage_test_mod.linkSystemLibrary("duckdb", .{});

    const storage_tests = b.addTest(.{
        .root_module = storage_test_mod,
    });
    storage_tests.addRPath(b.path("lib"));

    const collector_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/agent/collector.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    const logs_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/agent/logs.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const rules_test_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/rules.zig"),
        .target = target,
        .optimize = optimize,
    });
    rules_test_mod.addImport("logs", logs_mod);

    const rules_tests = b.addTest(.{
        .root_module = rules_test_mod,
    });

    const push_test_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/push.zig"),
        .target = target,
        .optimize = optimize,
    });
    push_test_mod.addImport("collector", collector_mod);
    push_test_mod.addImport("logs", logs_mod);
    push_test_mod.addImport("rules", rules_mod);
    push_test_mod.addImport("proc_self", proc_self_mod);
    push_test_mod.addImport("proxmox", proxmox_mod);
    push_test_mod.addOptions("build_options", options);

    const push_tests = b.addTest(.{
        .root_module = push_test_mod,
    });

    const proc_self_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/agent/proc_self.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const proxmox_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/agent/proxmox.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // ── Parquet hot tier (plan 25): staging + roll + query ──
    // Self-contained modules that share the typed structs and link libduckdb
    // like storage. Not yet wired into the daemon loop (deliberate follow-up).
    const staging_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/staging.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    staging_mod.addImport("collector", collector_mod);
    staging_mod.addImport("logs", logs_mod);
    staging_mod.addImport("proxmox", proxmox_mod);

    const roll_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/roll.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    roll_mod.addImport("collector", collector_mod);
    roll_mod.addImport("logs", logs_mod);
    roll_mod.addImport("proxmox", proxmox_mod);
    roll_mod.addImport("staging", staging_mod);
    roll_mod.addIncludePath(b.path("lib"));
    roll_mod.addLibraryPath(b.path("lib"));
    roll_mod.linkSystemLibrary("duckdb", .{});

    const parquet_query_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/parquet_query.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    parquet_query_mod.addImport("collector", collector_mod);
    parquet_query_mod.addImport("logs", logs_mod);
    parquet_query_mod.addImport("proxmox", proxmox_mod);
    parquet_query_mod.addImport("staging", staging_mod);
    parquet_query_mod.addImport("roll", roll_mod);
    parquet_query_mod.addIncludePath(b.path("lib"));
    parquet_query_mod.addLibraryPath(b.path("lib"));
    parquet_query_mod.linkSystemLibrary("duckdb", .{});

    const staging_tests = b.addTest(.{ .root_module = staging_mod });

    const roll_tests = b.addTest(.{ .root_module = roll_mod });
    roll_tests.addRPath(b.path("lib"));

    const parquet_query_tests = b.addTest(.{ .root_module = parquet_query_mod });
    parquet_query_tests.addRPath(b.path("lib"));

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&b.addRunArtifact(storage_tests).step);
    test_step.dependOn(&b.addRunArtifact(collector_tests).step);
    test_step.dependOn(&b.addRunArtifact(logs_tests).step);
    test_step.dependOn(&b.addRunArtifact(rules_tests).step);
    test_step.dependOn(&b.addRunArtifact(push_tests).step);
    test_step.dependOn(&b.addRunArtifact(proc_self_tests).step);
    test_step.dependOn(&b.addRunArtifact(proxmox_tests).step);
    test_step.dependOn(&b.addRunArtifact(staging_tests).step);
    test_step.dependOn(&b.addRunArtifact(roll_tests).step);
    test_step.dependOn(&b.addRunArtifact(parquet_query_tests).step);

    // ── Bench (resource usage check) ──
    const bench = b.addSystemCommand(&.{ "bash", "bench.sh" });
    bench.step.dependOn(&agent.step);
    const bench_step = b.step("bench", "Check agent resource usage (RSS < 96MB, CPU < 2%)");
    bench_step.dependOn(&bench.step);

    // ── Buffer-pool regression bench ──
    // Exercises the collection / buffer-pool loop over many cycles and
    // fails if resident-memory growth outruns a measured slope threshold.
    // Defaults to fast mode (< 1 min); set BENCH_MODE=soak for the long
    // run. See scripts/bench/README.md.
    const bench_bufferpool = b.addSystemCommand(&.{ "bash", "scripts/bench/buffer_pool_soak.sh" });
    bench_bufferpool.step.dependOn(&agent.step);
    const bench_bufferpool_step = b.step(
        "bench-buffer-pool",
        "Check daemon RSS does not leak across collection cycles",
    );
    bench_bufferpool_step.dependOn(&bench_bufferpool.step);
}
