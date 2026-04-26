const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

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

    const push_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/push.zig"),
        .target = target,
        .optimize = optimize,
    });
    push_mod.addImport("collector", collector_mod);

    const storage_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/storage.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    storage_mod.addImport("collector", collector_mod);
    storage_mod.addImport("logs", logs_mod);
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

    const push_test_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/push.zig"),
        .target = target,
        .optimize = optimize,
    });
    push_test_mod.addImport("collector", collector_mod);

    const push_tests = b.addTest(.{
        .root_module = push_test_mod,
    });

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&b.addRunArtifact(storage_tests).step);
    test_step.dependOn(&b.addRunArtifact(collector_tests).step);
    test_step.dependOn(&b.addRunArtifact(logs_tests).step);
    test_step.dependOn(&b.addRunArtifact(push_tests).step);

    // ── Bench (resource usage check) ──
    const bench = b.addSystemCommand(&.{ "bash", "bench.sh" });
    bench.step.dependOn(&agent.step);
    const bench_step = b.step("bench", "Check agent resource usage (RSS < 50MB, CPU < 2%)");
    bench_step.dependOn(&bench.step);
}
