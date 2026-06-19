const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const version = b.option([]const u8, "version", "Sermon Daemon version") orelse "dev";
    const ner_enabled = b.option(bool, "ner", "Build the model-backed NER preprocessor (links libpf/ggml/libstdc++)") orelse false;
    // Local parquet hot tier (staging append-log + roll + on-demand query) links
    // duckdb/libstdc++. GATED behind -Dstore (default true): when disabled the
    // roll/storage/parquet_query modules are never created, so the daemon and CLI
    // never reference duckdb and build with lib/libduckdb.so absent. The CLI query
    // path degrades to a clear EX_CONFIG error; the daemon still collects+pushes.
    const store_enabled = b.option(bool, "store", "Build the local parquet hot tier (links duckdb/libstdc++)") orelse true;

    const options = b.addOptions();
    options.addOption([]const u8, "version", version);
    options.addOption(bool, "ner_enabled", ner_enabled);
    options.addOption(bool, "store_enabled", store_enabled);
    // Share ONE build_options module across consumers. Calling addOptions on each
    // module would make the same generated options.zig the root of multiple
    // distinct modules, which Zig rejects ("file exists in modules ...").
    const options_mod = options.createModule();

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
    push_mod.addImport("build_options", options_mod);

    // Parquet hot tier modules (staging + roll) are declared early so both the
    // daemon and the test step can import them. The on-demand query module is
    // declared lower with the other test targets.
    const staging_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/staging.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    staging_mod.addImport("collector", collector_mod);
    staging_mod.addImport("logs", logs_mod);
    staging_mod.addImport("proxmox", proxmox_mod);

    // roll / storage / parquet_query all link duckdb. GATED behind -Dstore:
    // created only when store_enabled, so a -Dstore=false graph never names
    // duckdb/libstdc++ and builds with lib/libduckdb.so absent. staging_mod above
    // is a PURE Zig append-log (links nothing native) and a transitive dep of
    // roll/parquet_query plus a direct import of agent/cli, so it stays
    // unconditional - mirroring the ner_pf optional-module pattern.
    var roll_mod: ?*std.Build.Module = null;
    var storage_mod: ?*std.Build.Module = null;
    var parquet_query_mod: ?*std.Build.Module = null;
    if (store_enabled) {
        const rm = b.createModule(.{
            .root_source_file = b.path("src/agent/roll.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        rm.addImport("collector", collector_mod);
        rm.addImport("logs", logs_mod);
        rm.addImport("proxmox", proxmox_mod);
        rm.addImport("staging", staging_mod);
        rm.addIncludePath(b.path("lib"));
        rm.addLibraryPath(b.path("lib"));
        rm.linkSystemLibrary("duckdb", .{});
        roll_mod = rm;

        const sm = b.createModule(.{
            .root_source_file = b.path("src/agent/storage.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        sm.addImport("collector", collector_mod);
        sm.addImport("logs", logs_mod);
        sm.addImport("proxmox", proxmox_mod);
        sm.addIncludePath(b.path("lib"));
        sm.addLibraryPath(b.path("lib"));
        sm.linkSystemLibrary("duckdb", .{});
        storage_mod = sm;

        // On-demand parquet query module. Declared here (alongside staging/roll)
        // because the CLI read path imports it; the test target below reuses it.
        const pq = b.createModule(.{
            .root_source_file = b.path("src/agent/parquet_query.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        pq.addImport("collector", collector_mod);
        pq.addImport("logs", logs_mod);
        pq.addImport("proxmox", proxmox_mod);
        pq.addImport("staging", staging_mod);
        pq.addImport("roll", roll_mod.?);
        pq.addIncludePath(b.path("lib"));
        pq.addLibraryPath(b.path("lib"));
        pq.linkSystemLibrary("duckdb", .{});
        parquet_query_mod = pq;
    }

    // ── NER adapter (edge PII, model-backed) ──
    // ner.zig is the PURE backend-agnostic interface: no C, no link. redact.zig
    // and main.zig depend only on it, so the FFI backend can be swapped for a
    // pure-Zig one with a one-line constructor change.
    const ner_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/ner.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ner_pf.zig is the FFI-GGML backend: the only module that links libpf and
    // names native labels. Mirrors the duckdb module linking exactly (include +
    // library path into lib/, linkSystemLibrary). libpf transitively pulls in
    // libstdc++ via the co-shipped ggml .so, so link the C++ runtime too.
    // GATED behind -Dner: when disabled the module is never created, so the base
    // build never references libpf/ggml/libstdc++ and links with lib/libpf.so
    // absent.
    var ner_pf_mod: ?*std.Build.Module = null;
    if (ner_enabled) {
        const m = b.createModule(.{
            .root_source_file = b.path("src/agent/ner_pf.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        m.addImport("ner", ner_mod);
        m.addIncludePath(b.path("lib"));
        m.addLibraryPath(b.path("lib"));
        m.linkSystemLibrary("pf", .{});
        m.link_libcpp = true;
        ner_pf_mod = m;
    }

    // ── redact (edge PII redaction) ──
    // Pure byte-scanner module; depends only on the struct definitions it
    // redacts (collector + logs) plus the pure NER interface (ner) for the
    // merge pass. It does NOT link libpf - it only sees ner.Span / ner.Kind.
    // Wired into the daemon AND given its own test target below.
    const redact_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/redact.zig"),
        .target = target,
        .optimize = optimize,
    });
    redact_mod.addImport("collector", collector_mod);
    redact_mod.addImport("logs", logs_mod);
    redact_mod.addImport("ner", ner_mod);

    // ── Preprocessor pipeline + redaction adapters ──
    // Both pure-Zig and backend-agnostic: they import only struct defs, the pure
    // ner interface, and redact_mod. Neither links anything native, so the base
    // build (no -Dner) includes them with lib/libpf.so absent.
    const preprocessor_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/preprocessor.zig"),
        .target = target,
        .optimize = optimize,
    });
    preprocessor_mod.addImport("collector", collector_mod);
    preprocessor_mod.addImport("logs", logs_mod);
    preprocessor_mod.addImport("ner", ner_mod);

    const redact_adapter_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/redact_adapter.zig"),
        .target = target,
        .optimize = optimize,
    });
    redact_adapter_mod.addImport("preprocessor", preprocessor_mod);
    redact_adapter_mod.addImport("collector", collector_mod);
    redact_adapter_mod.addImport("logs", logs_mod);
    redact_adapter_mod.addImport("redact", redact_mod);
    redact_adapter_mod.addImport("ner", ner_mod);

    // ── sermon-agent (daemon) ──
    const agent_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    agent_mod.addImport("build_options", options_mod);
    agent_mod.addImport("redact", redact_mod);
    agent_mod.addImport("ner", ner_mod);
    agent_mod.addImport("preprocessor", preprocessor_mod);
    agent_mod.addImport("redact_adapter", redact_adapter_mod);
    agent_mod.addImport("collector", collector_mod);
    agent_mod.addImport("logs", logs_mod);
    agent_mod.addImport("rules", rules_mod);
    agent_mod.addImport("proc_self", proc_self_mod);
    agent_mod.addImport("proxmox", proxmox_mod);
    agent_mod.addImport("push", push_mod);
    agent_mod.addImport("staging", staging_mod);
    // The local hot tier (roll + duckdb) is ONLY linked under -Dstore. staging
    // stays unconditional (pure). storage_mod is no longer imported by the daemon
    // at all post-cutover (its back-compat constant moved to a local in main.zig),
    // so under -Dstore=false storage.zig + duckdb are fully unreferenced.
    if (store_enabled) {
        agent_mod.addImport("roll", roll_mod.?);
        agent_mod.addIncludePath(b.path("lib"));
        agent_mod.addLibraryPath(b.path("lib"));
        agent_mod.linkSystemLibrary("duckdb", .{});
    }
    // libpf (NER backend) is ONLY linked under -Dner. It resolves at runtime
    // next to libduckdb via the same $ORIGIN/../lib rpath - no new rpath needed.
    // link_libcpp because libpf's co-shipped ggml .so pull in the C++ runtime.
    // Gated so the base build references zero native processing deps.
    if (ner_enabled) {
        agent_mod.addImport("ner_pf", ner_pf_mod.?);
        agent_mod.linkSystemLibrary("pf", .{});
        agent_mod.link_libcpp = true;
    }
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
    // The CLI read path goes through the parquet hot tier (plan 25 cutover), so
    // it imports parquet_query and its transitive deps (staging, roll, proxmox)
    // and links duckdb, mirroring the agent module. It no longer needs storage.
    cli_mod.addImport("collector", collector_mod);
    cli_mod.addImport("logs", logs_mod);
    cli_mod.addImport("proxmox", proxmox_mod);
    cli_mod.addImport("staging", staging_mod);
    cli_mod.addImport("build_options", options_mod);
    // Query path (parquet_query + roll + duckdb) is ONLY linked under -Dstore.
    // With -Dstore=false the CLi still builds; query commands degrade to a clear
    // EX_CONFIG error (see src/cli/main.zig) instead of touching parquet_query.
    if (store_enabled) {
        cli_mod.addImport("roll", roll_mod.?);
        cli_mod.addImport("parquet_query", parquet_query_mod.?);
        cli_mod.addIncludePath(b.path("lib"));
        cli_mod.addLibraryPath(b.path("lib"));
        cli_mod.linkSystemLibrary("duckdb", .{});
    }
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
    // The four duckdb-linking test targets (storage / staging-roll / parquet_query)
    // are GATED behind -Dstore: created only when store_enabled so `zig build
    // -Dstore=false test` never names duckdb. They are wired into test_step below
    // under the same guard. Mirrors the ner_pf_tests optional-pointer pattern.
    var storage_tests: ?*std.Build.Step.Compile = null;
    if (store_enabled) {
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

        const t = b.addTest(.{ .root_module = storage_test_mod });
        t.addRPath(b.path("lib"));
        storage_tests = t;
    }

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
    push_test_mod.addImport("build_options", options_mod);

    const push_tests = b.addTest(.{
        .root_module = push_test_mod,
    });

    const redact_tests = b.addTest(.{
        .root_module = redact_mod,
    });

    // NER interface tests (pure, no link) always run.
    const ner_tests = b.addTest(.{
        .root_module = ner_mod,
    });

    // NER FFI-backend tests link libpf and only make sense when lib/libpf.so is
    // present. We detect the .so at configure time; when absent the target is
    // skipped entirely (the end-to-end NER regression lives in redact.zig with
    // a stub backend, so it still runs without the model).
    const libpf_present = blk: {
        std.fs.cwd().access("lib/libpf.so", .{}) catch break :blk false;
        break :blk true;
    };
    // The ner_pf test target only exists under -Dner (the module is only created
    // then). An unconditional addRunArtifact on a target built only under -Dner
    // would fail `zig build test` on the base build, so keep it optional.
    var ner_pf_tests: ?*std.Build.Step.Compile = null;
    if (ner_enabled) {
        const t = b.addTest(.{
            .root_module = ner_pf_mod.?,
        });
        t.addRPath(b.path("lib"));
        ner_pf_tests = t;
    }

    // Pipeline + adapter tests: pure, no native deps, always run.
    const preprocessor_tests = b.addTest(.{
        .root_module = preprocessor_mod,
    });
    const redact_adapter_tests = b.addTest(.{
        .root_module = redact_adapter_mod,
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

    // ── Parquet hot tier (plan 25): test targets ──
    // staging_mod is PURE (links nothing native) so its test target ALWAYS runs -
    // dropping it under -Dstore=false would silently shrink the pure-test count.
    // roll_mod / parquet_query_mod link duckdb, so their test targets are GATED
    // behind -Dstore and wired into test_step under the same guard below.
    const staging_tests = b.addTest(.{ .root_module = staging_mod });

    var roll_tests: ?*std.Build.Step.Compile = null;
    var parquet_query_tests: ?*std.Build.Step.Compile = null;
    if (store_enabled) {
        const rt = b.addTest(.{ .root_module = roll_mod.? });
        rt.addRPath(b.path("lib"));
        roll_tests = rt;

        const pqt = b.addTest(.{ .root_module = parquet_query_mod.? });
        pqt.addRPath(b.path("lib"));
        parquet_query_tests = pqt;
    }

    // Pure-Zig config + store-gate tests. Exercises the Config parse + local_store
    // default and the store_active composition. Imports build_options (the gate)
    // and staging (Table.all, used by the synthetic-cycle test) but NOTHING
    // native, so it runs identically in all three build variants.
    const config_test_mod = b.createModule(.{
        .root_source_file = b.path("src/agent/config_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    config_test_mod.addImport("build_options", options_mod);
    config_test_mod.addImport("staging", staging_mod);
    config_test_mod.addImport("collector", collector_mod);
    config_test_mod.addImport("logs", logs_mod);
    config_test_mod.addImport("proxmox", proxmox_mod);
    const config_gate_tests = b.addTest(.{ .root_module = config_test_mod });

    // CLI store-disabled smoke test. Only built under -Dstore=false: in that build
    // commands.zig uses the parquet_query stand-in (no duckdb) and its degrade test
    // compiles + runs. Under -Dstore=true the test is comptime-gated out and the
    // bodies need the real duckdb-linked handle, so we do not build it there.
    var cli_commands_tests: ?*std.Build.Step.Compile = null;
    if (!store_enabled) {
        const cli_test_mod = b.createModule(.{
            .root_source_file = b.path("src/cli/commands.zig"),
            .target = target,
            .optimize = optimize,
        });
        cli_test_mod.addImport("build_options", options_mod);
        cli_test_mod.addImport("collector", collector_mod);
        cli_test_mod.addImport("logs", logs_mod);
        cli_commands_tests = b.addTest(.{ .root_module = cli_test_mod });
    }

    const test_step = b.step("test", "Run all tests");
    if (cli_commands_tests) |t| test_step.dependOn(&b.addRunArtifact(t).step);
    if (storage_tests) |t| test_step.dependOn(&b.addRunArtifact(t).step);
    test_step.dependOn(&b.addRunArtifact(config_gate_tests).step);
    test_step.dependOn(&b.addRunArtifact(collector_tests).step);
    test_step.dependOn(&b.addRunArtifact(logs_tests).step);
    test_step.dependOn(&b.addRunArtifact(rules_tests).step);
    test_step.dependOn(&b.addRunArtifact(redact_tests).step);
    test_step.dependOn(&b.addRunArtifact(ner_tests).step);
    test_step.dependOn(&b.addRunArtifact(preprocessor_tests).step);
    test_step.dependOn(&b.addRunArtifact(redact_adapter_tests).step);
    if (ner_enabled and libpf_present) {
        if (ner_pf_tests) |t| test_step.dependOn(&b.addRunArtifact(t).step);
    }
    test_step.dependOn(&b.addRunArtifact(push_tests).step);
    test_step.dependOn(&b.addRunArtifact(proc_self_tests).step);
    test_step.dependOn(&b.addRunArtifact(proxmox_tests).step);
    test_step.dependOn(&b.addRunArtifact(staging_tests).step);
    if (roll_tests) |t| test_step.dependOn(&b.addRunArtifact(t).step);
    if (parquet_query_tests) |t| test_step.dependOn(&b.addRunArtifact(t).step);

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
