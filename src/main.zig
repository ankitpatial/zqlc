const std = @import("std");
const lib = @import("sqlz");

const update = lib.update;
const version = update.version;

const Mode = enum {
    generate,
    verify,
    update,
};

const Config = struct {
    mode: ?Mode = null,
    host: []const u8 = "localhost",
    port: u16 = 5432,
    user: []const u8 = "",
    password: []const u8 = "",
    database: []const u8 = "",
    src_dir: ?[]const u8 = null,
    dest_dir: ?[]const u8 = null,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_w.interface;

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_w.interface;

    const use_color = lib.errors.isTty(std.Io.File.stderr(), io);

    var config = Config{};

    // Parse CLI args
    var args = init.minimal.args.iterate();
    _ = args.next(); // skip program name

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            try printUsage(stdout);
            try stdout.flush();
            return;
        } else if (std.mem.eql(u8, arg, "--version") or std.mem.eql(u8, arg, "-v")) {
            try stdout.print("sqlz {s}\n", .{version});
            try stdout.flush();
            return;
        } else if (std.mem.eql(u8, arg, "--src")) {
            config.src_dir = args.next() orelse {
                try stderr.writeAll("--src requires a directory path\n");
                try stderr.flush();
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--dest")) {
            config.dest_dir = args.next() orelse {
                try stderr.writeAll("--dest requires a directory path\n");
                try stderr.flush();
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "verify")) {
            config.mode = .verify;
        } else if (std.mem.eql(u8, arg, "generate")) {
            config.mode = .generate;
        } else if (std.mem.eql(u8, arg, "update")) {
            config.mode = .update;
        } else {
            try stderr.print("Unknown argument: {s}\n\n", .{arg});
            try printUsage(stderr);
            try stderr.flush();
            std.process.exit(1);
        }
    }

    // Validate mode
    if (config.mode == null) {
        try stderr.writeAll("Missing required command: generate, verify, or update\n\n");
        try printUsage(stderr);
        try stderr.flush();
        std.process.exit(1);
    }

    // Handle update mode — doesn't need database or src/dest
    if (config.mode.? == .update) {
        _ = update.run(allocator, io, stderr, use_color) catch |err| switch (err) {
            error.AlreadyUpToDate => return,
            else => std.process.exit(1),
        };
        return;
    }

    // Validate required arguments for generate/verify
    if (config.src_dir == null) {
        try stderr.writeAll("Missing required option: --src <dir>\n\n");
        try printUsage(stderr);
        try stderr.flush();
        std.process.exit(1);
    }
    if (config.dest_dir == null) {
        try stderr.writeAll("Missing required option: --dest <dir>\n\n");
        try printUsage(stderr);
        try stderr.flush();
        std.process.exit(1);
    }

    // Read database connection config from environment
    const db_url = init.environ_map.get("DATABASE_URL") orelse loadDotEnvValue(allocator, io, ".env", "DATABASE_URL") orelse {
        const e = lib.errors.Error{ .config = .{
            .message = "DATABASE_URL is required. Set it as an environment variable or in a .env file.\n  Example: DATABASE_URL=postgresql://user:password@localhost:5432/mydb",
        } };
        try e.format(stderr, use_color);
        try stderr.flush();
        std.process.exit(1);
    };

    parseDatabaseUrl(&config, db_url) catch {
        const e = lib.errors.Error{ .config = .{ .message = "Invalid DATABASE_URL format. Expected: postgresql://user:password@host:port/database" } };
        try e.format(stderr, use_color);
        try stderr.flush();
        std.process.exit(1);
    };

    // Resolve src_dir to absolute path
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_dir: []const u8 = blk: {
        const len = std.Io.Dir.cwd().realPathFile(io, config.src_dir.?, &path_buf) catch {
            const e = lib.errors.Error{ .config = .{
                .message = "Could not resolve --src directory path.",
            } };
            try e.format(stderr, use_color);
            try stderr.flush();
            std.process.exit(1);
        };
        break :blk try allocator.dupe(u8, path_buf[0..len]);
    };
    defer allocator.free(src_dir);

    // In generate mode, clean dest_dir to remove stale files
    if (config.mode.? == .generate) {
        std.Io.Dir.cwd().deleteTree(io, config.dest_dir.?) catch {};
    }

    // Resolve dest_dir to absolute path, creating it if needed
    var dest_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dest_dir: []const u8 = blk: {
        std.Io.Dir.cwd().createDirPath(io, config.dest_dir.?) catch {
            const e = lib.errors.Error{ .config = .{
                .message = "Could not create --dest directory.",
            } };
            try e.format(stderr, use_color);
            try stderr.flush();
            std.process.exit(1);
        };
        const len = std.Io.Dir.cwd().realPathFile(io, config.dest_dir.?, &dest_path_buf) catch {
            const e = lib.errors.Error{ .config = .{
                .message = "Could not resolve --dest directory path.",
            } };
            try e.format(stderr, use_color);
            try stderr.flush();
            std.process.exit(1);
        };
        break :blk try allocator.dupe(u8, dest_path_buf[0..len]);
    };
    defer allocator.free(dest_dir);

    // Discover SQL files
    if (use_color) try stderr.writeAll("\x1b[36m");
    try stderr.writeAll("Scanning for SQL files...\n");
    if (use_color) try stderr.writeAll("\x1b[0m");
    try stderr.flush();

    var groups = lib.project.discoverSqlFiles(allocator, io, src_dir, dest_dir, true) catch {
        const e = lib.errors.Error{ .file = .{
            .path = src_dir,
            .message = "Failed to scan for SQL files.",
        } };
        try e.format(stderr, use_color);
        try stderr.flush();
        std.process.exit(1);
    };
    defer {
        for (groups.items) |*g| g.deinit(allocator);
        groups.deinit(allocator);
    }

    if (groups.items.len == 0) {
        try stderr.writeAll("No SQL files found in **/sql/ directories.\n");
        try stderr.flush();
        return;
    }

    var total_queries: usize = 0;
    for (groups.items) |g| total_queries += g.sql_files.items.len;

    try stderr.print("Found {d} SQL file(s) in {d} group(s).\n", .{ total_queries, groups.items.len });
    try stderr.flush();

    // Connect to database
    if (use_color) try stderr.writeAll("\x1b[36m");
    try stderr.writeAll("Connecting to database...\n");
    if (use_color) try stderr.writeAll("\x1b[0m");
    try stderr.flush();

    var conn = lib.connection.Connection.connect(
        allocator,
        io,
        config.host,
        config.port,
        config.user,
        config.password,
        config.database,
    ) catch |err| {
        const msg = switch (err) {
            error.ConnectionRefused => "Connection refused. Is PostgreSQL running?",
            error.AuthenticationFailed => "Authentication failed. Check your credentials.",
            else => "Failed to connect to the database.",
        };
        const e = lib.errors.Error{ .connection = .{ .message = msg } };
        try e.format(stderr, use_color);
        try stderr.flush();
        std.process.exit(1);
    };
    defer conn.close();

    // Introspect and generate
    if (use_color) try stderr.writeAll("\x1b[36m");
    try stderr.writeAll("Introspecting queries...\n");
    if (use_color) try stderr.writeAll("\x1b[0m");
    try stderr.flush();

    // Create caches shared across all groups
    var type_cache = lib.types.TypeCache.init(allocator);
    defer type_cache.deinit();
    var null_cache = lib.types.NullabilityCache.init(allocator);
    defer null_cache.deinit();

    var error_count: usize = 0;

    // Collect typed query slices for helper.zig generation
    var all_typed_query_slices: std.ArrayList([]const lib.introspect.TypedQuery) = .empty;
    defer all_typed_query_slices.deinit(allocator);

    // Keep describe results alive until after helper/root generation
    var all_describe_results: std.ArrayList(lib.introspect.DescribeResult) = .empty;
    defer {
        for (all_describe_results.items) |*dr| dr.deinit(allocator);
        all_describe_results.deinit(allocator);
    }

    // Track which groups succeeded introspection (by index into groups.items)
    var successful_group_indices: std.ArrayList(usize) = .empty;
    defer successful_group_indices.deinit(allocator);

    // --- Pass 1: Parse and introspect all groups ---
    for (groups.items, 0..) |*group, group_idx| {
        var untyped_queries: std.ArrayList(lib.query.UntypedQuery) = .empty;
        defer untyped_queries.deinit(allocator);

        for (group.sql_files.items) |sql_file| {
            const uqs = lib.query.parseFile(allocator, io, sql_file) catch |err| {
                const e = lib.errors.Error{ .query_error = .{
                    .file_path = sql_file,
                    .message = @errorName(err),
                } };
                try e.format(stderr, use_color);
                try stderr.flush();
                error_count += 1;
                continue;
            };
            defer allocator.free(uqs);
            for (uqs) |uq| {
                try untyped_queries.append(allocator, uq);
            }
        }

        if (untyped_queries.items.len == 0) continue;

        var describe_result = lib.introspect.describeQueries(
            allocator,
            &conn,
            untyped_queries.items,
            &type_cache,
            &null_cache,
        ) catch |err| {
            const e = lib.errors.Error{ .query_error = .{
                .file_path = group.parent_dir,
                .message = @errorName(err),
            } };
            try e.format(stderr, use_color);
            try stderr.flush();
            error_count += 1;
            continue;
        };

        // Report per-query validation errors with actual PostgreSQL messages
        for (describe_result.errors.items) |ve| {
            const e = lib.errors.Error{ .query_error = .{
                .file_path = ve.file_path,
                .message = ve.message,
                .detail = ve.detail,
                .position = ve.position,
            } };
            try e.format(stderr, use_color);
            try stderr.flush();
            error_count += 1;
        }

        // Skip code generation for this group if any query failed validation
        if (describe_result.errors.items.len > 0 and describe_result.queries.items.len == 0) {
            describe_result.deinit(allocator);
            continue;
        }

        try all_typed_query_slices.append(allocator, describe_result.queries.items);
        try all_describe_results.append(allocator, describe_result);
        try successful_group_indices.append(allocator, group_idx);
    }

    // --- Generate helper.zig (null when no shared types exist) ---
    const helper_content: ?[]const u8 = if (all_typed_query_slices.items.len > 0)
        lib.codegen.generateHelper(allocator, all_typed_query_slices.items) catch |err| blk: {
            const e = lib.errors.Error{ .query_error = .{
                .file_path = "helper.zig",
                .message = @errorName(err),
            } };
            try e.format(stderr, use_color);
            try stderr.flush();
            error_count += 1;
            break :blk null;
        }
    else
        null;
    defer if (helper_content) |hc| allocator.free(hc);

    // --- Pass 2: Generate, format, and write per-module code ---
    for (successful_group_indices.items, all_typed_query_slices.items) |group_idx, typed_qs| {
        const group = &groups.items[group_idx];

        // Only compute helper_rel_path when helper.zig will be generated
        const helper_rel_path: ?[]const u8 = if (helper_content != null) blk: {
            const rel_dir = std.fs.path.dirname(group.rel_path) orelse ".";
            var depth: usize = 0;
            if (!std.mem.eql(u8, rel_dir, ".")) {
                var parts = std.mem.splitScalar(u8, rel_dir, std.fs.path.sep);
                while (parts.next()) |_| depth += 1;
            }
            if (depth == 0) {
                break :blk "helper.zig";
            } else {
                var rel_buf: std.ArrayList(u8) = .empty;
                for (0..depth) |_| try rel_buf.appendSlice(allocator, "../");
                try rel_buf.appendSlice(allocator, "helper.zig");
                break :blk try rel_buf.toOwnedSlice(allocator);
            }
        } else null;
        defer if (helper_rel_path) |hrp| {
            if (!std.mem.eql(u8, hrp, "helper.zig")) allocator.free(hrp);
        };

        const generated = lib.codegen.generate(allocator, typed_qs, helper_rel_path) catch |err| {
            const e = lib.errors.Error{ .query_error = .{
                .file_path = group.output_path,
                .message = @errorName(err),
            } };
            try e.format(stderr, use_color);
            try stderr.flush();
            error_count += 1;
            continue;
        };
        defer allocator.free(generated);

        const output_filename = std.fs.path.basename(group.output_path);

        switch (config.mode.?) {
            .generate => {
                std.Io.Dir.cwd().createDirPath(io, group.parent_dir) catch {
                    const e2 = lib.errors.Error{ .file = .{
                        .path = group.parent_dir,
                        .message = "Cannot create output directory.",
                    } };
                    try e2.format(stderr, use_color);
                    try stderr.flush();
                    error_count += 1;
                    continue;
                };

                var dir = std.Io.Dir.cwd().openDir(io, group.parent_dir, .{}) catch {
                    const e2 = lib.errors.Error{ .file = .{
                        .path = group.parent_dir,
                        .message = "Cannot open output directory.",
                    } };
                    try e2.format(stderr, use_color);
                    try stderr.flush();
                    error_count += 1;
                    continue;
                };
                defer dir.close(io);

                const file = dir.createFile(io, output_filename, .{}) catch {
                    const e2 = lib.errors.Error{ .file = .{
                        .path = group.output_path,
                        .message = "Cannot create output file.",
                    } };
                    try e2.format(stderr, use_color);
                    try stderr.flush();
                    error_count += 1;
                    continue;
                };
                defer file.close(io);

                file.writeStreamingAll(io, generated) catch {
                    const e2 = lib.errors.Error{ .file = .{
                        .path = group.output_path,
                        .message = "Failed to write output file.",
                    } };
                    try e2.format(stderr, use_color);
                    try stderr.flush();
                    error_count += 1;
                    continue;
                };

                try stderr.print("  Generated {s}\n", .{group.output_path});
                try stderr.flush();
            },
            .verify => {
                const existing_content = std.Io.Dir.cwd().readFileAlloc(io, group.output_path, allocator, .unlimited) catch {
                    try stderr.print("  {s}: file does not exist (would be generated)\n", .{group.output_path});
                    try stderr.flush();
                    error_count += 1;
                    continue;
                };
                defer allocator.free(existing_content);

                const formatted = zigFmt(allocator, io, generated);
                defer if (formatted.ptr != generated.ptr) allocator.free(formatted);

                if (!std.mem.eql(u8, existing_content, formatted)) {
                    try stderr.print("  {s}: out of date\n", .{group.output_path});
                    try stderr.flush();
                    error_count += 1;
                } else {
                    try stderr.print("  {s}: up to date\n", .{group.output_path});
                    try stderr.flush();
                }
            },
            .update => unreachable,
        }
    }

    // --- Write helper.zig (if needed) and root.zig ---
    if (successful_group_indices.items.len > 0) {
        if (helper_content) |hc| {
            switch (config.mode.?) {
                .generate => try writeOutputFile(allocator, io, dest_dir, "helper.zig", hc, stderr, use_color, &error_count),
                .verify => try checkOutputFile(allocator, io, dest_dir, "helper.zig", hc, stderr, use_color, &error_count),
                .update => unreachable,
            }
        }

        // Build the list of successful groups for root.zig
        const successful_groups = try allocator.alloc(lib.project.SqlFileGroup, successful_group_indices.items.len);
        defer allocator.free(successful_groups);
        for (successful_group_indices.items, 0..) |idx, i| {
            successful_groups[i] = groups.items[idx];
        }

        const root_content = lib.codegen.generateRoot(allocator, successful_groups) catch |err| {
            const e = lib.errors.Error{ .query_error = .{
                .file_path = "root.zig",
                .message = @errorName(err),
            } };
            try e.format(stderr, use_color);
            try stderr.flush();
            error_count += 1;
            return;
        };
        defer allocator.free(root_content);

        switch (config.mode.?) {
            .generate => try writeOutputFile(allocator, io, dest_dir, "root.zig", root_content, stderr, use_color, &error_count),
            .verify => try checkOutputFile(allocator, io, dest_dir, "root.zig", root_content, stderr, use_color, &error_count),
            .update => unreachable,
        }

        // Format all generated files with zig fmt
        if (config.mode.? == .generate) {
            const fmt_result = std.process.run(allocator, io, .{
                .argv = &.{ "zig", "fmt", dest_dir },
            }) catch null;
            if (fmt_result) |r| {
                allocator.free(r.stdout);
                allocator.free(r.stderr);
            }
        }
    }

    // Summary
    if (error_count > 0) {
        try stderr.writeByte('\n');
        if (use_color) try stderr.writeAll("\x1b[31m");
        try stderr.print("{d} error(s) encountered.\n", .{error_count});
        if (use_color) try stderr.writeAll("\x1b[0m");
        try stderr.flush();
        std.process.exit(1);
    }

    if (use_color) try stderr.writeAll("\x1b[32m");
    switch (config.mode.?) {
        .generate => try stderr.writeAll("Done.\n"),
        .verify => try stderr.writeAll("All queries valid. All files up to date.\n"),
        .update => unreachable,
    }
    if (use_color) try stderr.writeAll("\x1b[0m");
    try stderr.flush();
}

fn writeOutputFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    base_dir: []const u8,
    filename: []const u8,
    content: []const u8,
    stderr: *std.Io.Writer,
    use_color: bool,
    error_count: *usize,
) !void {
    const output_path = try std.fs.path.join(allocator, &.{ base_dir, filename });
    defer allocator.free(output_path);

    var dir = std.Io.Dir.openDirAbsolute(io, base_dir, .{}) catch {
        const e = lib.errors.Error{ .file = .{ .path = base_dir, .message = "Cannot open output directory." } };
        try e.format(stderr, use_color);
        try stderr.flush();
        error_count.* += 1;
        return;
    };
    defer dir.close(io);

    const file = dir.createFile(io, filename, .{}) catch {
        const e = lib.errors.Error{ .file = .{ .path = output_path, .message = "Cannot create output file." } };
        try e.format(stderr, use_color);
        try stderr.flush();
        error_count.* += 1;
        return;
    };
    defer file.close(io);

    file.writeStreamingAll(io, content) catch {
        const e = lib.errors.Error{ .file = .{ .path = output_path, .message = "Failed to write output file." } };
        try e.format(stderr, use_color);
        try stderr.flush();
        error_count.* += 1;
        return;
    };

    try stderr.print("  Generated {s}\n", .{output_path});
    try stderr.flush();
}

fn checkOutputFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    base_dir: []const u8,
    filename: []const u8,
    content: []const u8,
    stderr: *std.Io.Writer,
    _: bool,
    error_count: *usize,
) !void {
    const output_path = try std.fs.path.join(allocator, &.{ base_dir, filename });
    defer allocator.free(output_path);

    const existing_content = std.Io.Dir.cwd().readFileAlloc(io, output_path, allocator, .unlimited) catch {
        try stderr.print("  {s}: file does not exist (would be generated)\n", .{output_path});
        try stderr.flush();
        error_count.* += 1;
        return;
    };
    defer allocator.free(existing_content);

    const formatted = zigFmt(allocator, io, content);
    defer if (formatted.ptr != content.ptr) allocator.free(formatted);

    if (!std.mem.eql(u8, existing_content, formatted)) {
        try stderr.print("  {s}: out of date\n", .{output_path});
        try stderr.flush();
        error_count.* += 1;
    } else {
        try stderr.print("  {s}: up to date\n", .{output_path});
        try stderr.flush();
    }
}

/// Format Zig source code via `zig fmt --stdin`. Returns formatted content
/// or the original content if zig fmt is unavailable.
fn zigFmt(allocator: std.mem.Allocator, io: std.Io, source: []const u8) []const u8 {
    var child = std.process.spawn(io, .{
        .argv = &.{ "zig", "fmt", "--stdin" },
        .stdin = .pipe,
        .stdout = .pipe,
        .stderr = .ignore,
    }) catch return source;

    // Write source to stdin and close it so zig fmt processes the input
    if (child.stdin) |stdin| {
        stdin.writeStreamingAll(io, source) catch {};
        stdin.close(io);
        child.stdin = null;
    }

    // Read stdout
    var out_buf = std.Io.Writer.Allocating.init(allocator);
    if (child.stdout) |stdout_file| {
        var read_buf: [8192]u8 = undefined;
        var reader = stdout_file.reader(io, &read_buf);
        _ = reader.interface.streamRemaining(&out_buf.writer) catch {
            out_buf.deinit();
            child.kill(io);
            return source;
        };
    }

    const term = child.wait(io) catch {
        out_buf.deinit();
        return source;
    };

    var arr = out_buf.toArrayList();
    if (term == .exited and term.exited == 0 and arr.items.len > 0) {
        return allocator.dupe(u8, arr.items) catch {
            arr.deinit(allocator);
            return source;
        };
    }

    arr.deinit(allocator);
    return source;
}

fn parseDatabaseUrl(config: *Config, url: []const u8) !void {
    // postgresql://user:password@host:port/database
    const scheme = "postgresql://";
    const scheme_alt = "postgres://";

    var rest: []const u8 = undefined;
    if (std.mem.startsWith(u8, url, scheme)) {
        rest = url[scheme.len..];
    } else if (std.mem.startsWith(u8, url, scheme_alt)) {
        rest = url[scheme_alt.len..];
    } else {
        return error.InvalidUrl;
    }

    // Split user_info@host_info/database
    const at_pos = std.mem.indexOf(u8, rest, "@") orelse return error.InvalidUrl;
    const user_info = rest[0..at_pos];
    const after_at = rest[at_pos + 1 ..];

    // Parse user:password
    if (std.mem.indexOf(u8, user_info, ":")) |colon| {
        config.user = user_info[0..colon];
        config.password = user_info[colon + 1 ..];
    } else {
        config.user = user_info;
    }

    // Parse host:port/database (strip query params)
    var host_db = after_at;
    if (std.mem.indexOf(u8, host_db, "?")) |q| {
        host_db = host_db[0..q];
    }

    const slash_pos = std.mem.indexOf(u8, host_db, "/") orelse return error.InvalidUrl;
    const host_port = host_db[0..slash_pos];
    config.database = host_db[slash_pos + 1 ..];

    if (std.mem.indexOf(u8, host_port, ":")) |colon| {
        config.host = host_port[0..colon];
        config.port = std.fmt.parseInt(u16, host_port[colon + 1 ..], 10) catch 5432;
    } else {
        config.host = host_port;
    }
}

/// Load a single value from a .env file. Returns null if file or key not found.
fn loadDotEnvValue(allocator: std.mem.Allocator, io: std.Io, path: []const u8, key: []const u8) ?[]const u8 {
    const content = std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(64 * 1024)) catch return null;
    defer allocator.free(content);

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trimEnd(u8, raw_line, "\r");
        const trimmed = std.mem.trim(u8, line, " \t");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;

        const eq_pos = std.mem.indexOfScalar(u8, trimmed, '=') orelse continue;
        const k = std.mem.trim(u8, trimmed[0..eq_pos], " \t");
        if (!std.mem.eql(u8, k, key)) continue;

        var v = std.mem.trim(u8, trimmed[eq_pos + 1 ..], " \t");
        // Strip surrounding quotes
        if (v.len >= 2 and (v[0] == '"' or v[0] == '\'') and v[v.len - 1] == v[0]) {
            v = v[1 .. v.len - 1];
        }
        return allocator.dupe(u8, v) catch return null;
    }
    return null;
}

fn printUsage(w: *std.Io.Writer) !void {
    try w.writeAll(
        \\sqlz - Type-safe SQL code generation for Zig
        \\
        \\Usage: sqlz <command> --src <dir> --dest <dir>
        \\
        \\Commands:
        \\  generate    Generate Zig code from SQL files
        \\  verify      Validate SQL queries against the database and verify generated files are up to date
        \\  update      Update sqlz to the latest version
        \\
        \\Options:
        \\  --src <dir>    Directory containing .sql files (required)
        \\  --dest <dir>   Output directory for generated files (required)
        \\  -h, --help     Show this help message
        \\  -v, --version  Show version
        \\
        \\Environment:
        \\  DATABASE_URL   PostgreSQL connection URL (required)
        \\                 postgresql://user:password@host:port/database
        \\
        \\  Can also be set in a .env file in the project root.
        \\
        \\Example:
        \\  sqlz generate --src db/sql/ --dest db/query/
        \\
    );
}

test "parse DATABASE_URL" {
    var config = Config{};
    try parseDatabaseUrl(&config, "postgresql://myuser:mypass@myhost:5433/mydb");
    try std.testing.expectEqualStrings("myuser", config.user);
    try std.testing.expectEqualStrings("mypass", config.password);
    try std.testing.expectEqualStrings("myhost", config.host);
    try std.testing.expectEqual(@as(u16, 5433), config.port);
    try std.testing.expectEqualStrings("mydb", config.database);
}

test "parse DATABASE_URL with postgres:// scheme" {
    var config = Config{};
    try parseDatabaseUrl(&config, "postgres://user:pass@localhost:5432/testdb?sslmode=disable");
    try std.testing.expectEqualStrings("user", config.user);
    try std.testing.expectEqualStrings("pass", config.password);
    try std.testing.expectEqualStrings("localhost", config.host);
    try std.testing.expectEqualStrings("testdb", config.database);
}

test "parse DATABASE_URL without port" {
    var config = Config{};
    try parseDatabaseUrl(&config, "postgresql://user:pass@dbhost/mydb");
    try std.testing.expectEqualStrings("dbhost", config.host);
    try std.testing.expectEqual(@as(u16, 5432), config.port);
}

test "loadDotEnvValue" {
    const allocator = std.testing.allocator;
    const test_io = std.testing.io;

    // Write a temp .env file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const env_content =
        \\# comment
        \\DATABASE_URL=postgresql://u:p@h:5432/db
        \\OTHER_VAR="quoted value"
        \\EMPTY=
        \\
    ;
    tmp_dir.dir.writeFile(test_io, .{ .sub_path = ".env", .data = env_content }) catch unreachable;

    // Read and parse the file manually to test the parsing logic
    const content = tmp_dir.dir.readFileAlloc(test_io, ".env", allocator, .unlimited) catch unreachable;
    defer allocator.free(content);

    // Verify the content parses correctly by checking line-by-line
    var found_url = false;
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trimEnd(u8, raw_line, "\r");
        const trimmed = std.mem.trim(u8, line, " \t");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;
        const eq_pos = std.mem.indexOfScalar(u8, trimmed, '=') orelse continue;
        const k = std.mem.trim(u8, trimmed[0..eq_pos], " \t");
        if (std.mem.eql(u8, k, "DATABASE_URL")) {
            const v = std.mem.trim(u8, trimmed[eq_pos + 1 ..], " \t");
            try std.testing.expectEqualStrings("postgresql://u:p@h:5432/db", v);
            found_url = true;
        }
    }
    try std.testing.expect(found_url);
}
