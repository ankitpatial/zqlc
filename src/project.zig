const std = @import("std");

/// A group of SQL files that produce one sql.zig output.
pub const SqlFileGroup = struct {
    /// Absolute path to the output directory.
    parent_dir: []const u8,
    /// Full path to the output file (parent_dir/sql.zig).
    output_path: []const u8,
    /// Relative path from output base dir (e.g., "users/sql.zig") for root.zig imports.
    rel_path: []const u8,
    /// List of .sql file paths, sorted alphabetically.
    sql_files: std.ArrayList([]const u8),

    pub fn deinit(self: *SqlFileGroup, allocator: std.mem.Allocator) void {
        for (self.sql_files.items) |f| {
            allocator.free(f);
        }
        self.sql_files.deinit(allocator);
        allocator.free(self.parent_dir);
        allocator.free(self.output_path);
        allocator.free(self.rel_path);
    }
};

/// Find the project root by walking upward from CWD looking for build.zig.zon.
pub fn findProjectRoot(allocator: std.mem.Allocator) ![]const u8 {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const realpath = try std.fs.cwd().realpath(".", &path_buf);
    var current: []const u8 = realpath;

    while (true) {
        // Check if build.zig.zon exists in current directory
        var dir = std.fs.openDirAbsolute(current, .{}) catch return error.NotFound;
        defer dir.close();

        dir.access("build.zig.zon", .{}) catch {
            // Go to parent
            const parent = std.fs.path.dirname(current);
            if (parent == null or std.mem.eql(u8, parent.?, current)) {
                return error.NotFound;
            }
            current = parent.?;
            continue;
        };

        return allocator.dupe(u8, current);
    }
}

/// Discover SQL files and group them.
/// - src_dir: absolute path to scan.
/// - dest_dir: when set, output files go into dest_dir mirroring src_dir structure.
///   When null, output files are placed next to each sql/ directory (legacy behavior).
/// - explicit_src: when true, scans for **/*.sql and groups by parent dir.
///   When false, uses legacy convention: only files inside sql/ subdirectories.
pub fn discoverSqlFiles(allocator: std.mem.Allocator, src_dir: []const u8, dest_dir: ?[]const u8, explicit_src: bool) !std.ArrayList(SqlFileGroup) {
    var groups: std.ArrayList(SqlFileGroup) = .empty;
    errdefer {
        for (groups.items) |*g| g.deinit(allocator);
        groups.deinit(allocator);
    }

    var dir = std.fs.openDirAbsolute(src_dir, .{ .iterate = true }) catch {
        return groups; // Directory doesn't exist — return empty
    };
    defer dir.close();

    // Walk the directory tree
    var walker = try dir.walk(allocator);
    defer walker.deinit();

    // Collect .sql files grouped by their relative parent directory
    var sql_dirs = std.StringHashMap(std.ArrayList([]const u8)).init(allocator);
    defer {
        var it = sql_dirs.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.items) |f| allocator.free(f);
            entry.value_ptr.deinit(allocator);
            allocator.free(entry.key_ptr.*);
        }
        sql_dirs.deinit();
    }

    while (walker.next() catch null) |entry| {
        if (entry.kind != .file) continue;

        const path = entry.path;
        if (!std.mem.endsWith(u8, path, ".sql")) continue;

        if (!explicit_src) {
            // Legacy mode: only accept files inside a directory named "sql"
            const dir_name = std.fs.path.dirname(path) orelse continue;
            const base = std.fs.path.basename(dir_name);
            if (!std.mem.eql(u8, base, "sql")) continue;
        }

        const full_path = try std.fs.path.join(allocator, &.{ src_dir, path });

        // Group key is the relative parent directory of the .sql file
        const rel_parent = std.fs.path.dirname(path) orelse ".";
        const dir_key = try allocator.dupe(u8, rel_parent);
        const gop = try sql_dirs.getOrPut(dir_key);
        if (gop.found_existing) {
            allocator.free(dir_key);
            try gop.value_ptr.append(allocator, full_path);
        } else {
            gop.value_ptr.* = .empty;
            try gop.value_ptr.append(allocator, full_path);
        }
    }

    // Convert to SqlFileGroups
    var dir_iter = sql_dirs.iterator();
    while (dir_iter.next()) |entry| {
        const rel_parent = entry.key_ptr.*;

        if (explicit_src) {
            // --src mode: one .zig per .sql file
            // e.g., delete.sql → delete.zig, user/abc.sql → user/abc.zig
            const output_base = dest_dir orelse src_dir;
            const files = entry.value_ptr.*;
            std.mem.sort([]const u8, files.items, {}, sortStrings);

            for (files.items) |full_path| {
                // Get the relative path of this .sql file from src_dir
                const rel_sql = full_path[src_dir.len + 1 ..]; // e.g., "user/abc.sql"

                // Replace .sql extension with .zig
                const stem = rel_sql[0 .. rel_sql.len - 4]; // strip ".sql"
                const rel_zig = try std.fmt.allocPrint(allocator, "{s}.zig", .{stem});

                const output_path = try std.fs.path.join(allocator, &.{ output_base, rel_zig });
                const parent_dir = blk: {
                    const d = std.fs.path.dirname(output_path) orelse output_base;
                    break :blk try allocator.dupe(u8, d);
                };

                var sql_files: std.ArrayList([]const u8) = .empty;
                try sql_files.append(allocator, try allocator.dupe(u8, full_path));

                try groups.append(allocator, .{
                    .parent_dir = parent_dir,
                    .output_path = output_path,
                    .rel_path = rel_zig,
                    .sql_files = sql_files,
                });
            }
        } else {
            // Legacy mode: files are inside "sql/" dirs, output goes one level up
            const output_base = dest_dir orelse src_dir;
            const parent_rel = std.fs.path.dirname(rel_parent) orelse ".";

            const rel_path = try std.fs.path.join(allocator, &.{ parent_rel, "sql.zig" });
            const parent_dir = try std.fs.path.join(allocator, &.{ output_base, parent_rel });
            const output_path = try std.fs.path.join(allocator, &.{ parent_dir, "sql.zig" });

            var sql_files: std.ArrayList([]const u8) = .empty;
            const files = entry.value_ptr.*;
            std.mem.sort([]const u8, files.items, {}, sortStrings);
            for (files.items) |f| {
                try sql_files.append(allocator, try allocator.dupe(u8, f));
            }

            try groups.append(allocator, .{
                .parent_dir = parent_dir,
                .output_path = output_path,
                .rel_path = rel_path,
                .sql_files = sql_files,
            });
        }
    }

    // Sort groups by rel_path for deterministic output
    std.mem.sort(SqlFileGroup, groups.items, {}, struct {
        fn lessThan(_: void, a: SqlFileGroup, b: SqlFileGroup) bool {
            return std.mem.order(u8, a.rel_path, b.rel_path) == .lt;
        }
    }.lessThan);

    return groups;
}

fn sortStrings(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}

test "findProjectRoot from project dir" {
    const allocator = std.testing.allocator;
    const root = findProjectRoot(allocator) catch {
        return;
    };
    defer allocator.free(root);
    try std.testing.expect(root.len > 0);
}
