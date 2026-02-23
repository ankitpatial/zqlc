const std = @import("std");

/// Query execution kind, determined by SQL comment annotation.
pub const QueryKind = enum {
    /// Returns exactly one row (or null).
    one,
    /// Returns zero or more rows.
    many,
    /// Fire-and-forget, returns void.
    exec,
    /// Returns affected row count.
    execrows,
};

/// A parsed but not yet type-checked SQL query.
pub const UntypedQuery = struct {
    /// Query name from annotation (PascalCase) or derived from filename.
    name: []const u8,
    /// Absolute or relative path to the .sql file.
    file_path: []const u8,
    /// The SQL text.
    sql: []const u8,
    /// Leading comment block (from -- lines), if any.
    comment: ?[]const u8,
    /// Query execution kind from annotation (:one, :many, :exec, :execrows), or null for auto-detect.
    kind: ?QueryKind,
    /// Line offset of the SQL body within the file (for error reporting).
    line_offset: u32,
};

/// Result of parsing a sqlc-style annotation: `-- name: QueryName :kind`
pub const SqlcAnnotation = struct {
    name: []const u8,
    kind: QueryKind,
};

/// Parse a sqlc-style annotation from a comment line.
/// Expected format: `name: QueryName :kind`
/// Returns null if the line doesn't match.
pub fn parseSqlcAnnotation(line: []const u8) ?SqlcAnnotation {
    const trimmed = std.mem.trim(u8, line, " \t");

    // Must start with "name:"
    if (!std.mem.startsWith(u8, trimmed, "name:")) return null;

    const after_name = std.mem.trim(u8, trimmed["name:".len..], " \t");
    if (after_name.len == 0) return null;

    // Find the last `:` which should precede the kind
    const last_colon = std.mem.lastIndexOfScalar(u8, after_name, ':') orelse return null;
    if (last_colon == 0) return null;

    const query_name = std.mem.trim(u8, after_name[0..last_colon], " \t");
    const kind_str = std.mem.trim(u8, after_name[last_colon + 1 ..], " \t");

    if (query_name.len == 0) return null;

    const kind: QueryKind = if (std.mem.eql(u8, kind_str, "one"))
        .one
    else if (std.mem.eql(u8, kind_str, "many"))
        .many
    else if (std.mem.eql(u8, kind_str, "exec"))
        .exec
    else if (std.mem.eql(u8, kind_str, "execrows"))
        .execrows
    else
        return null;

    return .{ .name = query_name, .kind = kind };
}

/// Parse a .sql file into one or more UntypedQuery values.
/// Files with multiple `-- name:` annotations are split into separate queries.
/// Files without annotations fall back to single-query parsing using the filename.
pub fn parseFile(allocator: std.mem.Allocator, file_path: []const u8) ![]UntypedQuery {
    const content = try std.fs.cwd().readFileAlloc(allocator, file_path, 10 * 1024 * 1024);
    defer allocator.free(content);

    return parseContent(allocator, content, file_path);
}

/// Parse SQL content into one or more UntypedQuery values.
/// Extracted for testability (no filesystem access).
pub fn parseContent(allocator: std.mem.Allocator, content: []const u8, file_path: []const u8) ![]UntypedQuery {
    // First pass: check if the file has any -- name: annotations
    var has_annotations = false;
    {
        var lines_iter = std.mem.splitScalar(u8, content, '\n');
        while (lines_iter.next()) |line| {
            const trimmed = std.mem.trim(u8, line, " \t\r");
            if (std.mem.startsWith(u8, trimmed, "--")) {
                const comment_body = std.mem.trim(u8, trimmed[2..], " ");
                if (parseSqlcAnnotation(comment_body) != null) {
                    has_annotations = true;
                    break;
                }
            }
        }
    }

    if (!has_annotations) {
        // Fallback: single-query parsing from the whole file
        const uq = try parseSingleQuery(allocator, content, file_path);
        const result = try allocator.alloc(UntypedQuery, 1);
        result[0] = uq;
        return result;
    }

    // Multi-query mode: split on -- name: boundaries
    var queries: std.ArrayList(UntypedQuery) = .empty;
    errdefer {
        for (queries.items) |q| freeUntypedQuery(allocator, q);
        queries.deinit(allocator);
    }

    var current_annotation: ?SqlcAnnotation = null;
    var doc_lines: std.ArrayList([]const u8) = .empty;
    defer doc_lines.deinit(allocator);
    var sql_lines: std.ArrayList([]const u8) = .empty;
    defer sql_lines.deinit(allocator);
    var in_sql_body = false;
    var block_start_line: u32 = 0;
    var line_num: u32 = 0;

    var lines_iter = std.mem.splitScalar(u8, content, '\n');
    while (lines_iter.next()) |line| {
        line_num += 1;
        const trimmed = std.mem.trim(u8, line, " \t\r");

        if (std.mem.startsWith(u8, trimmed, "--")) {
            const comment_body = std.mem.trim(u8, trimmed[2..], " ");
            if (parseSqlcAnnotation(comment_body)) |ann| {
                // Flush previous block
                if (current_annotation != null) {
                    try flushBlock(allocator, &queries, current_annotation.?, doc_lines.items, sql_lines.items, file_path, block_start_line);
                    doc_lines.clearRetainingCapacity();
                    sql_lines.clearRetainingCapacity();
                    in_sql_body = false;
                }
                current_annotation = ann;
                block_start_line = line_num;
            } else if (in_sql_body) {
                // SQL comment inside the body — preserve it
                try sql_lines.append(allocator, trimmed);
            } else if (current_annotation != null) {
                // Doc comment line (between annotation and SQL body)
                try doc_lines.append(allocator, comment_body);
            }
        } else if (trimmed.len == 0) {
            if (in_sql_body) {
                // Blank line inside SQL body — preserve it
                try sql_lines.append(allocator, "");
            }
        } else {
            // Non-comment, non-empty line — this is SQL
            if (current_annotation != null) {
                if (!in_sql_body) {
                    in_sql_body = true;
                    block_start_line = line_num;
                }
                try sql_lines.append(allocator, trimmed);
            }
        }
    }

    // Flush final block
    if (current_annotation != null) {
        try flushBlock(allocator, &queries, current_annotation.?, doc_lines.items, sql_lines.items, file_path, block_start_line);
    }

    if (queries.items.len == 0) return error.EmptyQuery;

    return queries.toOwnedSlice(allocator);
}

/// Flush a parsed block into a UntypedQuery and append it to the list.
fn flushBlock(
    allocator: std.mem.Allocator,
    queries: *std.ArrayList(UntypedQuery),
    annotation: SqlcAnnotation,
    doc_lines: []const []const u8,
    sql_lines: []const []const u8,
    file_path: []const u8,
    line_offset: u32,
) !void {
    // Join SQL lines
    var sql_buf: std.ArrayList(u8) = .empty;
    defer sql_buf.deinit(allocator);
    for (sql_lines, 0..) |sline, i| {
        if (i > 0) try sql_buf.append(allocator, '\n');
        try sql_buf.appendSlice(allocator, sline);
    }
    var sql_str = std.mem.trim(u8, sql_buf.items, " \t\r\n");
    // Strip trailing semicolons
    while (sql_str.len > 0 and sql_str[sql_str.len - 1] == ';') {
        sql_str = std.mem.trimRight(u8, sql_str[0 .. sql_str.len - 1], " \t\r\n");
    }
    if (sql_str.len == 0) return; // skip empty blocks

    // Build doc comment
    var doc_buf: std.ArrayList(u8) = .empty;
    defer doc_buf.deinit(allocator);
    for (doc_lines) |dline| {
        if (doc_buf.items.len > 0) try doc_buf.append(allocator, '\n');
        try doc_buf.appendSlice(allocator, dline);
    }

    const comment: ?[]const u8 = if (doc_buf.items.len > 0)
        try allocator.dupe(u8, doc_buf.items)
    else
        null;

    try queries.append(allocator, .{
        .name = try allocator.dupe(u8, annotation.name),
        .file_path = try allocator.dupe(u8, file_path),
        .sql = try allocator.dupe(u8, sql_str),
        .comment = comment,
        .kind = annotation.kind,
        .line_offset = line_offset,
    });
}

/// Parse a single-query file (no annotations — name from filename).
fn parseSingleQuery(allocator: std.mem.Allocator, content: []const u8, file_path: []const u8) !UntypedQuery {
    var comment_lines: std.ArrayList([]const u8) = .empty;
    defer comment_lines.deinit(allocator);
    var line_offset: u32 = 0;
    var sql_start: usize = 0;

    var lines_iter = std.mem.splitScalar(u8, content, '\n');
    var line_num: u32 = 0;
    while (lines_iter.next()) |line| {
        line_num += 1;
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (std.mem.startsWith(u8, trimmed, "--")) {
            const comment_content = std.mem.trim(u8, trimmed[2..], " ");
            try comment_lines.append(allocator, comment_content);
            sql_start = lines_iter.index orelse content.len;
        } else if (trimmed.len == 0) {
            sql_start = lines_iter.index orelse content.len;
        } else {
            line_offset = line_num;
            break;
        }
    }

    var sql = std.mem.trim(u8, content[sql_start..], " \t\r\n");
    // Strip trailing semicolons
    while (sql.len > 0 and sql[sql.len - 1] == ';') {
        sql = std.mem.trimRight(u8, sql[0 .. sql.len - 1], " \t\r\n");
    }
    if (sql.len == 0) return error.EmptyQuery;

    // Try to parse sqlc annotation from the first comment line
    var annotation: ?SqlcAnnotation = null;
    var doc_start: usize = 0;
    if (comment_lines.items.len > 0) {
        if (parseSqlcAnnotation(comment_lines.items[0])) |ann| {
            annotation = ann;
            doc_start = 1;
        }
    }

    const name = if (annotation) |ann|
        try allocator.dupe(u8, ann.name)
    else blk: {
        const basename = std.fs.path.basename(file_path);
        const name_end = std.mem.lastIndexOfScalar(u8, basename, '.') orelse basename.len;
        const raw_name = basename[0..name_end];
        break :blk try validateIdentifier(allocator, raw_name);
    };

    var doc_buf: std.ArrayList(u8) = .empty;
    defer doc_buf.deinit(allocator);
    for (comment_lines.items[doc_start..]) |cline| {
        if (doc_buf.items.len > 0) try doc_buf.append(allocator, '\n');
        try doc_buf.appendSlice(allocator, cline);
    }

    const comment: ?[]const u8 = if (doc_buf.items.len > 0)
        try allocator.dupe(u8, doc_buf.items)
    else
        null;

    const kind: ?QueryKind = if (annotation) |ann| ann.kind else null;

    return .{
        .name = name,
        .file_path = try allocator.dupe(u8, file_path),
        .sql = try allocator.dupe(u8, sql),
        .comment = comment,
        .kind = kind,
        .line_offset = line_offset,
    };
}

/// Free an UntypedQuery's owned allocations.
fn freeUntypedQuery(allocator: std.mem.Allocator, q: UntypedQuery) void {
    allocator.free(q.name);
    allocator.free(q.file_path);
    allocator.free(q.sql);
    if (q.comment) |c| allocator.free(c);
}

/// Zig keywords that need to be escaped with @"keyword" syntax.
const zig_keywords = [_][]const u8{
    "addrspace",  "align",     "allowzero", "and",        "anyframe",
    "anytype",    "asm",       "async",     "await",      "break",
    "catch",      "comptime",  "const",     "continue",   "defer",
    "else",       "enum",      "errdefer",  "error",      "export",
    "extern",     "false",     "fn",        "for",        "if",
    "inline",     "noalias",   "nosuspend", "null",       "opaque",
    "or",         "orelse",    "packed",    "pub",        "resume",
    "return",     "struct",    "suspend",   "switch",     "test",
    "threadlocal","true",      "try",       "type",       "undefined",
    "union",      "unreachable","usingnamespace","var",   "volatile",
    "while",
};

/// Validate and possibly escape an identifier to be valid Zig.
pub fn validateIdentifier(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    if (name.len == 0) return error.InvalidIdentifier;

    for (zig_keywords) |kw| {
        if (std.mem.eql(u8, name, kw)) {
            return std.fmt.allocPrint(allocator, "@\"{s}\"", .{name});
        }
    }

    if (name[0] != '_' and !std.ascii.isAlphabetic(name[0])) {
        return error.InvalidIdentifier;
    }

    for (name[1..]) |c| {
        if (c != '_' and !std.ascii.isAlphanumeric(c)) {
            return error.InvalidIdentifier;
        }
    }

    return allocator.dupe(u8, name);
}

test "validateIdentifier" {
    const allocator = std.testing.allocator;

    {
        const name = try validateIdentifier(allocator, "find_user_by_id");
        defer allocator.free(name);
        try std.testing.expectEqualStrings("find_user_by_id", name);
    }

    {
        const name = try validateIdentifier(allocator, "error");
        defer allocator.free(name);
        try std.testing.expectEqualStrings("@\"error\"", name);
    }

    try std.testing.expectError(error.InvalidIdentifier, validateIdentifier(allocator, ""));
    try std.testing.expectError(error.InvalidIdentifier, validateIdentifier(allocator, "123abc"));
    try std.testing.expectError(error.InvalidIdentifier, validateIdentifier(allocator, "foo-bar"));
}

test "parseSqlcAnnotation" {
    // name: GetAuthor :one
    {
        const ann = parseSqlcAnnotation("name: GetAuthor :one");
        try std.testing.expect(ann != null);
        try std.testing.expectEqualStrings("GetAuthor", ann.?.name);
        try std.testing.expectEqual(QueryKind.one, ann.?.kind);
    }
    // name: ListAuthors :many
    {
        const ann = parseSqlcAnnotation("name: ListAuthors :many");
        try std.testing.expect(ann != null);
        try std.testing.expectEqualStrings("ListAuthors", ann.?.name);
        try std.testing.expectEqual(QueryKind.many, ann.?.kind);
    }
    // name: DeleteAuthor :exec
    {
        const ann = parseSqlcAnnotation("name: DeleteAuthor :exec");
        try std.testing.expect(ann != null);
        try std.testing.expectEqualStrings("DeleteAuthor", ann.?.name);
        try std.testing.expectEqual(QueryKind.exec, ann.?.kind);
    }
    // name: UpdateAuthor :execrows
    {
        const ann = parseSqlcAnnotation("name: UpdateAuthor :execrows");
        try std.testing.expect(ann != null);
        try std.testing.expectEqualStrings("UpdateAuthor", ann.?.name);
        try std.testing.expectEqual(QueryKind.execrows, ann.?.kind);
    }
    // No annotation
    {
        const ann = parseSqlcAnnotation("Just a comment.");
        try std.testing.expect(ann == null);
    }
    // Old-style annotation should not match
    {
        const ann = parseSqlcAnnotation(":one Find a user.");
        try std.testing.expect(ann == null);
    }
}

test "name derivation from filename" {
    const basename = std.fs.path.basename("src/users/sql/find_user_by_id.sql");
    const name_end = std.mem.lastIndexOfScalar(u8, basename, '.') orelse basename.len;
    const raw_name = basename[0..name_end];
    try std.testing.expectEqualStrings("find_user_by_id", raw_name);
}

test "parseContent single annotated query" {
    const allocator = std.testing.allocator;
    const content =
        \\-- name: FindUserById :one
        \\-- Find a user by their ID.
        \\SELECT id, name, email
        \\FROM users
        \\WHERE id = $1;
    ;

    const queries = try parseContent(allocator, content, "sql/select.sql");
    defer {
        for (queries) |q| freeUntypedQuery(allocator, q);
        allocator.free(queries);
    }

    try std.testing.expectEqual(@as(usize, 1), queries.len);
    try std.testing.expectEqualStrings("FindUserById", queries[0].name);
    try std.testing.expectEqual(QueryKind.one, queries[0].kind.?);
    try std.testing.expectEqualStrings("Find a user by their ID.", queries[0].comment.?);
    // Trailing semicolon should be stripped
    try std.testing.expect(!std.mem.endsWith(u8, queries[0].sql, ";"));
    try std.testing.expect(std.mem.startsWith(u8, queries[0].sql, "SELECT"));
}

test "parseContent multiple queries in one file" {
    const allocator = std.testing.allocator;
    const content =
        \\-- name: FindUserById :one
        \\-- Find a user by ID.
        \\SELECT id, name FROM users WHERE id = $1;
        \\
        \\-- name: ListUsers :many
        \\-- List all users.
        \\SELECT id, name FROM users ORDER BY name;
        \\
        \\-- name: DeleteUser :exec
        \\DELETE FROM users WHERE id = $1;
    ;

    const queries = try parseContent(allocator, content, "sql/users.sql");
    defer {
        for (queries) |q| freeUntypedQuery(allocator, q);
        allocator.free(queries);
    }

    try std.testing.expectEqual(@as(usize, 3), queries.len);

    try std.testing.expectEqualStrings("FindUserById", queries[0].name);
    try std.testing.expectEqual(QueryKind.one, queries[0].kind.?);
    try std.testing.expectEqualStrings("Find a user by ID.", queries[0].comment.?);

    try std.testing.expectEqualStrings("ListUsers", queries[1].name);
    try std.testing.expectEqual(QueryKind.many, queries[1].kind.?);
    try std.testing.expectEqualStrings("List all users.", queries[1].comment.?);

    try std.testing.expectEqualStrings("DeleteUser", queries[2].name);
    try std.testing.expectEqual(QueryKind.exec, queries[2].kind.?);
    try std.testing.expect(queries[2].comment == null);
}

test "parseContent SQL comments inside body preserved" {
    const allocator = std.testing.allocator;
    const content =
        \\-- name: ComplexQuery :many
        \\SELECT id, name
        \\FROM users
        \\-- Filter active users only
        \\WHERE is_active = true;
    ;

    const queries = try parseContent(allocator, content, "sql/test.sql");
    defer {
        for (queries) |q| freeUntypedQuery(allocator, q);
        allocator.free(queries);
    }

    try std.testing.expectEqual(@as(usize, 1), queries.len);
    try std.testing.expect(std.mem.indexOf(u8, queries[0].sql, "-- Filter active users only") != null);
}

test "parseContent fallback without annotations" {
    const allocator = std.testing.allocator;
    const content =
        \\-- Find a user by their ID.
        \\SELECT id, name FROM users WHERE id = $1;
    ;

    const queries = try parseContent(allocator, content, "sql/find_user.sql");
    defer {
        for (queries) |q| freeUntypedQuery(allocator, q);
        allocator.free(queries);
    }

    try std.testing.expectEqual(@as(usize, 1), queries.len);
    try std.testing.expectEqualStrings("find_user", queries[0].name);
    try std.testing.expect(queries[0].kind == null);
    // Trailing semicolon should be stripped
    try std.testing.expect(!std.mem.endsWith(u8, queries[0].sql, ";"));
}
