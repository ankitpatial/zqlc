const std = @import("std");
const connection = @import("connection.zig");
const protocol = @import("protocol.zig");
const types = @import("types.zig");
const query_mod = @import("query.zig");

/// A column in a typed query result.
pub const Column = struct {
    name: []const u8,
    zig_type: types.ZigType,
    nullable: bool,
    table_oid: u32,
    column_attr: i16,
};

/// A parameter in a typed query.
pub const Param = struct {
    index: u16,
    name: []const u8,
    zig_type: types.ZigType,
};

/// A fully typed query ready for code generation.
pub const TypedQuery = struct {
    name: []const u8,
    file_path: []const u8,
    sql: []const u8,
    comment: ?[]const u8,
    kind: query_mod.QueryKind,
    params: []Param,
    columns: []Column,

    pub fn deinit(self: *TypedQuery, allocator: std.mem.Allocator) void {
        allocator.free(self.params);
        allocator.free(self.columns);
    }
};

/// Introspect queries against a live PostgreSQL database using the
/// extended query protocol (Parse/Describe/Sync).
pub fn describeQueries(
    allocator: std.mem.Allocator,
    conn: *connection.Connection,
    untyped: []const query_mod.UntypedQuery,
) !std.ArrayList(TypedQuery) {
    var type_cache = types.TypeCache.init(allocator);
    defer type_cache.deinit();
    var null_cache = types.NullabilityCache.init(allocator);
    defer null_cache.deinit();

    var result: std.ArrayList(TypedQuery) = .empty;
    errdefer {
        for (result.items) |*tq| tq.deinit(allocator);
        result.deinit(allocator);
    }

    for (untyped) |uq| {
        const typed = try describeOne(allocator, conn, &type_cache, &null_cache, uq);
        try result.append(allocator, typed);
    }

    return result;
}

/// Quote column alias hints (! and ?) so PostgreSQL accepts them.
/// Converts `AS total!` into `AS "total!"` so the hint survives
/// in the RowDescription column name returned by the server.
fn quoteAliasHints(allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);

    var i: usize = 0;
    while (i < sql.len) {
        const c = sql[i];

        // Skip string literals
        if (c == '\'') {
            try result.append(allocator, c);
            i += 1;
            while (i < sql.len) {
                try result.append(allocator, sql[i]);
                if (sql[i] == '\'') {
                    i += 1;
                    if (i < sql.len and sql[i] == '\'') {
                        try result.append(allocator, sql[i]);
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            continue;
        }

        // Skip already-quoted identifiers
        if (c == '"') {
            try result.append(allocator, c);
            i += 1;
            while (i < sql.len) {
                try result.append(allocator, sql[i]);
                if (sql[i] == '"') {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // Look for identifier followed by ! or ?
        if (std.ascii.isAlphabetic(c) or c == '_') {
            const start = i;
            while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_')) {
                i += 1;
            }
            // Check for trailing ! or ?
            if (i < sql.len and (sql[i] == '!' or sql[i] == '?')) {
                const suffix_end = i + 1;
                // Quote the whole identifier+suffix
                try result.append(allocator, '"');
                try result.appendSlice(allocator, sql[start..suffix_end]);
                try result.append(allocator, '"');
                i = suffix_end;
            } else {
                try result.appendSlice(allocator, sql[start..i]);
            }
            continue;
        }

        try result.append(allocator, c);
        i += 1;
    }

    return result.toOwnedSlice(allocator);
}

fn describeOne(
    allocator: std.mem.Allocator,
    conn: *connection.Connection,
    type_cache: *types.TypeCache,
    null_cache: *types.NullabilityCache,
    uq: query_mod.UntypedQuery,
) !TypedQuery {
    const stmt_name = "";

    // Quote alias hints (! / ?) so PostgreSQL accepts them as quoted identifiers
    const clean_sql = try quoteAliasHints(allocator, uq.sql);
    defer allocator.free(clean_sql);

    // Send Parse → Describe(Statement) → Sync
    try conn.sendParse(stmt_name, clean_sql);
    try conn.sendDescribeStatement(stmt_name);
    try conn.sendSync();

    // Collect responses until ReadyForQuery
    var msgs = try conn.recvUntilReady();
    defer msgs.deinit(allocator);

    var param_oids: []const u32 = &.{};
    var row_fields: []const protocol.RowField = &.{};
    var has_error = false;
    var error_msg: []const u8 = "";

    for (msgs.items) |msg| {
        switch (msg) {
            .parse_complete => {},
            .parameter_description => |oids| {
                param_oids = oids;
            },
            .row_description => |fields| {
                row_fields = fields;
            },
            .no_data => {},
            .ready_for_query => {},
            .error_response => |e| {
                has_error = true;
                for (e.fields) |field| {
                    if (field.code == 'M') {
                        error_msg = field.value;
                        break;
                    }
                }
            },
            else => {},
        }
    }

    if (has_error) {
        std.log.err("Query {s}: {s}", .{ uq.name, error_msg });
        return error.QueryIntrospectionFailed;
    }

    // Build params with names extracted from SQL context
    const param_names = try extractParamNames(allocator, uq.sql, param_oids.len);
    const params = try allocator.alloc(Param, param_oids.len);
    for (param_oids, 0..) |param_oid, i| {
        var zig_type = type_cache.resolve(param_oid);
        if (zig_type == null) {
            zig_type = try resolveEnumType(allocator, conn, type_cache, param_oid);
        }
        params[i] = .{
            .index = @intCast(i),
            .name = param_names[i],
            .zig_type = zig_type orelse .{ .unknown = param_oid },
        };
    }

    // Build columns
    const columns = try allocator.alloc(Column, row_fields.len);
    for (row_fields, 0..) |field, i| {
        var zig_type = type_cache.resolve(field.type_oid);
        if (zig_type == null) {
            zig_type = try resolveEnumType(allocator, conn, type_cache, field.type_oid);
        }

        const name = field.name;
        var nullable = true;

        if (std.mem.endsWith(u8, name, "!")) {
            nullable = false;
        } else if (std.mem.endsWith(u8, name, "?")) {
            nullable = true;
        } else if (field.table_oid != 0 and field.column_attr > 0) {
            if (null_cache.isNotNull(field.table_oid, field.column_attr)) |not_null| {
                nullable = !not_null;
            } else {
                const not_null = try queryColumnNullability(allocator, conn, field.table_oid, field.column_attr);
                try null_cache.put(field.table_oid, field.column_attr, not_null);
                nullable = !not_null;
            }
        }

        var clean_name = name;
        if (std.mem.endsWith(u8, clean_name, "!") or std.mem.endsWith(u8, clean_name, "?")) {
            clean_name = clean_name[0 .. clean_name.len - 1];
        }

        columns[i] = .{
            .name = clean_name,
            .zig_type = zig_type orelse .{ .unknown = field.type_oid },
            .nullable = nullable,
            .table_oid = field.table_oid,
            .column_attr = field.column_attr,
        };
    }

    // Determine query kind: use annotation if present, otherwise auto-detect
    const kind: query_mod.QueryKind = uq.kind orelse if (columns.len > 0) .many else .exec;

    // Sanity check: execrows should not have columns
    if (kind == .execrows and columns.len > 0) {
        std.log.warn("Query {s}: :execrows ignoring returned columns", .{uq.name});
    }

    return .{
        .name = uq.name,
        .file_path = uq.file_path,
        .sql = uq.sql,
        .comment = uq.comment,
        .kind = kind,
        .params = params,
        .columns = columns,
    };
}

fn queryColumnNullability(
    allocator: std.mem.Allocator,
    conn: *connection.Connection,
    table_oid: u32,
    column_attr: i16,
) !bool {
    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT attnotnull FROM pg_attribute WHERE attrelid = {d} AND attnum = {d}",
        .{ table_oid, column_attr },
    );
    defer allocator.free(sql);

    try conn.sendQuery(sql);

    var msgs = try conn.recvUntilReady();
    defer msgs.deinit(allocator);

    for (msgs.items) |msg| {
        switch (msg) {
            .data_row => |values| {
                if (values.len > 0) {
                    if (values[0]) |val| {
                        return std.mem.eql(u8, val, "t");
                    }
                }
            },
            else => {},
        }
    }

    return false;
}

fn resolveEnumType(
    allocator: std.mem.Allocator,
    conn: *connection.Connection,
    type_cache: *types.TypeCache,
    type_oid: u32,
) !?types.ZigType {
    const sql = try std.fmt.allocPrint(
        allocator,
        \\SELECT t.typname, e.enumlabel
        \\FROM pg_type t
        \\JOIN pg_enum e ON e.enumtypid = t.oid
        \\WHERE t.oid = {d}
        \\ORDER BY e.enumsortorder
    ,
        .{type_oid},
    );
    defer allocator.free(sql);

    try conn.sendQuery(sql);

    var msgs = try conn.recvUntilReady();
    defer msgs.deinit(allocator);

    var enum_name: ?[]const u8 = null;
    var variants: std.ArrayList([]const u8) = .empty;
    defer variants.deinit(allocator);

    for (msgs.items) |msg| {
        switch (msg) {
            .data_row => |values| {
                if (values.len >= 2) {
                    if (enum_name == null) {
                        if (values[0]) |n| {
                            enum_name = try allocator.dupe(u8, n);
                        }
                    }
                    if (values[1]) |v| {
                        try variants.append(allocator, try allocator.dupe(u8, v));
                    }
                }
            },
            else => {},
        }
    }

    if (enum_name) |name| {
        const owned_variants = try variants.toOwnedSlice(allocator);
        const zig_type = types.ZigType{ .pg_enum = .{
            .name = name,
            .variants = owned_variants,
        } };
        try type_cache.put(type_oid, zig_type);
        return zig_type;
    }

    return null;
}

/// Extract parameter names from SQL context by inspecting surrounding tokens.
///
/// Supports patterns:
///   - `column = $N` / `column >= $N` etc.  → name is `column`
///   - `INSERT INTO t (col1, col2) VALUES ($1, $2)` → positional match
///   - `SET col = $N` → name is `col`
///   - `LIMIT $N` → `limit`, `OFFSET $N` → `offset`
///   - Fallback: `param_N`
pub fn extractParamNames(allocator: std.mem.Allocator, sql: []const u8, param_count: usize) ![][]const u8 {
    const names = try allocator.alloc([]const u8, param_count);
    for (0..param_count) |i| {
        names[i] = "";
    }

    // Try INSERT column list matching first
    if (tryInsertColumns(sql, names)) {
        // Filled in from column list
    }

    // Scan for contextual patterns around each $N
    var i: usize = 0;
    while (i < sql.len) : (i += 1) {
        // Skip string literals
        if (sql[i] == '\'') {
            i += 1;
            while (i < sql.len) : (i += 1) {
                if (sql[i] == '\'') {
                    if (i + 1 < sql.len and sql[i + 1] == '\'') {
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        if (sql[i] != '$') continue;

        // Parse the parameter number
        const num_start = i + 1;
        var num_end = num_start;
        while (num_end < sql.len and std.ascii.isDigit(sql[num_end])) : (num_end += 1) {}
        if (num_end == num_start) continue;

        const param_num = std.fmt.parseInt(usize, sql[num_start..num_end], 10) catch continue;
        if (param_num == 0 or param_num > param_count) continue;
        const idx = param_num - 1;

        // Already named (e.g., from INSERT column list)
        if (names[idx].len > 0) continue;

        // Look backward for context: skip whitespace/operators to find a preceding identifier
        if (findPrecedingContext(sql, i)) |ctx_name| {
            names[idx] = ctx_name;
            continue;
        }

        // Look forward for keyword context (LIMIT $N, OFFSET $N matched via preceding)
    }

    // Fill in any remaining unnamed params with fallback names
    for (0..param_count) |idx| {
        if (names[idx].len == 0) {
            names[idx] = try std.fmt.allocPrint(allocator, "param_{d}", .{idx + 1});
        } else {
            names[idx] = try allocator.dupe(u8, names[idx]);
        }
    }

    return names;
}

/// Try to match INSERT INTO t (col1, col2, ...) VALUES ($1, $2, ...) and
/// assign names positionally from the column list.
fn tryInsertColumns(sql: []const u8, names: [][]const u8) bool {
    const upper = struct {
        fn eqlFold(a: []const u8, b: []const u8) bool {
            if (a.len != b.len) return false;
            for (a, b) |ca, cb| {
                if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
            }
            return true;
        }
    };

    // Find "INSERT" then "INTO" then table, then "(" column list ")"
    // then "VALUES" then "(" $1, $2 ... ")"
    var pos: usize = 0;
    // Find INSERT
    pos = findKeyword(sql, pos, "INSERT") orelse return false;
    // Find INTO
    pos = findKeyword(sql, pos, "INTO") orelse return false;
    // Skip table name
    pos = skipWhitespace(sql, pos);
    pos = skipIdentifier(sql, pos);
    // Find opening paren for column list
    pos = skipWhitespace(sql, pos);
    if (pos >= sql.len or sql[pos] != '(') return false;
    pos += 1;

    // Parse column names
    var col_names: [64][]const u8 = undefined;
    var col_count: usize = 0;
    while (pos < sql.len and col_count < 64) {
        pos = skipWhitespace(sql, pos);
        if (sql[pos] == ')') {
            pos += 1;
            break;
        }
        if (sql[pos] == ',') {
            pos += 1;
            continue;
        }
        const id_start = pos;
        pos = skipIdentifier(sql, pos);
        if (pos == id_start) return false;
        col_names[col_count] = sql[id_start..pos];
        col_count += 1;
    }

    if (col_count == 0) return false;

    // Find VALUES
    pos = findKeyword(sql, pos, "VALUES") orelse return false;
    pos = skipWhitespace(sql, pos);
    if (pos >= sql.len or sql[pos] != '(') return false;
    pos += 1;

    // Match $N params positionally to column names
    var param_idx: usize = 0;
    while (pos < sql.len and param_idx < col_count) {
        pos = skipWhitespace(sql, pos);
        if (sql[pos] == ')') break;
        if (sql[pos] == ',') {
            pos += 1;
            continue;
        }
        if (sql[pos] == '$') {
            pos += 1;
            const num_start = pos;
            while (pos < sql.len and std.ascii.isDigit(sql[pos])) : (pos += 1) {}
            const param_num = std.fmt.parseInt(usize, sql[num_start..pos], 10) catch {
                param_idx += 1;
                continue;
            };
            if (param_num > 0 and param_num <= names.len and param_idx < col_count) {
                names[param_num - 1] = col_names[param_idx];
            }
            param_idx += 1;
        } else {
            // Skip non-param expression
            while (pos < sql.len and sql[pos] != ',' and sql[pos] != ')') : (pos += 1) {}
            param_idx += 1;
        }
    }

    _ = upper;
    return col_count > 0;
}

/// Look backward from a `$N` position to find the preceding column/keyword context.
fn findPrecedingContext(sql: []const u8, dollar_pos: usize) ?[]const u8 {
    var pos = dollar_pos;

    // Skip backward over whitespace
    while (pos > 0 and (sql[pos - 1] == ' ' or sql[pos - 1] == '\t' or sql[pos - 1] == '\n' or sql[pos - 1] == '\r')) {
        pos -= 1;
    }

    // Skip backward over operator(s): =, >=, <=, !=, <>, <, >
    if (pos > 0 and (sql[pos - 1] == '=' or sql[pos - 1] == '>' or sql[pos - 1] == '<' or sql[pos - 1] == '!')) {
        pos -= 1;
        // Multi-char operators
        if (pos > 0 and (sql[pos - 1] == '>' or sql[pos - 1] == '<' or sql[pos - 1] == '!')) {
            pos -= 1;
        }
    } else {
        // No operator — might be LIMIT/OFFSET or other keyword context
        // Check if preceding token is a keyword
        if (pos > 0) {
            while (pos > 0 and (sql[pos - 1] == ' ' or sql[pos - 1] == '\t')) pos -= 1;
            const end = pos;
            while (pos > 0 and (std.ascii.isAlphabetic(sql[pos - 1]) or sql[pos - 1] == '_')) pos -= 1;
            const word = sql[pos..end];
            if (eqlIgnoreCase(word, "LIMIT")) return "limit";
            if (eqlIgnoreCase(word, "OFFSET")) return "offset";
        }
        return null;
    }

    // Skip whitespace before operator
    while (pos > 0 and (sql[pos - 1] == ' ' or sql[pos - 1] == '\t')) {
        pos -= 1;
    }

    // Extract preceding identifier
    const end = pos;
    while (pos > 0 and (std.ascii.isAlphanumeric(sql[pos - 1]) or sql[pos - 1] == '_')) {
        pos -= 1;
    }

    if (pos == end) return null;
    const name = sql[pos..end];

    // Skip SQL keywords that aren't column names
    if (eqlIgnoreCase(name, "AND") or eqlIgnoreCase(name, "OR") or
        eqlIgnoreCase(name, "NOT") or eqlIgnoreCase(name, "IS") or
        eqlIgnoreCase(name, "IN") or eqlIgnoreCase(name, "LIKE") or
        eqlIgnoreCase(name, "SET") or eqlIgnoreCase(name, "WHERE") or
        eqlIgnoreCase(name, "HAVING") or eqlIgnoreCase(name, "ON") or
        eqlIgnoreCase(name, "THEN") or eqlIgnoreCase(name, "WHEN") or
        eqlIgnoreCase(name, "ELSE") or eqlIgnoreCase(name, "NULL"))
    {
        return null;
    }

    return name;
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}

fn findKeyword(sql: []const u8, start: usize, keyword: []const u8) ?usize {
    var pos = start;
    while (pos + keyword.len <= sql.len) {
        pos = skipWhitespace(sql, pos);
        if (pos + keyword.len > sql.len) return null;
        if (eqlIgnoreCase(sql[pos .. pos + keyword.len], keyword)) {
            const after = pos + keyword.len;
            if (after >= sql.len or !std.ascii.isAlphanumeric(sql[after])) {
                return after;
            }
        }
        // Skip one token
        if (std.ascii.isAlphabetic(sql[pos]) or sql[pos] == '_') {
            pos = skipIdentifier(sql, pos);
        } else {
            pos += 1;
        }
    }
    return null;
}

fn skipWhitespace(sql: []const u8, start: usize) usize {
    var pos = start;
    while (pos < sql.len and (sql[pos] == ' ' or sql[pos] == '\t' or sql[pos] == '\n' or sql[pos] == '\r')) {
        pos += 1;
    }
    return pos;
}

fn skipIdentifier(sql: []const u8, start: usize) usize {
    var pos = start;
    // Handle quoted identifiers
    if (pos < sql.len and sql[pos] == '"') {
        pos += 1;
        while (pos < sql.len and sql[pos] != '"') : (pos += 1) {}
        if (pos < sql.len) pos += 1;
        return pos;
    }
    while (pos < sql.len and (std.ascii.isAlphanumeric(sql[pos]) or sql[pos] == '_' or sql[pos] == '.')) {
        pos += 1;
    }
    return pos;
}

test "quoteAliasHints" {
    const allocator = std.testing.allocator;

    {
        const result = try quoteAliasHints(allocator, "SELECT COUNT(*) AS total!, name FROM t");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("SELECT COUNT(*) AS \"total!\", name FROM t", result);
    }
    {
        const result = try quoteAliasHints(allocator, "SELECT a?, b! FROM t");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("SELECT \"a?\", \"b!\" FROM t", result);
    }
    {
        // No hints — unchanged
        const result = try quoteAliasHints(allocator, "SELECT id, name FROM users");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("SELECT id, name FROM users", result);
    }
    {
        // String literals should not be modified
        const result = try quoteAliasHints(allocator, "SELECT 'hello!' AS greeting FROM t");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("SELECT 'hello!' AS greeting FROM t", result);
    }
}

test "TypedQuery struct size" {
    try std.testing.expect(@sizeOf(TypedQuery) > 0);
    try std.testing.expect(@sizeOf(Column) > 0);
    try std.testing.expect(@sizeOf(Param) > 0);
}
