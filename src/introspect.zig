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

/// Per-query validation error with the actual PostgreSQL error message.
pub const QueryValidationError = struct {
    query_name: []const u8,
    file_path: []const u8,
    message: []const u8,
    detail: ?[]const u8 = null,
    position: ?u32 = null,
};

/// Result of describing a batch of queries — includes both successes and per-query errors.
pub const DescribeResult = struct {
    queries: std.ArrayList(TypedQuery),
    errors: std.ArrayList(QueryValidationError),

    pub fn deinit(self: *DescribeResult, allocator: std.mem.Allocator) void {
        for (self.queries.items) |*tq| tq.deinit(allocator);
        self.queries.deinit(allocator);
        self.errors.deinit(allocator);
    }
};

/// Introspect queries against a live PostgreSQL database using the
/// extended query protocol (Parse/Describe/Sync).
/// Validates all queries and continues on per-query failures, collecting errors.
pub fn describeQueries(
    allocator: std.mem.Allocator,
    conn: *connection.Connection,
    untyped: []const query_mod.UntypedQuery,
    type_cache: *types.TypeCache,
    null_cache: *types.NullabilityCache,
) !DescribeResult {
    var result: DescribeResult = .{
        .queries = .empty,
        .errors = .empty,
    };
    errdefer result.deinit(allocator);

    for (untyped) |uq| {
        const typed = describeOne(allocator, conn, type_cache, null_cache, uq, &result.errors) catch |err| {
            if (err != error.QueryValidationFailed) {
                // Non-PG error (e.g., connection issue) — add generic entry
                try result.errors.append(allocator, .{
                    .query_name = uq.name,
                    .file_path = uq.file_path,
                    .message = @errorName(err),
                });
            }
            continue;
        };
        try result.queries.append(allocator, typed);
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

        // Skip block comments
        if (c == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            try result.appendSlice(allocator, "/*");
            i += 2;
            while (i + 1 < sql.len) {
                if (sql[i] == '*' and sql[i + 1] == '/') {
                    try result.appendSlice(allocator, "*/");
                    i += 2;
                    break;
                }
                try result.append(allocator, sql[i]);
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
    validation_errors: ?*std.ArrayList(QueryValidationError),
) !TypedQuery {
    const stmt_name = "";

    // Replace @name named parameters with $N positional parameters (sqlc-compatible).
    const named = try replaceNamedParams(allocator, uq.sql);
    const effective_sql = if (named) |n| n.sql else uq.sql;

    // Quote alias hints (! / ?) so PostgreSQL accepts them as quoted identifiers
    const clean_sql = try quoteAliasHints(allocator, effective_sql);
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
    var error_detail: ?[]const u8 = null;
    var error_position: ?u32 = null;

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
                    switch (field.code) {
                        'M' => error_msg = field.value,
                        'D' => error_detail = field.value,
                        'P' => error_position = std.fmt.parseInt(u32, field.value, 10) catch null,
                        else => {},
                    }
                }
            },
            else => {},
        }
    }

    if (has_error) {
        if (validation_errors) |errs| {
            try errs.append(allocator, .{
                .query_name = uq.name,
                .file_path = uq.file_path,
                .message = error_msg,
                .detail = error_detail,
                .position = error_position,
            });
            return error.QueryValidationFailed;
        }
        std.log.err("Query {s}: {s}", .{ uq.name, error_msg });
        return error.QueryIntrospectionFailed;
    }

    // Build param names — merge positional and @name sources when both exist.
    const param_names = if (named) |n| blk: {
        const total = n.positional_count + n.names.len;
        const all_names = try allocator.alloc([]const u8, total);
        // Extract names for pre-existing positional $N params from original SQL
        const pos_names = try extractParamNames(allocator, uq.sql, n.positional_count);
        for (0..n.positional_count) |idx| all_names[idx] = pos_names[idx];
        allocator.free(pos_names);
        // Named @params fill remaining slots
        for (0..n.names.len) |idx| all_names[n.positional_count + idx] = n.names[idx];
        break :blk try deduplicateParamNames(allocator, all_names);
    } else try deduplicateParamNames(
        allocator,
        try extractParamNames(allocator, uq.sql, param_oids.len),
    );

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
        .sql = effective_sql,
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

/// Result of replacing @name parameters with $N positional parameters.
const NamedParamResult = struct {
    sql: []const u8,
    names: [][]const u8,
    positional_count: usize, // number of pre-existing $N params
};

/// Replace sqlc-style @name named parameters with $N positional parameters.
/// Each unique @name gets a unique $N number (assigned in order of first appearance).
/// Repeated occurrences of the same @name reuse the same $N.
/// Returns null if no @name parameters are found (SQL uses $N already).
pub fn replaceNamedParams(allocator: std.mem.Allocator, sql: []const u8) !?NamedParamResult {
    // Quick scan: bail out early if no @ params exist
    var has_named = false;
    for (0..sql.len) |idx| {
        if (sql[idx] == '@' and idx + 1 < sql.len and
            (std.ascii.isAlphabetic(sql[idx + 1]) or sql[idx + 1] == '_'))
        {
            has_named = true;
            break;
        }
    }
    if (!has_named) return null;

    // Pre-scan: find the highest existing $N positional parameter
    var max_positional: usize = 0;
    {
        var j: usize = 0;
        while (j < sql.len) {
            // Skip string literals
            if (sql[j] == '\'') {
                j += 1;
                while (j < sql.len) {
                    if (sql[j] == '\'') {
                        if (j + 1 < sql.len and sql[j + 1] == '\'') {
                            j += 2;
                            continue;
                        }
                        j += 1;
                        break;
                    }
                    j += 1;
                }
                continue;
            }
            // Skip line comments
            if (sql[j] == '-' and j + 1 < sql.len and sql[j + 1] == '-') {
                while (j < sql.len and sql[j] != '\n') j += 1;
                continue;
            }
            // Check for $N
            if (sql[j] == '$' and j + 1 < sql.len and std.ascii.isDigit(sql[j + 1])) {
                const num_start = j + 1;
                var num_end = num_start;
                while (num_end < sql.len and std.ascii.isDigit(sql[num_end])) num_end += 1;
                const num = std.fmt.parseInt(usize, sql[num_start..num_end], 10) catch {
                    j = num_end;
                    continue;
                };
                if (num > max_positional) max_positional = num;
                j = num_end;
                continue;
            }
            j += 1;
        }
    }

    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(allocator);
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);

    var i: usize = 0;
    while (i < sql.len) {
        const c = sql[i];

        // Skip string literals (single-quoted)
        if (c == '\'') {
            try result.append(allocator, c);
            i += 1;
            while (i < sql.len) {
                try result.append(allocator, sql[i]);
                if (sql[i] == '\'') {
                    if (i + 1 < sql.len and sql[i + 1] == '\'') {
                        i += 1;
                        try result.append(allocator, sql[i]);
                        i += 1;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // Skip line comments (-- ...)
        if (c == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            while (i < sql.len and sql[i] != '\n') {
                try result.append(allocator, sql[i]);
                i += 1;
            }
            continue;
        }

        // Check for @name
        if (c == '@' and i + 1 < sql.len and
            (std.ascii.isAlphabetic(sql[i + 1]) or sql[i + 1] == '_'))
        {
            const name_start = i + 1;
            var name_end = name_start;
            while (name_end < sql.len and
                (std.ascii.isAlphanumeric(sql[name_end]) or sql[name_end] == '_'))
            {
                name_end += 1;
            }
            const name = sql[name_start..name_end];

            // Find existing position or assign a new one (offset by max_positional)
            var pos: usize = 0;
            for (names.items, 0..) |existing, idx| {
                if (std.mem.eql(u8, existing, name)) {
                    pos = max_positional + idx + 1;
                    break;
                }
            } else {
                try names.append(allocator, name);
                pos = max_positional + names.items.len;
            }

            // Emit $N
            var num_buf: [16]u8 = undefined;
            const num_str = std.fmt.bufPrint(&num_buf, "${d}", .{pos}) catch unreachable;
            try result.appendSlice(allocator, num_str);
            i = name_end;
            continue;
        }

        try result.append(allocator, c);
        i += 1;
    }

    // Dupe all names so they're owned by the allocator
    const final_names = try allocator.alloc([]const u8, names.items.len);
    for (names.items, 0..) |name, idx| {
        final_names[idx] = try allocator.dupe(u8, name);
    }

    return .{
        .sql = try result.toOwnedSlice(allocator),
        .names = final_names,
        .positional_count = max_positional,
    };
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

        // Look backward for context (handles column = $N, LIMIT $N, OFFSET $N, etc.)
        if (findPrecedingContext(sql, i)) |ctx_name| {
            names[idx] = ctx_name;
            continue;
        }
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

/// Deduplicate parameter names by appending _1, _2, ... to collisions.
/// Takes ownership of the input slice and its strings — the caller must
/// not free them separately.
fn deduplicateParamNames(allocator: std.mem.Allocator, names: [][]const u8) ![][]const u8 {
    for (names, 0..) |name, i| {
        var suffix: usize = 1;
        for (i + 1..names.len) |j| {
            if (std.mem.eql(u8, names[j], name)) {
                const new_name = try std.fmt.allocPrint(allocator, "{s}_{d}", .{ name, suffix });
                allocator.free(names[j]);
                names[j] = new_name;
                suffix += 1;
            }
        }
    }
    return names;
}

/// Try to match INSERT INTO t (col1, col2, ...) VALUES ($1, $2, ...) and
/// assign names positionally from the column list.
fn tryInsertColumns(sql: []const u8, names: [][]const u8) bool {
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
        if (pos >= sql.len) return false;
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
        if (pos >= sql.len) break;
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

test "replaceNamedParams basic" {
    const allocator = std.testing.allocator;

    {
        // Simple @name replacement
        const r = (try replaceNamedParams(allocator, "SELECT * FROM users WHERE id = @user_id")).?;
        defer {
            allocator.free(r.sql);
            for (r.names) |n| allocator.free(n);
            allocator.free(r.names);
        }
        try std.testing.expectEqualStrings("SELECT * FROM users WHERE id = $1", r.sql);
        try std.testing.expectEqual(@as(usize, 1), r.names.len);
        try std.testing.expectEqualStrings("user_id", r.names[0]);
    }
}

test "replaceNamedParams repeated name reuses same position" {
    const allocator = std.testing.allocator;

    {
        const r = (try replaceNamedParams(allocator,
            "WHERE (@author_id::int IS NULL OR p.user_id = @author_id)",
        )).?;
        defer {
            allocator.free(r.sql);
            for (r.names) |n| allocator.free(n);
            allocator.free(r.names);
        }
        try std.testing.expectEqualStrings(
            "WHERE ($1::int IS NULL OR p.user_id = $1)",
            r.sql,
        );
        try std.testing.expectEqual(@as(usize, 1), r.names.len);
        try std.testing.expectEqualStrings("author_id", r.names[0]);
    }
}

test "replaceNamedParams multiple distinct names" {
    const allocator = std.testing.allocator;

    {
        const r = (try replaceNamedParams(allocator,
            "WHERE id = @id AND name = @name LIMIT @limit OFFSET @offset",
        )).?;
        defer {
            allocator.free(r.sql);
            for (r.names) |n| allocator.free(n);
            allocator.free(r.names);
        }
        try std.testing.expectEqualStrings(
            "WHERE id = $1 AND name = $2 LIMIT $3 OFFSET $4",
            r.sql,
        );
        try std.testing.expectEqual(@as(usize, 4), r.names.len);
        try std.testing.expectEqualStrings("id", r.names[0]);
        try std.testing.expectEqualStrings("name", r.names[1]);
        try std.testing.expectEqualStrings("limit", r.names[2]);
        try std.testing.expectEqualStrings("offset", r.names[3]);
    }
}

test "replaceNamedParams returns null for positional params" {
    const allocator = std.testing.allocator;

    {
        // No @name params — should return null
        const r = try replaceNamedParams(allocator, "SELECT * FROM users WHERE id = $1");
        try std.testing.expect(r == null);
    }
}

test "replaceNamedParams preserves string literals" {
    const allocator = std.testing.allocator;

    {
        const r = (try replaceNamedParams(allocator,
            "SELECT * FROM users WHERE email = @email AND name = '@not_a_param'",
        )).?;
        defer {
            allocator.free(r.sql);
            for (r.names) |n| allocator.free(n);
            allocator.free(r.names);
        }
        try std.testing.expectEqualStrings(
            "SELECT * FROM users WHERE email = $1 AND name = '@not_a_param'",
            r.sql,
        );
        try std.testing.expectEqual(@as(usize, 1), r.names.len);
        try std.testing.expectEqualStrings("email", r.names[0]);
    }
}

test "replaceNamedParams preserves type casts" {
    const allocator = std.testing.allocator;

    {
        const r = (try replaceNamedParams(allocator,
            "WHERE (@published::boolean IS NULL OR p.published = @published)",
        )).?;
        defer {
            allocator.free(r.sql);
            for (r.names) |n| allocator.free(n);
            allocator.free(r.names);
        }
        try std.testing.expectEqualStrings(
            "WHERE ($1::boolean IS NULL OR p.published = $1)",
            r.sql,
        );
        try std.testing.expectEqual(@as(usize, 1), r.names.len);
        try std.testing.expectEqualStrings("published", r.names[0]);
    }
}

test "replaceNamedParams skips comments" {
    const allocator = std.testing.allocator;

    {
        const r = (try replaceNamedParams(allocator,
            "-- filter by @author\nWHERE id = @author_id",
        )).?;
        defer {
            allocator.free(r.sql);
            for (r.names) |n| allocator.free(n);
            allocator.free(r.names);
        }
        try std.testing.expectEqualStrings(
            "-- filter by @author\nWHERE id = $1",
            r.sql,
        );
        try std.testing.expectEqual(@as(usize, 1), r.names.len);
        try std.testing.expectEqualStrings("author_id", r.names[0]);
    }
}

test "replaceNamedParams full SearchPosts query" {
    const allocator = std.testing.allocator;

    const sql =
        \\SELECT p.id, p.title, p.body, p.published, p.created_at,
        \\       u.name AS author_name
        \\FROM posts p
        \\JOIN users u ON u.id = p.user_id
        \\WHERE (@author_id::int IS NULL OR p.user_id = @author_id)
        \\  AND (@title_keyword::text IS NULL OR p.title ILIKE '%' || @title_keyword || '%')
        \\  AND (@body_keyword::text IS NULL OR p.body ILIKE '%' || @body_keyword || '%')
        \\  AND (@published::boolean IS NULL OR p.published = @published)
        \\  AND (@created_after::timestamptz IS NULL OR p.created_at >= @created_after)
        \\  AND (@created_before::timestamptz IS NULL OR p.created_at <= @created_before)
        \\ORDER BY p.created_at DESC
        \\LIMIT @limit
        \\OFFSET @offset
    ;

    const r = (try replaceNamedParams(allocator, sql)).?;
    defer {
        allocator.free(r.sql);
        for (r.names) |n| allocator.free(n);
        allocator.free(r.names);
    }

    // 8 unique named params
    try std.testing.expectEqual(@as(usize, 8), r.names.len);
    try std.testing.expectEqualStrings("author_id", r.names[0]);
    try std.testing.expectEqualStrings("title_keyword", r.names[1]);
    try std.testing.expectEqualStrings("body_keyword", r.names[2]);
    try std.testing.expectEqualStrings("published", r.names[3]);
    try std.testing.expectEqualStrings("created_after", r.names[4]);
    try std.testing.expectEqualStrings("created_before", r.names[5]);
    try std.testing.expectEqualStrings("limit", r.names[6]);
    try std.testing.expectEqualStrings("offset", r.names[7]);

    // Repeated @author_id should map to same $1
    try std.testing.expect(std.mem.indexOf(u8, r.sql, "$1::int IS NULL OR p.user_id = $1") != null);
    // No @name tokens should remain
    for (r.sql, 0..) |c, idx| {
        if (c == '@' and idx + 1 < r.sql.len and std.ascii.isAlphabetic(r.sql[idx + 1])) {
            return error.UnexpectedNamedParam;
        }
    }
}

test "replaceNamedParams underscore-prefixed name" {
    const allocator = std.testing.allocator;

    const r = (try replaceNamedParams(allocator, "WHERE id = @_internal_id")).?;
    defer {
        allocator.free(r.sql);
        for (r.names) |n| allocator.free(n);
        allocator.free(r.names);
    }
    try std.testing.expectEqualStrings("WHERE id = $1", r.sql);
    try std.testing.expectEqualStrings("_internal_id", r.names[0]);
}

test "replaceNamedParams does not treat email @ as param" {
    const allocator = std.testing.allocator;

    // Bare @ followed by non-alpha/non-underscore should not be treated as a named param
    const r = try replaceNamedParams(allocator, "WHERE email = 'user@123.com'");
    try std.testing.expect(r == null);
}

test "replaceNamedParams adjacent to parentheses and operators" {
    const allocator = std.testing.allocator;

    const r = (try replaceNamedParams(allocator,
        "WHERE (id=@id) AND age>@min_age AND age<@max_age",
    )).?;
    defer {
        allocator.free(r.sql);
        for (r.names) |n| allocator.free(n);
        allocator.free(r.names);
    }
    try std.testing.expectEqualStrings("WHERE (id=$1) AND age>$2 AND age<$3", r.sql);
    try std.testing.expectEqual(@as(usize, 3), r.names.len);
    try std.testing.expectEqualStrings("id", r.names[0]);
    try std.testing.expectEqualStrings("min_age", r.names[1]);
    try std.testing.expectEqualStrings("max_age", r.names[2]);
}

test "replaceNamedParams mixed positional and named avoids collision" {
    const allocator = std.testing.allocator;

    // @name should be assigned $2 (after existing $1), not $1
    const r = (try replaceNamedParams(allocator,
        "WHERE id = $1 AND name = @name",
    )).?;
    defer {
        allocator.free(r.sql);
        for (r.names) |n| allocator.free(n);
        allocator.free(r.names);
    }
    try std.testing.expectEqualStrings("WHERE id = $1 AND name = $2", r.sql);
    try std.testing.expectEqual(@as(usize, 1), r.names.len);
    try std.testing.expectEqualStrings("name", r.names[0]);
    try std.testing.expectEqual(@as(usize, 1), r.positional_count);
}

test "replaceNamedParams lockAccount-style mixed params" {
    const allocator = std.testing.allocator;

    const r = (try replaceNamedParams(allocator,
        "UPDATE accounts SET locked_until_at = @locked_until_at WHERE id = $1 RETURNING id, locked_until_at",
    )).?;
    defer {
        allocator.free(r.sql);
        for (r.names) |n| allocator.free(n);
        allocator.free(r.names);
    }
    // @locked_until_at should become $2, not $1
    try std.testing.expectEqualStrings(
        "UPDATE accounts SET locked_until_at = $2 WHERE id = $1 RETURNING id, locked_until_at",
        r.sql,
    );
    try std.testing.expectEqual(@as(usize, 1), r.names.len);
    try std.testing.expectEqualStrings("locked_until_at", r.names[0]);
    try std.testing.expectEqual(@as(usize, 1), r.positional_count);
}

test "replaceNamedParams multiple positional and named" {
    const allocator = std.testing.allocator;

    const r = (try replaceNamedParams(allocator,
        "WHERE a = $1 AND b = $2 AND c = @foo AND d = @bar",
    )).?;
    defer {
        allocator.free(r.sql);
        for (r.names) |n| allocator.free(n);
        allocator.free(r.names);
    }
    // @foo → $3, @bar → $4
    try std.testing.expectEqualStrings("WHERE a = $1 AND b = $2 AND c = $3 AND d = $4", r.sql);
    try std.testing.expectEqual(@as(usize, 2), r.names.len);
    try std.testing.expectEqualStrings("foo", r.names[0]);
    try std.testing.expectEqualStrings("bar", r.names[1]);
    try std.testing.expectEqual(@as(usize, 2), r.positional_count);
}

test "deduplicateParamNames resolves duplicate names" {
    const allocator = std.testing.allocator;

    const names = try extractParamNames(allocator,
        "WHERE age >= $1 AND age <= $2 AND name = $3",
        3,
    );
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }

    const deduped = try deduplicateParamNames(allocator, names);
    try std.testing.expectEqualStrings("age", deduped[0]);
    try std.testing.expectEqualStrings("age_1", deduped[1]);
    try std.testing.expectEqualStrings("name", deduped[2]);
}

test "extractParamNames simple WHERE clause" {
    const allocator = std.testing.allocator;

    const names = try extractParamNames(allocator, "SELECT * FROM users WHERE id = $1", 1);
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("id", names[0]);
}

test "extractParamNames multiple comparison operators" {
    const allocator = std.testing.allocator;

    const names = try extractParamNames(allocator,
        "WHERE age >= $1 AND age <= $2 AND name = $3",
        3,
    );
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }
    try std.testing.expectEqualStrings("age", names[0]);
    try std.testing.expectEqualStrings("age", names[1]);
    try std.testing.expectEqualStrings("name", names[2]);
}

test "extractParamNames LIMIT and OFFSET" {
    const allocator = std.testing.allocator;

    const names = try extractParamNames(allocator,
        "SELECT * FROM users LIMIT $1 OFFSET $2",
        2,
    );
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }
    try std.testing.expectEqualStrings("limit", names[0]);
    try std.testing.expectEqualStrings("offset", names[1]);
}

test "extractParamNames INSERT column mapping" {
    const allocator = std.testing.allocator;

    const names = try extractParamNames(allocator,
        "INSERT INTO users (name, email, bio) VALUES ($1, $2, $3)",
        3,
    );
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }
    try std.testing.expectEqualStrings("name", names[0]);
    try std.testing.expectEqualStrings("email", names[1]);
    try std.testing.expectEqualStrings("bio", names[2]);
}

test "extractParamNames SET clause" {
    const allocator = std.testing.allocator;

    const names = try extractParamNames(allocator,
        "UPDATE users SET name = $1, email = $2 WHERE id = $3",
        3,
    );
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }
    try std.testing.expectEqualStrings("name", names[0]);
    try std.testing.expectEqualStrings("email", names[1]);
    try std.testing.expectEqualStrings("id", names[2]);
}

test "extractParamNames fallback naming" {
    const allocator = std.testing.allocator;

    // When context can't be determined, falls back to param_N
    const names = try extractParamNames(allocator,
        "SELECT * FROM users WHERE $1 AND $2",
        2,
    );
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }
    try std.testing.expectEqualStrings("param_1", names[0]);
    try std.testing.expectEqualStrings("param_2", names[1]);
}

test "extractParamNames skips string literals" {
    const allocator = std.testing.allocator;

    // $1 inside a string literal should not confuse the parser
    const names = try extractParamNames(allocator,
        "SELECT * FROM users WHERE name = $1 AND bio LIKE '$2 is not a param'",
        1,
    );
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("name", names[0]);
}

test "extractParamNames zero params" {
    const allocator = std.testing.allocator;

    const names = try extractParamNames(allocator, "SELECT * FROM users", 0);
    defer allocator.free(names);
    try std.testing.expectEqual(@as(usize, 0), names.len);
}

test "fuzz replaceNamedParams" {
    try std.testing.fuzz({}, struct {
        fn run(_: void, smith: *std.testing.Smith) anyerror!void {
            const allocator = std.testing.allocator;
            var buf: [256]u8 = undefined;
            const len = smith.sliceWeightedBytes(&buf, &.{
                .rangeAtMost(u8, 0x20, 0x7e, 1),
                .value(u8, '@', 2),
                .rangeAtMost(u8, 'a', 'z', 3),
                .value(u8, '_', 1),
            });
            const input = buf[0..len];
            const result = try replaceNamedParams(allocator, input);
            if (result) |r| {
                defer {
                    allocator.free(r.sql);
                    for (r.names) |n| allocator.free(n);
                    allocator.free(r.names);
                }
                var idx: usize = 0;
                while (idx < r.sql.len) {
                    if (r.sql[idx] == '\'') {
                        idx += 1;
                        while (idx < r.sql.len and r.sql[idx] != '\'') : (idx += 1) {}
                        if (idx < r.sql.len) idx += 1;
                        continue;
                    }
                    if (r.sql[idx] == '-' and idx + 1 < r.sql.len and r.sql[idx + 1] == '-') {
                        while (idx < r.sql.len and r.sql[idx] != '\n') : (idx += 1) {}
                        continue;
                    }
                    if (r.sql[idx] == '@' and idx + 1 < r.sql.len and
                        (std.ascii.isAlphabetic(r.sql[idx + 1]) or r.sql[idx + 1] == '_'))
                    {
                        return error.UnreplacedNamedParam;
                    }
                    idx += 1;
                }
                for (r.names) |name| {
                    if (name.len == 0) return error.EmptyParamName;
                }
            }
        }
    }.run, .{});
}

test "fuzz extractParamNames" {
    try std.testing.fuzz({}, struct {
        fn run(_: void, smith: *std.testing.Smith) anyerror!void {
            const allocator = std.testing.allocator;
            var buf: [256]u8 = undefined;
            const len = smith.sliceWeightedBytes(&buf, &.{
                .rangeAtMost(u8, 0x20, 0x7e, 1),
                .value(u8, '$', 2),
                .rangeAtMost(u8, '0', '9', 2),
                .rangeAtMost(u8, 'a', 'z', 3),
            });
            const input = buf[0..len];
            var max_param: usize = 0;
            var i: usize = 0;
            while (i < input.len) : (i += 1) {
                if (input[i] == '$' and i + 1 < input.len and std.ascii.isDigit(input[i + 1])) {
                    var end = i + 1;
                    while (end < input.len and std.ascii.isDigit(input[end])) : (end += 1) {}
                    const num = std.fmt.parseInt(usize, input[i + 1 .. end], 10) catch continue;
                    if (num > max_param) max_param = num;
                }
            }
            if (max_param > 64) return;
            const names = try extractParamNames(allocator, input, max_param);
            defer {
                for (names) |n| allocator.free(n);
                allocator.free(names);
            }
            if (names.len != max_param) return error.WrongNameCount;
            for (names) |name| {
                if (name.len == 0) return error.EmptyName;
            }
        }
    }.run, .{});
}
