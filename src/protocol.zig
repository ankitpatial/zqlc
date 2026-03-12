const std = @import("std");

/// A field in a RowDescription message.
pub const RowField = struct {
    name: []const u8,
    table_oid: u32,
    column_attr: i16,
    type_oid: u32,
    type_len: i16,
    type_mod: i32,
    format_code: i16,
};

/// Error/Notice field from the backend.
pub const ErrorField = struct {
    code: u8,
    value: []const u8,
};

// ─── Helpers ────────────────────────────────────────────────────────────────

fn appendI32(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, value: i32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(i32, &bytes, value, .big);
    try buf.appendSlice(allocator, &bytes);
}

fn appendU32(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, value: u32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .big);
    try buf.appendSlice(allocator, &bytes);
}

fn appendI16(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, value: i16) !void {
    var bytes: [2]u8 = undefined;
    std.mem.writeInt(i16, &bytes, value, .big);
    try buf.appendSlice(allocator, &bytes);
}

// ─── Frontend Messages ──────────────────────────────────────────────────────

/// Encode a StartupMessage (no message type byte).
pub fn encodeStartup(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), user: []const u8, database: []const u8) !void {
    buf.clearRetainingCapacity();

    try appendI32(buf, allocator, 0); // placeholder for length
    try appendI32(buf, allocator, 196608); // Protocol version 3.0
    try buf.appendSlice(allocator, "user\x00");
    try buf.appendSlice(allocator, user);
    try buf.append(allocator, 0);
    try buf.appendSlice(allocator, "database\x00");
    try buf.appendSlice(allocator, database);
    try buf.append(allocator, 0);
    try buf.append(allocator, 0); // terminator

    std.mem.writeInt(i32, buf.items[0..4], @intCast(buf.items.len), .big);
}

/// Encode a PasswordMessage ('p').
pub fn encodePassword(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), password: []const u8) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'p');
    const len: i32 = @intCast(4 + password.len + 1);
    try appendI32(buf, allocator, len);
    try buf.appendSlice(allocator, password);
    try buf.append(allocator, 0);
}

/// Encode a SASLInitialResponse message ('p').
pub fn encodeSaslInitial(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), mechanism: []const u8, client_first: []const u8) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'p');
    const len: i32 = @intCast(4 + mechanism.len + 1 + 4 + client_first.len);
    try appendI32(buf, allocator, len);
    try buf.appendSlice(allocator, mechanism);
    try buf.append(allocator, 0);
    try appendI32(buf, allocator, @intCast(client_first.len));
    try buf.appendSlice(allocator, client_first);
}

/// Encode a SASLResponse message ('p').
pub fn encodeSaslResponse(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), data: []const u8) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'p');
    const len: i32 = @intCast(4 + data.len);
    try appendI32(buf, allocator, len);
    try buf.appendSlice(allocator, data);
}

/// Encode a Parse message.
pub fn encodeParse(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), stmt_name: []const u8, query_sql: []const u8) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'P');
    const len: i32 = @intCast(4 + stmt_name.len + 1 + query_sql.len + 1 + 2);
    try appendI32(buf, allocator, len);
    try buf.appendSlice(allocator, stmt_name);
    try buf.append(allocator, 0);
    try buf.appendSlice(allocator, query_sql);
    try buf.append(allocator, 0);
    try appendI16(buf, allocator, 0); // let server decide param types
}

/// Encode a Describe message ('D').
pub fn encodeDescribe(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), target: u8, name: []const u8) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'D');
    const len: i32 = @intCast(4 + 1 + name.len + 1);
    try appendI32(buf, allocator, len);
    try buf.append(allocator, target); // 'S' for statement, 'P' for portal
    try buf.appendSlice(allocator, name);
    try buf.append(allocator, 0);
}

/// Encode a Sync message ('S').
pub fn encodeSync(allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'S');
    try appendI32(buf, allocator, 4);
}

/// Encode a Terminate message ('X').
pub fn encodeTerminate(allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'X');
    try appendI32(buf, allocator, 4);
}

/// Encode a simple Query message ('Q').
pub fn encodeQuery(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), sql: []const u8) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'Q');
    const len: i32 = @intCast(4 + sql.len + 1);
    try appendI32(buf, allocator, len);
    try buf.appendSlice(allocator, sql);
    try buf.append(allocator, 0);
}

/// Encode a Close statement message ('C').
pub fn encodeClose(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), target: u8, name: []const u8) !void {
    buf.clearRetainingCapacity();

    try buf.append(allocator, 'C');
    const len: i32 = @intCast(4 + 1 + name.len + 1);
    try appendI32(buf, allocator, len);
    try buf.append(allocator, target);
    try buf.appendSlice(allocator, name);
    try buf.append(allocator, 0);
}

// ─── Backend Messages ───────────────────────────────────────────────────────

/// Parsed backend message.
pub const BackendMsg = union(enum) {
    auth_ok,
    auth_cleartext,
    auth_md5: [4]u8,
    auth_sasl: []const u8,
    auth_sasl_continue: []const u8,
    auth_sasl_final: []const u8,
    parameter_status: struct { name: []const u8, value: []const u8 },
    backend_key_data: struct { pid: u32, secret: u32 },
    ready_for_query: u8,
    parse_complete,
    bind_complete,
    close_complete,
    no_data,
    parameter_description: []const u32,
    row_description: []const RowField,
    data_row: []const ?[]const u8,
    command_complete: []const u8,
    error_response: struct { fields: []const ErrorField },
    notice_response: struct { fields: []const ErrorField },
    empty_query_response,
};

/// Read a single backend message from a buffer. Returns the message and the
/// number of bytes consumed. The returned message borrows from `data`.
pub fn readBackendMsg(data: []const u8, allocator: std.mem.Allocator) !struct { msg: BackendMsg, consumed: usize } {
    if (data.len < 5) return error.NeedMoreData;

    const msg_type = data[0];
    const msg_len = std.mem.readInt(i32, data[1..5], .big);
    if (msg_len < 4) return error.ProtocolError;
    const total_len: usize = @intCast(1 + msg_len);
    if (data.len < total_len) return error.NeedMoreData;

    const payload = data[5..total_len];

    const msg: BackendMsg = switch (msg_type) {
        'R' => try parseAuth(payload),
        'S' => try parseParameterStatus(payload, allocator),
        'K' => try parseBackendKeyData(payload),
        'Z' => .{ .ready_for_query = if (payload.len > 0) payload[0] else 'I' },
        '1' => .parse_complete,
        '2' => .bind_complete,
        '3' => .close_complete,
        'n' => .no_data,
        't' => try parseParameterDescription(payload, allocator),
        'T' => try parseRowDescription(payload, allocator),
        'D' => try parseDataRow(payload, allocator),
        'C' => .{ .command_complete = try allocator.dupe(u8, cstring(payload)) },
        'E' => try parseErrorResponse(payload, allocator, false),
        'N' => try parseErrorResponse(payload, allocator, true),
        'I' => .empty_query_response,
        else => return error.UnknownMessageType,
    };

    return .{ .msg = msg, .consumed = total_len };
}

fn parseAuth(payload: []const u8) !BackendMsg {
    if (payload.len < 4) return error.ProtocolError;
    const auth_type = std.mem.readInt(i32, payload[0..4], .big);
    return switch (auth_type) {
        0 => .auth_ok,
        3 => .auth_cleartext,
        5 => blk: {
            if (payload.len < 8) return error.ProtocolError;
            break :blk .{ .auth_md5 = payload[4..8].* };
        },
        10 => .{ .auth_sasl = payload[4..] },
        11 => .{ .auth_sasl_continue = payload[4..] },
        12 => .{ .auth_sasl_final = payload[4..] },
        else => error.UnsupportedAuthMethod,
    };
}

fn parseParameterStatus(payload: []const u8, allocator: std.mem.Allocator) !BackendMsg {
    const name = cstring(payload);
    const rest = payload[name.len + 1 ..];
    const value = cstring(rest);
    return .{ .parameter_status = .{ .name = try allocator.dupe(u8, name), .value = try allocator.dupe(u8, value) } };
}

fn parseBackendKeyData(payload: []const u8) !BackendMsg {
    if (payload.len < 8) return error.ProtocolError;
    return .{ .backend_key_data = .{
        .pid = std.mem.readInt(u32, payload[0..4], .big),
        .secret = std.mem.readInt(u32, payload[4..8], .big),
    } };
}

fn parseParameterDescription(payload: []const u8, allocator: std.mem.Allocator) !BackendMsg {
    if (payload.len < 2) return error.ProtocolError;
    const count = std.mem.readInt(i16, payload[0..2], .big);
    if (count < 0) return error.ProtocolError;
    const n: usize = @intCast(count);

    const oids = try allocator.alloc(u32, n);
    var offset: usize = 2;
    for (0..n) |i| {
        oids[i] = std.mem.readInt(u32, payload[offset..][0..4], .big);
        offset += 4;
    }
    return .{ .parameter_description = oids };
}

fn parseRowDescription(payload: []const u8, allocator: std.mem.Allocator) !BackendMsg {
    if (payload.len < 2) return error.ProtocolError;
    const count = std.mem.readInt(i16, payload[0..2], .big);
    if (count < 0) return error.ProtocolError;
    const n: usize = @intCast(count);

    const fields = try allocator.alloc(RowField, n);
    var offset: usize = 2;
    for (0..n) |i| {
        const name = cstring(payload[offset..]);
        offset += name.len + 1;
        // Copy name — payload points into the recv buffer which gets shifted.
        const owned_name = try allocator.dupe(u8, name);
        fields[i] = .{
            .name = owned_name,
            .table_oid = std.mem.readInt(u32, payload[offset..][0..4], .big),
            .column_attr = std.mem.readInt(i16, payload[offset + 4 ..][0..2], .big),
            .type_oid = std.mem.readInt(u32, payload[offset + 6 ..][0..4], .big),
            .type_len = std.mem.readInt(i16, payload[offset + 10 ..][0..2], .big),
            .type_mod = std.mem.readInt(i32, payload[offset + 12 ..][0..4], .big),
            .format_code = std.mem.readInt(i16, payload[offset + 16 ..][0..2], .big),
        };
        offset += 18;
    }
    return .{ .row_description = fields };
}

fn parseDataRow(payload: []const u8, allocator: std.mem.Allocator) !BackendMsg {
    if (payload.len < 2) return error.ProtocolError;
    const count = std.mem.readInt(i16, payload[0..2], .big);
    if (count < 0) return error.ProtocolError;
    const n: usize = @intCast(count);

    const values = try allocator.alloc(?[]const u8, n);
    var offset: usize = 2;
    for (0..n) |i| {
        const col_len = std.mem.readInt(i32, payload[offset..][0..4], .big);
        offset += 4;
        if (col_len == -1) {
            values[i] = null;
        } else {
            const len: usize = @intCast(col_len);
            values[i] = try allocator.dupe(u8, payload[offset .. offset + len]);
            offset += len;
        }
    }
    return .{ .data_row = values };
}

fn parseErrorResponse(payload: []const u8, allocator: std.mem.Allocator, is_notice: bool) !BackendMsg {
    var fields_list: std.ArrayList(ErrorField) = .empty;
    var offset: usize = 0;
    while (offset < payload.len) {
        const code = payload[offset];
        offset += 1;
        if (code == 0) break;
        const value = cstring(payload[offset..]);
        offset += value.len + 1;
        try fields_list.append(allocator, .{ .code = code, .value = try allocator.dupe(u8, value) });
    }
    const fields = try fields_list.toOwnedSlice(allocator);
    if (is_notice) {
        return .{ .notice_response = .{ .fields = fields } };
    }
    return .{ .error_response = .{ .fields = fields } };
}

/// Extract a null-terminated string from a byte slice.
fn cstring(data: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, data, 0)) |end| {
        return data[0..end];
    }
    return data;
}

// ─── Tests ──────────────────────────────────────────────────────────────────

fn buildBackendMsg(allocator: std.mem.Allocator, msg_type: u8, payload: []const u8) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    try buf.append(allocator, msg_type);
    const len: i32 = @intCast(4 + payload.len);
    try appendI32(&buf, allocator, len);
    try buf.appendSlice(allocator, payload);
    return buf.toOwnedSlice(allocator);
}

test "encode startup message" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodeStartup(allocator, &buf, "testuser", "testdb");
    const proto = std.mem.readInt(i32, buf.items[4..8], .big);
    try std.testing.expectEqual(@as(i32, 196608), proto);
    const len = std.mem.readInt(i32, buf.items[0..4], .big);
    try std.testing.expectEqual(@as(i32, @intCast(buf.items.len)), len);
}

test "encode sync message" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodeSync(allocator, &buf);
    try std.testing.expectEqual(@as(usize, 5), buf.items.len);
    try std.testing.expectEqual(@as(u8, 'S'), buf.items[0]);
}

test "encode parse message" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodeParse(allocator, &buf, "", "SELECT 1");
    try std.testing.expectEqual(@as(u8, 'P'), buf.items[0]);
    try std.testing.expect(buf.items.len > 5);
}

test "encode query message" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodeQuery(allocator, &buf, "SELECT 1");
    try std.testing.expectEqual(@as(u8, 'Q'), buf.items[0]);
    const len = std.mem.readInt(i32, buf.items[1..5], .big);
    try std.testing.expectEqual(@as(i32, @intCast(buf.items.len - 1)), len);
    // Should end with null terminator
    try std.testing.expectEqual(@as(u8, 0), buf.items[buf.items.len - 1]);
}

test "encode terminate message" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodeTerminate(allocator, &buf);
    try std.testing.expectEqual(@as(usize, 5), buf.items.len);
    try std.testing.expectEqual(@as(u8, 'X'), buf.items[0]);
    const len = std.mem.readInt(i32, buf.items[1..5], .big);
    try std.testing.expectEqual(@as(i32, 4), len);
}

test "encode describe message" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodeDescribe(allocator, &buf, 'S', "");
    try std.testing.expectEqual(@as(u8, 'D'), buf.items[0]);
    // Target byte should be 'S' for statement
    try std.testing.expectEqual(@as(u8, 'S'), buf.items[5]);
}

test "encode password message" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodePassword(allocator, &buf, "secret");
    try std.testing.expectEqual(@as(u8, 'p'), buf.items[0]);
    const len = std.mem.readInt(i32, buf.items[1..5], .big);
    try std.testing.expectEqual(@as(i32, @intCast(4 + 6 + 1)), len);
}

test "encode close message" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodeClose(allocator, &buf, 'S', "stmt");
    try std.testing.expectEqual(@as(u8, 'C'), buf.items[0]);
    try std.testing.expectEqual(@as(u8, 'S'), buf.items[5]);
}

// ─── Backend message decoding tests ──────────────────────────────────────

test "decode auth_ok" {
    const allocator = std.testing.allocator;
    var payload: [4]u8 = undefined;
    std.mem.writeInt(i32, &payload, 0, .big); // auth type 0 = OK
    const data = try buildBackendMsg(allocator, 'R', &payload);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    try std.testing.expectEqual(BackendMsg.auth_ok, result.msg);
    try std.testing.expectEqual(data.len, result.consumed);
}

test "decode auth_cleartext" {
    const allocator = std.testing.allocator;
    var payload: [4]u8 = undefined;
    std.mem.writeInt(i32, &payload, 3, .big); // auth type 3 = cleartext
    const data = try buildBackendMsg(allocator, 'R', &payload);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    try std.testing.expectEqual(BackendMsg.auth_cleartext, result.msg);
}

test "decode auth_md5" {
    const allocator = std.testing.allocator;
    var payload: [8]u8 = undefined;
    std.mem.writeInt(i32, payload[0..4], 5, .big); // auth type 5 = MD5
    payload[4] = 0xAA;
    payload[5] = 0xBB;
    payload[6] = 0xCC;
    payload[7] = 0xDD;
    const data = try buildBackendMsg(allocator, 'R', &payload);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .auth_md5 => |salt| {
            try std.testing.expectEqual(@as(u8, 0xAA), salt[0]);
            try std.testing.expectEqual(@as(u8, 0xDD), salt[3]);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode ready_for_query" {
    const allocator = std.testing.allocator;
    const data = try buildBackendMsg(allocator, 'Z', &.{'I'});
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .ready_for_query => |status| try std.testing.expectEqual(@as(u8, 'I'), status),
        else => return error.UnexpectedMessage,
    }
}

test "decode parse_complete" {
    const allocator = std.testing.allocator;
    const data = try buildBackendMsg(allocator, '1', &.{});
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    try std.testing.expectEqual(BackendMsg.parse_complete, result.msg);
}

test "decode no_data" {
    const allocator = std.testing.allocator;
    const data = try buildBackendMsg(allocator, 'n', &.{});
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    try std.testing.expectEqual(BackendMsg.no_data, result.msg);
}

test "decode parameter_description" {
    const allocator = std.testing.allocator;
    // 2 params: OID 23 (int4) and OID 25 (text)
    var payload: [10]u8 = undefined;
    std.mem.writeInt(i16, payload[0..2], 2, .big);
    std.mem.writeInt(u32, payload[2..6], 23, .big);
    std.mem.writeInt(u32, payload[6..10], 25, .big);
    const data = try buildBackendMsg(allocator, 't', &payload);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .parameter_description => |oids| {
            defer allocator.free(oids);
            try std.testing.expectEqual(@as(usize, 2), oids.len);
            try std.testing.expectEqual(@as(u32, 23), oids[0]);
            try std.testing.expectEqual(@as(u32, 25), oids[1]);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode row_description" {
    const allocator = std.testing.allocator;
    // Build a RowDescription with 1 field: "id"
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    try appendI16(&buf, allocator, 1); // 1 field
    try buf.appendSlice(allocator, "id"); // field name
    try buf.append(allocator, 0); // null terminator
    try appendU32(&buf, allocator, 16384); // table OID
    try appendI16(&buf, allocator, 1); // column attr
    try appendU32(&buf, allocator, 23); // type OID (int4)
    try appendI16(&buf, allocator, 4); // type length
    try appendI32(&buf, allocator, -1); // type mod
    try appendI16(&buf, allocator, 0); // format code

    const data = try buildBackendMsg(allocator, 'T', buf.items);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .row_description => |fields| {
            defer {
                for (fields) |f| allocator.free(f.name);
                allocator.free(fields);
            }
            try std.testing.expectEqual(@as(usize, 1), fields.len);
            try std.testing.expectEqualStrings("id", fields[0].name);
            try std.testing.expectEqual(@as(u32, 23), fields[0].type_oid);
            try std.testing.expectEqual(@as(u32, 16384), fields[0].table_oid);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode data_row" {
    const allocator = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    try appendI16(&buf, allocator, 3); // 3 columns
    // Column 1: "hello"
    try appendI32(&buf, allocator, 5);
    try buf.appendSlice(allocator, "hello");
    // Column 2: NULL
    try appendI32(&buf, allocator, -1);
    // Column 3: "42"
    try appendI32(&buf, allocator, 2);
    try buf.appendSlice(allocator, "42");

    const data = try buildBackendMsg(allocator, 'D', buf.items);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .data_row => |values| {
            defer {
                for (values) |v| if (v) |s| allocator.free(s);
                allocator.free(values);
            }
            try std.testing.expectEqual(@as(usize, 3), values.len);
            try std.testing.expectEqualStrings("hello", values[0].?);
            try std.testing.expect(values[1] == null);
            try std.testing.expectEqualStrings("42", values[2].?);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode command_complete" {
    const allocator = std.testing.allocator;
    const data = try buildBackendMsg(allocator, 'C', "DELETE 3\x00");
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .command_complete => |tag| {
            defer allocator.free(tag);
            try std.testing.expectEqualStrings("DELETE 3", tag);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode error_response" {
    const allocator = std.testing.allocator;
    // Build: severity='S' + "ERROR\0" + message='M' + "test error\0" + terminator
    const payload = "SERROR\x00Mtest error\x00\x00";
    const data = try buildBackendMsg(allocator, 'E', payload);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .error_response => |e| {
            defer {
                for (e.fields) |f| allocator.free(f.value);
                allocator.free(e.fields);
            }
            try std.testing.expectEqual(@as(usize, 2), e.fields.len);
            try std.testing.expectEqual(@as(u8, 'S'), e.fields[0].code);
            try std.testing.expectEqualStrings("ERROR", e.fields[0].value);
            try std.testing.expectEqual(@as(u8, 'M'), e.fields[1].code);
            try std.testing.expectEqualStrings("test error", e.fields[1].value);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode empty_query_response" {
    const allocator = std.testing.allocator;
    const data = try buildBackendMsg(allocator, 'I', &.{});
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    try std.testing.expectEqual(BackendMsg.empty_query_response, result.msg);
}

test "decode unknown message type" {
    const allocator = std.testing.allocator;
    const data = try buildBackendMsg(allocator, 0xFF, &.{});
    defer allocator.free(data);

    const result = readBackendMsg(data, allocator);
    try std.testing.expectError(error.UnknownMessageType, result);
}

test "decode NeedMoreData for incomplete message" {
    const allocator = std.testing.allocator;

    // Less than 5 bytes
    try std.testing.expectError(error.NeedMoreData, readBackendMsg(&.{ 'Z', 0, 0 }, allocator));
    // Header says 10 bytes but only 7 provided
    try std.testing.expectError(error.NeedMoreData, readBackendMsg(&.{ 'Z', 0, 0, 0, 10, 'I', 0 }, allocator));
}

test "decode parameter_description zero params" {
    const allocator = std.testing.allocator;
    var payload: [2]u8 = undefined;
    std.mem.writeInt(i16, &payload, 0, .big);
    const data = try buildBackendMsg(allocator, 't', &payload);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .parameter_description => |oids| {
            defer allocator.free(oids);
            try std.testing.expectEqual(@as(usize, 0), oids.len);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode backend_key_data" {
    const allocator = std.testing.allocator;
    var payload: [8]u8 = undefined;
    std.mem.writeInt(u32, payload[0..4], 12345, .big);
    std.mem.writeInt(u32, payload[4..8], 67890, .big);
    const data = try buildBackendMsg(allocator, 'K', &payload);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .backend_key_data => |bkd| {
            try std.testing.expectEqual(@as(u32, 12345), bkd.pid);
            try std.testing.expectEqual(@as(u32, 67890), bkd.secret);
        },
        else => return error.UnexpectedMessage,
    }
}

test "decode parameter_status" {
    const allocator = std.testing.allocator;
    const payload = "server_version\x0016.2\x00";
    const data = try buildBackendMsg(allocator, 'S', payload);
    defer allocator.free(data);

    const result = try readBackendMsg(data, allocator);
    switch (result.msg) {
        .parameter_status => |ps| {
            defer {
                allocator.free(ps.name);
                allocator.free(ps.value);
            }
            try std.testing.expectEqualStrings("server_version", ps.name);
            try std.testing.expectEqualStrings("16.2", ps.value);
        },
        else => return error.UnexpectedMessage,
    }
}

test "roundtrip encode-decode parse" {
    const allocator = std.testing.allocator;

    // Encode a Parse message, then verify the structure
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try encodeParse(allocator, &buf, "stmt1", "SELECT $1::int");

    // Verify message structure: 'P' + length + stmt_name\0 + query\0 + param_count(i16)
    try std.testing.expectEqual(@as(u8, 'P'), buf.items[0]);
    const len = std.mem.readInt(i32, buf.items[1..5], .big);
    try std.testing.expectEqual(@as(usize, @intCast(1 + len)), buf.items.len);

    // stmt_name starts at byte 5
    const stmt_end = std.mem.indexOfScalarPos(u8, buf.items, 5, 0).?;
    try std.testing.expectEqualStrings("stmt1", buf.items[5..stmt_end]);
}
