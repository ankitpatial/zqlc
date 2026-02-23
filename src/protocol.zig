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

// ─── Frontend Messages ──────────────────────────────────────────────────────

/// Encode a StartupMessage (no message type byte).
pub fn encodeStartup(buf: []u8, user: []const u8, database: []const u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeInt(i32, 0, .big);
    try w.writeInt(i32, 196608, .big); // Protocol version 3.0
    try w.writeAll("user\x00");
    try w.writeAll(user);
    try w.writeByte(0);
    try w.writeAll("database\x00");
    try w.writeAll(database);
    try w.writeByte(0);
    try w.writeByte(0); // terminator

    const pos = fbs.pos;
    std.mem.writeInt(i32, buf[0..4], @intCast(pos), .big);
    return pos;
}

/// Encode a PasswordMessage ('p').
pub fn encodePassword(buf: []u8, password: []const u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('p');
    const len: i32 = @intCast(4 + password.len + 1);
    try w.writeInt(i32, len, .big);
    try w.writeAll(password);
    try w.writeByte(0);
    return fbs.pos;
}

/// Encode a SASLInitialResponse message ('p').
pub fn encodeSaslInitial(buf: []u8, mechanism: []const u8, client_first: []const u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('p');
    const len: i32 = @intCast(4 + mechanism.len + 1 + 4 + client_first.len);
    try w.writeInt(i32, len, .big);
    try w.writeAll(mechanism);
    try w.writeByte(0);
    try w.writeInt(i32, @intCast(client_first.len), .big);
    try w.writeAll(client_first);
    return fbs.pos;
}

/// Encode a SASLResponse message ('p').
pub fn encodeSaslResponse(buf: []u8, data: []const u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('p');
    const len: i32 = @intCast(4 + data.len);
    try w.writeInt(i32, len, .big);
    try w.writeAll(data);
    return fbs.pos;
}

/// Encode a Parse message.
pub fn encodeParse(buf: []u8, stmt_name: []const u8, query_sql: []const u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('P');
    const len: i32 = @intCast(4 + stmt_name.len + 1 + query_sql.len + 1 + 2);
    try w.writeInt(i32, len, .big);
    try w.writeAll(stmt_name);
    try w.writeByte(0);
    try w.writeAll(query_sql);
    try w.writeByte(0);
    try w.writeInt(i16, 0, .big); // let server decide param types
    return fbs.pos;
}

/// Encode a Describe message ('D').
pub fn encodeDescribe(buf: []u8, target: u8, name: []const u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('D');
    const len: i32 = @intCast(4 + 1 + name.len + 1);
    try w.writeInt(i32, len, .big);
    try w.writeByte(target); // 'S' for statement, 'P' for portal
    try w.writeAll(name);
    try w.writeByte(0);
    return fbs.pos;
}

/// Encode a Sync message ('S').
pub fn encodeSync(buf: []u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('S');
    try w.writeInt(i32, 4, .big);
    return fbs.pos;
}

/// Encode a Terminate message ('X').
pub fn encodeTerminate(buf: []u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('X');
    try w.writeInt(i32, 4, .big);
    return fbs.pos;
}

/// Encode a simple Query message ('Q').
pub fn encodeQuery(buf: []u8, sql: []const u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('Q');
    const len: i32 = @intCast(4 + sql.len + 1);
    try w.writeInt(i32, len, .big);
    try w.writeAll(sql);
    try w.writeByte(0);
    return fbs.pos;
}

/// Encode a Close statement message ('C').
pub fn encodeClose(buf: []u8, target: u8, name: []const u8) !usize {
    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeByte('C');
    const len: i32 = @intCast(4 + 1 + name.len + 1);
    try w.writeInt(i32, len, .big);
    try w.writeByte(target);
    try w.writeAll(name);
    try w.writeByte(0);
    return fbs.pos;
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
        'K' => parseBackendKeyData(payload),
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

fn parseBackendKeyData(payload: []const u8) BackendMsg {
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

test "encode startup message" {
    var buf: [256]u8 = undefined;
    const n = try encodeStartup(&buf, "testuser", "testdb");
    const proto = std.mem.readInt(i32, buf[4..8], .big);
    try std.testing.expectEqual(@as(i32, 196608), proto);
    const len = std.mem.readInt(i32, buf[0..4], .big);
    try std.testing.expectEqual(@as(i32, @intCast(n)), len);
}

test "encode sync message" {
    var buf: [8]u8 = undefined;
    const n = try encodeSync(&buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqual(@as(u8, 'S'), buf[0]);
}

test "encode parse message" {
    var buf: [256]u8 = undefined;
    const n = try encodeParse(&buf, "", "SELECT 1");
    try std.testing.expectEqual(@as(u8, 'P'), buf[0]);
    try std.testing.expect(n > 5);
}
