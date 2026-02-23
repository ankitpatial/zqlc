const std = @import("std");
const protocol = @import("protocol.zig");
const scram = @import("scram.zig");

/// Perform authentication handshake after receiving the initial auth request.
pub fn authenticate(
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    msg: protocol.BackendMsg,
    user: []const u8,
    password: []const u8,
) !void {
    switch (msg) {
        .auth_ok => return,
        .auth_cleartext => try handleCleartext(stream, password),
        .auth_md5 => |salt| try handleMd5(stream, user, password, salt),
        .auth_sasl => try handleScram(allocator, stream, user, password),
        else => return error.UnsupportedAuthMethod,
    }
}

fn handleCleartext(stream: std.net.Stream, password: []const u8) !void {
    var buf: [1024]u8 = undefined;
    const n = try protocol.encodePassword(&buf, password);
    try stream.writeAll(buf[0..n]);
}

fn handleMd5(stream: std.net.Stream, user: []const u8, password: []const u8, salt: [4]u8) !void {
    const Md5 = std.crypto.hash.Md5;
    const hex_chars = "0123456789abcdef";

    // md5(md5(password + user) + salt)
    var inner_hash: [16]u8 = undefined;
    var h = Md5.init(.{});
    h.update(password);
    h.update(user);
    h.final(&inner_hash);

    var inner_hex: [32]u8 = undefined;
    for (inner_hash, 0..) |byte, i| {
        inner_hex[i * 2] = hex_chars[byte >> 4];
        inner_hex[i * 2 + 1] = hex_chars[byte & 0x0f];
    }

    var outer_hash: [16]u8 = undefined;
    var h2 = Md5.init(.{});
    h2.update(&inner_hex);
    h2.update(&salt);
    h2.final(&outer_hash);

    var outer_hex: [32]u8 = undefined;
    for (outer_hash, 0..) |byte, i| {
        outer_hex[i * 2] = hex_chars[byte >> 4];
        outer_hex[i * 2 + 1] = hex_chars[byte & 0x0f];
    }

    // Send "md5" + hex
    var pw_buf: [35]u8 = undefined;
    @memcpy(pw_buf[0..3], "md5");
    @memcpy(pw_buf[3..35], &outer_hex);

    var buf: [1024]u8 = undefined;
    const n = try protocol.encodePassword(&buf, &pw_buf);
    try stream.writeAll(buf[0..n]);
}

fn handleScram(allocator: std.mem.Allocator, stream: std.net.Stream, user: []const u8, password: []const u8) !void {
    var buf: [4096]u8 = undefined;

    // Step 1: Send SASLInitialResponse with client-first-message
    const first = try scram.clientFirst(allocator, user);
    var state = first.state;
    defer state.deinit();
    defer allocator.free(first.message);

    const n1 = try protocol.encodeSaslInitial(&buf, "SCRAM-SHA-256", first.message);
    try stream.writeAll(buf[0..n1]);

    // Step 2: Receive server-first-message
    var recv_buf: [4096]u8 = undefined;
    const server_first_data = try recvAuthMsg(stream, &recv_buf);

    switch (server_first_data) {
        .auth_sasl_continue => |data| {
            // Step 3: Send client-final-message
            const client_final = try scram.clientFinal(allocator, &state, password, data);
            defer allocator.free(client_final);

            const n2 = try protocol.encodeSaslResponse(&buf, client_final);
            try stream.writeAll(buf[0..n2]);

            // Step 4: Receive server-final-message
            const server_final_data = try recvAuthMsg(stream, &recv_buf);
            switch (server_final_data) {
                .auth_sasl_final => |fdata| {
                    try scram.verifyServerFinal(&state, fdata);
                },
                .error_response => return error.AuthenticationFailed,
                else => return error.UnexpectedMessage,
            }

            // Step 5: Receive AuthenticationOk
            const ok_msg = try recvAuthMsg(stream, &recv_buf);
            switch (ok_msg) {
                .auth_ok => return,
                .error_response => return error.AuthenticationFailed,
                else => return error.UnexpectedMessage,
            }
        },
        .error_response => return error.AuthenticationFailed,
        else => return error.UnexpectedMessage,
    }
}

fn recvAuthMsg(stream: std.net.Stream, buf: []u8) !protocol.BackendMsg {
    const n = try stream.read(buf);
    if (n == 0) return error.ConnectionClosed;

    var fba = std.heap.FixedBufferAllocator.init(buf[n..]);
    const result = try protocol.readBackendMsg(buf[0..n], fba.allocator());
    return result.msg;
}

test "md5 auth hash computation" {
    const Md5 = std.crypto.hash.Md5;
    const hex_chars = "0123456789abcdef";
    var result: [16]u8 = undefined;
    var h = Md5.init(.{});
    h.update("testpass");
    h.update("testuser");
    h.final(&result);

    var hex: [32]u8 = undefined;
    for (result, 0..) |byte, i| {
        hex[i * 2] = hex_chars[byte >> 4];
        hex[i * 2 + 1] = hex_chars[byte & 0x0f];
    }
    try std.testing.expectEqual(@as(usize, 32), hex.len);
}
