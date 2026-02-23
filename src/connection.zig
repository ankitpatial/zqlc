const std = @import("std");
const protocol = @import("protocol.zig");
const auth = @import("auth.zig");

pub const Connection = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    recv_buf: [16384]u8 = undefined,
    recv_len: usize = 0,
    send_buf: [16384]u8 = undefined,

    /// Connect to a PostgreSQL server, authenticate, and wait for ReadyForQuery.
    pub fn connect(
        allocator: std.mem.Allocator,
        host: []const u8,
        port: u16,
        user: []const u8,
        password: []const u8,
        database: []const u8,
    ) !Connection {
        const stream = std.net.tcpConnectToHost(allocator, host, port) catch {
            return error.ConnectionRefused;
        };
        errdefer stream.close();

        var self = Connection{
            .allocator = allocator,
            .stream = stream,
        };

        // Send StartupMessage
        const n = try protocol.encodeStartup(&self.send_buf, user, database);
        try stream.writeAll(self.send_buf[0..n]);

        // Read messages until ReadyForQuery
        while (true) {
            const msg = try self.recvMsg();
            switch (msg) {
                .auth_ok => {},
                .auth_cleartext, .auth_md5, .auth_sasl => {
                    try auth.authenticate(allocator, stream, msg, user, password);
                },
                .parameter_status => {},
                .backend_key_data => {},
                .ready_for_query => return self,
                .error_response => return error.AuthenticationFailed,
                .notice_response => {},
                else => {},
            }
        }
    }

    /// Send a Parse message.
    pub fn sendParse(self: *Connection, stmt_name: []const u8, sql: []const u8) !void {
        const n = try protocol.encodeParse(&self.send_buf, stmt_name, sql);
        try self.stream.writeAll(self.send_buf[0..n]);
    }

    /// Send a Describe message for a statement.
    pub fn sendDescribeStatement(self: *Connection, stmt_name: []const u8) !void {
        const n = try protocol.encodeDescribe(&self.send_buf, 'S', stmt_name);
        try self.stream.writeAll(self.send_buf[0..n]);
    }

    /// Send a Sync message.
    pub fn sendSync(self: *Connection) !void {
        const n = try protocol.encodeSync(&self.send_buf);
        try self.stream.writeAll(self.send_buf[0..n]);
    }

    /// Send a simple Query message.
    pub fn sendQuery(self: *Connection, sql: []const u8) !void {
        const n = try protocol.encodeQuery(&self.send_buf, sql);
        try self.stream.writeAll(self.send_buf[0..n]);
    }

    /// Send a Close statement message.
    pub fn sendCloseStatement(self: *Connection, stmt_name: []const u8) !void {
        const n = try protocol.encodeClose(&self.send_buf, 'S', stmt_name);
        try self.stream.writeAll(self.send_buf[0..n]);
    }

    /// Receive the next backend message.
    pub fn recvMsg(self: *Connection) !protocol.BackendMsg {
        while (true) {
            if (self.recv_len >= 5) {
                const result = protocol.readBackendMsg(
                    self.recv_buf[0..self.recv_len],
                    self.allocator,
                ) catch |err| switch (err) {
                    error.NeedMoreData => {
                        const bytes_read = try self.stream.read(self.recv_buf[self.recv_len..]);
                        if (bytes_read == 0) return error.ConnectionClosed;
                        self.recv_len += bytes_read;
                        continue;
                    },
                    else => return err,
                };

                const remaining = self.recv_len - result.consumed;
                if (remaining > 0) {
                    std.mem.copyForwards(u8, self.recv_buf[0..remaining], self.recv_buf[result.consumed..self.recv_len]);
                }
                self.recv_len = remaining;

                return result.msg;
            }

            const bytes_read = try self.stream.read(self.recv_buf[self.recv_len..]);
            if (bytes_read == 0) return error.ConnectionClosed;
            self.recv_len += bytes_read;
        }
    }

    /// Receive messages until ReadyForQuery.
    pub fn recvUntilReady(self: *Connection) !std.ArrayList(protocol.BackendMsg) {
        var msgs: std.ArrayList(protocol.BackendMsg) = .empty;
        errdefer msgs.deinit(self.allocator);

        while (true) {
            const msg = try self.recvMsg();
            try msgs.append(self.allocator, msg);
            switch (msg) {
                .ready_for_query => return msgs,
                else => {},
            }
        }
    }

    /// Send a Terminate message and close the connection.
    pub fn close(self: *Connection) void {
        const n = protocol.encodeTerminate(&self.send_buf) catch return;
        self.stream.writeAll(self.send_buf[0..n]) catch {};
        self.stream.close();
    }
};

test "Connection struct layout" {
    try std.testing.expect(@sizeOf(Connection) > 0);
}
