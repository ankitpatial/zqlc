const std = @import("std");
const protocol = @import("protocol.zig");
const auth = @import("auth.zig");

pub const Connection = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,
    recv_buf: [16384]u8 = undefined,
    recv_len: usize = 0,
    recv_start: usize = 0,
    send_buf: std.ArrayList(u8) = .empty,

    /// Connect to a PostgreSQL server, authenticate, and wait for ReadyForQuery.
    pub fn connect(
        allocator: std.mem.Allocator,
        io: std.Io,
        host: []const u8,
        port: u16,
        user: []const u8,
        password: []const u8,
        database: []const u8,
    ) !Connection {
        const host_name: std.Io.net.HostName = .{ .bytes = host };
        const stream = std.Io.net.HostName.connect(host_name, io, port, .{ .mode = .stream }) catch {
            return error.ConnectionRefused;
        };
        errdefer stream.close(io);

        var self = Connection{
            .allocator = allocator,
            .io = io,
            .stream = stream,
        };
        errdefer self.send_buf.deinit(allocator);

        // Send StartupMessage
        try protocol.encodeStartup(allocator, &self.send_buf, user, database);
        try self.flushSendBuf();

        // Read messages until ReadyForQuery
        while (true) {
            const msg = try self.recvMsg();
            switch (msg) {
                .auth_ok => {},
                .auth_cleartext, .auth_md5, .auth_sasl => {
                    try auth.authenticate(allocator, &self, msg, user, password);
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

    /// Write send_buf contents to the stream and flush.
    pub fn flushSendBuf(self: *Connection) !void {
        var write_buf: [8192]u8 = undefined;
        var w = self.stream.writer(self.io, &write_buf);
        w.interface.writeAll(self.send_buf.items) catch return error.ConnectionRefused;
        w.interface.flush() catch return error.ConnectionRefused;
    }

    /// Send a Parse message.
    pub fn sendParse(self: *Connection, stmt_name: []const u8, sql: []const u8) !void {
        try protocol.encodeParse(self.allocator, &self.send_buf, stmt_name, sql);
        try self.flushSendBuf();
    }

    /// Send a Describe message for a statement.
    pub fn sendDescribeStatement(self: *Connection, stmt_name: []const u8) !void {
        try protocol.encodeDescribe(self.allocator, &self.send_buf, 'S', stmt_name);
        try self.flushSendBuf();
    }

    /// Send a Sync message.
    pub fn sendSync(self: *Connection) !void {
        try protocol.encodeSync(self.allocator, &self.send_buf);
        try self.flushSendBuf();
    }

    /// Send a simple Query message.
    pub fn sendQuery(self: *Connection, sql: []const u8) !void {
        try protocol.encodeQuery(self.allocator, &self.send_buf, sql);
        try self.flushSendBuf();
    }

    /// Send a Close statement message.
    pub fn sendCloseStatement(self: *Connection, stmt_name: []const u8) !void {
        try protocol.encodeClose(self.allocator, &self.send_buf, 'S', stmt_name);
        try self.flushSendBuf();
    }

    /// Read bytes from the network into recv_buf.
    fn readFromNetwork(self: *Connection) !usize {
        var bufs: [1][]u8 = .{self.recv_buf[self.recv_len..]};
        const n = self.io.vtable.netRead(self.io.userdata, self.stream.socket.handle, &bufs) catch {
            return error.ConnectionClosed;
        };
        if (n == 0) return error.ConnectionClosed;
        self.recv_len += n;
        return n;
    }

    /// Receive the next backend message.
    pub fn recvMsg(self: *Connection) !protocol.BackendMsg {
        while (true) {
            const available = self.recv_len - self.recv_start;
            if (available >= 5) {
                const result = protocol.readBackendMsg(
                    self.recv_buf[self.recv_start..self.recv_len],
                    self.allocator,
                ) catch |err| switch (err) {
                    error.NeedMoreData => {
                        // Compact before reading if no room at end
                        self.compactRecvBuf();
                        _ = try self.readFromNetwork();
                        continue;
                    },
                    else => return err,
                };

                self.recv_start += result.consumed;

                // Compact when cursor is past halfway
                if (self.recv_start > self.recv_buf.len / 2) {
                    self.compactRecvBuf();
                }

                return result.msg;
            }

            // Compact before reading if no room at end
            self.compactRecvBuf();
            _ = try self.readFromNetwork();
        }
    }

    fn compactRecvBuf(self: *Connection) void {
        if (self.recv_start == 0) return;
        const remaining = self.recv_len - self.recv_start;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.recv_buf[0..remaining], self.recv_buf[self.recv_start..self.recv_len]);
        }
        self.recv_len = remaining;
        self.recv_start = 0;
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
        protocol.encodeTerminate(self.allocator, &self.send_buf) catch {};
        self.flushSendBuf() catch {};
        self.send_buf.deinit(self.allocator);
        self.stream.close(self.io);
    }
};

test "Connection struct layout" {
    try std.testing.expect(@sizeOf(Connection) > 0);
}
