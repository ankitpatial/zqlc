const std = @import("std");

/// All error categories for zqlc.
pub const Error = union(enum) {
    connection: ConnectionError,
    auth: AuthError,
    protocol: ProtocolError,
    query_error: QueryError,
    file: FileError,
    config: ConfigError,

    pub fn format(self: Error, w: *std.Io.Writer, use_color: bool) !void {
        switch (self) {
            .connection => |e| try e.format(w, use_color),
            .auth => |e| try e.format(w, use_color),
            .protocol => |e| try e.format(w, use_color),
            .query_error => |e| try e.format(w, use_color),
            .file => |e| try e.format(w, use_color),
            .config => |e| try e.format(w, use_color),
        }
    }
};

pub const ConnectionError = struct {
    message: []const u8,

    pub fn format(self: ConnectionError, w: *std.Io.Writer, use_color: bool) !void {
        if (use_color) try w.writeAll("\x1b[31m");
        try w.writeAll("Connection error: ");
        if (use_color) try w.writeAll("\x1b[0m");
        try w.writeAll(self.message);
        try w.writeByte('\n');
    }
};

pub const AuthError = struct {
    message: []const u8,

    pub fn format(self: AuthError, w: *std.Io.Writer, use_color: bool) !void {
        if (use_color) try w.writeAll("\x1b[31m");
        try w.writeAll("Authentication error: ");
        if (use_color) try w.writeAll("\x1b[0m");
        try w.writeAll(self.message);
        try w.writeByte('\n');
    }
};

pub const ProtocolError = struct {
    message: []const u8,
    detail: ?[]const u8 = null,

    pub fn format(self: ProtocolError, w: *std.Io.Writer, use_color: bool) !void {
        if (use_color) try w.writeAll("\x1b[31m");
        try w.writeAll("Protocol error: ");
        if (use_color) try w.writeAll("\x1b[0m");
        try w.writeAll(self.message);
        try w.writeByte('\n');
        if (self.detail) |d| {
            try w.writeAll("  Detail: ");
            try w.writeAll(d);
            try w.writeByte('\n');
        }
    }
};

pub const QueryError = struct {
    file_path: []const u8,
    message: []const u8,
    detail: ?[]const u8 = null,
    line: ?u32 = null,
    position: ?u32 = null,

    pub fn format(self: QueryError, w: *std.Io.Writer, use_color: bool) !void {
        if (use_color) try w.writeAll("\x1b[1m");
        try w.writeAll(self.file_path);
        if (self.line) |l| {
            try w.print(":{d}", .{l});
        }
        if (use_color) try w.writeAll("\x1b[0m");
        try w.writeAll(": ");
        if (use_color) try w.writeAll("\x1b[31m");
        try w.writeAll("error: ");
        if (use_color) try w.writeAll("\x1b[0m");
        try w.writeAll(self.message);
        try w.writeByte('\n');
        if (self.detail) |d| {
            try w.writeAll("  ");
            try w.writeAll(d);
            try w.writeByte('\n');
        }
    }
};

pub const FileError = struct {
    path: []const u8,
    message: []const u8,

    pub fn format(self: FileError, w: *std.Io.Writer, use_color: bool) !void {
        if (use_color) try w.writeAll("\x1b[31m");
        try w.writeAll("File error: ");
        if (use_color) try w.writeAll("\x1b[0m");
        try w.print("{s}: {s}\n", .{ self.path, self.message });
    }
};

pub const ConfigError = struct {
    message: []const u8,

    pub fn format(self: ConfigError, w: *std.Io.Writer, use_color: bool) !void {
        if (use_color) try w.writeAll("\x1b[31m");
        try w.writeAll("Configuration error: ");
        if (use_color) try w.writeAll("\x1b[0m");
        try w.writeAll(self.message);
        try w.writeByte('\n');
    }
};

/// Detect whether a file descriptor is a TTY.
pub fn isTty(fd: std.posix.fd_t) bool {
    return std.posix.isatty(fd);
}

test "error formatting" {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    const err = Error{ .config = .{ .message = "DATABASE_URL not set" } };
    try err.format(&w, false);

    const output = buf[0..w.end];
    try std.testing.expectEqualStrings("Configuration error: DATABASE_URL not set\n", output);
}
