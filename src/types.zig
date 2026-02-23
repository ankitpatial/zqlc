const std = @import("std");

/// Represents a Zig type derived from a PostgreSQL OID.
pub const ZigType = union(enum) {
    bool_type,
    i16_type,
    i32_type,
    i64_type,
    f32_type,
    f64_type,
    text, // []const u8
    bytea, // []const u8
    uuid, // [16]u8
    json, // []const u8
    date, // Date struct
    time, // Time struct
    timestamp, // i64 (microseconds)
    array: *const ZigType,
    optional: *const ZigType,
    pg_enum: EnumInfo,
    unknown: u32, // unrecognized OID

    pub const EnumInfo = struct {
        name: []const u8,
        variants: []const []const u8,
    };

    /// Return the Zig type string representation.
    pub fn zigTypeName(self: ZigType) []const u8 {
        return switch (self) {
            .bool_type => "bool",
            .i16_type => "i16",
            .i32_type => "i32",
            .i64_type => "i64",
            .f32_type => "f32",
            .f64_type => "f64",
            .text => "[]const u8",
            .bytea => "[]const u8",
            .uuid => "[16]u8",
            .json => "[]const u8",
            .date => "Date",
            .time => "Time",
            .timestamp => "Timestamp",
            .array => "array", // handled specially in codegen
            .optional => "optional", // handled specially in codegen
            .pg_enum => "enum", // handled specially in codegen
            .unknown => "[]const u8", // fallback
        };
    }

    /// Format a full type string, handling optional and array wrappers.
    pub fn formatType(self: ZigType, writer: anytype) !void {
        switch (self) {
            .optional => |inner| {
                try writer.writeByte('?');
                try inner.formatType(writer);
            },
            .array => |inner| {
                try writer.writeAll("[]const ");
                try inner.formatType(writer);
            },
            .pg_enum => |info| {
                try writer.writeAll(info.name);
            },
            else => {
                try writer.writeAll(self.zigTypeName());
            },
        }
    }
};

/// Well-known PostgreSQL OIDs.
pub const oid = struct {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const INTERVAL: u32 = 1186;
    pub const TIMETZ: u32 = 1266;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;

    // Array OIDs
    pub const BOOL_ARRAY: u32 = 1000;
    pub const INT2_ARRAY: u32 = 1005;
    pub const INT4_ARRAY: u32 = 1007;
    pub const INT8_ARRAY: u32 = 1016;
    pub const TEXT_ARRAY: u32 = 1009;
    pub const VARCHAR_ARRAY: u32 = 1015;
    pub const FLOAT4_ARRAY: u32 = 1021;
    pub const FLOAT8_ARRAY: u32 = 1022;
    pub const UUID_ARRAY: u32 = 2951;
    pub const JSON_ARRAY: u32 = 199;
    pub const JSONB_ARRAY: u32 = 3807;
};

/// Map a PostgreSQL OID to a ZigType. Returns null for unknown types
/// that need further resolution (e.g., enums).
pub fn oidToZigType(type_oid: u32) ?ZigType {
    return switch (type_oid) {
        oid.BOOL => .bool_type,
        oid.INT2 => .i16_type,
        oid.INT4, oid.OID => .i32_type,
        oid.INT8 => .i64_type,
        oid.FLOAT4 => .f32_type,
        oid.FLOAT8, oid.NUMERIC => .f64_type,
        oid.TEXT, oid.VARCHAR => .text,
        oid.BYTEA => .bytea,
        oid.UUID => .uuid,
        oid.JSON, oid.JSONB => .json,
        oid.DATE => .date,
        oid.TIME, oid.TIMETZ => .time,
        oid.TIMESTAMP, oid.TIMESTAMPTZ => .timestamp,
        oid.INTERVAL => .text, // interval as text
        else => null,
    };
}

/// Map an array OID to its element type.
pub fn arrayElementType(type_oid: u32) ?ZigType {
    return switch (type_oid) {
        oid.BOOL_ARRAY => .bool_type,
        oid.INT2_ARRAY => .i16_type,
        oid.INT4_ARRAY => .i32_type,
        oid.INT8_ARRAY => .i64_type,
        oid.FLOAT4_ARRAY => .f32_type,
        oid.FLOAT8_ARRAY => .f64_type,
        oid.TEXT_ARRAY, oid.VARCHAR_ARRAY => .text,
        oid.UUID_ARRAY => .uuid,
        oid.JSON_ARRAY, oid.JSONB_ARRAY => .json,
        else => null,
    };
}

/// Cache for resolved types (OID → ZigType).
pub const TypeCache = struct {
    map: std.AutoHashMap(u32, ZigType),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TypeCache {
        return .{
            .map = std.AutoHashMap(u32, ZigType).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TypeCache) void {
        self.map.deinit();
    }

    pub fn resolve(self: *TypeCache, type_oid: u32) ?ZigType {
        // Check cache first
        if (self.map.get(type_oid)) |t| return t;
        // Try well-known OIDs
        if (oidToZigType(type_oid)) |t| return t;
        // Try array types
        if (arrayElementType(type_oid)) |_| return .text; // TODO: proper array wrapping
        return null;
    }

    pub fn put(self: *TypeCache, type_oid: u32, zig_type: ZigType) !void {
        try self.map.put(type_oid, zig_type);
    }
};

/// Cache for nullability info (table_oid, column_attr → is_not_null).
pub const NullabilityCache = struct {
    map: std.AutoHashMap(u64, bool),

    pub fn init(allocator: std.mem.Allocator) NullabilityCache {
        return .{ .map = std.AutoHashMap(u64, bool).init(allocator) };
    }

    pub fn deinit(self: *NullabilityCache) void {
        self.map.deinit();
    }

    fn key(table_oid: u32, column_attr: i16) u64 {
        return @as(u64, table_oid) << 32 | @as(u64, @as(u32, @bitCast(@as(i32, column_attr))));
    }

    pub fn isNotNull(self: *const NullabilityCache, table_oid: u32, column_attr: i16) ?bool {
        return self.map.get(key(table_oid, column_attr));
    }

    pub fn put(self: *NullabilityCache, table_oid: u32, column_attr: i16, not_null: bool) !void {
        try self.map.put(key(table_oid, column_attr), not_null);
    }
};

test "oidToZigType basic mappings" {
    try std.testing.expectEqual(ZigType.bool_type, oidToZigType(16).?);
    try std.testing.expectEqual(ZigType.i32_type, oidToZigType(23).?);
    try std.testing.expectEqual(ZigType.i64_type, oidToZigType(20).?);
    try std.testing.expectEqual(ZigType.text, oidToZigType(25).?);
    try std.testing.expectEqual(ZigType.uuid, oidToZigType(2950).?);
    try std.testing.expectEqual(ZigType.json, oidToZigType(114).?);
    try std.testing.expectEqual(ZigType.timestamp, oidToZigType(1114).?);
    try std.testing.expect(oidToZigType(99999) == null);
}

test "TypeCache" {
    var cache = TypeCache.init(std.testing.allocator);
    defer cache.deinit();

    // Well-known type
    try std.testing.expect(cache.resolve(23) != null);
    // Unknown type
    try std.testing.expect(cache.resolve(99999) == null);
    // Cached type
    try cache.put(99999, .text);
    try std.testing.expect(cache.resolve(99999) != null);
}
