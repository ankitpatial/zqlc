// zqlc: Type-safe SQL code generation for Zig.
//
// Library root — re-exports public modules.

pub const protocol = @import("protocol.zig");
pub const connection = @import("connection.zig");
pub const auth = @import("auth.zig");
pub const scram = @import("scram.zig");
pub const types = @import("types.zig");
pub const introspect = @import("introspect.zig");
pub const query = @import("query.zig");
pub const project = @import("project.zig");
pub const codegen = @import("codegen.zig");
pub const errors = @import("errors.zig");

test {
    const std = @import("std");
    std.testing.refAllDecls(@This());
}
