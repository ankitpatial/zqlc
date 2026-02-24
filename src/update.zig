const std = @import("std");
const builtin = @import("builtin");

/// Single source of truth for the zqlc version.
pub const version = "0.1.0";

/// GitHub repository path.
const repo = "ankitpatial/zqlc";

pub const UpdateError = error{
    UnsupportedPlatform,
    CurlFailed,
    VersionParseFailed,
    DownloadFailed,
    ExtractionFailed,
    ReplaceFailed,
    AlreadyUpToDate,
};

/// Returns the platform-specific archive suffix, e.g. "macos-aarch64" or "linux-x86_64".
/// Produces a compile error for unsupported OS/architecture combinations.
pub fn platformString() []const u8 {
    const os = builtin.os.tag;
    const arch = builtin.cpu.arch;

    return switch (os) {
        .macos => switch (arch) {
            .aarch64 => "macos-aarch64",
            .x86_64 => "macos-x86_64",
            else => @compileError("unsupported macOS architecture"),
        },
        .linux => switch (arch) {
            .x86_64 => "linux-x86_64",
            .aarch64 => "linux-aarch64",
            else => @compileError("unsupported Linux architecture"),
        },
        .windows => switch (arch) {
            .x86_64 => "windows-x86_64",
            .aarch64 => "windows-aarch64",
            else => @compileError("unsupported Windows architecture"),
        },
        else => @compileError("unsupported operating system"),
    };
}

/// Returns the archive file extension for the current platform.
/// Windows uses ".zip", all other platforms use ".tar.gz".
pub fn archiveExt() []const u8 {
    return if (builtin.os.tag == .windows) ".zip" else ".tar.gz";
}

/// Constructs the full GitHub release download URL for the given tag.
/// The caller owns the returned memory.
///
/// Example result: "https://github.com/ankitpatial/zqlc/releases/download/v0.2.0/zqlc-macos-aarch64.tar.gz"
pub fn buildDownloadUrl(allocator: std.mem.Allocator, tag: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "https://github.com/{s}/releases/download/{s}/zqlc-{s}{s}", .{
        repo,
        tag,
        platformString(),
        archiveExt(),
    });
}

test "platformString returns a non-empty string" {
    const platform = platformString();
    try std.testing.expect(platform.len > 0);

    // Verify it contains a hyphen separating OS and arch
    const hyphen = std.mem.indexOfScalar(u8, platform, '-');
    try std.testing.expect(hyphen != null);
}

test "archiveExt returns expected extension" {
    const ext = archiveExt();
    // On non-Windows test hosts this will be ".tar.gz"
    // On Windows it would be ".zip"
    if (builtin.os.tag == .windows) {
        try std.testing.expectEqualStrings(".zip", ext);
    } else {
        try std.testing.expectEqualStrings(".tar.gz", ext);
    }
}

test "buildDownloadUrl constructs correct URL" {
    const allocator = std.testing.allocator;

    const url = try buildDownloadUrl(allocator, "v0.2.0");
    defer allocator.free(url);

    // Verify it starts with the expected prefix
    try std.testing.expect(std.mem.startsWith(u8, url, "https://github.com/ankitpatial/zqlc/releases/download/v0.2.0/zqlc-"));

    // Verify it contains the platform string
    try std.testing.expect(std.mem.indexOf(u8, url, platformString()) != null);

    // Verify it ends with the correct archive extension
    try std.testing.expect(std.mem.endsWith(u8, url, archiveExt()));
}

test "buildDownloadUrl with different tags" {
    const allocator = std.testing.allocator;

    const url1 = try buildDownloadUrl(allocator, "v1.0.0");
    defer allocator.free(url1);
    try std.testing.expect(std.mem.indexOf(u8, url1, "/v1.0.0/") != null);

    const url2 = try buildDownloadUrl(allocator, "v0.1.0-beta");
    defer allocator.free(url2);
    try std.testing.expect(std.mem.indexOf(u8, url2, "/v0.1.0-beta/") != null);
}
