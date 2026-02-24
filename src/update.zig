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

/// Shells out to curl to fetch the latest release tag from GitHub.
/// Returns the tag name string (e.g. "v0.2.0"). Caller owns the returned memory.
pub fn fetchLatestTag(allocator: std.mem.Allocator) ![]const u8 {
    const result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &.{
            "curl",
            "-sfL",
            "-H",
            "Accept: application/vnd.github.v3+json",
            "https://api.github.com/repos/" ++ repo ++ "/releases/latest",
        },
    }) catch {
        return error.CurlFailed;
    };
    defer allocator.free(result.stderr);
    defer allocator.free(result.stdout);

    switch (result.term) {
        .Exited => |code| {
            if (code != 0) return error.CurlFailed;
        },
        else => return error.CurlFailed,
    }

    return parseTagName(allocator, result.stdout);
}

/// Extracts the "tag_name" value from a GitHub API JSON response
/// using simple string searching. Caller owns the returned memory.
fn parseTagName(allocator: std.mem.Allocator, json: []const u8) ![]const u8 {
    const key = "\"tag_name\"";

    // Find the key in the JSON string
    const key_pos = std.mem.indexOf(u8, json, key) orelse return error.VersionParseFailed;

    // Move past the key
    var pos = key_pos + key.len;

    // Skip whitespace
    while (pos < json.len and (json[pos] == ' ' or json[pos] == '\t' or json[pos] == '\n' or json[pos] == '\r')) {
        pos += 1;
    }

    // Expect a colon
    if (pos >= json.len or json[pos] != ':') return error.VersionParseFailed;
    pos += 1;

    // Skip whitespace after colon
    while (pos < json.len and (json[pos] == ' ' or json[pos] == '\t' or json[pos] == '\n' or json[pos] == '\r')) {
        pos += 1;
    }

    // Expect opening quote
    if (pos >= json.len or json[pos] != '"') return error.VersionParseFailed;
    pos += 1;

    // Find closing quote
    const value_start = pos;
    while (pos < json.len and json[pos] != '"') {
        pos += 1;
    }
    if (pos >= json.len) return error.VersionParseFailed;

    const value = json[value_start..pos];
    return allocator.dupe(u8, value);
}

test "parseTagName extracts tag from normal JSON" {
    const allocator = std.testing.allocator;
    const json =
        \\{"tag_name": "v0.2.0", "name": "Release v0.2.0"}
    ;
    const tag = try parseTagName(allocator, json);
    defer allocator.free(tag);
    try std.testing.expectEqualStrings("v0.2.0", tag);
}

test "parseTagName handles whitespace variations" {
    const allocator = std.testing.allocator;
    const json =
        \\{ "tag_name" : "v1.0.0" }
    ;
    const tag = try parseTagName(allocator, json);
    defer allocator.free(tag);
    try std.testing.expectEqualStrings("v1.0.0", tag);
}

test "parseTagName returns error for missing key" {
    const allocator = std.testing.allocator;
    const result = parseTagName(allocator, "{}");
    try std.testing.expectError(error.VersionParseFailed, result);
}
