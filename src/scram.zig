const std = @import("std");
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const ScramState = struct {
    allocator: std.mem.Allocator,
    client_nonce: [24]u8,
    client_first_bare: []const u8,
    salted_password: [32]u8 = undefined,
    auth_message: []const u8 = "",

    pub fn deinit(self: *ScramState) void {
        if (self.client_first_bare.len > 0) self.allocator.free(self.client_first_bare);
        if (self.auth_message.len > 0) self.allocator.free(self.auth_message);
    }
};

/// Generate the client-first message for SCRAM-SHA-256.
pub fn clientFirst(allocator: std.mem.Allocator, user: []const u8) !struct { state: ScramState, message: []const u8 } {
    var nonce_bytes: [18]u8 = undefined;
    std.crypto.random.bytes(&nonce_bytes);

    var nonce: [24]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&nonce, &nonce_bytes);

    const bare = try std.fmt.allocPrint(allocator, "n={s},r={s}", .{ user, nonce });
    const msg = try std.fmt.allocPrint(allocator, "n,,{s}", .{bare});

    return .{
        .state = .{
            .allocator = allocator,
            .client_nonce = nonce,
            .client_first_bare = bare,
        },
        .message = msg,
    };
}

/// Parse the server-first message and produce the client-final message.
pub fn clientFinal(
    allocator: std.mem.Allocator,
    state: *ScramState,
    password: []const u8,
    server_first: []const u8,
) ![]const u8 {
    var server_nonce: []const u8 = "";
    var salt_b64: []const u8 = "";
    var iterations: u32 = 0;

    var iter = std.mem.splitScalar(u8, server_first, ',');
    while (iter.next()) |part| {
        if (part.len < 2) continue;
        if (part[0] == 'r' and part[1] == '=') {
            server_nonce = part[2..];
        } else if (part[0] == 's' and part[1] == '=') {
            salt_b64 = part[2..];
        } else if (part[0] == 'i' and part[1] == '=') {
            iterations = std.fmt.parseInt(u32, part[2..], 10) catch return error.InvalidServerResponse;
        }
    }

    if (server_nonce.len == 0 or salt_b64.len == 0 or iterations == 0) {
        return error.InvalidServerResponse;
    }

    if (!std.mem.startsWith(u8, server_nonce, &state.client_nonce)) {
        return error.InvalidServerNonce;
    }

    // Decode salt
    const salt_len = std.base64.standard.Decoder.calcSizeForSlice(salt_b64) catch return error.InvalidServerResponse;
    const salt = try allocator.alloc(u8, salt_len);
    defer allocator.free(salt);
    std.base64.standard.Decoder.decode(salt, salt_b64) catch return error.InvalidServerResponse;

    // PBKDF2
    const salted_password = pbkdf2(password, salt, iterations);
    state.salted_password = salted_password;

    // client-final-message-without-proof
    const cfm_without_proof = try std.fmt.allocPrint(allocator, "c=biws,r={s}", .{server_nonce});
    defer allocator.free(cfm_without_proof);

    // AuthMessage
    const auth_message = try std.fmt.allocPrint(allocator, "{s},{s},{s}", .{
        state.client_first_bare,
        server_first,
        cfm_without_proof,
    });
    state.auth_message = auth_message;

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    var client_key: [32]u8 = undefined;
    HmacSha256.create(&client_key, "Client Key", &salted_password);

    // StoredKey = SHA256(ClientKey)
    var stored_key: [32]u8 = undefined;
    Sha256.hash(&client_key, &stored_key, .{});

    // ClientSignature = HMAC(StoredKey, AuthMessage)
    var client_sig: [32]u8 = undefined;
    HmacSha256.create(&client_sig, auth_message, &stored_key);

    // ClientProof = ClientKey XOR ClientSignature
    var proof: [32]u8 = undefined;
    for (0..32) |i| proof[i] = client_key[i] ^ client_sig[i];

    var proof_b64: [44]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&proof_b64, &proof);

    return std.fmt.allocPrint(allocator, "{s},p={s}", .{ cfm_without_proof, proof_b64 });
}

/// Verify the server-final message.
pub fn verifyServerFinal(state: *const ScramState, server_final: []const u8) !void {
    if (!std.mem.startsWith(u8, server_final, "v=")) return error.InvalidServerResponse;
    const sig_b64 = server_final[2..];

    // ServerKey = HMAC(SaltedPassword, "Server Key")
    var server_key: [32]u8 = undefined;
    HmacSha256.create(&server_key, "Server Key", &state.salted_password);

    // ServerSignature = HMAC(ServerKey, AuthMessage)
    var expected_sig: [32]u8 = undefined;
    HmacSha256.create(&expected_sig, state.auth_message, &server_key);

    var expected_b64: [44]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&expected_b64, &expected_sig);

    if (!std.mem.eql(u8, sig_b64, &expected_b64)) {
        return error.ServerSignatureMismatch;
    }
}

/// PBKDF2-HMAC-SHA256 key derivation.
fn pbkdf2(password: []const u8, salt: []const u8, iterations: u32) [32]u8 {
    // U1 = HMAC(password, salt || INT(1))
    var hmac = HmacSha256.init(password);
    hmac.update(salt);
    hmac.update(&[_]u8{ 0, 0, 0, 1 });
    var u_prev: [32]u8 = undefined;
    hmac.final(&u_prev);
    var result = u_prev;

    // U2..Ui
    var i: u32 = 1;
    while (i < iterations) : (i += 1) {
        var h = HmacSha256.init(password);
        h.update(&u_prev);
        h.final(&u_prev);
        for (0..32) |j| result[j] ^= u_prev[j];
    }

    return result;
}

test "pbkdf2 basic" {
    const result = pbkdf2("password", "salt", 1);
    try std.testing.expectEqual(@as(usize, 32), result.len);
}
