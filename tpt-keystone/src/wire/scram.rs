//! Server-side SCRAM-SHA-256 (RFC 5802), the same mechanism real Postgres
//! uses for password auth — implementing it exactly means real drivers
//! (psql, libpq-based clients, JDBC, node-postgres) that already speak
//! SCRAM-SHA-256 need no special-casing to talk to this server. Only the
//! server half is implemented; there's no SCRAM client anywhere in this
//! codebase.
//!
//! Credential storage: never the plaintext password, only `StoredKey`/
//! `ServerKey` (`ScramCredential`), computed once via `derive_credential`
//! when a role is created/bootstrapped and persisted in `_tpt_roles`
//! (`wire::roles`). The live handshake below only ever does HMAC-SHA256,
//! matching real SCRAM — PBKDF2 (the expensive step) runs once per
//! credential, not once per login.

use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};

pub const MECHANISM: &str = "SCRAM-SHA-256";

#[derive(Debug, Clone)]
pub struct ScramCredential {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

/// Derive `StoredKey`/`ServerKey` from a plaintext password — called once
/// when seeding/creating a role, never on the login hot path.
pub fn derive_credential(password: &str, salt: &[u8], iterations: u32) -> ScramCredential {
    let mut salted_password = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);

    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();
    let server_key = hmac_sha256(&salted_password, b"Server Key");

    ScramCredential { salt: salt.to_vec(), iterations, stored_key, server_key }
}

/// Generates a fresh random salt for a new/rotated credential.
pub fn random_salt() -> Vec<u8> {
    let mut salt = vec![0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    salt
}

/// Server-side state carried between `server_first` and
/// `verify_client_final` for one login attempt.
pub struct ServerFirst {
    client_first_bare: String,
    server_first_message: String,
    combined_nonce: String,
}

/// Handles the client's `SASLInitialResponse` (`client-first-message`,
/// gs2-header `n,,` + `n=<user>,r=<client-nonce>`) and returns the
/// `server-first-message` to send back as `AuthenticationSASLContinue`.
pub fn server_first(client_first: &[u8], cred: &ScramCredential) -> anyhow::Result<ServerFirst> {
    let s = std::str::from_utf8(client_first)?;
    // gs2-header is `n,,` (client doesn't support channel binding) or `y,,`
    // (client supports it but won't use it because we only advertise
    // `SCRAM-SHA-256`, not the `-PLUS` variant — this is what real `libpq`
    // sends over a TLS connection). `p=...` (demanding channel binding) is
    // the only case we don't support, since we never advertise `-PLUS`.
    let bare = s
        .strip_prefix("n,,")
        .or_else(|| s.strip_prefix("y,,"))
        .ok_or_else(|| anyhow::anyhow!("unsupported SCRAM gs2-header (channel binding not supported)"))?;
    let client_nonce = field(bare, 'r').ok_or_else(|| anyhow::anyhow!("missing client nonce"))?;

    let mut server_nonce_bytes = vec![0u8; 18];
    rand::thread_rng().fill_bytes(&mut server_nonce_bytes);
    let server_nonce = STANDARD.encode(server_nonce_bytes);
    let combined_nonce = format!("{client_nonce}{server_nonce}");

    let server_first_message = format!(
        "r={combined_nonce},s={},i={}",
        STANDARD.encode(&cred.salt),
        cred.iterations
    );

    Ok(ServerFirst { client_first_bare: bare.to_string(), server_first_message, combined_nonce })
}

impl ServerFirst {
    pub fn message_bytes(&self) -> Vec<u8> {
        self.server_first_message.as_bytes().to_vec()
    }
}

/// Verifies the client's `client-final-message` (`c=biws,r=<nonce>,p=<proof>`)
/// against `cred`, returning the `server-final-message` (`v=<ServerSignature>`)
/// bytes on success. Any mismatch — wrong nonce, wrong proof — is a plain
/// error; the caller sends `ErrorResponse` and closes the connection, same
/// as an unknown username, so a client can't distinguish "bad password" from
/// "no such user".
pub fn verify_client_final(
    client_final: &[u8],
    first: &ServerFirst,
    cred: &ScramCredential,
) -> anyhow::Result<Vec<u8>> {
    let s = std::str::from_utf8(client_final)?;
    let channel_binding = field(s, 'c').ok_or_else(|| anyhow::anyhow!("missing channel binding"))?;
    let nonce = field(s, 'r').ok_or_else(|| anyhow::anyhow!("missing nonce"))?;
    if nonce != first.combined_nonce {
        anyhow::bail!("nonce mismatch");
    }
    let proof_b64 = field(s, 'p').ok_or_else(|| anyhow::anyhow!("missing proof"))?;
    let proof = STANDARD.decode(proof_b64.as_bytes())?;
    if proof.len() != 32 {
        anyhow::bail!("malformed client proof");
    }

    let without_proof = format!("c={channel_binding},r={nonce}");
    let auth_message = format!("{},{},{}", first.client_first_bare, first.server_first_message, without_proof);

    let client_signature = hmac_sha256(&cred.stored_key, auth_message.as_bytes());
    let mut client_key = [0u8; 32];
    for i in 0..32 {
        client_key[i] = proof[i] ^ client_signature[i];
    }
    let computed_stored_key: [u8; 32] = Sha256::digest(client_key).into();
    if computed_stored_key != cred.stored_key {
        anyhow::bail!("SCRAM proof verification failed");
    }

    let server_signature = hmac_sha256(&cred.server_key, auth_message.as_bytes());
    Ok(format!("v={}", STANDARD.encode(server_signature)).into_bytes())
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// Extracts `key=value` from a comma-separated SCRAM attribute list
/// (`n=user,r=nonce`, `c=biws,r=nonce,p=proof`, ...).
fn field(s: &str, key: char) -> Option<&str> {
    s.split(',').find_map(|part| {
        let mut chars = part.chars();
        if chars.next() == Some(key) && chars.next() == Some('=') {
            Some(&part[2..])
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs a full client-side SCRAM-SHA-256 exchange against the server
    /// implementation to prove correct/incorrect passwords are told apart —
    /// there's no SCRAM client elsewhere in this codebase, so the client
    /// half is reimplemented here, test-only, from the same RFC 5802 spec.
    fn client_exchange(gs2_header: &str, password_for_client: &str, cred: &ScramCredential) -> anyhow::Result<Vec<u8>> {
        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = format!("n=tester,r={client_nonce}");
        let client_first = format!("{gs2_header}{client_first_bare}");

        let first = server_first(client_first.as_bytes(), cred)?;

        let mut salted_password = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password_for_client.as_bytes(), &cred.salt, cred.iterations, &mut salted_password);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();

        let channel_binding = STANDARD.encode(gs2_header.as_bytes());
        let without_proof = format!("c={channel_binding},r={}", first.combined_nonce);
        let auth_message = format!("{},{},{}", client_first_bare, first.server_first_message, without_proof);
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ client_signature[i];
        }
        let client_final = format!("{without_proof},p={}", STANDARD.encode(proof));

        verify_client_final(client_final.as_bytes(), &first, cred)
    }

    #[test]
    fn correct_password_succeeds() {
        let salt = random_salt();
        let cred = derive_credential("hunter2", &salt, 4096);
        let result = client_exchange("n,,", "hunter2", &cred);
        assert!(result.is_ok(), "{result:?}");
        assert!(String::from_utf8(result.unwrap()).unwrap().starts_with("v="));
    }

    /// Real `libpq` sends `y,,` (not `n,,`) over a TLS connection — it
    /// supports channel binding but falls back since we only advertise
    /// plain `SCRAM-SHA-256`, not `-PLUS`. Caught via a real `psql
    /// sslmode=require` connection failing with this exact gs2-header
    /// before this case was handled.
    #[test]
    fn correct_password_succeeds_with_y_gs2_header() {
        let salt = random_salt();
        let cred = derive_credential("hunter2", &salt, 4096);
        let result = client_exchange("y,,", "hunter2", &cred);
        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn wrong_password_fails() {
        let salt = random_salt();
        let cred = derive_credential("hunter2", &salt, 4096);
        let result = client_exchange("n,,", "wrong-password", &cred);
        assert!(result.is_err());
    }

    #[test]
    fn malformed_client_first_is_rejected() {
        let salt = random_salt();
        let cred = derive_credential("hunter2", &salt, 4096);
        assert!(server_first(b"not a valid scram message", &cred).is_err());
    }

    #[test]
    fn malformed_client_final_is_rejected() {
        let salt = random_salt();
        let cred = derive_credential("hunter2", &salt, 4096);
        let client_first = "n,,n=tester,r=fyko+d2lbbFgONRv9qkxdawL";
        let first = server_first(client_first.as_bytes(), &cred).unwrap();
        assert!(verify_client_final(b"garbage", &first, &cred).is_err());
    }
}
