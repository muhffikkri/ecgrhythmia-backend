use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

/// Alat admin untuk menautkan akun lokal dengan UUID user Supabase Auth.
///
/// User Supabase Auth tidak bisa performing login lewat endpoint publik
/// (role dibatasi `pasien`/`dokter`), jadi `accounts.auth_id` diisi manual
/// memakai UUID dari Supabase Dashboard → Authentication → Users.
///
/// Pemakaian:
///   cargo run --bin link_auth                  # daftar akun yang belum tertaut
///   cargo run --bin link_auth <email> <uuid>   # tautkan satu akun
///   cargo run --bin link_auth --by-email       # tautkan otomatis: auth_id = sub JWT terakhir
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL belum diset di environment / .env")?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await?;

    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.len() == 2 {
        let (email, auth_id) = (&args[0], &args[1]);
        let updated = sqlx::query(
            "UPDATE accounts SET auth_id = $1 WHERE lower(email) = lower($2) RETURNING id, email, role",
        )
        .bind(auth_id)
        .bind(email)
        .fetch_optional(&pool)
        .await?;

        return match updated {
            Some(row) => {
                println!(
                    "OK  {} tertaut ke auth_id={} (role={})",
                    row.get::<String, _>("email"),
                    auth_id,
                    row.get::<String, _>("role")
                );
                Ok(())
            }
            None => {
                eprintln!("GAGAL: tidak ada akun dengan email '{}'", email);
                std::process::exit(1);
            }
        };
    }

    let rows = sqlx::query(
        "SELECT id, email, role, auth_id FROM accounts ORDER BY auth_id NULLS FIRST, email",
    )
    .fetch_all(&pool)
    .await?;

    let mut linked = 0usize;
    let mut unlinked = 0usize;

    println!(
        "{:<38} {:<10} {:<38} ACCOUNT_ID",
        "EMAIL", "ROLE", "AUTH_ID"
    );
    for row in &rows {
        match row.get::<Option<String>, _>("auth_id") {
            Some(auth_id) => {
                linked += 1;
                if args.len() == 1 {
                    println!(
                        "{:<38} {:<10} {:<38} {}",
                        row.get::<String, _>("email"),
                        row.get::<String, _>("role"),
                        auth_id,
                        row.get::<String, _>("id")
                    );
                }
            }
            None => {
                unlinked += 1;
                println!(
                    "{:<38} {:<10} {:<38} {}   <- belum tertaut",
                    row.get::<String, _>("email"),
                    row.get::<String, _>("role"),
                    "-",
                    row.get::<String, _>("id")
                );
            }
        }
    }

    println!("\n{} tertaut, {} belum tertaut", linked, unlinked);
    if unlinked > 0 {
        println!("Tautkan dengan: cargo run --bin link_auth <email> <uuid-dari-supabase-auth>");
    }

    Ok(())
}
