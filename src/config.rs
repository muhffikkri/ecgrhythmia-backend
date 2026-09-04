use dotenvy::dotenv;
use std::env;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct AppConfig {
    pub host_ip: String,
    pub rest_port: String,
    pub ws_port: String,
    pub mqtt_broker: String,
    pub mqtt_port: u16,
    pub mqtt_topic: String,
    pub mqtt_username: String,
    pub mqtt_password: String,
    pub supabase_jwt_secret: String,
    pub database_url: String,
}

impl AppConfig {
    pub fn load() -> Self {
        dotenv().ok();

        let host_ip = env::var("HOST_IP")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let rest_port = env::var("REST_PORT")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .unwrap_or_else(|| "8081".to_string());
        let ws_port = env::var("WS_PORT")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .unwrap_or_else(|| "8080".to_string());
        
        let mqtt_broker = env::var("MQTT_BROKER")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .expect("[Config] ERROR: MQTT_BROKER belum diset di .env!");
        
        let mqtt_port = env::var("MQTT_PORT")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .unwrap_or_else(|| "8883".to_string())
            .parse::<u16>()
            .unwrap_or(8883);
            
        let mqtt_topic = env::var("MQTT_TOPIC")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .expect("[Config] ERROR: MQTT_TOPIC belum diset di .env!");
            
        let mqtt_username = env::var("MQTT_USERNAME")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .expect("[Config] ERROR: MQTT_USERNAME belum diset di .env!");
            
        let mqtt_password = env::var("MQTT_PASSWORD")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .expect("[Config] ERROR: MQTT_PASSWORD belum diset di .env!");
        
        let supabase_jwt_secret = env::var("SUPABASE_JWT_SECRET")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .expect("[Config] ERROR: SUPABASE_JWT_SECRET belum diset di .env!");
            
        let database_url = env::var("DATABASE_URL")
            .ok()
            .filter(|s| !s.is_empty()).map(|s| s.replace("\"", ""))
            .expect("[Config] ERROR: DATABASE_URL belum diset di .env!");

        AppConfig {
            host_ip,
            rest_port,
            ws_port,
            mqtt_broker,
            mqtt_port,
            mqtt_topic,
            mqtt_username,
            mqtt_password,
            supabase_jwt_secret,
            database_url,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_load() {
        std::env::set_var("HOST_IP", "127.0.0.9");
        std::env::set_var("REST_PORT", "9999");
        std::env::set_var("WS_PORT", "9998");
        std::env::set_var("MQTT_BROKER", "broker.test");
        std::env::set_var("MQTT_PORT", "1883");
        std::env::set_var("MQTT_TOPIC", "test/topic");
        std::env::set_var("MQTT_USERNAME", "testuser");
        std::env::set_var("MQTT_PASSWORD", "testpass");
        std::env::set_var("SUPABASE_JWT_SECRET", "testsecret");
        std::env::set_var("DATABASE_URL", "postgres://test");

        let config = AppConfig::load();
        assert_eq!(config.host_ip, "127.0.0.9");
        assert_eq!(config.rest_port, "9999");
        assert_eq!(config.ws_port, "9998");
        assert_eq!(config.mqtt_broker, "broker.test");
        assert_eq!(config.mqtt_port, 1883);
        assert_eq!(config.mqtt_topic, "test/topic");
        assert_eq!(config.mqtt_username, "testuser");
        assert_eq!(config.mqtt_password, "testpass");
        assert_eq!(config.supabase_jwt_secret, "testsecret");
        assert_eq!(config.database_url, "postgres://test");
    }
}
