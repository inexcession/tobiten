CREATE TABLE test_profiles (
    user_id TEXT PRIMARY KEY REFERENCES test_users(id),
    bio TEXT NOT NULL
);
