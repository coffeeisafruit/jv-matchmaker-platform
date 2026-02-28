from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('matching', '0012_add_social_proof'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                -- Enable pgvector extension (idempotent)
                CREATE EXTENSION IF NOT EXISTS vector;

                -- Embedding vectors (1024-dim for BAAI/bge-large-en-v1.5)
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embedding_seeking vector(1024);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embedding_offering vector(1024);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embedding_who_you_serve vector(1024);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embedding_what_you_do vector(1024);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embeddings_model varchar(100);
                ALTER TABLE profiles ADD COLUMN IF NOT EXISTS embeddings_updated_at timestamptz;

                -- IVFFlat indexes for cosine similarity search
                CREATE INDEX IF NOT EXISTS idx_profiles_embedding_seeking
                    ON profiles USING ivfflat (embedding_seeking vector_cosine_ops) WITH (lists = 50);
                CREATE INDEX IF NOT EXISTS idx_profiles_embedding_offering
                    ON profiles USING ivfflat (embedding_offering vector_cosine_ops) WITH (lists = 50);
            """,
            reverse_sql="""
                DROP INDEX IF EXISTS idx_profiles_embedding_offering;
                DROP INDEX IF EXISTS idx_profiles_embedding_seeking;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embedding_seeking;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embedding_offering;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embedding_who_you_serve;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embedding_what_you_do;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embeddings_model;
                ALTER TABLE profiles DROP COLUMN IF EXISTS embeddings_updated_at;
            """,
        ),
    ]
