from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('matching', '0013_add_embedding_columns'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                -- Raw event store for client-side analytics from outreach pages.
                -- Populated directly by browser JS via Supabase REST API (PostgREST).
                -- Read by Django ORM via unmanaged OutreachEvent model.
                CREATE TABLE IF NOT EXISTS outreach_events (
                    id          bigserial PRIMARY KEY,
                    report_id   integer NOT NULL REFERENCES matching_memberreport(id) ON DELETE CASCADE,
                    access_code varchar(20) NOT NULL,
                    event_type  varchar(50) NOT NULL,
                    partner_id  varchar(255),
                    details     jsonb DEFAULT '{}',
                    session_id  varchar(64),
                    created_at  timestamptz DEFAULT now()
                );

                CREATE INDEX IF NOT EXISTS idx_outreach_events_report
                    ON outreach_events(report_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_outreach_events_type
                    ON outreach_events(event_type);
                CREATE INDEX IF NOT EXISTS idx_outreach_events_session
                    ON outreach_events(session_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_outreach_events_partner
                    ON outreach_events(report_id, partner_id);

                -- RLS: browser inserts must provide a valid access_code for an active report
                ALTER TABLE outreach_events ENABLE ROW LEVEL SECURITY;

                CREATE POLICY "outreach_events_insert" ON outreach_events FOR INSERT
                    WITH CHECK (EXISTS (
                        SELECT 1 FROM matching_memberreport
                        WHERE id = outreach_events.report_id
                          AND access_code = outreach_events.access_code
                          AND is_active = true
                    ));

                CREATE POLICY "outreach_events_select" ON outreach_events FOR SELECT
                    USING (EXISTS (
                        SELECT 1 FROM matching_memberreport
                        WHERE id = outreach_events.report_id
                          AND access_code = outreach_events.access_code
                          AND is_active = true
                    ));
            """,
            reverse_sql="""
                DROP POLICY IF EXISTS "outreach_events_select" ON outreach_events;
                DROP POLICY IF EXISTS "outreach_events_insert" ON outreach_events;
                DROP INDEX IF EXISTS idx_outreach_events_partner;
                DROP INDEX IF EXISTS idx_outreach_events_session;
                DROP INDEX IF EXISTS idx_outreach_events_type;
                DROP INDEX IF EXISTS idx_outreach_events_report;
                DROP TABLE IF EXISTS outreach_events;
            """,
        ),
    ]
