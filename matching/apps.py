from django.apps import AppConfig


class MatchingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "matching"

    def ready(self):
        import matching.signals  # noqa: F401

        # Disable Supabase's 2-minute statement_timeout on every new connection
        from django.db.backends.signals import connection_created

        def _disable_statement_timeout(sender, connection, **kwargs):
            if connection.vendor == "postgresql":
                connection.cursor().execute("SET statement_timeout = 0")

        connection_created.connect(_disable_statement_timeout)
