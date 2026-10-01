CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_servers_client_id ON public.servers USING btree (client_id);
