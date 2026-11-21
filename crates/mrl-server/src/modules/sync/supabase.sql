-- Apply after creating public.setup_profiles and enabling its per-user RLS.
-- Receipts keep operation IDs after their corresponding backups leave the five-copy history.
create table if not exists public.setup_push_receipts (
  user_id uuid not null references public.setup_profiles (user_id) on delete cascade,
  op_id uuid not null,
  version bigint not null,
  saved_at text not null,
  primary key (user_id, op_id)
);
alter table public.setup_push_receipts enable row level security;
drop policy if exists "own receipts: read" on public.setup_push_receipts;
create policy "own receipts: read" on public.setup_push_receipts for select to authenticated
  using (auth.uid() = user_id);
drop policy if exists "own receipts: insert" on public.setup_push_receipts;
create policy "own receipts: insert" on public.setup_push_receipts for insert to authenticated
  with check (auth.uid() = user_id);
revoke all on public.setup_push_receipts from public, anon, authenticated;
grant select, insert on public.setup_push_receipts to authenticated;

drop function if exists public.push_setup_copy(jsonb);
create or replace function public.push_setup_copy(p_blob jsonb, p_op uuid) returns jsonb
language plpgsql security invoker set search_path = '' as $$
declare
  uid uuid := auth.uid();
  row_now public.setup_profiles;
  v bigint;
  stamp text;
begin
  if uid is null then
    raise exception 'sign in first' using errcode = '28000';
  end if;
  if p_op is null then
    raise exception 'a push needs an operation id' using errcode = '22023';
  end if;

  insert into public.setup_profiles (user_id) values (uid) on conflict (user_id) do nothing;
  select * into row_now from public.setup_profiles where user_id = uid for update;
  -- Preserve retained pre-upgrade operation IDs before any new push can evict them.
  insert into public.setup_push_receipts (user_id, op_id, version, saved_at)
    select uid, (c->>'op')::uuid, (c->>'version')::bigint, c->>'at'
    from jsonb_array_elements(row_now.copies) as c
    where c->>'op' ~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      and jsonb_typeof(c->'version') = 'number'
      and jsonb_typeof(c->'at') = 'string'
    on conflict do nothing;
  select version, saved_at into v, stamp from public.setup_push_receipts
    where user_id = uid and op_id = p_op;
  if v is not null then
    return jsonb_build_object('version', v, 'at', stamp, 'copies', jsonb_array_length(row_now.copies));
  end if;

  v := row_now.last_version + 1;
  stamp := to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
  insert into public.setup_push_receipts values (uid, p_op, v, stamp);
  update public.setup_profiles set last_version = v, copies = (
    select jsonb_agg(e.c order by e.n)
    from jsonb_array_elements(
      jsonb_build_array(jsonb_build_object('version', v, 'at', stamp, 'blob', p_blob)) || row_now.copies
    ) with ordinality as e(c, n)
    where e.n <= 5
  ), updated_at = now()
  where user_id = uid;
  return jsonb_build_object('version', v, 'at', stamp, 'copies', least(jsonb_array_length(row_now.copies) + 1, 5));
end $$;
revoke all on function public.push_setup_copy(jsonb, uuid) from public, anon;
grant execute on function public.push_setup_copy(jsonb, uuid) to authenticated;
