create extension if not exists pgcrypto;

create table if not exists public.users (
  id text primary key,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

create table if not exists public.bullex_connections (
  id bigint generated always as identity primary key,
  user_id text not null references public.users(id) on delete cascade,
  bullex_email text,
  connected boolean not null default false,
  requires_2fa boolean not null default false,
  account_mode text,
  currency text,
  last_balance numeric,
  last_connected_at timestamptz,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

alter table public.bullex_connections
  add column if not exists last_connected_at timestamptz;

create unique index if not exists bullex_connections_user_id_key
  on public.bullex_connections (user_id);

create table if not exists public.market_assets (
  id bigint generated always as identity primary key,
  user_id text not null references public.users(id) on delete cascade,
  active_id integer,
  symbol text not null,
  name text,
  enabled boolean not null default true,
  payout numeric,
  last_seen_at timestamptz,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

create unique index if not exists market_assets_user_symbol_key
  on public.market_assets (user_id, symbol);

create table if not exists public.robot_states (
  id uuid primary key default gen_random_uuid(),
  user_id text not null references public.users(id) on delete cascade,
  state jsonb not null default '{}'::jsonb,
  enabled boolean not null default false,
  account_mode text not null default 'DEMO',
  entry_value numeric not null default 2,
  cycle_minutes integer not null default 10,
  min_confidence integer not null default 85,
  min_payout numeric not null default 80,
  stop_win numeric not null default 50,
  stop_loss numeric not null default 30,
  wins integer not null default 0,
  losses integer not null default 0,
  profit numeric not null default 0,
  accuracy numeric not null default 0,
  state_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

alter table public.robot_states
  add column if not exists id uuid default gen_random_uuid(),
  add column if not exists state jsonb not null default '{}'::jsonb,
  add column if not exists enabled boolean not null default false,
  add column if not exists account_mode text not null default 'DEMO',
  add column if not exists entry_value numeric not null default 2,
  add column if not exists cycle_minutes integer not null default 10,
  add column if not exists min_confidence integer not null default 85,
  add column if not exists min_payout numeric not null default 80,
  add column if not exists stop_win numeric not null default 50,
  add column if not exists stop_loss numeric not null default 30,
  add column if not exists wins integer not null default 0,
  add column if not exists losses integer not null default 0,
  add column if not exists profit numeric not null default 0,
  add column if not exists accuracy numeric not null default 0,
  add column if not exists state_json jsonb not null default '{}'::jsonb,
  add column if not exists created_at timestamptz not null default timezone('utc'::text, now()),
  add column if not exists updated_at timestamptz not null default timezone('utc'::text, now());

alter table public.robot_states
  alter column id set default gen_random_uuid();

update public.robot_states
set id = gen_random_uuid()
where id is null;

alter table public.robot_states
  alter column id set not null;

update public.robot_states
set state = state_json
where state = '{}'::jsonb
  and state_json <> '{}'::jsonb;

update public.robot_states
set state_json = state
where state_json = '{}'::jsonb
  and state <> '{}'::jsonb;

create unique index if not exists robot_states_id_key
  on public.robot_states (id);

create unique index if not exists robot_states_user_id_key
  on public.robot_states (user_id);

create table if not exists public.robot_trades (
  id bigint generated always as identity primary key,
  user_id text not null references public.users(id) on delete cascade,
  order_id text not null,
  active text,
  direction text,
  entry_value numeric,
  result text,
  payout numeric,
  profit numeric,
  executed_at timestamptz not null default timezone('utc'::text, now()),
  trade_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

alter table public.robot_trades
  add column if not exists active text,
  add column if not exists direction text,
  add column if not exists entry_value numeric,
  add column if not exists result text,
  add column if not exists payout numeric,
  add column if not exists profit numeric,
  add column if not exists executed_at timestamptz not null default timezone('utc'::text, now()),
  add column if not exists trade_json jsonb not null default '{}'::jsonb,
  add column if not exists created_at timestamptz not null default timezone('utc'::text, now()),
  add column if not exists updated_at timestamptz not null default timezone('utc'::text, now());

create unique index if not exists robot_trades_user_order_key
  on public.robot_trades (user_id, order_id);

create table if not exists public.robot_trade_history (
  id bigint generated always as identity primary key,
  user_id text not null references public.users(id) on delete cascade,
  created_at timestamptz not null default timezone('utc'::text, now()),
  account_mode text not null check (account_mode in ('DEMO', 'REAL')),
  active text not null,
  direction text not null check (direction in ('CALL', 'PUT')),
  amount numeric not null,
  confidence numeric not null,
  payout numeric not null,
  order_id text not null,
  result text not null check (result in ('WIN', 'LOSS')),
  profit numeric not null,
  opened_at timestamptz not null,
  finished_at timestamptz not null,
  timeframe text not null
);

create unique index if not exists robot_trade_history_user_order_key
  on public.robot_trade_history (user_id, order_id);

create index if not exists robot_trade_history_user_finished_idx
  on public.robot_trade_history (user_id, finished_at desc);

create table if not exists public.robot_history (
  id uuid primary key default gen_random_uuid(),
  user_id text not null references public.users(id) on delete cascade,
  event_type text,
  history_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc'::text, now()),
  updated_at timestamptz not null default timezone('utc'::text, now())
);

alter table public.robot_history
  add column if not exists event_type text,
  add column if not exists history_json jsonb not null default '{}'::jsonb,
  add column if not exists created_at timestamptz not null default timezone('utc'::text, now()),
  add column if not exists updated_at timestamptz not null default timezone('utc'::text, now());

create index if not exists robot_history_user_created_at_idx
  on public.robot_history (user_id, created_at desc);

create table if not exists public.robot_restore_status (
  user_id text primary key references public.users(id) on delete cascade,
  session_restored boolean not null default false,
  robot_restored boolean not null default false,
  last_restore_at timestamptz,
  updated_at timestamptz not null default timezone('utc'::text, now())
);

alter table public.robot_restore_status
  add column if not exists session_restored boolean not null default false,
  add column if not exists robot_restored boolean not null default false,
  add column if not exists last_restore_at timestamptz,
  add column if not exists updated_at timestamptz not null default timezone('utc'::text, now());

create unique index if not exists robot_restore_status_user_id_key
  on public.robot_restore_status (user_id);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = timezone('utc'::text, now());
  return new;
end;
$$;

create or replace function public.sync_robot_state_json_alias()
returns trigger
language plpgsql
as $$
begin
  if coalesce(new.state, '{}'::jsonb) = '{}'::jsonb
     and coalesce(new.state_json, '{}'::jsonb) <> '{}'::jsonb then
    new.state = new.state_json;
  elsif coalesce(new.state_json, '{}'::jsonb) = '{}'::jsonb
     and coalesce(new.state, '{}'::jsonb) <> '{}'::jsonb then
    new.state_json = new.state;
  end if;

  return new;
end;
$$;

drop trigger if exists set_users_updated_at on public.users;
create trigger set_users_updated_at
before update on public.users
for each row
execute function public.set_updated_at();

drop trigger if exists set_bullex_connections_updated_at on public.bullex_connections;
create trigger set_bullex_connections_updated_at
before update on public.bullex_connections
for each row
execute function public.set_updated_at();

drop trigger if exists set_market_assets_updated_at on public.market_assets;
create trigger set_market_assets_updated_at
before update on public.market_assets
for each row
execute function public.set_updated_at();

drop trigger if exists sync_robot_states_state_alias on public.robot_states;
create trigger sync_robot_states_state_alias
before insert or update on public.robot_states
for each row
execute function public.sync_robot_state_json_alias();

drop trigger if exists set_robot_states_updated_at on public.robot_states;
create trigger set_robot_states_updated_at
before update on public.robot_states
for each row
execute function public.set_updated_at();

drop trigger if exists set_robot_trades_updated_at on public.robot_trades;
create trigger set_robot_trades_updated_at
before update on public.robot_trades
for each row
execute function public.set_updated_at();

drop trigger if exists set_robot_history_updated_at on public.robot_history;
create trigger set_robot_history_updated_at
before update on public.robot_history
for each row
execute function public.set_updated_at();

drop trigger if exists set_robot_restore_status_updated_at on public.robot_restore_status;
create trigger set_robot_restore_status_updated_at
before update on public.robot_restore_status
for each row
execute function public.set_updated_at();
